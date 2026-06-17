#!/usr/bin/env python3
"""
TAILOR — Fact Derivation (Step 19)

Generates derived facts ("derives" relationship) by analyzing clusters of 
atomic facts for the same entity and inferring new knowledge.

Approach:
1. Group facts by entity (fuzzy merge of name variants)
2. For entities with >= MIN_FACTS non-superseded facts, cluster by category/theme
3. Ask LLM to infer new knowledge from each cluster
4. Save derived facts with relation_type='derived' and derived_from=[source_fact_ids]

Usage:
  python3 19_derive_facts.py                  # Full batch (all entities >= 5 facts)
  python3 19_derive_facts.py --entity "Gianluca Manni"  # Single entity
  python3 19_derive_facts.py --top 100        # Top 100 entities by fact count
  python3 19_derive_facts.py --nightly        # Only entities with new facts since last run
"""

import os, sys, json, time, sqlite3, argparse, re, signal, tempfile, requests
from collections import defaultdict
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_DIR = os.path.join(BASE_DIR, "db")
FACTS_DB = os.path.join(DB_DIR, "facts.sqlite3")
CHECKPOINT_FILE = os.path.join(DB_DIR, "checkpoints", "derives_checkpoint.json")

# ── Backend rotation ──────────────────────────────────────────
# Backends list comes from config/tailor.yaml `enrichment.fact_derivation.backends`.
# Provider rotation on rate-limit / transient error is handled by the
# shared BackendManager in scripts/lib/backend_manager.py.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.lib.backend_manager import BackendManager

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
GEMINI_URL_TMPL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"

MIN_FACTS = 5          # Minimum facts to attempt derivation
MAX_FACTS_PER_CALL = 50  # Cap facts sent to LLM per entity
MAX_DERIVES_PER_ENTITY = 5  # Cap derived facts per entity
RATE_LIMIT_DELAY = 0.3  # Courtesy throttle between API calls (seconds)

# ── Entity Normalization ──────────────────────────────────────

def normalize_entity(name: str) -> str:
    """Normalize entity name for fuzzy grouping.
    Handles: case, company suffixes, name order (2-3 words), middle names."""
    n = name.strip().lower()
    # Remove company suffixes
    for suffix in [" srl", " s.r.l.", " s.r.l", " ltd", " limited", " inc", 
                   " spa", " s.p.a.", " s.p.a", " srls", " gmbh", " ag", " co"]:
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()
    # Remove punctuation except hyphens
    n = re.sub(r"[.,;:!?()\[\]{}'\"]+", "", n)
    parts = n.split()
    if len(parts) <= 3:
        # Sort words alphabetically → "avitabile daniele" matches regardless of order
        # For 3-word names like "daniele avitabile cattani", take first 2 sorted as key
        # This groups "Daniele Avitabile" with "Daniele Avitabile Cattani"
        return " ".join(sorted(parts[:2]))
    return n

# ── Database Operations ───────────────────────────────────────

def get_db():
    db = sqlite3.connect(FACTS_DB)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db

def load_entity_groups(db, min_facts=MIN_FACTS, entity_filter=None):
    """Load ALL facts grouped by normalized entity.

    Returns {normalized: {names: set, facts: list, max_created_at: str}}.

    Always loads the full set (no created_at pre-filter): the new-vs-backlog
    partition and the durable per-entity watermark skip live in main(), so the
    loader must see backlog entities (no recent facts) too. `max_created_at` is
    the newest *source* fact timestamp for the entity (relation_type != 'derived')
    — the value compared against the per-entity watermark to decide whether the
    entity is "due". Derived rows are excluded from the cursor on purpose: they
    are this script's own output, written with created_at=now(), so counting them
    would make every entity that produced a derive look "newer than its
    watermark" and re-open itself every night. The cursor must measure "how far
    I've seen facts TO derive from", not what derivation produced.
    """
    where = "WHERE (superseded_by = '' OR superseded_by IS NULL) AND entity_tags != '[]' AND entity_tags != ''"

    rows = db.execute(f"""
        SELECT id, fact, category, entity_tags, event_date, created_at, relation_type
        FROM facts {where}
    """).fetchall()

    groups = defaultdict(lambda: {"names": set(), "facts": []})
    for r in rows:
        try:
            tags = json.loads(r["entity_tags"])
        except Exception:
            continue
        for tag in tags:
            norm = normalize_entity(tag)
            if entity_filter and normalize_entity(entity_filter) not in norm and norm not in normalize_entity(entity_filter):
                continue
            groups[norm]["names"].add(tag)
            groups[norm]["facts"].append({
                "id": r["id"],
                "fact": r["fact"],
                "category": r["category"],
                "event_date": r["event_date"] or "",
                "created_at": r["created_at"] or "",
                "relation_type": r["relation_type"] or "",
            })

    # Filter by min facts, deduplicate facts within each group, compute watermark
    filtered = {}
    for norm, data in groups.items():
        # Deduplicate by fact text
        seen = set()
        unique_facts = []
        for f in data["facts"]:
            if f["fact"] not in seen:
                seen.add(f["fact"])
                unique_facts.append(f)
        data["facts"] = unique_facts
        if len(unique_facts) >= min_facts:
            # Cursor over SOURCE facts only — never the script's own derived rows.
            data["max_created_at"] = max(
                (f["created_at"] for f in unique_facts if f["relation_type"] != "derived"),
                default="",
            )
            filtered[norm] = data

    return filtered

def get_existing_derives(db, entity_norm: str) -> set:
    """Get already-derived facts for this entity to avoid duplicates."""
    rows = db.execute("""
        SELECT fact FROM facts 
        WHERE relation_type = 'derived' 
        AND entity_tags LIKE ?
    """, (f"%{entity_norm[:30]}%",)).fetchall()
    return {r["fact"].lower().strip() for r in rows}

def save_derived_facts(db, derives: list, source_fact_ids: list, entity_names: set):
    """Save derived facts to the DB."""
    entity_tags = json.dumps(sorted(entity_names)[:5])  # Keep top 5 name variants
    saved = 0
    for d in derives:
        db.execute("""
            INSERT INTO facts (chunk_id, fact, category, entity_tags, event_date, 
                             confidence, created_at, relation_type, derived_from)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'derived', ?)
        """, (
            f"derived_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{saved}",
            d["fact"],
            d.get("category", "derived"),
            entity_tags,
            d.get("event_date", ""),
            d.get("confidence", 0.8),
            datetime.now().isoformat(),
            json.dumps(source_fact_ids[:20]),  # Cap source refs
        ))
        saved += 1
    db.commit()
    return saved

# ── Provider Calls ────────────────────────────────────────────
# Each returns the raw response text, "RATE_LIMITED", or None on error.
# Parsing of the JSON-array body lives in derive_for_entity.

def _call_anthropic(prompt: str, model: str, api_key: str, system: str):
    try:
        resp = requests.post(
            ANTHROPIC_API_URL,
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": model,
                "max_tokens": 800,
                "temperature": 0.3,
                "system": system,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        if resp.status_code == 429:
            return "RATE_LIMITED"
        if resp.status_code != 200:
            return None
        data = resp.json()
        if "content" not in data:
            return None
        return data["content"][0].get("text", "").strip()
    except Exception:
        return None


def _call_google(prompt: str, model: str, api_key: str, system: str):
    try:
        url = GEMINI_URL_TMPL.format(model=model, api_key=api_key)
        resp = requests.post(
            url,
            headers={"content-type": "application/json"},
            json={
                "systemInstruction": {"parts": [{"text": system}]},
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0.3,
                    "maxOutputTokens": 800,
                    "thinkingConfig": {"thinkingBudget": 0},
                },
            },
            timeout=30,
        )
        if resp.status_code == 429:
            return "RATE_LIMITED"
        if resp.status_code != 200:
            return None
        data = resp.json()
        candidates = data.get("candidates", [])
        if not candidates:
            return None
        parts = candidates[0].get("content", {}).get("parts", [])
        if not parts:
            return None
        return parts[0].get("text", "").strip()
    except Exception:
        return None


def call_provider(backend: dict, prompt: str, system: str):
    """Dispatch by backend['name']. Returns raw text, None, or 'RATE_LIMITED'."""
    name = backend["name"]
    model = backend["model"]
    api_key = backend["api_key"]
    if name == "anthropic":
        return _call_anthropic(prompt, model, api_key, system)
    if name == "google":
        return _call_google(prompt, model, api_key, system)
    # OpenAI not currently used for derivation; can be added when present in config.
    return None


# ── LLM Derivation ───────────────────────────────────────────

DERIVE_SYSTEM = """You analyze atomic facts about an entity and infer NEW knowledge that isn't explicitly stated in any single fact.

TYPES OF DERIVATION:
1. ROLE INFERENCE: Multiple interactions reveal a person's role/relationship (e.g., "X sends tax calculations + X manages property leasing → X is the fiscal/property advisor")
2. PATTERN DETECTION: Recurring events reveal patterns (e.g., "IMU calculated in June and December → biannual tax schedule")
3. CROSS-ENTITY CONNECTION: Facts linking two entities reveal their relationship (e.g., "X works for company A + Y is CEO of A → X reports to Y")
4. STATUS SYNTHESIS: Multiple status facts reveal current state (e.g., "contract signed 2020 + renewal 2023 + active discussions 2025 → long-term ongoing relationship")
5. BEHAVIORAL INFERENCE: Actions reveal preferences or patterns (e.g., "always responds in Italian + uses formal tone → Italian-speaking, formal communication style")

RULES:
- Generate ONLY facts that require combining 2+ source facts — never restate a single fact
- Each derived fact must be a single, atomic statement
- Include confidence (0.0-1.0): 0.9+ for near-certain, 0.7-0.8 for likely, 0.5-0.6 for possible
- Skip trivial or obvious derivations
- Max 5 derived facts per entity
- If no meaningful derivations are possible, return empty array

Respond ONLY with JSON array:
[{"fact": "...", "category": "derived", "confidence": 0.85, "source_ids": [1, 5, 12]}]

source_ids = indices (0-based) of the input facts that support this derivation."""

def derive_for_entity(entity_norm: str, data: dict, existing_derives: set,
                      manager: BackendManager) -> list:
    """Call LLM to derive new facts for an entity. Routes through
    `manager` for backend selection; on RATE_LIMITED or transient
    error, rotates and retries the call once against the next backend.
    Returns [] if no backend produces a valid response."""
    facts = data["facts"]
    names = data["names"]

    # Sort by event_date (most recent first), limit to MAX_FACTS_PER_CALL
    facts_sorted = sorted(facts, key=lambda f: f.get("event_date", "") or "", reverse=True)
    facts_limited = facts_sorted[:MAX_FACTS_PER_CALL]

    # Build fact list for prompt
    fact_lines = []
    for i, f in enumerate(facts_limited):
        date_str = f" ({f['event_date']})" if f["event_date"] else ""
        fact_lines.append(f"[{i}] [{f['category']}]{date_str} {f['fact']}")

    user_prompt = (
        f"Entity: {', '.join(sorted(names)[:3])}\n"
        f"Total facts: {len(facts)} (showing {len(facts_limited)} most recent)\n\n"
        f"Facts:\n" + "\n".join(fact_lines)
    )

    # Try current backend, then one rotation attempt on rate-limit/error.
    raw = None
    for attempt in range(2):
        backend = manager.current()
        if backend is None:
            print("  SKIP: all backends exhausted", file=sys.stderr)
            return []
        raw = call_provider(backend, user_prompt, DERIVE_SYSTEM)
        if raw == "RATE_LIMITED":
            manager.mark_rate_limited()
            raw = None
            continue
        if raw is None:
            manager.mark_error()
            continue
        # Success
        manager.mark_success()
        break
    if raw is None:
        return []

    # Parse JSON from response (handle markdown wrapping)
    raw = re.sub(r'^```json\s*', '', raw)
    raw = re.sub(r'\s*```$', '', raw)

    try:
        derives = json.loads(raw)
    except json.JSONDecodeError:
        # Try to find JSON array in response
        match = re.search(r'\[.*\]', raw, re.DOTALL)
        if match:
            try:
                derives = json.loads(match.group())
            except json.JSONDecodeError:
                print(f"  Parse error: {raw[:200]}", file=sys.stderr)
                return []
        else:
            print(f"  Parse error: {raw[:200]}", file=sys.stderr)
            return []

    if not isinstance(derives, list):
        return []

    # Filter out duplicates and low-confidence
    result = []
    for d in derives[:MAX_DERIVES_PER_ENTITY]:
        if not isinstance(d, dict) or "fact" not in d:
            continue
        if d["fact"].lower().strip() in existing_derives:
            continue
        if d.get("confidence", 0) < 0.5:
            continue
        # Map source_ids to actual fact IDs
        source_indices = d.get("source_ids", [])
        source_fact_ids = [facts_limited[i]["id"] for i in source_indices if i < len(facts_limited)]
        d["_source_fact_ids"] = source_fact_ids
        result.append(d)

    return result

# ── Checkpoint ────────────────────────────────────────────────

def load_checkpoint() -> dict:
    """Load the derivation checkpoint.

    Schema (current):
      last_run            boundary "fully derived up to here"; advances ONLY on a
                          full drain of a completed nightly run (never on SIGTERM).
      derived_watermarks  {entity_norm: max(created_at) at time of derivation} —
                          durable per-entity cursor, never wholesale-reset. An
                          entity is skipped iff its current max(created_at) <= its
                          stored watermark (so new facts always re-open it; this is
                          what avoids the 2026-05-25 cumulative silent-skip bug
                          without needing the old 1h reset).
      entities_processed  last run's processed count (for reporting).
      facts_derived       last run's derived count (for reporting).
    """
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE) as f:
            cp = json.load(f)
        # Migrate legacy schema: drop the cumulative processed_entities list (the
        # source of the 1h-reset hack) and seed the watermark dict empty. last_run
        # is preserved as the boundary; entities are re-evaluated by watermark.
        if "derived_watermarks" not in cp:
            cp["derived_watermarks"] = {}
            cp.pop("processed_entities", None)
        return cp
    return {"last_run": "", "derived_watermarks": {}, "entities_processed": 0, "facts_derived": 0}

def save_checkpoint(cp: dict):
    """Atomic + reentrant-safe write. A unique tmp per call (tempfile.mkstemp)
    means a SIGTERM landing mid-save — whose handler also calls save_checkpoint —
    can't clobber the in-flight tmp of the interrupted call; os.replace is atomic,
    so the checkpoint is never truncated regardless of which save wins."""
    d = os.path.dirname(CHECKPOINT_FILE)
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".derives_ckpt.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(cp, f, indent=2)
        os.replace(tmp, CHECKPOINT_FILE)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise

# ── Main ──────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Derive facts from entity clusters")
    parser.add_argument("--entity", type=str, help="Process single entity (fuzzy match)")
    parser.add_argument("--top", type=int, help="Process top N entities by fact count")
    parser.add_argument("--nightly", action="store_true", help="Only entities with new facts since last run")
    parser.add_argument("--min-facts", type=int, default=MIN_FACTS, help=f"Minimum facts per entity (default {MIN_FACTS})")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be derived, don't save")
    args = parser.parse_args()
    
    manager = BackendManager("fact_derivation")
    if not manager.backends:
        print("ERROR: No backend available for fact_derivation. Check enrichment.fact_derivation.backends in config/tailor.yaml", file=sys.stderr)
        sys.exit(1)

    db = get_db()
    cp = load_checkpoint()

    # run_start is captured up-front: last_run advances to THIS value (not now())
    # on a full drain, so entities created during the run stay > last_run and are
    # treated as "new" (high priority) next night rather than silently dropped.
    run_start = datetime.now().isoformat()
    last_run = cp.get("last_run", "")
    watermarks = cp.get("derived_watermarks", {})  # mutated in place; persisted by _flush

    entity_filter = args.entity if args.entity else None
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Loading entity groups...", file=sys.stderr)
    groups = load_entity_groups(db, min_facts=args.min_facts, entity_filter=entity_filter)

    # ── Build the work queue ──────────────────────────────────────
    if args.nightly:
        # Durable per-entity cursor: an entity is "due" only if it has facts newer
        # than the last time we derived it. New facts re-open an entity → no silent
        # skip (the 2026-05-25 cumulative bug), and a killed run resumes here next
        # night because the watermarks it wrote survive (invariant 1).
        due = [(norm, data) for norm, data in groups.items()
               if data["max_created_at"] > watermarks.get(norm, "")]
        # Priority to new (invariant 3): facts created after the last full drain
        # (last_run) go first, newest first; the older arrears follow, ordered by
        # fact count, draining with whatever step time is left.
        new_tier = sorted([d for d in due if d[1]["max_created_at"] > last_run],
                          key=lambda d: d[1]["max_created_at"], reverse=True)
        backlog_tier = sorted([d for d in due if d[1]["max_created_at"] <= last_run],
                              key=lambda d: -len(d[1]["facts"]))
        work = new_tier + backlog_tier
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Queue: {len(new_tier)} new + "
              f"{len(backlog_tier)} backlog ({len(groups) - len(due)} up-to-date, skipped)",
              file=sys.stderr)
    else:
        # Manual runs (full batch / --top / --entity): process everything matching,
        # by fact count. No watermark skip and no last_run advance — these are
        # operator-driven, explicit reprocessing.
        work = sorted(groups.items(), key=lambda d: -len(d[1]["facts"]))
        if args.top:
            work = work[:args.top]

    total_work = len(work)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Entities to process: {total_work}", file=sys.stderr)

    total_derived = 0
    total_processed = 0
    errors = 0

    def _flush(advance_last_run=False):
        """Persist progress. advance_last_run=True ONLY on a full drain of a
        completed nightly run — never from the SIGTERM handler (invariant 2).
        No-op outside nightly mode: manual runs (--top/--entity/full) must stay
        fully isolated from the automatic cycle and never touch the checkpoint
        (neither derived_watermarks nor last_run)."""
        if args.dry_run or not args.nightly:
            return
        cp["derived_watermarks"] = watermarks
        cp["entities_processed"] = total_processed
        cp["facts_derived"] = total_derived
        if advance_last_run:
            cp["last_run"] = run_start
            cp["last_run_completed"] = True
        else:
            cp.setdefault("last_run", last_run)
            cp["last_run_completed"] = False
        save_checkpoint(cp)

    def _on_sigterm(signum, frame):
        # The pipeline kills derivation with SIGTERM on step timeout. Persist the
        # work done so far (watermarks + counts) WITHOUT advancing last_run, then
        # emit the summary so the caller reports the real entity count, not 0.
        _flush(advance_last_run=False)
        print(json.dumps({"entities_processed": total_processed, "facts_derived": total_derived,
                          "errors": errors, "killed": True}))
        sys.stdout.flush()
        sys.exit(0)
    signal.signal(signal.SIGTERM, _on_sigterm)

    for norm, data in work:
        n_facts = len(data["facts"])
        names_str = ", ".join(sorted(data["names"])[:3])
        print(f"[{total_processed + 1}/{total_work}] {names_str} ({n_facts} facts)", file=sys.stderr, end="")

        # Check existing derives
        existing = get_existing_derives(db, norm)

        # Derive (manager passed for backend rotation)
        derives = derive_for_entity(norm, data, existing, manager)

        if derives and not args.dry_run:
            source_ids = []
            for d in derives:
                source_ids.extend(d.get("_source_fact_ids", []))
            saved = save_derived_facts(db, derives, source_ids, data["names"])
            total_derived += saved
            print(f" → {saved} derived", file=sys.stderr)
        elif derives:
            print(f" → {len(derives)} (dry run)", file=sys.stderr)
            for d in derives:
                print(f"    [{d.get('confidence',0):.1f}] {d['fact']}", file=sys.stderr)
        else:
            print(f" → 0", file=sys.stderr)

        # Durable cursor: mark this entity derived up to its newest source fact.
        # Done whether or not anything was derived — an attempted entity is
        # "current" until new source facts arrive (max_created_at moves past this
        # watermark). Nightly only: manual runs stay isolated (no state writes).
        if args.nightly and not args.dry_run:
            watermarks[norm] = data["max_created_at"]
        total_processed += 1

        # Periodic checkpoint floors the progress a hard SIGKILL can lose. 10 (not
        # 50) keeps that floor small; the SIGTERM handler flushes the rest, so only
        # a SIGKILL after a stuck request loses up to 9 completed entities.
        if total_processed % 10 == 0:
            _flush(advance_last_run=False)

        time.sleep(RATE_LIMIT_DELAY)

    # Reached only on normal completion — the SIGTERM handler exits the process,
    # so getting here means the whole queue drained. Advance last_run ONLY in
    # nightly mode (invariant 2 + 4: manual runs never move the boundary).
    _flush(advance_last_run=args.nightly)

    # Summary
    print(f"\n{'='*50}", file=sys.stderr)
    print(f"DERIVATION COMPLETE", file=sys.stderr)
    print(f"  Entities processed: {total_processed}", file=sys.stderr)
    print(f"  Facts derived: {total_derived}", file=sys.stderr)
    print(f"  Errors: {errors}", file=sys.stderr)

    # Return stats for pipeline integration
    print(json.dumps({"entities_processed": total_processed, "facts_derived": total_derived, "errors": errors}))

    db.close()

if __name__ == "__main__":
    main()
