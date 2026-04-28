"""
TAILOR — 18: Fact-Level Supersession (v2 — scalabile)
Compares atomic facts to find supersessions using textual similarity.

v2: invece di brute-force su entity_tags (esplode con 230k fatti),
usa un approccio a cluster:
  1. Raggruppa fatti per categoria + entity overlap
  2. Compares only facts in the same cluster with similar text
  3. LLM judges only high-similarity pairs

Per il confronto testuale usa fuzzy matching locale (no LLM, no API).
The LLM is only used for the SUPERSEDES/INDEPENDENT decision on candidate pairs.

Backend rotation: backends list comes from
config/tailor.yaml `enrichment.fact_supersession.backends`. Rotation on
rate-limit / transient error is handled by the shared BackendManager
in scripts/lib/backend_manager.py.

Uso:
  python scripts/enrichment/fact_supersession.py                     # Incrementale, config-driven rotation
  python scripts/enrichment/fact_supersession.py --test 100           # Test (no write)
  python scripts/enrichment/fact_supersession.py --stats              # Statistiche
  python scripts/enrichment/fact_supersession.py --backend anthropic  # Override: force one provider
  python scripts/enrichment/fact_supersession.py --backend google     # Override: force one provider
  python scripts/enrichment/fact_supersession.py --backend openai     # Override: force one provider
"""

import json
import os
import sys
import time
import asyncio
import sqlite3
import argparse
import aiohttp
from datetime import datetime
from difflib import SequenceMatcher

# Load user name variants from config
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "lib"))
from config import get as cfg
_USER_VARIANTS = set(cfg("user", "name_variants") or [])

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_DIR = os.path.join(BASE_DIR, "db")
FACTS_DB_PATH = os.path.join(DB_DIR, "facts.sqlite3")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.lib.backend_manager import BackendManager

DEFAULT_WORKERS = 5
SIMILARITY_THRESHOLD = 0.45  # soglia fuzzy match per candidare una coppia
MAX_COMPARISONS = 50000  # No daily cap on this side; backend daily limits enforced by BackendManager

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
GEMINI_URL_TMPL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
OPENAI_URL = "https://api.openai.com/v1/chat/completions"

COMPARISON_PROMPT = """Do these two facts describe the same thing? If yes, does the NEW fact make the OLD fact outdated or wrong?

NEW fact (date: {new_date}): {new_fact}
OLD fact (date: {old_date}): {old_fact}

Respond with ONLY one JSON object:
{{"result": "SUPERSEDES" or "INDEPENDENT", "reason": "one short sentence"}}

SUPERSEDES: The NEW fact updates, replaces, or contradicts the OLD fact about the SAME subject.
INDEPENDENT: The facts are about different things, or both remain true.
When in doubt: INDEPENDENT."""


# ============================================================
# FIND CANDIDATE PAIRS — scalabile
# ============================================================

def find_candidate_pairs(max_pairs=MAX_COMPARISONS):
    """Trova coppie di fatti candidati per supersession.

    Strategia:
    1. Raggruppa fatti per categoria
    2. For each category, group by primary entity (first entity_tag)
    3. Within each group, compare textual similarity with SequenceMatcher
    4. Returns only pairs with similarity > threshold
    """
    conn = sqlite3.connect(FACTS_DB_PATH, timeout=30)
    conn.execute('PRAGMA journal_mode=WAL')
    cur = conn.cursor()

    # Load facts not yet superseded, with category and entity_tags
    cur.execute("""
        SELECT id, fact, category, entity_tags, event_date, chunk_id
        FROM facts
        WHERE superseded_by IS NULL
        ORDER BY id
    """)
    all_facts = []
    for row in cur.fetchall():
        tags = []
        try:
            tags = json.loads(row[3]) if row[3] else []
        except Exception:
            pass
        all_facts.append({
            "id": row[0], "fact": row[1], "category": row[2] or "",
            "tags": [t.lower().strip() for t in tags if t],
            "event_date": row[4] or "", "chunk_id": row[5],
            "primary_entity": tags[0].lower().strip() if tags else ""
        })
    conn.close()

    print(f"  Fatti attivi: {len(all_facts):,}")

    # Raggruppa per (category, primary_entity)
    groups = {}
    for f in all_facts:
        if not f["primary_entity"] or not f["category"]:
            continue
        # Skip overly generic entities
        if f["primary_entity"] in _USER_VARIANTS:
            # For user entity, use category + secondary entity as key
            secondary = f["tags"][1].lower().strip() if len(f["tags"]) > 1 else "general"
            key = (f["category"], f"user_{secondary}")
        else:
            key = (f["category"], f["primary_entity"])

        if key not in groups:
            groups[key] = []
        groups[key].append(f)

    print(f"  Gruppi (category+entity): {len(groups):,}")

    # Filter groups with at least 2 facts
    groups = {k: v for k, v in groups.items() if len(v) >= 2}
    print(f"  Gruppi con 2+ fatti: {len(groups):,}")

    # For each group, find pairs with similarity > threshold
    pairs = []
    groups_checked = 0
    for key, facts in groups.items():
        if len(pairs) >= max_pairs:
            break

        # Ordina per id (cronologico)
        facts.sort(key=lambda f: f["id"])

        # Compare each fact with subsequent ones in the group
        for i in range(len(facts)):
            if len(pairs) >= max_pairs:
                break
            for j in range(i + 1, min(i + 20, len(facts))):  # max 20 confronti per fatto
                # Fuzzy match sulla stringa del fatto
                sim = SequenceMatcher(None,
                    facts[i]["fact"][:100].lower(),
                    facts[j]["fact"][:100].lower()
                ).ratio()

                if sim >= SIMILARITY_THRESHOLD:
                    # The fact with higher id is the "new" (more recent)
                    new_f = facts[j]
                    old_f = facts[i]
                    pairs.append((new_f, old_f, sim))

        groups_checked += 1
        if groups_checked % 1000 == 0:
            print(f"    ...{groups_checked:,} gruppi, {len(pairs):,} coppie")

    print(f"  Coppie candidate: {len(pairs):,}")
    return pairs


# ============================================================
# PROVIDER CALLS — async
# Each returns one of: parsed JSON dict, "RATE_LIMITED", or None on error.
# ============================================================

async def call_openai(session, semaphore, new_fact, old_fact, new_date, old_date,
                      *, model: str, api_key: str):
    prompt = COMPARISON_PROMPT.format(
        new_fact=new_fact[:300], old_fact=old_fact[:300],
        new_date=new_date or "unknown", old_date=old_date or "unknown"
    )
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model, "max_tokens": 80, "temperature": 0.0,
        "messages": [{"role": "user", "content": prompt}],
        "response_format": {"type": "json_object"}
    }
    async with semaphore:
        try:
            async with session.post(OPENAI_URL, json=payload, headers=headers) as resp:
                if resp.status == 429:
                    return "RATE_LIMITED"
                if resp.status != 200:
                    return None
                data = await resp.json()
                content = data["choices"][0]["message"]["content"].strip()
                return json.loads(content)
        except Exception:
            return None


async def call_gemini(session, semaphore, new_fact, old_fact, new_date, old_date,
                      *, model: str, api_key: str):
    prompt = COMPARISON_PROMPT.format(
        new_fact=new_fact[:300], old_fact=old_fact[:300],
        new_date=new_date or "unknown", old_date=old_date or "unknown"
    )
    url = GEMINI_URL_TMPL.format(model=model, api_key=api_key)
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.0, "maxOutputTokens": 100,
                             "thinkingConfig": {"thinkingBudget": 0}}
    }
    async with semaphore:
        try:
            async with session.post(url, json=payload, headers={"Content-Type": "application/json"}) as resp:
                if resp.status == 429:
                    return "RATE_LIMITED"
                if resp.status != 200:
                    return None
                data = await resp.json()
                content = data["candidates"][0]["content"]["parts"][0]["text"].strip()
                content = content.replace("```json", "").replace("```", "").strip()
                start = content.find("{")
                end = content.rfind("}") + 1
                if start >= 0 and end > start:
                    return json.loads(content[start:end])
                return None
        except Exception:
            return None


async def call_anthropic(session, semaphore, new_fact, old_fact, new_date, old_date,
                         *, model: str, api_key: str):
    prompt = COMPARISON_PROMPT.format(
        new_fact=new_fact[:300], old_fact=old_fact[:300],
        new_date=new_date or "unknown", old_date=old_date or "unknown"
    )
    headers = {"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"}
    payload = {"model": model, "max_tokens": 80, "temperature": 0.0,
               "messages": [{"role": "user", "content": prompt}]}
    async with semaphore:
        for attempt in range(3):
            try:
                async with session.post(ANTHROPIC_API_URL, json=payload, headers=headers) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("retry-after", 3))
                        await asyncio.sleep(retry_after)
                        continue
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    content = data["content"][0]["text"].strip()
                    # Strip markdown backticks
                    for marker in ['json', '']:
                        content = content.replace('```' + marker, '')
                    content = content.strip()
                    start = content.find('{')
                    end = content.rfind('}') + 1
                    if start >= 0 and end > start:
                        return json.loads(content[start:end])
                    return None
            except (json.JSONDecodeError, KeyError, IndexError):
                return None
            except Exception:
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return None
    return None


async def call_backend(session, semaphore, backend, new_fact, old_fact, new_date, old_date):
    """Dispatch to the provider-specific call function for `backend`."""
    name = backend["name"]
    model = backend["model"]
    api_key = backend["api_key"]
    if name == "anthropic":
        return await call_anthropic(session, semaphore, new_fact, old_fact, new_date, old_date,
                                    model=model, api_key=api_key)
    if name == "google":
        return await call_gemini(session, semaphore, new_fact, old_fact, new_date, old_date,
                                 model=model, api_key=api_key)
    if name == "openai":
        return await call_openai(session, semaphore, new_fact, old_fact, new_date, old_date,
                                 model=model, api_key=api_key)
    return None


# ============================================================
# BATCH RUNNER — rotates on rate-limit/error, retries each item
# at most once against the next backend.
# ============================================================

async def run_batch(session, semaphore, manager, batch, max_retry: int = 1):
    """Run a batch of pair-tuples against `manager.current()`, then rotate
    and retry failed pairs up to `max_retry` times against the next
    backend. Returns a list of results aligned with `batch` (each entry
    is a parsed dict, "RATE_LIMITED", or None).

    Backend rotation rules:
    - On any pair returning RATE_LIMITED: manager.mark_rate_limited()
      (exhaust + advance current_idx). Subsequent attempt uses next backend.
    - On any pair returning None / Exception (and no rate-limit): manager.mark_error()
      (advance current_idx without exhausting).
    - Successes recorded once per attempt via mark_success(n).
    """
    results: list = [None] * len(batch)
    pending = list(range(len(batch)))
    attempt = 0

    while pending and attempt <= max_retry:
        backend = manager.current()
        if backend is None:
            # All backends exhausted; remaining pending become None
            break

        backend_at_dispatch = backend
        tasks = [
            call_backend(
                session, semaphore, backend_at_dispatch,
                batch[i][0]["fact"], batch[i][1]["fact"],
                batch[i][0]["event_date"], batch[i][1]["event_date"],
            )
            for i in pending
        ]
        attempt_results = await asyncio.gather(*tasks, return_exceptions=True)

        new_pending: list[int] = []
        n_success = 0
        n_rate = 0
        n_err = 0
        for idx, r in zip(pending, attempt_results):
            if r == "RATE_LIMITED":
                results[idx] = "RATE_LIMITED"
                new_pending.append(idx)
                n_rate += 1
            elif r is None or isinstance(r, Exception):
                results[idx] = None
                new_pending.append(idx)
                n_err += 1
            else:
                results[idx] = r
                n_success += 1

        # Record successes once for this attempt (atomic, avoids index drift)
        manager.mark_success(n_success)

        # Decide rotation only if mark_success didn't already auto-rotate
        # (i.e. backend_at_dispatch is still current).
        backend_after = None
        if manager.backends and 0 <= manager.current_idx < len(manager.backends):
            backend_after = manager.backends[manager.current_idx]
        if backend_after is backend_at_dispatch:
            if n_rate > 0:
                manager.mark_rate_limited()
            elif n_err > 0:
                manager.mark_error()

        pending = new_pending
        attempt += 1

    return results


# ============================================================
# STATS
# ============================================================

def show_stats():
    if not os.path.exists(FACTS_DB_PATH):
        print("❌ facts.sqlite3 non esiste.")
        return
    conn = sqlite3.connect(f"file:{FACTS_DB_PATH}?mode=ro", uri=True)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM facts")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM facts WHERE superseded_by IS NOT NULL")
    superseded = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT chunk_id) FROM facts WHERE superseded_by IS NOT NULL")
    chunks_affected = cur.fetchone()[0]

    # Top superseded facts
    cur.execute("""
        SELECT f1.fact, f2.fact, f1.category
        FROM facts f1
        JOIN facts f2 ON f1.superseded_by = f2.id
        ORDER BY f1.id DESC
        LIMIT 5
    """)
    examples = cur.fetchall()

    conn.close()
    print(f"\n📊 Fact Supersession Stats")
    print(f"{'─' * 50}")
    print(f"  Fatti totali:              {total:>8,}")
    print(f"  Fatti superati:            {superseded:>8,}")
    print(f"  Chunk con fatti superati:  {chunks_affected:>5,}")
    if examples:
        print(f"\n  Ultimi esempi:")
        for old, new, cat in examples:
            print(f"    [{cat}] \"{old[:50]}\" → \"{new[:50]}\"")
    print()


# ============================================================
# MAIN
# ============================================================

async def main():
    parser = argparse.ArgumentParser(description="TAILOR Fact-Level Supersession v2")
    parser.add_argument("--test", type=int, default=0, help="Test su N comparisons (no write)")
    parser.add_argument("--stats", action="store_true")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help="Parallel call workers (auto-tuned to current backend if <=0).")
    parser.add_argument("--backend", type=str, default=None,
                        choices=["anthropic", "google", "openai"],
                        help="Force a single provider; bypasses config-driven rotation.")
    parser.add_argument("--max-pairs", type=int, default=MAX_COMPARISONS)
    parser.add_argument("--threshold", type=float, default=SIMILARITY_THRESHOLD)
    args = parser.parse_args()

    if args.stats:
        show_stats()
        return

    if not os.path.exists(FACTS_DB_PATH):
        print("❌ facts.sqlite3 non esiste.")
        sys.exit(1)

    manager = BackendManager("fact_supersession", restrict_provider=args.backend)
    if not manager.backends:
        print("❌ No backend available. Check enrichment.fact_supersession.backends in config/tailor.yaml")
        sys.exit(1)

    # Trova coppie candidate
    print("🔍 Ricerca coppie candidate...")
    pairs = find_candidate_pairs(args.max_pairs)

    if args.test:
        pairs = pairs[:args.test]

    if not pairs:
        print("✅ No pairs to compare.")
        show_stats()
        return

    print(f"📋 {len(pairs):,} coppie da confrontare (workers={args.workers}, backends={manager.status()})")

    facts_conn = None
    if not args.test:
        facts_conn = sqlite3.connect(FACTS_DB_PATH, timeout=30)
        facts_conn.execute('PRAGMA journal_mode=WAL')
        facts_conn.execute('PRAGMA busy_timeout=30000')
    current_worker_count = args.workers if args.workers > 0 else manager.current_workers()
    semaphore = asyncio.Semaphore(current_worker_count)
    total = len(pairs)
    processed = 0
    superseded_count = 0
    errors = 0
    start_time = time.time()

    async with aiohttp.ClientSession() as session:
        BATCH_SIZE = 20
        for batch_start in range(0, total, BATCH_SIZE):
            if manager.all_exhausted():
                print(f"\n⚠️  All backends exhausted. Stopped at {processed:,} coppie.")
                break

            batch = pairs[batch_start:batch_start + BATCH_SIZE]
            results = await run_batch(session, semaphore, manager, batch)

            for (new_f, old_f, sim), result in zip(batch, results):
                processed += 1

                if isinstance(result, Exception) or result is None or result == "RATE_LIMITED":
                    errors += 1
                    continue

                verdict = result.get("result", "INDEPENDENT")
                reason = result.get("reason", "")

                if verdict == "SUPERSEDES":
                    superseded_count += 1
                    print(f"  🔄 [{new_f['category']}] \"{new_f['fact'][:45]}\" → \"{old_f['fact'][:45]}\" (sim={sim:.2f}) — {reason}")

                    if facts_conn:
                        cur = facts_conn.cursor()
                        cur.execute("UPDATE facts SET superseded_by = ?, superseded_at = ? WHERE id = ?",
                                    (new_f["id"], datetime.now().strftime("%Y-%m-%d"), old_f["id"]))

            if facts_conn:
                facts_conn.commit()

            # Re-tune semaphore if backend changed and workers were auto
            if args.workers <= 0:
                nw = manager.current_workers()
                if nw != current_worker_count:
                    semaphore = asyncio.Semaphore(nw)
                    current_worker_count = nw

            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 0
            eta = (total - processed) / rate if rate > 0 else 0
            if processed % 200 < BATCH_SIZE:
                print(f"  [{processed:,}/{total:,}] {rate:.1f}/s | SUP={superseded_count} ERR={errors} | "
                      f"{manager.status()} | ETA {eta/60:.0f}m")

    if facts_conn:
        facts_conn.commit()
        facts_conn.close()

    elapsed = time.time() - start_time
    print(f"\n{'═' * 50}")
    print(f"✅ Completato in {elapsed/60:.1f} minuti")
    print(f"  Coppie confrontate:  {processed:,}")
    print(f"  SUPERSEDES:          {superseded_count:,}")
    print(f"  Errori:              {errors:,}")
    print(f"  Rate:                {processed/elapsed:.1f}/s")
    print(f"  Backends:            {manager.status()}")
    if args.test:
        print(f"\n  ⚠️  TEST mode — no changes")
    else:
        show_stats()


if __name__ == "__main__":
    asyncio.run(main())
