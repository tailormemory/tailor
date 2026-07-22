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
MAX_COMPARISONS = 15000  # No daily cap on this side; backend daily limits enforced by BackendManager
MAX_PAIRS_PER_CLUSTER = 500  # cap per single (cat, entity) cluster to ensure coverage diversity
                              # (added 2026-05-24 to prevent user_general mega-cluster monopolizing the budget)

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
GEMINI_URL_TMPL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
OPENAI_URL = "https://api.openai.com/v1/chat/completions"

# Explicit HTTP timeout, uniform across providers (same 60s budget as
# extract_facts_nightly). Without it aiohttp falls back to its 300s
# default, and the anthropic retry loop turned a stalled TCP connection
# into a ~15-minute hang with zero CPU (osservato 2026-07-22).
HTTP_TIMEOUT_SECONDS = 60
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)

COMPARISON_PROMPT = """Do these two facts describe the same thing? If yes, does the NEW fact make the OLD fact outdated or wrong?

NEW fact (date: {new_date}): {new_fact}
OLD fact (date: {old_date}): {old_fact}

Respond with ONLY one JSON object:
{{"result": "SUPERSEDES" or "INDEPENDENT", "reason": "one short sentence"}}

SUPERSEDES: The NEW fact updates, replaces, or contradicts the OLD fact about the SAME subject.
INDEPENDENT: The facts are about different things, or both remain true.
When in doubt: INDEPENDENT."""


# ============================================================
# MEMOIZATION — verdetti già giudicati (fix #2)
# ============================================================

def ensure_judged_pairs_table(conn):
    """Crea la tabella judged_pairs se non esiste (self-bootstrap).

    Schema canonico in db/migrations/005_judged_pairs.sql.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS judged_pairs (
            fact_id_a INTEGER NOT NULL,
            fact_id_b INTEGER NOT NULL,
            verdict   TEXT NOT NULL,
            judged_at TEXT NOT NULL,
            PRIMARY KEY (fact_id_a, fact_id_b)
        )
    """)
    conn.commit()


def load_judged_memo(conn):
    """Carica i verdetti memoizzati in un dict {(id_a, id_b): judged_at}.

    Chiave normalizzata (id_a < id_b). judged_at è full ISO timestamp,
    confrontabile lessicograficamente con facts.created_at.
    """
    memo = {}
    for a, b, judged_at in conn.execute(
        "SELECT fact_id_a, fact_id_b, judged_at FROM judged_pairs"
    ):
        memo[(a, b)] = judged_at
    return memo


def memo_is_valid(memo, old_f, new_f):
    """True se la coppia ha un verdetto cached ancora valido.

    Valido iff entrambi i fatti sono immutati dal giudizio:
    created_at di entrambi < judged_at. old_f ha l'id più basso.
    """
    key = (old_f["id"], new_f["id"])
    judged_at = memo.get(key)
    if judged_at is None:
        return False
    return (old_f["created_at"] < judged_at) and (new_f["created_at"] < judged_at)


# ============================================================
# FIND CANDIDATE PAIRS — scalabile
# ============================================================

def find_candidate_pairs(max_pairs=MAX_COMPARISONS, memo=None):
    """Trova coppie di fatti candidati per supersession.

    Strategia:
    1. Raggruppa fatti per categoria
    2. For each category, group by primary entity (first entity_tag)
    3. Within each group, compare textual similarity with SequenceMatcher
    4. Returns only pairs with similarity > threshold

    Memoization (fix #2): se `memo` è fornito, le coppie con un verdetto cached
    ancora valido vengono skippate — non consumano né `max_pairs` né
    MAX_PAIRS_PER_CLUSTER, così il budget LLM resta riempito di sole coppie nuove.
    """
    memo = memo or {}
    skipped_memo = 0
    conn = sqlite3.connect(FACTS_DB_PATH, timeout=30)
    conn.execute('PRAGMA journal_mode=WAL')
    cur = conn.cursor()

    # Load facts not yet superseded, with category and entity_tags
    cur.execute("""
        SELECT id, fact, category, entity_tags, event_date, chunk_id, created_at
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
            "created_at": row[6] or "",
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

    # Sort clusters by recency (max fact id within cluster, DESC) so that
    # clusters touched by recently-ingested facts get explored first.
    # Combined with MAX_PAIRS_PER_CLUSTER below, this guarantees coverage
    # diversity across the cluster space instead of exhausting the budget
    # on a single mega-cluster (the user_general failure mode pre-2026-05-24).
    sorted_items = sorted(
        groups.items(),
        key=lambda kv: max(f["id"] for f in kv[1]),
        reverse=True,
    )

    # For each group, find pairs with similarity > threshold
    pairs = []
    groups_checked = 0
    for key, facts in sorted_items:
        if len(pairs) >= max_pairs:
            break

        # Ordina per id (cronologico)
        facts.sort(key=lambda f: f["id"])

        # Compare each fact with subsequent ones in the group
        cluster_pairs_count = 0  # per-cluster cap (MAX_PAIRS_PER_CLUSTER)
        for i in range(len(facts)):
            if len(pairs) >= max_pairs or cluster_pairs_count >= MAX_PAIRS_PER_CLUSTER:
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
                    # Skip se già giudicata e entrambi i fatti immutati (memo).
                    # Non consuma budget né cluster cap: solo coppie nuove pesano.
                    if memo_is_valid(memo, old_f, new_f):
                        skipped_memo += 1
                        continue
                    pairs.append((new_f, old_f, sim))
                    cluster_pairs_count += 1
                    if cluster_pairs_count >= MAX_PAIRS_PER_CLUSTER:
                        break

        groups_checked += 1
        if groups_checked % 1000 == 0:
            print(f"    ...{groups_checked:,} gruppi, {len(pairs):,} coppie")

    print(f"  Coppie candidate: {len(pairs):,} ({groups_checked:,} cluster esplorati, "
          f"{skipped_memo:,} skippate da memo)")
    return pairs


# ============================================================
# PROVIDER CALLS — async
# Each returns one of: parsed JSON dict, "RATE_LIMITED", "TIMEOUT", or
# None on error. "TIMEOUT" is never retried on the same backend: the
# caller rotates and re-queues the item on the next provider.
# ============================================================

_TIMEOUT_EXC = (asyncio.TimeoutError, aiohttp.ServerTimeoutError)

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
            async with session.post(OPENAI_URL, json=payload, headers=headers,
                                    timeout=HTTP_TIMEOUT) as resp:
                if resp.status == 429:
                    return "RATE_LIMITED"
                if resp.status != 200:
                    return None
                data = await resp.json()
                content = data["choices"][0]["message"]["content"].strip()
                return json.loads(content)
        except _TIMEOUT_EXC:
            return "TIMEOUT"
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
            async with session.post(url, json=payload, headers={"Content-Type": "application/json"},
                                    timeout=HTTP_TIMEOUT) as resp:
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
        except _TIMEOUT_EXC:
            return "TIMEOUT"
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
                async with session.post(ANTHROPIC_API_URL, json=payload, headers=headers,
                                        timeout=HTTP_TIMEOUT) as resp:
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
            except _TIMEOUT_EXC:
                # Do NOT retry on the same backend: 3 attempts × 60s would
                # re-create the multi-minute stall this timeout exists to
                # prevent. Rotate instead — the caller re-queues the pair.
                return "TIMEOUT"
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
    is a parsed dict, "RATE_LIMITED", "TIMEOUT", or None).

    Backend rotation rules:
    - On any pair returning RATE_LIMITED: manager.mark_rate_limited()
      (exhaust + advance current_idx). Subsequent attempt uses next backend.
    - On any pair returning TIMEOUT (and no rate-limit): manager.mark_timeout()
      — WARN + advance current_idx, exhausting only after N consecutive
      timeouts. Same as a 429 from the caller's side: the pair stays pending
      and is retried on the next backend, the worker never blocks.
    - On any pair returning None / Exception (and no rate-limit / timeout):
      manager.mark_error() (advance current_idx without exhausting).
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
        n_timeout = 0
        n_err = 0
        for idx, r in zip(pending, attempt_results):
            if r == "RATE_LIMITED":
                results[idx] = "RATE_LIMITED"
                new_pending.append(idx)
                n_rate += 1
            elif r == "TIMEOUT":
                results[idx] = "TIMEOUT"
                new_pending.append(idx)
                n_timeout += 1
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
            elif n_timeout > 0:
                manager.mark_timeout()
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

    # Memoization stats (fix #2) — graceful se la tabella non esiste ancora
    memo_total = memo_sup = memo_ind = None
    try:
        cur.execute("SELECT verdict, COUNT(*) FROM judged_pairs GROUP BY verdict")
        by_verdict = dict(cur.fetchall())
        memo_sup = by_verdict.get("SUPERSEDES", 0)
        memo_ind = by_verdict.get("INDEPENDENT", 0)
        memo_total = memo_sup + memo_ind
    except sqlite3.OperationalError:
        pass

    conn.close()
    print(f"\n📊 Fact Supersession Stats")
    print(f"{'─' * 50}")
    print(f"  Fatti totali:              {total:>8,}")
    print(f"  Fatti superati:            {superseded:>8,}")
    print(f"  Chunk con fatti superati:  {chunks_affected:>5,}")
    if memo_total is not None:
        # Hit-rate atteso a regime: ogni coppia memoizzata è una chiamata LLM
        # risparmiata finché i due fatti restano immutati. Gli INDEPENDENT sono
        # il risparmio ricorrente (i SUPERSEDES non riappaiono come candidati).
        hit_rate = (memo_ind / memo_total * 100) if memo_total else 0.0
        print(f"  Verdetti memoizzati:       {memo_total:>8,}  "
              f"(SUP={memo_sup:,} IND={memo_ind:,}, {hit_rate:.1f}% riusabili)")
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

    # Carica i verdetti memoizzati (fix #2): skip coppie già giudicate e immutate
    memo_conn = sqlite3.connect(FACTS_DB_PATH, timeout=30)
    memo_conn.execute('PRAGMA journal_mode=WAL')
    ensure_judged_pairs_table(memo_conn)
    memo = load_judged_memo(memo_conn)
    memo_conn.close()
    print(f"🧠 Verdetti memoizzati: {len(memo):,}")

    # Trova coppie candidate
    print("🔍 Ricerca coppie candidate...")
    pairs = find_candidate_pairs(args.max_pairs, memo=memo)

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
    # judged_at unico per l'intera run; full ISO timestamp così da essere
    # confrontabile lessicograficamente con facts.created_at nella memo.
    run_judged_at = datetime.now().isoformat()

    # Session-level timeout as defence in depth: every call_* passes its
    # own HTTP_TIMEOUT, this covers any future call site that forgets to.
    async with aiohttp.ClientSession(timeout=HTTP_TIMEOUT) as session:
        BATCH_SIZE = 20
        for batch_start in range(0, total, BATCH_SIZE):
            if manager.all_exhausted():
                print(f"\n⚠️  All backends exhausted. Stopped at {processed:,} coppie.")
                break

            batch = pairs[batch_start:batch_start + BATCH_SIZE]
            results = await run_batch(session, semaphore, manager, batch)

            for (new_f, old_f, sim), result in zip(batch, results):
                processed += 1

                if (isinstance(result, Exception) or result is None
                        or result in ("RATE_LIMITED", "TIMEOUT")):
                    errors += 1
                    continue

                verdict = result.get("result", "INDEPENDENT")
                reason = result.get("reason", "")

                # Memoizza il verdetto (entrambi: audit log + skip futuri).
                # Chiave normalizzata: old_f ha sempre l'id più basso.
                if facts_conn:
                    facts_conn.execute(
                        "INSERT OR REPLACE INTO judged_pairs "
                        "(fact_id_a, fact_id_b, verdict, judged_at) VALUES (?, ?, ?, ?)",
                        (old_f["id"], new_f["id"], verdict, run_judged_at))

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
