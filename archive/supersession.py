"""
TAILOR — 16: Temporal Supersession & Extends (v1.1)
Compares semantically similar chunks to identify temporal relationships.

Relazioni rilevate:
- SUPERSEDES: new chunk makes old one obsolete (address change, updated decision, etc.)
- EXTENDS: new chunk enriches old without contradicting it (adds details, context)
- INDEPENDENT: chunks on similar topics but without temporal relationship

Metadata aggiunti:
- superseded_by: chunk ID that makes this chunk obsolete (comma-separated if multiple)
- superseded_at: ISO date of when it was superseded
- supersedes: chunk ID that this chunk makes obsolete (backlink)
- extended_by: chunk ID that enriches this chunk
- extends: chunk ID that this chunk enriches (backlink)
- event_date: ISO date of the described event (vs date = document/conversation date)

v1.1: fix filtro data (usa create_time float $lt invece di date string), fix numpy .tolist()

Uso:
  python 16_supersession.py                        # Incremental (resumes from checkpoint)
  python 16_supersession.py --full                 # Re-process all chunks
  python 16_supersession.py --test 20              # Try on 20 chunks, show output without writing
  python 16_supersession.py --stats                # Show existing relationship statistics
  python 16_supersession.py --source document      # Only chunks from a specific source
  python 16_supersession.py --category conv_summary # Only chunks of a specific category
  python 16_supersession.py --workers 5            # Numero worker paralleli (default 5)
  python 16_supersession.py --threshold 0.82       # Soglia cosine similarity (default 0.82)
  python 16_supersession.py --confidence 0.7       # Soglia confidence LLM (default 0.7)
"""

import json
import os
import sys
import time
import asyncio
import sqlite3
import argparse
import subprocess
import aiohttp
import chromadb
import numpy as np
from datetime import datetime

# ============================================================
# CONFIGURAZIONE
# ============================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)
from scripts.lib.config import get as cfg, get_enrichment

DB_DIR = os.path.join(BASE_DIR, "db")
COLLECTION_NAME = cfg("kb", "collection") or "tailor_kb"
CHROMA_DB_PATH = os.path.join(DB_DIR, "chroma.sqlite3")

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"

_USER_NAME = cfg("user", "name") or "the user"
_fd_cfg = get_enrichment("fact_derivation")
HAIKU_MODEL = _fd_cfg["model"]

CHECKPOINT_FILE = os.path.join(DB_DIR, "checkpoints", "supersession_checkpoint.json")
CHECKPOINT_EVERY = 100

DEFAULT_COSINE_THRESHOLD = 0.82
DEFAULT_CONFIDENCE_THRESHOLD = 0.7
DEFAULT_WORKERS = 5
DEFAULT_TOP_K = 5


def get_anthropic_key():
    """Recupera ANTHROPIC_API_KEY da ambiente o dal plist del LaunchDaemon."""
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if key:
        return key
    plist_path = "/Library/LaunchDaemons/com.tailor.mcp.plist"
    try:
        result = subprocess.run(
            ["/usr/bin/plutil", "-extract", "EnvironmentVariables.ANTHROPIC_API_KEY", "raw", plist_path],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    try:
        result = subprocess.run(
            ["grep", "-A1", "ANTHROPIC_API_KEY", plist_path],
            capture_output=True, text=True, timeout=5
        )
        for line in result.stdout.split("\n"):
            line = line.strip()
            if line.startswith("<string>") and line.endswith("</string>"):
                return line[8:-9]
    except Exception:
        pass
    return ""


ANTHROPIC_API_KEY = get_anthropic_key()

# ============================================================
# PROMPT
# ============================================================

SUPERSESSION_PROMPT = """You are a knowledge base consistency analyzer for a personal knowledge base belonging to {user_name}, an Italian entrepreneur.

Given a NEW chunk and an OLD chunk, determine their temporal relationship.

NEW chunk (document date: {new_date}):
{new_text}

OLD chunk (document date: {old_date}):
{old_text}

Respond with EXACTLY one JSON object, no other text:
{{
  "relation": "SUPERSEDES" | "EXTENDS" | "INDEPENDENT",
  "event_date_new": "YYYY-MM-DD or empty string",
  "event_date_old": "YYYY-MM-DD or empty string",
  "confidence": 0.0 to 1.0,
  "reason": "one sentence explanation in English"
}}

Rules:
- SUPERSEDES: The NEW chunk contains updated information that makes the OLD chunk factually obsolete or outdated ON THE SAME ENTITY OR FACT. The key test: would someone reading the OLD chunk get a wrong or outdated picture of reality? If yes → SUPERSEDES. Examples: address change, job change, new decision replacing old one, updated plan, revised financial numbers, changed preference, updated supplement protocol.
- EXTENDS: The NEW chunk adds meaningful detail, context, or nuance to the OLD chunk without contradicting it. Both remain valid and useful together. Examples: adding a job title to employment info, elaborating on a decision rationale, providing follow-up results, a product description enriching a company overview.
- INDEPENDENT: The chunks discuss similar topics but neither updates nor meaningfully enriches the other. Also use this when the relationship is ambiguous or unclear.

Critical guard-rails for SUPERSEDES:
- NEVER mark as SUPERSEDES when the chunks describe DIFFERENT entity types (e.g., a company vs. a product it launched, a person vs. a project they work on). A product created by a company does NOT supersede the company description — that is EXTENDS or INDEPENDENT.
- NEVER mark as SUPERSEDES when both chunks remain factually valid at their respective points in time and describe different aspects or phases of the same topic.
- SUPERSEDES requires that the OLD chunk would give WRONG or MISLEADING information to someone reading it today. If it is merely incomplete or less detailed, that is EXTENDS, not SUPERSEDES.

- event_date_new / event_date_old: Extract the date of the FACT described, not the conversation date. If the chunk says "I moved to Rome in March 2023", event_date is "2023-03-01". If it says "last week we decided X" and the document date is 2025-06-15, event_date is approximately "2025-06-08". If no specific date for the fact is mentioned or inferable, use empty string "".
- confidence: How certain you are. Use 0.9+ only for unambiguous cases, 0.7-0.9 for probable, below 0.7 for uncertain.
- When in doubt, prefer INDEPENDENT. False positives (wrong SUPERSEDES) are far worse than false negatives (missed SUPERSEDES)."""


# ============================================================
# UTILITY
# ============================================================

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {"processed_ids": [], "stats": {"supersedes": 0, "extends": 0, "independent": 0, "errors": 0, "skipped": 0}}


def save_checkpoint(checkpoint):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f, indent=2)


def get_internal_id(conn, embedding_id):
    cur = conn.cursor()
    cur.execute("SELECT id FROM embeddings WHERE embedding_id = ?", (embedding_id,))
    row = cur.fetchone()
    return row[0] if row else None


def set_metadata(conn, internal_id, key, value):
    cur = conn.cursor()
    cur.execute("UPDATE embedding_metadata SET string_value = ? WHERE id = ? AND key = ?",
                (value, internal_id, key))
    if cur.rowcount == 0:
        cur.execute("INSERT INTO embedding_metadata (id, key, string_value, int_value, float_value, bool_value) VALUES (?, ?, ?, NULL, NULL, NULL)",
                    (internal_id, key, value))


def append_metadata(conn, internal_id, key, value):
    cur = conn.cursor()
    cur.execute("SELECT string_value FROM embedding_metadata WHERE id = ? AND key = ?",
                (internal_id, key))
    row = cur.fetchone()
    if row and row[0]:
        existing = row[0]
        if value not in existing.split(","):
            cur.execute("UPDATE embedding_metadata SET string_value = ? WHERE id = ? AND key = ?",
                        (existing + "," + value, internal_id, key))
    else:
        set_metadata(conn, internal_id, key, value)


# ============================================================
# CHUNK LOADING
# ============================================================

def load_chunks_to_process(conn, args, checkpoint):
    """Load the sorted list of chunks to process."""
    processed_set = set(checkpoint["processed_ids"])
    cur = conn.cursor()

    cur.execute("""
        SELECT e.embedding_id,
               MAX(CASE WHEN m.key = 'date' THEN m.string_value END) as date,
               MAX(CASE WHEN m.key = 'source' THEN m.string_value END) as source,
               MAX(CASE WHEN m.key = 'category' THEN m.string_value END) as category,
               MAX(CASE WHEN m.key = 'create_time' THEN m.float_value END) as create_time,
               MAX(CASE WHEN m.key = 'supersession_checked' THEN m.string_value END) as checked
        FROM embeddings e
        JOIN embedding_metadata m ON m.id = e.id
        WHERE m.key IN ('date', 'source', 'category', 'create_time', 'supersession_checked')
        GROUP BY e.embedding_id
    """)
    rows = cur.fetchall()

    chunks = []
    for row in rows:
        eid, date, source, category, create_time, checked = row

        if eid in processed_set:
            continue
        if checked == "1" and not args.full:
            continue
        if args.source and source != args.source:
            continue
        if args.category and category != args.category:
            continue

        if category == "conv_summary":
            priority = 0
        elif category == "doc_summary":
            priority = 1
        else:
            priority = 2

        chunks.append({
            "id": eid,
            "date": date or "",
            "source": source or "",
            "category": category or "",
            "create_time": create_time or 0.0,
            "priority": priority
        })

    chunks.sort(key=lambda c: (c["priority"], c["date"]))
    return chunks


# ============================================================
# HAIKU API CALL
# ============================================================

async def call_haiku(session, semaphore, new_text, old_text, new_date, old_date):
    prompt = SUPERSESSION_PROMPT.format(user_name=_USER_NAME, 
        new_date=new_date or "unknown",
        old_date=old_date or "unknown",
        new_text=new_text[:2000],
        old_text=old_text[:2000]
    )

    payload = {
        "model": HAIKU_MODEL,
        "max_tokens": 200,
        "messages": [{"role": "user", "content": prompt}]
    }

    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }

    async with semaphore:
        for attempt in range(3):
            try:
                async with session.post(ANTHROPIC_API_URL, json=payload, headers=headers) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("retry-after", 5))
                        await asyncio.sleep(retry_after)
                        continue
                    if resp.status != 200:
                        text = await resp.text()
                        print(f"  ⚠️  Haiku API error {resp.status}: {text[:200]}")
                        return None
                    data = await resp.json()
                    content = data["content"][0]["text"].strip()
                    content = content.replace("```json", "").replace("```", "").strip()
                    return json.loads(content)
            except (json.JSONDecodeError, KeyError, IndexError) as e:
                print(f"  ⚠️  Parse error: {e}")
                return None
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                    continue
                print(f"  ⚠️  Request error: {e}")
                return None
    return None


# ============================================================
# PROCESSING SINGOLO CHUNK
# ============================================================

async def process_chunk(chunk, collection, session, semaphore, args):
    """Process a single chunk: find similar older candidates, compare with Haiku."""
    chunk_id = chunk["id"]
    chunk_date = chunk["date"]
    chunk_create_time = chunk["create_time"]

    # Recupera embedding e testo del chunk da ChromaDB
    try:
        chunk_data = collection.get(ids=[chunk_id], include=["embeddings", "documents"])
        embs = chunk_data["embeddings"]
        docs = chunk_data["documents"]
        # ChromaDB restituisce numpy array — check sicuro
        if embs is None or len(embs) == 0 or docs is None or len(docs) == 0:
            return {"id": chunk_id, "status": "skipped", "reason": "no embedding/document"}
        embedding = embs[0]
        text = docs[0]
        # Converti numpy → list per ChromaDB query
        if isinstance(embedding, np.ndarray):
            embedding = embedding.tolist()
    except Exception as e:
        return {"id": chunk_id, "status": "error", "reason": str(e)}

    if not text or len(text.strip()) < 50:
        return {"id": chunk_id, "status": "skipped", "reason": "text too short"}

    # Query top-K similar — filter: create_time < chunk (only older chunks)
    try:
        where_filter = None
        if chunk_create_time > 0:
            where_filter = {"create_time": {"$lt": chunk_create_time}}

        results = collection.query(
            query_embeddings=[embedding],
            n_results=args.top_k + 10,  # extra per compensare filtri post-query
            where=where_filter,
            include=["documents", "metadatas", "distances"]
        )
    except Exception as e:
        return {"id": chunk_id, "status": "error", "reason": f"query error: {e}"}

    if not results["ids"] or not results["ids"][0]:
        return {"id": chunk_id, "status": "skipped", "reason": "no similar chunks found"}

    # Filter candidates by cosine threshold and status
    candidates = []
    for i, cid in enumerate(results["ids"][0]):
        if cid == chunk_id:
            continue

        distance = results["distances"][0][i]
        cosine_sim = 1.0 - distance
        if cosine_sim < args.threshold:
            continue

        meta = results["metadatas"][0][i] if results["metadatas"] else {}
        if meta.get("superseded_by", ""):
            continue

        doc = results["documents"][0][i] if results["documents"] else ""
        if not doc or len(doc.strip()) < 50:
            continue

        candidates.append({
            "id": cid,
            "text": doc,
            "date": meta.get("date", ""),
            "cosine_sim": cosine_sim
        })

        if len(candidates) >= args.top_k:
            break

    if not candidates:
        return {"id": chunk_id, "status": "skipped", "reason": "no candidates above threshold"}

    # Compare each candidate with Haiku
    actions = []
    event_date_new = ""

    for cand in candidates:
        result = await call_haiku(
            session, semaphore,
            new_text=text,
            old_text=cand["text"],
            new_date=chunk_date,
            old_date=cand["date"]
        )

        if result is None:
            actions.append({"type": "error", "old_id": cand["id"]})
            continue

        relation = result.get("relation", "INDEPENDENT")
        confidence = result.get("confidence", 0.0)
        reason = result.get("reason", "")
        ed_new = result.get("event_date_new", "")
        ed_old = result.get("event_date_old", "")

        if ed_new and not event_date_new:
            event_date_new = ed_new
        if confidence < args.confidence:
            relation = "INDEPENDENT"

        actions.append({
            "type": relation,
            "old_id": cand["id"],
            "confidence": confidence,
            "reason": reason,
            "event_date_old": ed_old,
            "cosine_sim": cand["cosine_sim"]
        })

    return {
        "id": chunk_id,
        "status": "processed",
        "actions": actions,
        "event_date_new": event_date_new
    }


# ============================================================
# APPLY ACTIONS
# ============================================================

def apply_actions(conn_rw, result):
    if result["status"] != "processed":
        return result["status"]

    chunk_id = result["id"]
    internal_id = get_internal_id(conn_rw, chunk_id)
    if not internal_id:
        return "error"

    if result.get("event_date_new"):
        set_metadata(conn_rw, internal_id, "event_date", result["event_date_new"])

    stats = {"supersedes": 0, "extends": 0, "independent": 0, "errors": 0}

    for action in result.get("actions", []):
        if action["type"] == "error":
            stats["errors"] += 1
            continue
        if action["type"] == "INDEPENDENT":
            stats["independent"] += 1
            continue

        old_id = action["old_id"]
        old_internal_id = get_internal_id(conn_rw, old_id)
        if not old_internal_id:
            stats["errors"] += 1
            continue

        if action["type"] == "SUPERSEDES":
            append_metadata(conn_rw, old_internal_id, "superseded_by", chunk_id)
            set_metadata(conn_rw, old_internal_id, "superseded_at", datetime.now().strftime("%Y-%m-%d"))
            append_metadata(conn_rw, internal_id, "supersedes", old_id)
            stats["supersedes"] += 1
        elif action["type"] == "EXTENDS":
            append_metadata(conn_rw, old_internal_id, "extended_by", chunk_id)
            append_metadata(conn_rw, internal_id, "extends", old_id)
            stats["extends"] += 1

        if action.get("event_date_old"):
            set_metadata(conn_rw, old_internal_id, "event_date", action["event_date_old"])

    set_metadata(conn_rw, internal_id, "supersession_checked", "1")
    return stats


# ============================================================
# STATS
# ============================================================

def show_stats(conn):
    cur = conn.cursor()

    cur.execute("SELECT COUNT(DISTINCT id) FROM embedding_metadata WHERE key = 'superseded_by' AND string_value != ''")
    superseded = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT id) FROM embedding_metadata WHERE key = 'extends' AND string_value != ''")
    extending = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT id) FROM embedding_metadata WHERE key = 'extended_by' AND string_value != ''")
    extended = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT id) FROM embedding_metadata WHERE key = 'event_date' AND string_value != ''")
    with_event_date = cur.fetchone()[0]
    cur.execute("SELECT COUNT(DISTINCT id) FROM embedding_metadata WHERE key = 'supersession_checked' AND string_value = '1'")
    checked = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM embeddings")
    total = cur.fetchone()[0]

    print(f"\n📊 Supersession Stats")
    print(f"{'─' * 45}")
    print(f"  Chunk totali:          {total:>8,}")
    print(f"  Already checked:       {checked:>8,}")
    print(f"  Da controllare:        {total - checked:>8,}")
    print(f"{'─' * 45}")
    print(f"  Chunk superati:        {superseded:>8,}")
    print(f"  Chunk che estendono:   {extending:>8,}")
    print(f"  Chunk estesi:          {extended:>8,}")
    print(f"  Con event_date:        {with_event_date:>8,}")
    print()


# ============================================================
# MAIN
# ============================================================

async def main():
    parser = argparse.ArgumentParser(description="TAILOR Temporal Supersession")
    parser.add_argument("--full", action="store_true", help="Re-process all (ignore checkpoint)")
    parser.add_argument("--test", type=int, default=0, help="Process N chunks in test mode (no write)")
    parser.add_argument("--stats", action="store_true", help="Show statistics and exit")
    parser.add_argument("--source", type=str, default="", help="Filter by source")
    parser.add_argument("--category", type=str, default="", help="Filter by category")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--threshold", type=float, default=DEFAULT_COSINE_THRESHOLD)
    parser.add_argument("--confidence", type=float, default=DEFAULT_CONFIDENCE_THRESHOLD)
    parser.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    args = parser.parse_args()

    if not ANTHROPIC_API_KEY:
        print("❌ ANTHROPIC_API_KEY not found (neither in environment nor in plist).")
        sys.exit(1)

    print(f"🔑 API key: {ANTHROPIC_API_KEY[:12]}...")

    conn_ro = sqlite3.connect(f"file:{CHROMA_DB_PATH}?mode=ro", uri=True)

    if args.stats:
        show_stats(conn_ro)
        conn_ro.close()
        return

    # ChromaDB per query semantiche (WARNING: conflicts with MCP server if active)
    client = chromadb.PersistentClient(path=DB_DIR)
    collection = client.get_collection(COLLECTION_NAME)

    if args.full:
        checkpoint = {"processed_ids": [], "stats": {"supersedes": 0, "extends": 0, "independent": 0, "errors": 0, "skipped": 0}}
    else:
        checkpoint = load_checkpoint()

    print(f"🔍 Loading chunks to process...")
    chunks = load_chunks_to_process(conn_ro, args, checkpoint)

    if args.test:
        chunks = chunks[:args.test]

    total = len(chunks)
    if total == 0:
        print("✅ No chunks to process.")
        show_stats(conn_ro)
        conn_ro.close()
        return

    print(f"📋 {total:,} chunk (threshold={args.threshold}, confidence={args.confidence}, top_k={args.top_k}, workers={args.workers})")

    by_cat = {}
    for c in chunks:
        cat = c["category"] or "other"
        by_cat[cat] = by_cat.get(cat, 0) + 1
    for cat, count in sorted(by_cat.items(), key=lambda x: -x[1])[:10]:
        print(f"  {cat}: {count:,}")

    conn_rw = sqlite3.connect(CHROMA_DB_PATH) if not args.test else None
    semaphore = asyncio.Semaphore(args.workers)
    processed = 0
    batch_stats = {"supersedes": 0, "extends": 0, "independent": 0, "errors": 0, "skipped": 0}
    start_time = time.time()

    async with aiohttp.ClientSession() as session:
        BATCH_SIZE = 20
        for batch_start in range(0, total, BATCH_SIZE):
            batch = chunks[batch_start:batch_start + BATCH_SIZE]

            tasks = [process_chunk(c, collection, session, semaphore, args) for c in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    print(f"  ❌ Exception: {result}")
                    batch_stats["errors"] += 1
                    processed += 1
                    continue

                if result["status"] == "skipped":
                    batch_stats["skipped"] += 1
                    checkpoint["processed_ids"].append(result["id"])
                    processed += 1
                    continue

                if result["status"] == "error":
                    print(f"  ⚠️  Error on {result['id'][:40]}: {result.get('reason', '?')}")
                    batch_stats["errors"] += 1
                    processed += 1
                    continue

                for action in result.get("actions", []):
                    if action["type"] in ("SUPERSEDES", "EXTENDS"):
                        emoji = "🔄" if action["type"] == "SUPERSEDES" else "➕"
                        print(f"  {emoji} {action['type']}: {result['id'][:50]} → {action['old_id'][:50]} "
                              f"(conf={action['confidence']:.2f}, cos={action['cosine_sim']:.3f}) — {action['reason']}")

                if not args.test and conn_rw:
                    action_stats = apply_actions(conn_rw, result)
                    if isinstance(action_stats, dict):
                        for k, v in action_stats.items():
                            batch_stats[k] += v
                else:
                    for action in result.get("actions", []):
                        atype = action["type"]
                        if atype == "SUPERSEDES": batch_stats["supersedes"] += 1
                        elif atype == "EXTENDS": batch_stats["extends"] += 1
                        elif atype == "INDEPENDENT": batch_stats["independent"] += 1

                checkpoint["processed_ids"].append(result["id"])
                processed += 1

            if conn_rw:
                conn_rw.commit()

            if processed % CHECKPOINT_EVERY < BATCH_SIZE:
                checkpoint["stats"] = batch_stats
                save_checkpoint(checkpoint)

            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 0
            eta = (total - processed) / rate if rate > 0 else 0
            print(f"  [{processed:,}/{total:,}] {rate:.1f} chunk/s | "
                  f"SUP={batch_stats['supersedes']} EXT={batch_stats['extends']} IND={batch_stats['independent']} "
                  f"ERR={batch_stats['errors']} SKIP={batch_stats['skipped']} | ETA {eta/60:.0f}m")

    if conn_rw:
        conn_rw.commit()
        conn_rw.close()

    checkpoint["stats"] = batch_stats
    save_checkpoint(checkpoint)
    conn_ro.close()

    elapsed = time.time() - start_time
    print(f"\n{'═' * 50}")
    print(f"✅ Completato in {elapsed/60:.1f} minuti")
    print(f"  Chunks processed:  {processed:,}")
    print(f"  SUPERSEDES:        {batch_stats['supersedes']:,}")
    print(f"  EXTENDS:           {batch_stats['extends']:,}")
    print(f"  INDEPENDENT:       {batch_stats['independent']:,}")
    print(f"  Errori:            {batch_stats['errors']:,}")
    print(f"  Skippati:          {batch_stats['skipped']:,}")
    print(f"  Rate:              {processed/elapsed:.1f} chunk/s")
    if not args.test:
        print(f"\n  Checkpoint salvato in {CHECKPOINT_FILE}")
    else:
        print(f"\n  ⚠️  TEST mode — no DB changes")


if __name__ == "__main__":
    asyncio.run(main())
