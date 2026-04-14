#!/usr/bin/env python3
"""
TAILOR — 10f: Crea summary per conversations (ChatGPT, Claude, Email)
Uses LLM to generate structured conversation summaries
that don't yet have a summary in the KB.

Uso:
  python scripts/enrichment/create_conv_summaries.py                  # Crea summary mancanti
  python scripts/enrichment/create_conv_summaries.py --dry-run        # Show what it would do
  python scripts/enrichment/create_conv_summaries.py --stats          # Statistiche per source
  python scripts/enrichment/create_conv_summaries.py --limit 50       # Max N conversations
  python scripts/enrichment/create_conv_summaries.py --source chatgpt # Only one source
  python scripts/enrichment/create_conv_summaries.py --workers 3      # Parallelismo API

Requisiti:
  pip install openai chromadb requests aiohttp
"""

import os
import sys
import json
import asyncio
import aiohttp
import sqlite3
import time
import requests
import argparse
from datetime import datetime

# ============================================================
# CONFIGURAZIONE
# ============================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "lib"))
from embedding import get_embedding, get_embeddings
DB_DIR = os.path.join(BASE_DIR, "db")
DB_PATH = os.path.join(DB_DIR, "chroma.sqlite3")
LOG_PATH = os.path.join(BASE_DIR, "logs", "conv_summaries.log")

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_URL = "https://api.openai.com/v1/chat/completions"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.lib.config import get_enrichment

# User name from config (for prompt templates)
from config import get as cfg
_USER_NAME = cfg("user", "name") or "the user"
_USER_LANG = cfg("user", "language") or "en"
_sum_cfg = get_enrichment("summaries")
MODEL = _sum_cfg["model"]

COLLECTION_NAME = cfg("kb", "collection") or "tailor_kb"

# Quanti chars di testo per conversazione mandare al modello
MAX_INPUT_CHARS = 8000
# Max chunk da leggere per conversazione
MAX_CHUNKS_PER_CONV = 8
# Parallelismo API
MAX_WORKERS = 3
# Max token output
SUMMARY_MAX_TOKENS = 500

VALID_SOURCES = {"chatgpt", "claude", "email"}

# ============================================================
# PROMPT
# ============================================================

PROMPT_CONV = """Generate a structured summary for {user_name}'s personal Knowledge Base.

Conversazione: {title}
Fonte: {source}
Data: {date}

{type_instructions}

RULES:
- Reply ONLY with the summary, no preamble or commentary
- 3-8 sentences, max 800 characters
- Include ALL numbers, amounts, dates and proper nouns you find
- Include keywords a user would search for to find this conversation
- No bullet points, no markdown, just flowing text
- If content is fragmentary, extract all available data anyway
- Do NOT start with "In this conversation" or similar formulas
- Respond in {language}

Text:
{text}"""

SOURCE_INSTRUCTIONS = {
    "chatgpt": "This is a ChatGPT conversation. Identify the main topic, decisions made, conclusions reached, and any relevant factual information (names, numbers, dates, URLs, code, configurations).",
    "claude": "This is a Claude conversation. Identify the main topic, decisions made, code or configurations produced, and any relevant factual information. If it concerns TAILOR or technical systems, include specific details about architecture and configuration.",
    "email": "This is an email or email thread. Identify: sender and recipient, subject, dates, requests or agreed actions, amounts if present, and any expected follow-up.",
}


# ============================================================
# UTILITIES
# ============================================================

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")


def get_openai_key():
    """Recupera la API key da env o dal plist del LaunchDaemon."""
    global OPENAI_API_KEY
    if OPENAI_API_KEY:
        return OPENAI_API_KEY
    try:
        import subprocess
        OPENAI_API_KEY = subprocess.check_output(
            ["/usr/bin/plutil", "-extract", "EnvironmentVariables.OPENAI_API_KEY", "raw",
             "/Library/LaunchDaemons/com.tailor.mcp.plist"], text=True
        ).strip()
        return OPENAI_API_KEY
    except Exception:
        return ""


# get_embedding provided by scripts/lib/embedding.py (imported above)


# ============================================================
# FIND MISSING SUMMARIES
# ============================================================

def find_existing_summaries(conn):
    """Find conv_ids that already have a summary."""
    cur = conn.cursor()
    # Summaries created by this script (category = conv_summary)
    cur.execute("""
        SELECT DISTINCT ci.string_value
        FROM embedding_metadata cat
        JOIN embedding_metadata ci ON cat.id = ci.id AND ci.key = 'original_conv_id'
        WHERE cat.key = 'category' AND cat.string_value = 'conv_summary'
    """)
    existing = set(r[0] for r in cur.fetchall())

    # Also doc_summary (to avoid duplicating if already present)
    cur.execute("""
        SELECT DISTINCT ci.string_value
        FROM embedding_metadata cat
        JOIN embedding_metadata ci ON cat.id = ci.id AND ci.key = 'conv_id'
        WHERE cat.key = 'category' AND cat.string_value = 'doc_summary'
    """)
    existing.update(r[0] for r in cur.fetchall())

    return existing


def find_conversations_without_summary(source_filter=None):
    """Find all conv_ids without summary for the specified sources."""
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    existing = find_existing_summaries(conn)
    log(f"Summary esistenti: {len(existing)}")

    sources = [source_filter] if source_filter else list(VALID_SOURCES)
    placeholders = ",".join("?" for _ in sources)

    cur = conn.cursor()
    cur.execute(f"""
        SELECT DISTINCT
            c.string_value as conv_id,
            s.string_value as source,
            t.string_value as title,
            d.string_value as date
        FROM embedding_metadata s
        JOIN embedding_metadata c ON s.id = c.id AND c.key = 'conv_id'
        JOIN embedding_metadata t ON s.id = t.id AND t.key = 'title'
        LEFT JOIN embedding_metadata d ON s.id = d.id AND d.key = 'date'
        WHERE s.key = 'source' AND s.string_value IN ({placeholders})
        ORDER BY d.string_value DESC
    """, sources)

    missing = []
    seen = set()
    for conv_id, source, title, date in cur.fetchall():
        if conv_id in existing or conv_id in seen:
            continue
        seen.add(conv_id)
        missing.append({
            "conv_id": conv_id,
            "source": source,
            "title": title or "Untitled",
            "date": date or "",
        })

    conn.close()
    return missing


def get_conv_text(conv_id):
    """Recupera il testo di una conversazione dai chunk in ChromaDB."""
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    cur = conn.cursor()

    # Find chunk IDs for this conv_id, sorted by chunk_index
    cur.execute("""
        SELECT c.id
        FROM embedding_metadata c
        LEFT JOIN embedding_metadata ci ON c.id = ci.id AND ci.key = 'chunk_index'
        WHERE c.key = 'conv_id' AND c.string_value = ?
        ORDER BY CAST(COALESCE(ci.int_value, ci.float_value, 0) AS INTEGER) ASC
    """, (conv_id,))
    chunk_ids = [r[0] for r in cur.fetchall()]

    text_parts = []
    total_chars = 0
    for cid in chunk_ids[:MAX_CHUNKS_PER_CONV]:
        cur.execute("SELECT c0 FROM embedding_fulltext_search_content WHERE id = ?", (cid,))
        r = cur.fetchone()
        if r and r[0]:
            text_parts.append(r[0])
            total_chars += len(r[0])
            if total_chars >= MAX_INPUT_CHARS:
                break

    conn.close()
    return "\n---\n".join(text_parts)[:MAX_INPUT_CHARS]


# ============================================================
# API CALL
# ============================================================

async def generate_summary(session, title, source, date, text, semaphore):
    """Call LLM to generate the summary."""
    async with semaphore:
        prompt = PROMPT_CONV.format(user_name=_USER_NAME, language=_USER_LANG, 
            title=title,
            source=source,
            date=date,
            type_instructions=SOURCE_INSTRUCTIONS.get(source, ""),
            text=text,
        )
        try:
            async with session.post(
                OPENAI_URL,
                json={
                    "model": MODEL,
                    "max_tokens": SUMMARY_MAX_TOKENS,
                    "temperature": 0.3,
                    "messages": [{"role": "user", "content": prompt}],
                },
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=60),
            ) as resp:
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", "30"))
                    return None, f"rate_limit:{retry_after}"
                data = await resp.json()
                if resp.status != 200:
                    err = data.get("error", {}).get("message", str(resp.status))
                    return None, err
                return data["choices"][0]["message"]["content"].strip(), None
        except asyncio.TimeoutError:
            return None, "timeout"
        except Exception as e:
            return None, str(e)


# ============================================================
# WRITE SUMMARY TO CHROMADB
# ============================================================

def write_summary_chunk(col, conv_id, source, title, date, summary):
    """Write the summary as a chunk in ChromaDB."""
    ts = int(time.time() * 1000)
    summary_id = f"conv_summary_{conv_id}_{ts}"

    # Testo ricco per embedding (include metadati per migliorare la ricerca)
    content = f"{title} — {source}\n\n"
    content += f"[RIASSUNTO CONVERSAZIONE] {title}\n"
    content += f"Fonte: {source} | Data: {date}\n\n"
    content += summary

    embedding = get_embedding(content)

    metadata = {
        "source": source,
        "title": title,
        "date": date or datetime.now().strftime("%Y-%m-%d"),
        "create_time": time.time(),
        "conv_id": summary_id,
        "original_conv_id": conv_id,
        "category": "conv_summary",
        "chunk_index": -1,
        "char_count": len(content),
        "turn_count": 1,
        "default_model": "",
    }

    col.add(
        ids=[summary_id],
        documents=[content],
        embeddings=[embedding],
        metadatas=[metadata],
    )
    return summary_id


# ============================================================
# MAIN
# ============================================================

async def main():
    parser = argparse.ArgumentParser(description="Crea summary per conversations ChatGPT/Claude/Email")
    parser.add_argument("--dry-run", action="store_true", help="Show what it would do without executing")
    parser.add_argument("--stats", action="store_true", help="Show statistics")
    parser.add_argument("--limit", type=int, default=0, help="Max conversations to process")
    parser.add_argument("--source", type=str, default=None, choices=list(VALID_SOURCES), help="Filter by source")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS, help="Parallelismo API")
    args = parser.parse_args()

    api_key = get_openai_key()
    if not api_key and not args.stats and not args.dry_run:
        log("ERROR: OPENAI_API_KEY non trovata")
        sys.exit(1)

    log(f"Searching conversations without summary (source={args.source or 'all'})...")
    missing = find_conversations_without_summary(args.source)
    log(f"Conversations without summary: {len(missing)}")

    # ── STATS ──
    if args.stats:
        by_source = {}
        for m in missing:
            by_source[m["source"]] = by_source.get(m["source"], 0) + 1
        print(f"\nConversations without summary:")
        for source, count in sorted(by_source.items(), key=lambda x: -x[1]):
            print(f"  {source:10s}: {count:,}")
        print(f"  {'TOTALE':10s}: {len(missing):,}")
        return

    if not missing:
        log("All conversations already have a summary!")
        return

    # ── DRY RUN ──
    if args.limit:
        missing = missing[:args.limit]

    if args.dry_run:
        by_source = {}
        for m in missing:
            by_source[m["source"]] = by_source.get(m["source"], 0) + 1
        print(f"\nTo process: {len(missing)} conversations")
        for source, count in sorted(by_source.items(), key=lambda x: -x[1]):
            print(f"  {source}: {count}")
        print(f"\nCampione:")
        for m in missing[:20]:
            print(f"  [{m['source']:7s}] {m['date'][:10] if m['date'] else '          '} | {m['title'][:60]}")
        if len(missing) > 20:
            print(f"  ... e altre {len(missing) - 20}")
        return

    # ── PROCESSING ──
    import chromadb
    client = chromadb.PersistentClient(path=DB_DIR)
    col = client.get_collection(COLLECTION_NAME)
    semaphore = asyncio.Semaphore(args.workers)

    created = 0
    errors = 0
    skipped = 0
    start_time = time.time()

    async with aiohttp.ClientSession() as session:
        for i, m in enumerate(missing):
            # Recupera testo
            text = get_conv_text(m["conv_id"])
            if not text or len(text.strip()) < 50:
                skipped += 1
                continue

            # Generate summary
            summary, error = await generate_summary(
                session, m["title"], m["source"], m["date"], text, semaphore
            )

            # Retry su rate limit
            if error and error.startswith("rate_limit"):
                wait = int(error.split(":")[1]) if ":" in error else 30
                log(f"[{i+1}/{len(missing)}] Rate limit, attendo {wait}s...")
                await asyncio.sleep(wait)
                summary, error = await generate_summary(
                    session, m["title"], m["source"], m["date"], text, semaphore
                )

            # Retry generico (1 tentativo)
            if error and not error.startswith("rate_limit"):
                await asyncio.sleep(2)
                summary, error = await generate_summary(
                    session, m["title"], m["source"], m["date"], text, semaphore
                )

            if error:
                log(f"[{i+1}/{len(missing)}] ERROR {m['title'][:40]}: {error}")
                errors += 1
                continue

            # Sanity check: summary not empty and not too long
            if not summary or len(summary) < 20:
                log(f"[{i+1}/{len(missing)}] SKIP summary too short: {m['title'][:40]}")
                skipped += 1
                continue

            # Write to ChromaDB
            try:
                write_summary_chunk(col, m["conv_id"], m["source"], m["title"], m["date"], summary)
                created += 1
                if created % 25 == 0 or created <= 5:
                    elapsed = time.time() - start_time
                    rate = created / elapsed * 3600 if elapsed > 0 else 0
                    log(f"[{i+1}/{len(missing)}] OK {m['source']}:{m['title'][:40]} ({rate:.0f}/h)")
            except Exception as e:
                log(f"[{i+1}/{len(missing)}] ERROR chunk {m['title'][:40]}: {e}")
                errors += 1

    # Cleanup
    del client

    elapsed = time.time() - start_time
    log(f"DONE: {created} summary creati, {errors} errori, {skipped} skippati in {elapsed:.0f}s")


if __name__ == "__main__":
    asyncio.run(main())