"""
TAILOR — 17: Atomic Fact Extraction (v2 — Gemini 2.5 Flash)
Extracts atomic facts from each KB chunk and saves them to facts.sqlite3.

Each fact is a single standalone assertion: "Crowe Horwath commission: 70%",
"User lives in Berlin since 2023", "Decision: use ChromaDB as vector DB".

Backend LLM (selezionato con --backend):
  gemini: Gemini 2.5 Flash via Google AI Studio API (default, best quality/cost)
  openai: GPT-4o-mini via OpenAI API
  ollama: Gemma 4 26B locale via Ollama (costo zero, lento)

Uso:
  python 17_extract_facts.py                        # Incrementale, Gemini 2.5 Flash
  python 17_extract_facts.py --full                 # Re-process everything
  python 17_extract_facts.py --test 20              # Test su 20 chunk (no write)
  python 17_extract_facts.py --stats                # Statistiche
  python 17_extract_facts.py --source document      # Only one source
  python 17_extract_facts.py --workers 10           # Worker paralleli
  python 17_extract_facts.py --backend openai       # GPT-4o-mini
  python 17_extract_facts.py --backend ollama       # Gemma 4 locale
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
import requests
from datetime import datetime

# ============================================================
# CONFIGURAZIONE
# ============================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_DIR = os.path.join(BASE_DIR, "db")
CHROMA_DB_PATH = os.path.join(DB_DIR, "chroma.sqlite3")
FACTS_DB_PATH = os.path.join(DB_DIR, "facts.sqlite3")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.lib.config import get as cfg, get_enrichment_backends
_USER_LANG = cfg("user", "language") or "en"

OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"
OLLAMA_CHAT_URL = os.environ.get("OLLAMA_URL", cfg("ollama", "base_url") or "http://localhost:11434") + "/api/chat"

# Build model lookup from config backends list
_backends = get_enrichment_backends("fact_extraction")
_model_for = {}
for _b in _backends:
    _model_for[_b["provider"]] = _b["model"]
# Aliases: "haiku" maps to anthropic, "gemini" maps to google
_PROVIDER_ALIAS = {"haiku": "anthropic", "gemini": "google"}
GEMINI_MODEL = _model_for.get("google", "gemini-2.5-flash")
OPENAI_MODEL = _model_for.get("openai", "gpt-4o-mini")
ANTHROPIC_MODEL = _model_for.get("anthropic", "claude-haiku-4-5-20251001")

DEFAULT_WORKERS = 10
DEFAULT_BACKEND = "gemini"

def get_key(name):
    key = os.environ.get(name, "")
    if key:
        return key
    try:
        result = subprocess.run(
            ["/usr/bin/plutil", "-extract", f"EnvironmentVariables.{name}", "raw",
             "/Library/LaunchDaemons/com.tailor.mcp.plist"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    return ""

OPENAI_API_KEY = get_key("OPENAI_API_KEY")
ANTHROPIC_API_KEY = get_key("ANTHROPIC_API_KEY")
ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
GOOGLE_API_KEY = get_key("GOOGLE_API_KEY")

# ============================================================
# PROMPT
# ============================================================

EXTRACTION_PROMPT = """Extract atomic facts from this knowledge base chunk. Each fact should be a single, self-contained statement that could be independently verified or updated.

Text (date: {date}, source: {source}):
{text}

Respond with ONLY a JSON array. No other text, no markdown, no explanation.
[
  {{"fact": "concise standalone statement", "category": "category", "entity_tags": ["entity1"], "event_date": "YYYY-MM-DD or empty string"}},
  ...
]

Categories (pick one per fact):
- decision: a choice or decision made ("Decided to use ChromaDB")
- preference: a preference or opinion ("Prefers TypeScript over Python")
- status: current state of something ("Lives in Berlin", "CEO of Acme Corp")
- relationship: connection between entities ("Daniele is 50/50 partner")
- financial: money, revenue, costs, prices ("Commission rate: 70%")
- event: something that happened ("Moved to Rome in March 2023")
- technical: technical fact or config ("MCP server runs on port 8787")
- health: health-related ("Takes magnesium glycinate daily")
- personal: personal info ("Has a son named Alex, born 2015")
- legal: legal/contractual ("Contract signed with Crowe Horwath")

Rules:
- Extract ONLY factual statements, not opinions or filler
- Each fact must make sense on its own without the original context
- Include entity names in entity_tags (people, companies, projects)
- event_date: the date the fact refers to, NOT the document date. Empty if not inferable.
- Skip greetings, pleasantries, and meta-conversation ("User said hello")
- For financial documents: extract key figures, dates, parties
- For emails: extract decisions, action items, key facts
- For conversations: extract decisions, preferences, facts learned
- Aim for 3-15 facts per chunk. If the chunk is noise/filler, return []
- Write facts in {language}"""


# ============================================================
# DATABASE SETUP
# ============================================================

def init_facts_db():
    conn = sqlite3.connect(FACTS_DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chunk_id TEXT NOT NULL,
            fact TEXT NOT NULL,
            category TEXT DEFAULT '',
            entity_tags TEXT DEFAULT '[]',
            event_date TEXT DEFAULT '',
            confidence REAL DEFAULT 1.0,
            superseded_by INTEGER DEFAULT NULL,
            superseded_at TEXT DEFAULT '',
            created_at TEXT NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS extraction_log (
            chunk_id TEXT PRIMARY KEY,
            facts_count INTEGER DEFAULT 0,
            model TEXT DEFAULT '',
            extracted_at TEXT NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_facts_chunk ON facts(chunk_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_facts_entity ON facts(entity_tags)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_facts_superseded ON facts(superseded_by)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_facts_category ON facts(category)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_facts_event_date ON facts(event_date)")
    conn.commit()
    conn.close()


def get_extracted_chunks():
    if not os.path.exists(FACTS_DB_PATH):
        return set()
    conn = sqlite3.connect(f"file:{FACTS_DB_PATH}?mode=ro", uri=True)
    cur = conn.cursor()
    cur.execute("SELECT chunk_id FROM extraction_log")
    result = {r[0] for r in cur.fetchall()}
    conn.close()
    return result


# ============================================================
# CHUNK LOADING
# ============================================================

def load_chunks_to_process(args, extracted_set):
    conn = sqlite3.connect(f"file:{CHROMA_DB_PATH}?mode=ro", uri=True)
    cur = conn.cursor()

    cur.execute("""
        SELECT e.embedding_id,
               MAX(CASE WHEN m.key = 'date' THEN m.string_value END) as date,
               MAX(CASE WHEN m.key = 'source' THEN m.string_value END) as source,
               MAX(CASE WHEN m.key = 'category' THEN m.string_value END) as category
        FROM embeddings e
        JOIN embedding_metadata m ON m.id = e.id
        WHERE m.key IN ('date', 'source', 'category')
        GROUP BY e.embedding_id
    """)
    rows = cur.fetchall()

    text_map = {}
    cur.execute("""
        SELECT e.embedding_id, fts.c0
        FROM embeddings e
        JOIN embedding_fulltext_search_content fts ON fts.id = e.id
    """)
    for eid, text in cur.fetchall():
        text_map[eid] = text

    conn.close()

    chunks = []
    for row in rows:
        eid, date, source, category = row
        if eid in extracted_set and not args.full:
            continue
        if args.source and source != args.source:
            continue
        if args.category and category != args.category:
            continue

        text = text_map.get(eid, "")
        if not text or len(text.strip()) < 50:
            continue

        chunks.append({
            "id": eid, "date": date or "", "source": source or "",
            "category": category or "", "text": text
        })

    def priority(c):
        if c["category"] == "conv_summary": return 0
        if c["category"] == "doc_summary": return 1
        return 2
    chunks.sort(key=lambda c: (priority(c), c["date"]))
    return chunks


# ============================================================
# LLM CALLERS
# ============================================================

def _parse_facts_json(content):
    """Parse JSON array of facts from any response format."""
    content = content.replace("```json", "").replace("```", "").strip()
    if "<channel|>" in content:
        content = content.split("<channel|>")[-1].strip()
    # Prova array
    start = content.find("[")
    end = content.rfind("]") + 1
    if start >= 0 and end > start:
        parsed = json.loads(content[start:end])
        if isinstance(parsed, list):
            return parsed
    # Prova oggetto con chiave "facts"
    start = content.find("{")
    end = content.rfind("}") + 1
    if start >= 0 and end > start:
        parsed = json.loads(content[start:end])
        if isinstance(parsed, dict) and "facts" in parsed:
            return parsed["facts"]
    return None


async def call_gemini(session, semaphore, prompt):
    """Chiama Gemini 2.5 Flash via Google AI Studio API."""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GOOGLE_API_KEY}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 1500,
            "thinkingConfig": {"thinkingBudget": 0}
        }
    }
    async with semaphore:
        for attempt in range(3):
            try:
                async with session.post(url, json=payload, headers={"Content-Type": "application/json"}) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("retry-after", 10))
                        await asyncio.sleep(retry_after)
                        continue
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    content = data["candidates"][0]["content"]["parts"][0]["text"].strip()
                    return _parse_facts_json(content)
            except (json.JSONDecodeError, KeyError, IndexError):
                return None
            except Exception:
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return None
    return None


async def call_openai(session, semaphore, prompt):
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": OPENAI_MODEL, "max_tokens": 1500, "temperature": 0.1,
        "messages": [{"role": "user", "content": prompt}],
        "response_format": {"type": "json_object"}
    }
    async with semaphore:
        for attempt in range(3):
            try:
                async with session.post(OPENAI_API_URL, json=payload, headers=headers) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("retry-after", 5))
                        await asyncio.sleep(retry_after)
                        continue
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    content = data["choices"][0]["message"]["content"].strip()
                    return _parse_facts_json(content)
            except (json.JSONDecodeError, KeyError, IndexError):
                return None
            except Exception:
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return None
    return None


async def call_ollama(session, semaphore, prompt, model="gemma4:26b"):
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "Respond with only valid JSON array. No thinking, no explanation."},
            {"role": "user", "content": prompt}
        ],
        "stream": False,
        "options": {"temperature": 0.1, "num_predict": 1500}
    }
    async with semaphore:
        for attempt in range(2):
            try:
                async with session.post(OLLAMA_CHAT_URL, json=payload) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    content = data.get("message", {}).get("content", "").strip()
                    return _parse_facts_json(content)
            except Exception:
                if attempt < 1:
                    await asyncio.sleep(2)
                    continue
                return None
    return None




async def call_haiku(session, semaphore, prompt):
    """Chiama Claude Haiku 4.5 via Anthropic API."""
    headers = {"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"}
    payload = {"model": ANTHROPIC_MODEL, "max_tokens": 1500, "temperature": 0.1,
               "messages": [{"role": "user", "content": prompt}]}
    async with semaphore:
        for attempt in range(3):
            try:
                async with session.post(ANTHROPIC_API_URL, json=payload, headers=headers) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("retry-after", 5))
                        await asyncio.sleep(retry_after)
                        continue
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    content = data["content"][0]["text"].strip()
                    return _parse_facts_json(content)
            except (json.JSONDecodeError, KeyError, IndexError):
                return None
            except Exception:
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return None
    return None

# ============================================================
# PROCESSING
# ============================================================

async def process_chunk(chunk, session, semaphore, backend):
    prompt = EXTRACTION_PROMPT.format(language=_USER_LANG, 
        date=chunk["date"] or "unknown",
        source=chunk["source"] or "unknown",
        text=chunk["text"][:3000]
    )

    if backend == "gemini":
        facts = await call_gemini(session, semaphore, prompt)
    elif backend == "openai":
        facts = await call_openai(session, semaphore, prompt)
    elif backend == "ollama":
        facts = await call_ollama(session, semaphore, prompt)
    elif backend == "haiku":
        facts = await call_haiku(session, semaphore, prompt)
    else:
        return None

    if facts is None:
        return None

    valid_facts = []
    for f in facts:
        if not isinstance(f, dict):
            continue
        fact_text = f.get("fact", "").strip()
        if not fact_text or len(fact_text) < 10:
            continue
        valid_facts.append({
            "fact": fact_text[:500],
            "category": f.get("category", "")[:50].lower(),
            "entity_tags": json.dumps(f.get("entity_tags", [])[:10]),
            "event_date": f.get("event_date", "") or "",
            "confidence": min(max(float(f.get("confidence", 1.0)), 0), 1)
        })

    return valid_facts


# ============================================================
# STATS
# ============================================================

def show_stats():
    if not os.path.exists(FACTS_DB_PATH):
        print("❌ facts.sqlite3 non esiste ancora.")
        return

    conn = sqlite3.connect(f"file:{FACTS_DB_PATH}?mode=ro", uri=True)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM facts")
    total_facts = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM extraction_log")
    total_chunks = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM facts WHERE superseded_by IS NOT NULL")
    superseded = cur.fetchone()[0]
    cur.execute("SELECT category, COUNT(*) FROM facts GROUP BY category ORDER BY COUNT(*) DESC LIMIT 15")
    by_category = cur.fetchall()
    cur.execute("SELECT AVG(facts_count) FROM extraction_log WHERE facts_count > 0")
    avg = cur.fetchone()[0] or 0
    cur.execute("SELECT model, COUNT(*) FROM extraction_log GROUP BY model")
    by_model = cur.fetchall()

    conn.close()

    print(f"\n📊 Atomic Facts Stats")
    print(f"{'─' * 45}")
    print(f"  Chunk processati:      {total_chunks:>8,}")
    print(f"  Fatti totali:          {total_facts:>8,}")
    print(f"  Media fatti/chunk:     {avg:>8.1f}")
    print(f"  Fatti superati:        {superseded:>8,}")
    print(f"{'─' * 45}")
    for cat, count in by_category:
        print(f"  {cat or 'other':<20} {count:>8,}")
    if by_model:
        print(f"{'─' * 45}")
        for model, count in by_model:
            print(f"  [{model}] {count:,} chunk")
    print()


# ============================================================
# MAIN
# ============================================================

async def main():
    parser = argparse.ArgumentParser(description="TAILOR Atomic Fact Extraction")
    parser.add_argument("--full", action="store_true", help="Re-process everything")
    parser.add_argument("--test", type=int, default=0, help="Test su N chunk (no write)")
    parser.add_argument("--stats", action="store_true", help="Show statistics")
    parser.add_argument("--source", type=str, default="", help="Filter by source")
    parser.add_argument("--category", type=str, default="", help="Filter by category")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--backend", type=str, default=DEFAULT_BACKEND,
                        choices=["gemini", "openai", "ollama", "haiku"])
    args = parser.parse_args()

    if args.stats:
        show_stats()
        return

    if args.backend == "gemini" and not GOOGLE_API_KEY:
        print("❌ GOOGLE_API_KEY non trovata.")
        sys.exit(1)
    if args.backend == "haiku" and not ANTHROPIC_API_KEY:
        print("❌ ANTHROPIC_API_KEY non trovata.")
        sys.exit(1)
    if args.backend == "openai" and not OPENAI_API_KEY:
        print("❌ OPENAI_API_KEY non trovata.")
        sys.exit(1)

    model_label = {"gemini": f"Gemini 2.5 Flash ({GEMINI_MODEL})",
                   "openai": "GPT-4o-mini", "ollama": "Gemma 4 26B locale", "haiku": "Claude Haiku 4.5"}[args.backend]

    init_facts_db()
    extracted_set = get_extracted_chunks() if not args.full else set()

    print(f"🔍 Loading chunks...")
    chunks = load_chunks_to_process(args, extracted_set)

    if args.test:
        chunks = chunks[:args.test]

    total = len(chunks)
    if total == 0:
        print("✅ No chunks to process.")
        show_stats()
        return

    # Gemini paid tier: 2000 RPM. No cap on workers.

    print(f"📋 {total:,} chunk da processare (backend={model_label}, workers={args.workers})")

    by_cat = {}
    for c in chunks:
        cat = c["category"] or "other"
        by_cat[cat] = by_cat.get(cat, 0) + 1
    for cat, count in sorted(by_cat.items(), key=lambda x: -x[1])[:5]:
        print(f"  {cat}: {count:,}")

    facts_conn = sqlite3.connect(FACTS_DB_PATH) if not args.test else None
    # Ollama is sequential
    worker_count = args.workers if args.backend != "ollama" else 1
    semaphore = asyncio.Semaphore(worker_count)

    processed = 0
    total_facts = 0
    errors = 0
    start_time = time.time()
    now_str = datetime.now().isoformat()

    async with aiohttp.ClientSession() as session:
        BATCH_SIZE = 50 if args.backend != "ollama" else 5
        for batch_start in range(0, total, BATCH_SIZE):
            batch = chunks[batch_start:batch_start + BATCH_SIZE]

            tasks = [process_chunk(c, session, semaphore, args.backend) for c in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for chunk, result in zip(batch, results):
                if isinstance(result, Exception) or result is None:
                    errors += 1
                    processed += 1
                    continue

                n_facts = len(result)
                total_facts += n_facts

                if n_facts > 0:
                    sample = result[0]["fact"][:60]
                    print(f"  [{processed+1}/{total}] {chunk['id'][:40]}: {n_facts} facts — \"{sample}...\"")

                if facts_conn and not args.test:
                    cur = facts_conn.cursor()
                    for f in result:
                        cur.execute("""
                            INSERT INTO facts (chunk_id, fact, category, entity_tags, event_date, confidence, created_at, document_date)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (chunk["id"], f["fact"], f["category"], f["entity_tags"],
                              f["event_date"], f["confidence"], now_str, chunk.get("date", "")))
                    cur.execute("""
                        INSERT OR REPLACE INTO extraction_log (chunk_id, facts_count, model, extracted_at)
                        VALUES (?, ?, ?, ?)
                    """, (chunk["id"], n_facts, args.backend, now_str))

                processed += 1

            if facts_conn:
                facts_conn.commit()

            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 0
            eta = (total - processed) / rate if rate > 0 else 0
            print(f"  [{processed:,}/{total:,}] {rate:.1f} chunk/s | "
                  f"Facts={total_facts:,} ERR={errors} | ETA {eta/60:.0f}m")

    if facts_conn:
        facts_conn.commit()
        facts_conn.close()

    elapsed = time.time() - start_time
    print(f"\n{'═' * 50}")
    print(f"✅ Completato in {elapsed/60:.1f} minuti")
    print(f"  Backend:           {model_label}")
    print(f"  Chunk processati:  {processed:,}")
    print(f"  Fatti estratti:    {total_facts:,}")
    print(f"  Media fatti/chunk: {total_facts/max(processed-errors,1):.1f}")
    print(f"  Errori:            {errors:,}")
    print(f"  Rate:              {processed/elapsed:.1f} chunk/s")
    if args.test:
        print(f"\n  ⚠️  TEST mode — no DB changes")
    else:
        show_stats()


if __name__ == "__main__":
    asyncio.run(main())
