"""
TAILOR — 17b: Fact Extraction Nightly Runner (multi-backend rotation)
Processes chunks in daily batches, rotating between Gemini and OpenAI to stay within rate limits.

Logica:
  1. Try Gemini 2.5 Flash (cheapest)
  2. When Gemini returns 429, switches to OpenAI GPT-4o-mini
  3. When OpenAI returns 429, switches to Anthropic Haiku (fast, generous limits)
  4. When all backends exhausted, stops
  5. Next night resumes — incremental

Integrated in the nightly pipeline as step 5e.

Uso:
  python 17b_extract_facts_nightly.py              # Run notturno (max 20k chunk)
  python 17b_extract_facts_nightly.py --limit 5000 # Limita a N chunk
  python 17b_extract_facts_nightly.py --test 10    # Test mode (no write)
"""

import json
import re
import os
import sys
import time
import asyncio
import sqlite3
import argparse
import subprocess
import aiohttp
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.lib.config import get_enrichment_backends, get_enrichment_daily_limit
from scripts.lib.config import get as cfg
_USER_LANG = cfg("user", "language") or "en"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_DIR = os.path.join(BASE_DIR, "db")
CHROMA_DB_PATH = os.path.join(DB_DIR, "chroma.sqlite3")
FACTS_DB_PATH = os.path.join(DB_DIR, "facts.sqlite3")

# Read backends and daily limit from config (supports old and new YAML format)
_BACKENDS_CFG = get_enrichment_backends("fact_extraction")
DAILY_LIMIT_PER_BACKEND = get_enrichment_daily_limit("fact_extraction")

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

# API keys — loaded dynamically based on which providers are in config
_KEY_NAMES = {"google": "GOOGLE_API_KEY", "openai": "OPENAI_API_KEY", "anthropic": "ANTHROPIC_API_KEY"}
_API_KEYS = {}
for _prov in set(b["provider"] for b in _BACKENDS_CFG):
    _kname = _KEY_NAMES.get(_prov)
    if _kname:
        _API_KEYS[_prov] = get_key(_kname)

EXTRACTION_PROMPT = """Extract atomic facts from this knowledge base chunk. Each fact should be a single, self-contained statement that could be independently verified or updated.

Text (date: {date}, source: {source}):
{text}

Respond with ONLY a JSON array. No other text, no markdown, no explanation.
[
  {{"fact": "concise standalone statement", "category": "category", "entity_tags": ["entity1"], "event_date": "YYYY-MM-DD or empty string"}},
  ...
]

Categories: decision, preference, status, relationship, financial, event, technical, health, personal, legal

Rules:
- Extract ONLY factual statements, not filler or meta-info
- Each fact must make sense on its own
- Include entity names in entity_tags
- event_date: date the fact refers to, NOT the document date. Empty if not inferable.
- Aim for 3-15 facts per chunk. Return [] for noise/filler.
- Write facts in {language}."""


def _parse_facts_json(content):
    content = content.replace("```json", "").replace("```", "").strip()
    start = content.find("[")
    end = content.rfind("]") + 1
    if start >= 0 and end > start:
        parsed = json.loads(content[start:end])
        if isinstance(parsed, list):
            return parsed
    start = content.find("{")
    end = content.rfind("}") + 1
    if start >= 0 and end > start:
        parsed = json.loads(content[start:end])
        if isinstance(parsed, dict) and "facts" in parsed:
            return parsed["facts"]
    return None


# ============================================================
# BACKEND CALLERS
# ============================================================

class BackendManager:
    """Gestisce la rotazione tra backend con tracking dei rate limit."""

    def __init__(self):
        self.backends = []
        for bcfg in _BACKENDS_CFG:
            provider = bcfg["provider"]
            model = bcfg["model"]
            api_key = _API_KEYS.get(provider, "")
            # Ollama doesn't need an API key
            if provider == "ollama" or api_key:
                self.backends.append({
                    "name": provider, "model": model, "calls": 0,
                    "limit": DAILY_LIMIT_PER_BACKEND, "exhausted": False,
                    "workers": bcfg.get("workers", 5)
                })
            else:
                print(f"  ⚠️  Skipping {provider}/{model} — no API key found")
        self.current_idx = 0

    def current(self):
        """Returns the current backend, or None if all exhausted."""
        for _ in range(len(self.backends)):
            b = self.backends[self.current_idx]
            if not b["exhausted"]:
                return b
            self.current_idx = (self.current_idx + 1) % len(self.backends)
        return None

    def mark_success(self):
        b = self.backends[self.current_idx]
        b["calls"] += 1
        if b["calls"] >= b["limit"]:
            b["exhausted"] = True
            print(f"  ⚠️  {b['name']} ha raggiunto il limite di {b['limit']} call. Switching...")
            self.current_idx = (self.current_idx + 1) % len(self.backends)

    def mark_rate_limited(self):
        b = self.backends[self.current_idx]
        b["exhausted"] = True
        print(f"  ⚠️  {b['name']} rate limited (429). Switching...")
        self.current_idx = (self.current_idx + 1) % len(self.backends)

    def all_exhausted(self):
        return all(b["exhausted"] for b in self.backends)

    def status(self):
        parts = []
        for b in self.backends:
            status = "✅" if not b["exhausted"] else "❌"
            parts.append(f"{b['name']}:{b['calls']}/{b['limit']}{status}")
        return " | ".join(parts)

    def current_workers(self):
        """Return optimal worker count for the current backend."""
        b = self.current()
        return b["workers"] if b else 5


async def call_gemini(session, semaphore, prompt, model, api_key):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.1, "maxOutputTokens": 4000,
                             "thinkingConfig": {"thinkingBudget": 0}}
    }
    async with semaphore:
        for attempt in range(3):
            try:
                async with session.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                    if resp.status == 429:
                        return "RATE_LIMITED"
                    if resp.status == 503:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    content = data["candidates"][0]["content"]["parts"][0]["text"].strip()
                    return _parse_facts_json(content)
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return None
        return None


async def call_openai(session, semaphore, prompt, model, api_key):
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model, "max_tokens": 1500, "temperature": 0.1,
        "messages": [{"role": "user", "content": prompt}],
        "response_format": {"type": "json_object"}
    }
    async with semaphore:
        try:
            async with session.post("https://api.openai.com/v1/chat/completions", json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status == 429:
                    return "RATE_LIMITED"
                if resp.status != 200:
                    return None
                data = await resp.json()
                content = data["choices"][0]["message"]["content"].strip()
                return _parse_facts_json(content)
        except Exception:
            return None


async def call_anthropic(session, semaphore, prompt, model, api_key):
    headers = {
        "x-api-key": api_key,
        "content-type": "application/json",
        "anthropic-version": "2023-06-01"
    }
    payload = {
        "model": model,
        "max_tokens": 1500,
        "temperature": 0.1,
        "messages": [{"role": "user", "content": prompt}]
    }
    async with semaphore:
        try:
            async with session.post("https://api.anthropic.com/v1/messages", json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status == 429:
                    return "RATE_LIMITED"
                if resp.status != 200:
                    return None
                data = await resp.json()
                content = data["content"][0]["text"].strip()
                return _parse_facts_json(content)
        except Exception:
            return None


# ============================================================
# CHUNK LOADING
# ============================================================

# Regex for detecting code-heavy lines
_CODE_LINE_RE = re.compile(
    r'<[a-z/][^>]*>|[{}];|class="|style="|function\s|const\s|let\s|var\s'
    r'|import\s.+from|def\s|return\s|console\.|addEventListener|querySelector'
    r'|getElementById|onClick|onChange|padding:|margin:|font-|border:|display:'
    r'|background:|width:|height:|\.(css|js|py|html)$'
)

def is_code_heavy(text, threshold=0.6):
    """Returns True if >threshold of lines look like code/markup."""
    lines = text.split('\n')
    if len(lines) < 3:
        return False
    code_lines = sum(1 for l in lines if _CODE_LINE_RE.search(l))
    return (code_lines / len(lines)) > threshold


def load_chunks(limit=20000):
    # Already-extracted chunks
    extracted = set()
    if os.path.exists(FACTS_DB_PATH):
        conn = sqlite3.connect(f"file:{FACTS_DB_PATH}?mode=ro", uri=True)
        for r in conn.execute("SELECT chunk_id FROM extraction_log"):
            extracted.add(r[0])
        conn.close()

    conn = sqlite3.connect(f"file:{CHROMA_DB_PATH}?mode=ro", uri=True)
    cur = conn.cursor()
    cur.execute("""
        SELECT e.embedding_id, fts.c0,
               MAX(CASE WHEN m.key='date' THEN m.string_value END),
               MAX(CASE WHEN m.key='source' THEN m.string_value END)
        FROM embeddings e
        JOIN embedding_fulltext_search_content fts ON fts.id = e.id
        JOIN embedding_metadata m ON m.id = e.id
        WHERE m.key IN ('date','source')
        GROUP BY e.embedding_id
    """)
    chunks = []
    skipped_code = []
    for eid, text, date, source in cur.fetchall():
        if eid in extracted:
            continue
        if not text or len(text.strip()) < 50:
            continue
        # Skip pure code/markup chunks (>60% code lines — no useful facts)
        if is_code_heavy(text):
            skipped_code.append(eid)
            continue
        chunks.append({"id": eid, "text": text[:3000], "date": date or "", "source": source or ""})
        if len(chunks) >= limit:
            break
    conn.close()

    # Mark skipped code chunks in extraction_log so they're not retried
    if skipped_code and os.path.exists(FACTS_DB_PATH):
        fconn = sqlite3.connect(FACTS_DB_PATH, timeout=30)
        fconn.execute("PRAGMA journal_mode=WAL")
        now_str = datetime.now().isoformat()
        for cid in skipped_code:
            fconn.execute(
                "INSERT OR IGNORE INTO extraction_log (chunk_id, model, facts_count, extracted_at) VALUES (?, ?, ?, ?)",
                (cid, "skipped_code", 0, now_str)
            )
        fconn.commit()
        fconn.close()
        if skipped_code:
            print(f"  Skipped {len(skipped_code):,} code-heavy chunks (marked in extraction_log)")

    return chunks


# ============================================================
# MAIN
# ============================================================

async def call_backend(session, semaphore, prompt, backend):
    """Dispatch to the correct provider's API call."""
    provider = backend["name"]
    model = backend["model"]
    api_key = _API_KEYS.get(provider, "")
    if provider == "google":
        return await call_gemini(session, semaphore, prompt, model, api_key)
    elif provider == "anthropic":
        return await call_anthropic(session, semaphore, prompt, model, api_key)
    elif provider == "openai":
        return await call_openai(session, semaphore, prompt, model, api_key)
    elif provider == "ollama":
        return await call_ollama(session, semaphore, prompt, model)
    else:
        print(f"  ❌ Unknown provider: {provider}")
        return None


async def call_ollama(session, semaphore, prompt, model):
    """Call Ollama local API."""
    ollama_url = os.environ.get("OLLAMA_URL", cfg("ollama", "base_url") or "http://localhost:11434") + "/api/chat"
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
        "options": {"temperature": 0.1}
    }
    async with semaphore:
        try:
            async with session.post(ollama_url, json=payload, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                content = data.get("message", {}).get("content", "").strip()
                return _parse_facts_json(content)
        except Exception:
            return None


async def main():
    parser = argparse.ArgumentParser(description="TAILOR Nightly Fact Extraction (multi-backend)")
    parser.add_argument("--limit", type=int, default=20000, help="Max chunk da processare (default 20k)")
    parser.add_argument("--test", type=int, default=0, help="Test mode (no write)")
    parser.add_argument("--workers", type=int, default=0, help="Worker paralleli (0 = auto-tune per backend)")
    args = parser.parse_args()

    manager = BackendManager()
    if not manager.backends:
        print("❌ No backend available. Check enrichment.fact_extraction.backends in config/tailor.yaml")
        sys.exit(1)

    # Init facts DB
    if not os.path.exists(FACTS_DB_PATH):
        print("❌ facts.sqlite3 does not exist. Run 17_extract_facts.py --stats first")
        sys.exit(1)

    print(f"🔍 Loading chunks...")
    chunks = load_chunks(args.limit)
    if args.test:
        chunks = chunks[:args.test]

    total = len(chunks)
    if total == 0:
        print("✅ No chunks to process.")
        return

    print(f"📋 {total:,} chunk da processare")
    print(f"  Backend: {manager.status()}")

    facts_conn = None
    if not args.test:
        facts_conn = sqlite3.connect(FACTS_DB_PATH, timeout=30)
        facts_conn.execute("PRAGMA journal_mode=WAL")
        facts_conn.execute("PRAGMA busy_timeout=30000")
    current_worker_count = args.workers if args.workers > 0 else manager.current_workers()
    semaphore = asyncio.Semaphore(current_worker_count)
    print(f"  Workers: {current_worker_count} (auto-tuned for {manager.current()['name']})")
    processed = 0
    total_facts = 0
    errors = 0
    start_time = time.time()
    now_str = datetime.now().isoformat()

    async with aiohttp.ClientSession() as session:
        print(f'  Session created, starting loop on {len(chunks):,} chunks...', flush=True)
        for chunk in chunks:
            # Check if all backends are exhausted
            backend = manager.current()
            if backend is None:
                print(f"\n⚠️  All backends exhausted. Stopped at {processed:,} chunk.")
                break

            prompt = EXTRACTION_PROMPT.format(language=_USER_LANG, 
                date=chunk["date"], source=chunk["source"], text=chunk["text"]
            )

            # Call al backend corrente
            if processed == 0:
                print(f'  First call: {backend["name"]}/{backend["model"]}...', flush=True)
            result = await call_backend(session, semaphore, prompt, backend)
            if processed == 0:
                print(f'  First result: {type(result).__name__} = {str(result)[:80] if result else "None"}', flush=True)

            # Gestisci rate limit
            if result == "RATE_LIMITED":
                manager.mark_rate_limited()
                backend = manager.current()
                if backend and args.workers <= 0:
                    nw = manager.current_workers()
                    if nw != current_worker_count:
                        semaphore = asyncio.Semaphore(nw)
                        current_worker_count = nw
                        print(f"  Workers: {nw} (auto-tuned for {backend['name']})")
                if backend is None:
                    print(f"\n⚠️  All backends exhausted after rate limit. Stopped at {processed:,} chunk.")
                    break
                result = await call_backend(session, semaphore, prompt, backend)
                if result == "RATE_LIMITED":
                    manager.mark_rate_limited()
                    break

            if result is None or result == "RATE_LIMITED":
                errors += 1
                processed += 1
                continue

            manager.mark_success()

            # Valida fatti
            valid_facts = []
            for f in result:
                if not isinstance(f, dict):
                    continue
                fact_text = f.get("fact", "").strip()
                if not fact_text or len(fact_text) < 10:
                    continue
                valid_facts.append(f)

            n_facts = len(valid_facts)
            total_facts += n_facts

            # Salva
            if facts_conn:
                cur = facts_conn.cursor()
                backend_name = backend["name"]
                for f in valid_facts:
                    cur.execute("""
                        INSERT INTO facts (chunk_id, fact, category, entity_tags, event_date, confidence, created_at, document_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (chunk["id"], f["fact"][:500], f.get("category", "")[:50].lower(),
                          json.dumps(f.get("entity_tags", [])[:10]),
                          f.get("event_date", "") or "", 1.0, now_str, chunk.get("date", "")))
                cur.execute("""
                    INSERT OR REPLACE INTO extraction_log (chunk_id, facts_count, model, extracted_at)
                    VALUES (?, ?, ?, ?)
                """, (chunk["id"], n_facts, backend_name, now_str))

            processed += 1

            # Commit and log every 100
            if processed % 100 == 0:
                if facts_conn:
                    facts_conn.commit()
                elapsed = time.time() - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                remaining = total - processed
                eta = remaining / rate if rate > 0 else 0
                print(f"  [{processed:,}/{total:,}] {rate:.1f}/s | "
                      f"Facts={total_facts:,} ERR={errors} | "
                      f"{manager.status()} | ETA {eta/60:.0f}m")

    if facts_conn:
        facts_conn.commit()
        facts_conn.close()

    elapsed = time.time() - start_time
    print(f"\n{'═' * 55}")
    print(f"✅ Completato in {elapsed/60:.1f} minuti")
    print(f"  Chunk processati:  {processed:,}")
    print(f"  Fatti estratti:    {total_facts:,}")
    print(f"  Errori:            {errors:,}")
    print(f"  Backend: {manager.status()}")
    if args.test:
        print(f"  ⚠️  TEST mode — no changes")


if __name__ == "__main__":
    asyncio.run(main())
