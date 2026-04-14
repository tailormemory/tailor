#!/usr/bin/env python3
"""
TAILOR — 10e: Crea doc_summary per file che non ce l'hanno.
PREREQUISITO: il server MCP deve essere SPENTO.

Uso:
  python scripts/enrichment/create_missing_summaries.py                  # Crea summary mancanti
  python scripts/enrichment/create_missing_summaries.py --dry-run        # Show what it would do
  python scripts/enrichment/create_missing_summaries.py --stats          # Statistiche
  python scripts/enrichment/create_missing_summaries.py --limit 10       # Max N file
  python scripts/enrichment/create_missing_summaries.py --workers 2      # Parallelismo Haiku
"""

import os, sys, json, asyncio, aiohttp, sqlite3, time, requests, argparse
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "lib"))
from embedding import get_embedding, get_embeddings
DB_DIR = os.path.join(BASE_DIR, "db")
DB_PATH = os.path.join(DB_DIR, "chroma.sqlite3")
REGISTRY_FILE = os.path.join(DB_DIR, "doc_registry.json")
LOG_PATH = os.path.join(BASE_DIR, "logs", "create_summaries.log")

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from scripts.lib.config import get_enrichment

# User name from config (for prompt templates)
from config import get as cfg
_USER_NAME = cfg("user", "name") or "the user"
_USER_LANG = cfg("user", "language") or "en"

# Cloud sync local root (for relative path display)
_cs = cfg("cloud_sync") or {}
if isinstance(_cs, list):
    CLOUD_LOCAL_ROOT = _cs[0].get("local_root", "") if _cs else ""
else:
    CLOUD_LOCAL_ROOT = _cs.get("local_root", "")
_sum_cfg = get_enrichment("summaries")
MODEL = _sum_cfg["model"]

COLLECTION_NAME = cfg("kb", "collection") or "tailor_kb"

MAX_INPUT_CHARS = 8000
MAX_WORKERS = 2
SUMMARY_MAX_TOKENS = 500

PROMPT_BASE = """Generate a structured summary for {user_name}'s personal Knowledge Base.

File: {filename}
Folder: {folder}
Document type: {doc_type}

{type_instructions}

RULES:
- 3-8 sentences, max 800 characters
- Include ALL numbers, amounts, dates and proper nouns you find
- Include keywords a user would search for to find this document
- No bullet points, no markdown, just flowing text
- If the document is fragmentary, extract all available data anyway
- Respond in {language}

Document text:
{text}"""

TYPE_INSTRUCTIONS = {
    "bilancio": "This is a BALANCE SHEET. Mandatory extraction: company and year/period, total revenue, total costs and main items, net profit or loss, share capital, equity, any financial KPIs.",
    "atto_legale": "This is a LEGAL DOCUMENT. Extract: proceeding type, parties involved, legal articles, outcome, amounts, key dates.",
    "contratto": "This is a CONTRACT. Extract: contracting parties, subject, amount/consideration, duration and dates, relevant clauses.",
    "fattura": "This is an INVOICE. Extract: issuer and recipient, number and date, total amount, description of goods/services.",
    "estratto_conto": "This is a BANK STATEMENT. Extract: bank, account holder, period, opening and closing balance, main transactions.",
    "dichiarazione_fiscale": "This is a TAX RETURN. Extract: type and tax year, taxpayer, declared income, taxes.",
    "relazione_gestione": "This is a MANAGEMENT REPORT. Extract: company and year, economic performance, relevant facts.",
    "polizza_assicurativa": "This is an INSURANCE POLICY. Extract: company, policy number, insured, coverage, limits, premium, expiry.",
}
DEFAULT_INSTRUCTIONS = "Estrai le informazioni chiave: di cosa tratta, persone/aziende coinvolte, date e importi rilevanti."


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")


# get_embedding provided by scripts/lib/embedding.py (imported above)


def find_missing_summaries():
    with open(REGISTRY_FILE) as f:
        registry = json.load(f)
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    cur = conn.cursor()
    cur.execute("""
        SELECT t.string_value, f.string_value
        FROM embedding_metadata c
        JOIN embedding_metadata t ON c.id = t.id AND t.key = 'title'
        JOIN embedding_metadata f ON c.id = f.id AND f.key = 'folder'
        WHERE c.key = 'category' AND c.string_value = 'doc_summary'
    """)
    existing = set((title, folder) for title, folder in cur.fetchall())
    conn.close()

    log(f"Registry: {len(registry)} file | Existing summaries: {len(existing)}")
    missing = []
    for path, info in registry.items():
        if info.get("chunks", 0) == 0:
            continue
        filename = os.path.basename(path)
        rel = path.replace(CLOUD_LOCAL_ROOT + "/", "") if CLOUD_LOCAL_ROOT else path
        folder = rel.split("/")[0] if "/" in rel else "Unknown"
        if (filename, folder) not in existing:
            missing.append({"path": path, "filename": filename, "folder": folder, "chunks": info.get("chunks", 0)})
    return missing


def get_file_text_and_metadata(file_path, filename, folder):
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    cur = conn.cursor()
    rel_path = file_path.replace(CLOUD_LOCAL_ROOT + "/", "") if CLOUD_LOCAL_ROOT else path
    cur.execute("SELECT e.id FROM embedding_metadata e WHERE e.key = 'file_path' AND e.string_value = ? ORDER BY e.id ASC", (rel_path,))
    chunk_ids = [r[0] for r in cur.fetchall()]
    if not chunk_ids:
        cur.execute("""
            SELECT t.id FROM embedding_metadata t
            JOIN embedding_metadata f ON t.id = f.id AND f.key = 'folder' AND f.string_value = ?
            JOIN embedding_metadata s ON t.id = s.id AND s.key = 'source' AND s.string_value = 'document'
            WHERE t.key = 'title' AND t.string_value = ?
            AND t.id NOT IN (SELECT c.id FROM embedding_metadata c WHERE c.key = 'category' AND c.string_value = 'doc_summary')
            ORDER BY t.id ASC
        """, (folder, filename))
        chunk_ids = [r[0] for r in cur.fetchall()]

    text_parts = []
    total = 0
    for cid in chunk_ids[:8]:
        cur.execute("SELECT c0 FROM embedding_fulltext_search_content WHERE id = ?", (cid,))
        r = cur.fetchone()
        if r and r[0]:
            text_parts.append(r[0])
            total += len(r[0])
            if total >= MAX_INPUT_CHARS:
                break

    doc_type = date = file_type = ""
    if chunk_ids:
        for key in ["doc_type", "date", "file_type"]:
            cur.execute("SELECT string_value FROM embedding_metadata WHERE id = ? AND key = ?", (chunk_ids[0], key))
            r = cur.fetchone()
            if r and r[0]:
                if key == "doc_type": doc_type = r[0]
                elif key == "date": date = r[0]
                elif key == "file_type": file_type = r[0]
    conn.close()
    return {"text": "\n---\n".join(text_parts)[:MAX_INPUT_CHARS], "doc_type": doc_type, "date": date, "file_type": file_type}


async def generate_summary_api(session, filename, folder, doc_type, text, semaphore):
    async with semaphore:
        prompt = PROMPT_BASE.format(user_name=_USER_NAME, language=_USER_LANG, 
            filename=filename, folder=folder, doc_type=doc_type,
            type_instructions=TYPE_INSTRUCTIONS.get(doc_type, DEFAULT_INSTRUCTIONS),
            text=text,
        )
        try:
            async with session.post(
                ANTHROPIC_URL,
                json={"model": MODEL, "max_tokens": SUMMARY_MAX_TOKENS, "messages": [{"role": "user", "content": prompt}]},
                headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status == 429:
                    await asyncio.sleep(15)
                    return None, "rate_limit"
                data = await resp.json()
                if resp.status != 200:
                    return None, data.get("error", {}).get("message", str(resp.status))
                return data["content"][0]["text"].strip(), None
        except Exception as e:
            return None, str(e)


def create_summary_chunk(col, filename, folder, doc_type, date, file_type, file_path, summary):
    summary_id = f"doc_summary_{abs(hash(filename + folder + str(time.time()))) % 10**10}"
    content = f"{filename} \u2014 {folder} \u2014 {doc_type}\n\n"
    content += f"[RIASSUNTO DOCUMENTO] {filename}\n"
    content += f"Cartella: {folder} | Tipo: {doc_type} | File: {file_type}\n\n"
    content += summary
    embedding = get_embedding(content)
    rel_path = file_path.replace(CLOUD_LOCAL_ROOT + "/", "") if CLOUD_LOCAL_ROOT else path
    metadata = {
        "source": "document", "title": filename, "folder": folder, "doc_type": doc_type,
        "file_path": rel_path, "file_type": file_type,
        "date": date or datetime.now().strftime("%Y-%m-%d"),
        "category": "doc_summary", "chunk_index": -1, "char_count": len(content),
        "conv_id": f"doc_summary_{datetime.now().strftime('%Y%m%d')}",
        "create_time": time.time(),
    }
    col.add(ids=[summary_id], documents=[content], embeddings=[embedding], metadatas=[metadata])
    return summary_id


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--stats", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    args = parser.parse_args()

    if not ANTHROPIC_API_KEY and not args.stats and not args.dry_run:
        log("ERROR: ANTHROPIC_API_KEY non impostata")
        sys.exit(1)

    missing = find_missing_summaries()
    log(f"Files without doc_summary: {len(missing)}")

    if args.stats:
        by_folder = {}
        for m in missing:
            by_folder[m["folder"]] = by_folder.get(m["folder"], 0) + 1
        for folder, count in sorted(by_folder.items(), key=lambda x: -x[1]):
            print(f"  {count:4d}  {folder}")
        return

    if not missing:
        log("No missing summaries!")
        return

    if args.limit:
        missing = missing[:args.limit]

    if args.dry_run:
        for m in missing[:30]:
            print(f"  [NEW] {m['folder']} | {m['filename']} ({m['chunks']} chunks)")
        if len(missing) > 30:
            print(f"  ... e altri {len(missing) - 30}")
        return

    import chromadb
    client = chromadb.PersistentClient(path=DB_DIR)
    col = client.get_collection(COLLECTION_NAME)
    semaphore = asyncio.Semaphore(args.workers)
    created = errors = 0

    async with aiohttp.ClientSession() as session:
        for i, m in enumerate(missing):
            info = get_file_text_and_metadata(m["path"], m["filename"], m["folder"])
            if not info["text"].strip():
                log(f"[{i+1}/{len(missing)}] SKIP {m['filename']} - no text")
                continue
            summary, error = await generate_summary_api(session, m["filename"], m["folder"], info["doc_type"], info["text"], semaphore)
            if error:
                if error == "rate_limit":
                    log(f"[{i+1}/{len(missing)}] RATE LIMIT, retry...")
                    await asyncio.sleep(15)
                    summary, error = await generate_summary_api(session, m["filename"], m["folder"], info["doc_type"], info["text"], semaphore)
                if error:
                    log(f"[{i+1}/{len(missing)}] ERROR {m['filename']}: {error}")
                    errors += 1
                    continue
            try:
                sid = create_summary_chunk(col, m["filename"], m["folder"], info["doc_type"], info["date"], info["file_type"], m["path"], summary)
                created += 1
                if created % 10 == 0 or created <= 3:
                    log(f"[{i+1}/{len(missing)}] OK {m['filename']}")
            except Exception as e:
                log(f"[{i+1}/{len(missing)}] ERROR chunk {m['filename']}: {e}")
                errors += 1

    del client
    log(f"DONE: {created} summary creati, {errors} errori")

if __name__ == "__main__":
    asyncio.run(main())
