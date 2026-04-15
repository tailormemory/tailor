"""
TAILOR — 5: Entity Extraction (v2 — parallel)
Extracts named entities from all ChromaDB chunks and saves them as metadata.

Extracted entities:
- person: nomi di persone
- company: companies, organizations
- project: progetti, prodotti, piattaforme
- amount: monetary amounts, percentages, significant numeric quantities
- location: places, cities, countries, addresses
- date_ref: riferimenti temporali espliciti

Funzionamento:
- Processes only chunks without "entities_extracted" field in metadata (incremental)
- For each chunk: call LLM → JSON → update metadata in ChromaDB
- File checkpoint every CHECKPOINT_EVERY chunks (resumable if interrupted)
- Does not modify text or embeddings — only adds metadata

Backend LLM (selezionato con --backend):
- openai: GPT-4o-mini via API OpenAI (default, PARALLELO con N worker)
- ollama: Qwen 2.5:7b locale via Ollama (sequenziale)

Uso:
  python scripts/enrichment/extract_entities.py                         # Incrementale con OpenAI (10 worker)
  python scripts/enrichment/extract_entities.py --workers 20            # 20 worker paralleli
  python scripts/enrichment/extract_entities.py --backend ollama        # Usa Ollama locale (sequenziale)
  python scripts/enrichment/extract_entities.py --full                  # Re-process all chunks
  python scripts/enrichment/extract_entities.py --test 20               # Prova su 20 chunk e mostra output
  python scripts/enrichment/extract_entities.py --stats                 # Show statistics for already-extracted entities
  python scripts/enrichment/extract_entities.py --source chatgpt        # Only chunks from a specific source

Requisiti:
  Per backend openai: pip install aiohttp, variabile OPENAI_API_KEY impostata
  Per backend ollama: Ollama con qwen2.5:7b installato
  ChromaDB con collection tailor_kb
"""

import json
import os
import sys
import time
import asyncio
import requests
import chromadb

# ============================================================
# CONFIGURAZIONE
# ============================================================

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

DB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "db")
COLLECTION_NAME = "tailor_kb"

# --- Backend Ollama ---
from scripts.lib.config import get as _cfg
OLLAMA_BASE = os.environ.get("OLLAMA_URL", _cfg("ollama", "base_url") or "http://localhost:11434")
OLLAMA_GENERATE_URL = OLLAMA_BASE + "/api/generate"
EXTRACT_MODEL_OLLAMA = "qwen2.5:7b"

# --- Backend OpenAI ---
OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"
from scripts.lib.config import get_enrichment, get_enrichment_fallback
_ee_cfg = get_enrichment("entity_extraction")
_ee_fb = get_enrichment_fallback("entity_extraction")
EXTRACT_MODEL_OPENAI = _ee_fb["model"] if _ee_fb and _ee_fb["provider"] == "openai" else "gpt-4o-mini"
EXTRACT_MODEL_ANTHROPIC = _ee_cfg["model"] if _ee_cfg["provider"] == "anthropic" else "claude-haiku-4-5-20251001" 
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

# --- Backend Anthropic ---
ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
# EXTRACT_MODEL_ANTHROPIC set above from config
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# --- Parametri comuni ---
ACTIVE_BACKEND = "openai"
CONCURRENT_WORKERS = 10

READ_BATCH_SIZE = 500
WRITE_BATCH_SIZE = 50
CHECKPOINT_EVERY = 200
CHECKPOINT_FILE = os.path.join(DB_DIR, "checkpoints", "entity_extraction_checkpoint.json")
LLM_TIMEOUT = 60
MAX_TEXT_FOR_EXTRACTION = 1500
ENTITY_TYPES = ["person", "company", "project", "amount", "location", "date_ref"]

# ============================================================
# PROMPT
# ============================================================

EXTRACTION_PROMPT = """Extract named entities from the following text.
Reply ONLY with a valid JSON object, nothing else. No text before or after.

Types to extract:
- person: people names (e.g. "John", "Marco Rossi")
- company: companies, organizations, brand (es. "Acme Corp", "Fineco", "Apple")
- project: projects, products, platforms, services (e.g. "StayChic", "TAILOR", "AdSense arbitrage")
- amount: monetary amounts, percentages, significant numeric quantities (es. "50.000 euro", "3%", "10k")
- location: places, cities, countries, addresses (es. "Roma", "Irlanda", "Via Crispi")
- date_ref: explicit time references (e.g. "Q3 2024", "March 2026", "2019")

Rules:
- Include only entities clearly present in the text, do not infer
- Normalize names (e.g. "AC" → "Acme Corp" only if explicitly explained in the text)
- For amount: include only significant figures, not generic numbers
- Maximum 20 entities per chunk
- If no relevant entities found: {"entities": [], "types": []}

Response format (pure JSON, no markdown):
{"entities": ["nome1", "nome2", "nome3"], "types": ["tipo1", "tipo2", "tipo3"]}

Text to analyze:
"""

# ============================================================
# CHECKPOINT
# ============================================================

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {"processed_ids": [], "last_update": ""}


def save_checkpoint(processed_ids):
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({
            "processed_ids": list(processed_ids),
            "last_update": time.strftime("%Y-%m-%d %H:%M:%S"),
            "count": len(processed_ids)
        }, f)


# ============================================================
# BACKEND CHECK
# ============================================================

def check_backend():
    if ACTIVE_BACKEND == "openai":
        if not OPENAI_API_KEY:
            print("ERROR: OPENAI_API_KEY not set.")
            print("Esegui: export OPENAI_API_KEY='sk-...'")
            sys.exit(1)
        try:
            r = requests.post(
                OPENAI_API_URL,
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={"model": EXTRACT_MODEL_OPENAI, "messages": [{"role": "user", "content": "ping"}], "max_tokens": 5},
                timeout=10
            )
            if r.status_code == 401:
                print("ERROR: OPENAI_API_KEY invalid.")
                sys.exit(1)
            print(f"OpenAI OK — modello {EXTRACT_MODEL_OPENAI}")
        except requests.ConnectionError:
            print("ERROR: cannot reach api.openai.com")
            sys.exit(1)

    elif ACTIVE_BACKEND == "anthropic":
        if not ANTHROPIC_API_KEY:
            print("ERROR: ANTHROPIC_API_KEY not set.")
            print("Esegui: export ANTHROPIC_API_KEY='sk-ant-...'")
            sys.exit(1)
        try:
            r = requests.post(
                ANTHROPIC_API_URL,
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={"model": EXTRACT_MODEL_ANTHROPIC, "max_tokens": 5,
                      "messages": [{"role": "user", "content": "ping"}]},
                timeout=10
            )
            if r.status_code == 401:
                print("ERROR: ANTHROPIC_API_KEY invalid.")
                sys.exit(1)
            print(f"Anthropic OK — modello {EXTRACT_MODEL_ANTHROPIC}")
        except requests.ConnectionError:
            print("ERROR: cannot reach api.anthropic.com")
            sys.exit(1)

    elif ACTIVE_BACKEND == "ollama":
        try:
            r = requests.get(OLLAMA_BASE + "/api/tags", timeout=5)
            r.raise_for_status()
            models = [m["name"] for m in r.json().get("models", [])]
            if not any(EXTRACT_MODEL_OLLAMA in m for m in models):
                print(f"ERROR: model '{EXTRACT_MODEL_OLLAMA}' not found.")
                sys.exit(1)
            print(f"Ollama OK — modello {EXTRACT_MODEL_OLLAMA}")
        except requests.ConnectionError:
            print("ERROR: Ollama not reachable.")
            sys.exit(1)


# ============================================================
# ENTITY PARSING (shared)
# ============================================================

def _parse_entity_response(raw_response):
    if raw_response.startswith("```"):
        lines = raw_response.split("\n")
        raw_response = "\n".join(l for l in lines if not l.startswith("```")).strip()

    try:
        parsed = json.loads(raw_response)
    except json.JSONDecodeError:
        return [], []

    entities = parsed.get("entities", [])
    types = parsed.get("types", [])

    if len(entities) != len(types):
        min_len = min(len(entities), len(types))
        entities = entities[:min_len]
        types = types[:min_len]

    valid_pairs = [
        (e, t) for e, t in zip(entities, types)
        if t in ENTITY_TYPES and isinstance(e, str) and e.strip()
    ]

    if not valid_pairs:
        return [], []

    entities_clean, types_clean = zip(*valid_pairs)
    return list(entities_clean), list(types_clean)


# ============================================================
# ASYNC OPENAI EXTRACTION (parallel)
# ============================================================

async def _extract_one_openai(session, semaphore, chunk_text):
    """Extract entities from a chunk via OpenAI, with semaphore for concurrency."""
    truncated = chunk_text[:MAX_TEXT_FOR_EXTRACTION]
    prompt_text = EXTRACTION_PROMPT + truncated

    async with semaphore:
        for attempt in range(3):  # retry su rate limit
            try:
                async with session.post(
                    OPENAI_API_URL,
                    headers={
                        "Authorization": f"Bearer {OPENAI_API_KEY}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": EXTRACT_MODEL_OPENAI,
                        "messages": [
                            {"role": "system", "content": "You are an entity extraction tool. Respond ONLY with valid JSON, nothing else."},
                            {"role": "user", "content": prompt_text}
                        ],
                        "temperature": 0.0,
                        "max_tokens": 512,
                        "response_format": {"type": "json_object"}
                    },
                    timeout=asyncio.timeout(LLM_TIMEOUT)
                ) as resp:
                    if resp.status == 429:
                        wait = min(2 ** attempt * 2, 30)
                        await asyncio.sleep(wait)
                        continue
                    if resp.status != 200:
                        return [], []
                    data = await resp.json()
                    raw = data["choices"][0]["message"]["content"].strip()
                    return _parse_entity_response(raw)

            except asyncio.TimeoutError:
                return None, None
            except Exception:
                return [], []

    return [], []


async def process_batch_openai(chunks_batch):
    """Process a batch of chunks in parallel with aiohttp."""
    import aiohttp

    semaphore = asyncio.Semaphore(CONCURRENT_WORKERS)
    connector = aiohttp.TCPConnector(limit=CONCURRENT_WORKERS + 5)

    if ACTIVE_BACKEND == "anthropic":
        classify_fn = _extract_one_anthropic
    else:
        classify_fn = _extract_one_openai

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            classify_fn(session, semaphore, chunk["text"])
            for chunk in chunks_batch
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    parsed_results = []
    for r in results:
        if isinstance(r, Exception):
            parsed_results.append(([], []))
        elif r[0] is None:
            parsed_results.append(([], []))  # timeout
        else:
            parsed_results.append(r)

    return parsed_results


async def _extract_one_anthropic(session, semaphore, chunk_text):
    """Extract entities from a chunk via Claude Haiku."""
    import aiohttp
    truncated = chunk_text[:MAX_TEXT_FOR_EXTRACTION]
    prompt_text = EXTRACTION_PROMPT + truncated

    async with semaphore:
        for attempt in range(3):
            try:
                async with session.post(
                    ANTHROPIC_API_URL,
                    headers={
                        "x-api-key": ANTHROPIC_API_KEY,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json={
                        "model": EXTRACT_MODEL_ANTHROPIC,
                        "max_tokens": 512,
                        "system": "You are an entity extraction tool. Respond ONLY with valid JSON, nothing else.",
                        "messages": [
                            {"role": "user", "content": prompt_text}
                        ],
                    },
                    timeout=aiohttp.ClientTimeout(total=LLM_TIMEOUT)
                ) as resp:
                    if resp.status == 429:
                        wait = min(2 ** attempt * 5, 60)
                        await asyncio.sleep(wait)
                        continue
                    if resp.status == 529:
                        wait = min(2 ** attempt * 10, 120)
                        await asyncio.sleep(wait)
                        continue
                    if resp.status != 200:
                        return [], []
                    data = await resp.json()
                    raw = data["content"][0]["text"].strip()
                    # Strip markdown wrapper
                    if raw.startswith("```"):
                        lines = raw.split("\n")
                        raw = "\n".join(l for l in lines if not l.startswith("```")).strip()
                    return _parse_entity_response(raw)

            except asyncio.TimeoutError:
                return None, None
            except Exception:
                return [], []

    return [], []


# ============================================================
# SEQUENTIAL OLLAMA EXTRACTION
# ============================================================

def extract_entities_ollama(text):
    truncated = text[:MAX_TEXT_FOR_EXTRACTION]
    prompt_text = EXTRACTION_PROMPT + truncated
    try:
        r = requests.post(
            OLLAMA_GENERATE_URL,
            json={
                "model": EXTRACT_MODEL_OLLAMA, "prompt": prompt_text, "stream": False,
                "options": {"temperature": 0.0, "top_p": 1.0, "num_predict": 512}
            },
            timeout=LLM_TIMEOUT
        )
        r.raise_for_status()
        return _parse_entity_response(r.json().get("response", "").strip())
    except requests.exceptions.Timeout:
        return None, None
    except Exception:
        return [], []


# ============================================================
# CHROMADB HELPERS
# ============================================================

def get_chunks_to_process(collection, full_mode=False, source_filter="", limit=None):
    print("Reading chunks from ChromaDB...")
    total = collection.count()
    print(f"  Total chunks in KB: {total:,}")

    all_ids, all_docs, all_metas, all_embeddings = [], [], [], []

    offset = 0
    while offset < total:
        batch = collection.get(include=["documents", "metadatas", "embeddings"], limit=READ_BATCH_SIZE, offset=offset)
        if not batch["ids"]:
            break
        all_ids.extend(batch["ids"])
        all_docs.extend(batch["documents"])
        all_metas.extend(batch["metadatas"])
        all_embeddings.extend(batch["embeddings"])
        offset += len(batch["ids"])
        print(f"  Read {offset:,}/{total:,} chunks...", end="\r")

    print(f"\n  Read {len(all_ids):,} chunks total")

    to_process = []
    already_done = 0
    for chunk_id, doc, meta, emb in zip(all_ids, all_docs, all_metas, all_embeddings):
        if source_filter and meta.get("source", "") != source_filter:
            continue
        if not full_mode and meta.get("entities_extracted", 0) == 1:
            already_done += 1
            continue
        to_process.append({"id": chunk_id, "text": doc, "metadata": meta, "embedding": emb})

    print(f"  Already processed: {already_done:,}")
    print(f"  Da processare: {len(to_process):,}")
    if limit:
        to_process = to_process[:limit]
        print(f"  (limitato a {limit} per test)")
    return to_process


def update_chunk_metadata(existing_metadata, entities, types):
    updated_meta = dict(existing_metadata)
    updated_meta["entities"] = json.dumps(entities, ensure_ascii=False)
    updated_meta["entity_types"] = json.dumps(types, ensure_ascii=False)
    updated_meta["entities_extracted"] = 1
    updated_meta["entities_count"] = len(entities)
    return updated_meta


# ============================================================
# STATS MODE
# ============================================================

def show_stats(collection):
    print("Computing entity statistics...")
    total = collection.count()
    all_metas = []
    offset = 0
    while offset < total:
        batch = collection.get(include=["metadatas"], limit=READ_BATCH_SIZE, offset=offset)
        if not batch["ids"]:
            break
        all_metas.extend(batch["metadatas"])
        offset += len(batch["ids"])

    processed = sum(1 for m in all_metas if m.get("entities_extracted", 0) == 1)
    entity_counts, type_counts, total_entities = {}, {}, 0
    for meta in all_metas:
        if meta.get("entities_extracted", 0) != 1:
            continue
        try:
            entities = json.loads(meta.get("entities", "[]"))
            types = json.loads(meta.get("entity_types", "[]"))
            for e, t in zip(entities, types):
                entity_counts[e] = entity_counts.get(e, 0) + 1
                type_counts[t] = type_counts.get(t, 0) + 1
                total_entities += 1
        except Exception:
            pass

    print(f"\n=== Entity Extraction Stats ===")
    print(f"Total chunks:      {total:,}")
    print(f"Processed:         {processed:,} ({processed/total*100:.1f}%)")
    print(f"Not processed:     {total - processed:,}")
    print(f"Total entities:    {total_entities:,}")
    print(f"\nBy type:")
    for t in ENTITY_TYPES:
        print(f"  {t:<12}: {type_counts.get(t, 0):,}")
    print(f"\nMost frequent entities (top 20):")
    for entity, cnt in sorted(entity_counts.items(), key=lambda x: -x[1])[:20]:
        print(f"  {cnt:>5}x  {entity}")


# ============================================================
# MAIN — PARALLEL (OpenAI) or SEQUENTIAL (Ollama)
# ============================================================

def main():
    global ACTIVE_BACKEND, CONCURRENT_WORKERS

    full_mode = "--full" in sys.argv
    stats_mode = "--stats" in sys.argv
    test_limit = None
    source_filter = ""

    if "--backend" in sys.argv:
        idx = sys.argv.index("--backend")
        try:
            b = sys.argv[idx + 1]
            if b in ("openai", "ollama", "anthropic", "haiku", "claude"):
                if b in ("haiku", "claude"):
                    b = "anthropic"
                ACTIVE_BACKEND = b
            else:
                print(f"ERROR: backend '{b}' invalid."); sys.exit(1)
        except IndexError:
            print("ERROR: specify a backend."); sys.exit(1)

    if "--workers" in sys.argv:
        idx = sys.argv.index("--workers")
        try:
            CONCURRENT_WORKERS = int(sys.argv[idx + 1])
        except (IndexError, ValueError):
            CONCURRENT_WORKERS = 10

    if "--test" in sys.argv:
        idx = sys.argv.index("--test")
        try:
            test_limit = int(sys.argv[idx + 1])
        except (IndexError, ValueError):
            test_limit = 20
        print(f"TEST MODE: processing {test_limit} chunk")

    if "--source" in sys.argv:
        idx = sys.argv.index("--source")
        try:
            source_filter = sys.argv[idx + 1]
        except IndexError:
            pass

    os.makedirs(DB_DIR, exist_ok=True)
    client = chromadb.PersistentClient(path=DB_DIR)
    collection = client.get_collection(name=COLLECTION_NAME)

    if stats_mode:
        show_stats(collection)
        return

    print(f"Backend: {ACTIVE_BACKEND}" + (f" | Workers: {CONCURRENT_WORKERS}" if ACTIVE_BACKEND in ("openai", "anthropic") else ""))
    check_backend()

    chunks = get_chunks_to_process(collection, full_mode=full_mode, source_filter=source_filter, limit=test_limit)
    if not chunks:
        print("\nNo chunks to process. Use --full to re-process everything.")
        return

    total = len(chunks)
    model_name = EXTRACT_MODEL_OPENAI if ACTIVE_BACKEND == "openai" else (EXTRACT_MODEL_ANTHROPIC if ACTIVE_BACKEND == "anthropic" else EXTRACT_MODEL_OLLAMA)
    print(f"\nStarting entity extraction on {total:,} chunks...")
    print(f"Backend: {ACTIVE_BACKEND} | Model: {model_name}")
    if ACTIVE_BACKEND in ("openai", "anthropic"):
        est_cost = total * 500 / 1_000_000 * 0.75
        print(f"Estimated cost: ~${est_cost:.2f} | Parallel workers: {CONCURRENT_WORKERS}\n")
    else:
        print(f"Estimated time: {total * 3 // 60} - {total * 5 // 60} minutes\n")

    checkpoint = load_checkpoint()
    already_processed_ids = set(checkpoint.get("processed_ids", []))

    # Filter chunks already in checkpoint
    remaining = [c for c in chunks if c["id"] not in already_processed_ids or full_mode]
    skipped_checkpoint = len(chunks) - len(remaining)
    if skipped_checkpoint > 0:
        print(f"Skipped from checkpoint: {skipped_checkpoint:,}")
    chunks = remaining
    total = len(chunks)

    if total == 0:
        print("All chunks are already in the checkpoint.")
        return

    processed = 0
    errors = 0
    timeouts = 0
    start_time = time.time()

    # ---- PARALLEL MODE (OpenAI / Anthropic) ----
    if ACTIVE_BACKEND in ("openai", "anthropic"):
        PARALLEL_BATCH = CONCURRENT_WORKERS * 2  # processa 20 chunk per batch async

        for batch_start in range(0, total, PARALLEL_BATCH):
            batch_chunks = chunks[batch_start:batch_start + PARALLEL_BATCH]

            # Chiama OpenAI in parallelo
            batch_results = asyncio.run(process_batch_openai(batch_chunks))

            # Process results and write to ChromaDB
            write_buffer = []
            for chunk, (entities, types) in zip(batch_chunks, batch_results):
                if entities is None:
                    timeouts += 1
                    entities, types = [], []

                updated_meta = update_chunk_metadata(chunk["metadata"], entities, types)

                write_buffer.append({
                    "id": chunk["id"],
                    "metadata": updated_meta,
                    "document": chunk["text"],
                    "embedding": chunk["embedding"]
                })

                processed += 1
                already_processed_ids.add(chunk["id"])

                if test_limit:
                    ents = json.loads(updated_meta.get("entities", "[]"))
                    typs = json.loads(updated_meta.get("entity_types", "[]"))
                    print(f"\n--- Chunk ---")
                    print(f"ID: {chunk['id']}")
                    print(f"Source: {chunk['metadata'].get('source', 'N/A')} | Title: {chunk['metadata'].get('title', 'N/A')[:60]}")
                    print(f"Text: {chunk['text'][:200]}")
                    print(f"Entities ({len(ents)}):")
                    for e, t in zip(ents, typs):
                        print(f"  [{t}] {e}")

            # Write batch to ChromaDB
            if write_buffer:
                try:
                    collection.upsert(
                        ids=[c["id"] for c in write_buffer],
                        embeddings=[c["embedding"] for c in write_buffer],
                        documents=[c["document"] for c in write_buffer],
                        metadatas=[c["metadata"] for c in write_buffer]
                    )
                except Exception as e:
                    print(f"\nERRORE scrittura ChromaDB: {e}")
                    errors += len(write_buffer)

            # Checkpoint
            if processed % CHECKPOINT_EVERY < PARALLEL_BATCH:
                save_checkpoint(already_processed_ids)

            # Progresso
            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 0
            eta_min = int((total - batch_start - len(batch_chunks)) / rate / 60) if rate > 0 else 0
            print(
                f"[{batch_start + len(batch_chunks):,}/{total:,}] "
                f"processati={processed:,} errori={errors} timeout={timeouts} | "
                f"{rate:.1f} chunk/s | ETA ~{eta_min}min"
            )

    # ---- SEQUENTIAL MODE (Ollama) ----
    else:
        write_buffer = []
        for i, chunk in enumerate(chunks):
            if i % 50 == 0 and i > 0:
                elapsed = time.time() - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                eta_min = int((total - i) / rate / 60) if rate > 0 else 0
                print(
                    f"[{i:,}/{total:,}] processati={processed:,} errori={errors} "
                    f"timeout={timeouts} | {rate:.1f} chunk/s | ETA ~{eta_min}min"
                )

            entities, types = extract_entities_ollama(chunk["text"])
            if entities is None:
                timeouts += 1
                entities, types = [], []

            updated_meta = update_chunk_metadata(chunk["metadata"], entities, types)
            write_buffer.append({
                "id": chunk["id"], "metadata": updated_meta,
                "document": chunk["text"], "embedding": chunk["embedding"]
            })
            processed += 1
            already_processed_ids.add(chunk["id"])

            if len(write_buffer) >= WRITE_BATCH_SIZE:
                try:
                    collection.upsert(
                        ids=[c["id"] for c in write_buffer],
                        embeddings=[c["embedding"] for c in write_buffer],
                        documents=[c["document"] for c in write_buffer],
                        metadatas=[c["metadata"] for c in write_buffer]
                    )
                    write_buffer = []
                except Exception as e:
                    print(f"\nERRORE ChromaDB: {e}")
                    errors += len(write_buffer)
                    write_buffer = []

            if processed % CHECKPOINT_EVERY == 0:
                save_checkpoint(already_processed_ids)
                print(f"  [checkpoint saved: {processed:,}]")

            if test_limit:
                ents = json.loads(updated_meta.get("entities", "[]"))
                typs = json.loads(updated_meta.get("entity_types", "[]"))
                print(f"\n--- Chunk {i+1} ---")
                print(f"ID: {chunk['id']}")
                print(f"Source: {chunk['metadata'].get('source', 'N/A')} | Title: {chunk['metadata'].get('title', 'N/A')[:60]}")
                print(f"Text: {chunk['text'][:200]}")
                print(f"Entities ({len(ents)}):")
                for e, t in zip(ents, typs):
                    print(f"  [{t}] {e}")

        if write_buffer:
            try:
                collection.upsert(
                    ids=[c["id"] for c in write_buffer],
                    embeddings=[c["embedding"] for c in write_buffer],
                    documents=[c["document"] for c in write_buffer],
                    metadatas=[c["metadata"] for c in write_buffer]
                )
            except Exception as e:
                print(f"\nERRORE finale ChromaDB: {e}")
                errors += len(write_buffer)

    # Checkpoint finale
    save_checkpoint(already_processed_ids)

    elapsed = time.time() - start_time
    print(f"\n{'='*50}")
    print(f"Extraction completed:")
    print(f"  Chunks processed: {processed:,}")
    print(f"  Skipped (checkpoint): {skipped_checkpoint:,}")
    print(f"  Errors: {errors:,}")
    print(f"  Timeouts: {timeouts:,}")
    print(f"  Total time: {elapsed/60:.1f} minutes")
    if elapsed > 0:
        print(f"  Average speed: {processed/elapsed:.2f} chunk/s")
    print(f"\nTo view statistics: python scripts/enrichment/extract_entities.py --stats")
    print(f"To rebuild index: python scripts/enrichment/build_entity_index.py")


if __name__ == "__main__":
    main()