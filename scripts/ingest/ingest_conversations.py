#!/usr/bin/env python3
"""
TAILOR — Unified Conversation Ingest Pipeline
Reads conversation sources from YAML config, parses, chunks, embeds, and ingests
into ChromaDB in a single pass.

Supported sources: chatgpt, claude, gemini (pluggable parsers)

Pipeline per source:
  1. Parser (source-specific) -> normalized records [{id, title, text, ...}]
  2. Chunker (unified) -> chunks [{chunk_id, text, metadata, ...}]
  3. Embed + Ingest -> ChromaDB with source tag

Usage:
  python ingest_conversations.py                    # Ingest all enabled sources
  python ingest_conversations.py --source chatgpt   # Ingest only ChatGPT
  python ingest_conversations.py --source claude     # Ingest only Claude
  python ingest_conversations.py --stats             # Show stats, no ingest
  python ingest_conversations.py --dry-run           # Parse + chunk, no embed/ingest
"""

import json
import os
import sys
import time
import argparse
import importlib
import chromadb
from datetime import datetime

# ── Path setup ────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "lib"))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "ingest"))

from config import get as cfg, load_config
from embedding import get_embeddings, info as embedding_info

# ── Constants ─────────────────────────────────────────────────
DB_DIR = os.path.join(BASE_DIR, "db")
DATA_DIR = os.path.join(BASE_DIR, "data")
COLLECTION_NAME = cfg("kb", "collection") or "tailor_kb"

# Chunking parameters
TARGET_CHUNK_CHARS = 1200
MAX_CHUNK_CHARS = 2000
OVERLAP_CHARS = 200
BATCH_SIZE = 10
MAX_TEXT_CHARS = 4000


# ── Chunker (unified) ────────────────────────────────────────

def split_text_at_boundaries(text, max_chars):
    """Split text at natural boundaries: paragraph > newline > sentence > space."""
    if len(text) <= max_chars:
        return [text]

    segments = []
    remaining = text

    while len(remaining) > max_chars:
        chunk = remaining[:max_chars]
        cut_at = -1

        for sep, offset in [("\n\n", 2), ("\n", 1), (". ", 2), (" ", 1)]:
            pos = chunk.rfind(sep)
            if pos > max_chars * 0.3:
                cut_at = pos + offset
                break

        if cut_at == -1:
            cut_at = max_chars

        segments.append(remaining[:cut_at].rstrip())
        remaining = remaining[cut_at:].lstrip()

    if remaining.strip():
        segments.append(remaining.strip())

    return segments


def chunk_record(record):
    """Chunk a normalized record into ChromaDB-ready chunks."""
    rec_id = record.get("id", "")
    title = record.get("title", "Untitled")
    text = record.get("text", "")
    create_time = record.get("create_time", 0)
    date_str = record.get("date", "")
    source = record.get("source", "unknown")

    if not text.strip():
        return []

    parts = split_text_at_boundaries(text, TARGET_CHUNK_CHARS)

    chunks = []
    prev_text = ""

    for idx, part in enumerate(parts):
        # Add overlap from previous chunk
        if prev_text and OVERLAP_CHARS > 0:
            overlap = prev_text[-OVERLAP_CHARS:].strip()
            space_pos = overlap.find(" ")
            if space_pos > 0:
                overlap = overlap[space_pos + 1:]
            full_text = f"[...contesto precedente:] {overlap}\n\n{part}"
        else:
            full_text = part

        chunk_id = f"{rec_id}_chunk_{idx:04d}"

        # Date from create_time if not provided
        if not date_str and create_time:
            try:
                date_str = datetime.fromtimestamp(create_time).strftime("%Y-%m-%d")
            except (OSError, ValueError):
                date_str = ""

        chunks.append({
            "chunk_id": chunk_id,
            "conv_id": rec_id,
            "title": title,
            "date": date_str,
            "create_time": create_time,
            "chunk_index": idx,
            "text": full_text,
            "char_count": len(full_text),
            "source": source,
            "has_overlap": idx > 0,
        })

        prev_text = part

    return chunks


# ── Parser dispatch ───────────────────────────────────────────

PARSER_REGISTRY = {}

def get_parser(name: str):
    """Lazy-load parser module from parsers/ directory."""
    if name in PARSER_REGISTRY:
        return PARSER_REGISTRY[name]

    try:
        mod = importlib.import_module(f"parsers.{name}")
        if not hasattr(mod, "parse"):
            print(f"  ERROR: parsers/{name}.py has no parse() function")
            return None
        PARSER_REGISTRY[name] = mod
        return mod
    except ImportError as e:
        print(f"  ERROR: Parser '{name}' not found: {e}")
        return None


# ── Embed + Ingest ────────────────────────────────────────────

def delete_source_chunks(collection, source: str):
    """Delete all chunks for a given source from ChromaDB."""
    try:
        results = collection.get(where={"source": source}, include=[])
        ids = results["ids"]
        if not ids:
            return 0

        # ChromaDB delete batch limit
        for i in range(0, len(ids), 500):
            collection.delete(ids=ids[i:i+500])

        return len(ids)
    except Exception as e:
        print(f"  Warning: error deleting {source} chunks: {e}")
        return 0


def ingest_chunks(chunks: list[dict], collection, dry_run: bool = False):
    """Embed and ingest chunks into ChromaDB."""
    if not chunks:
        return 0, 0

    total = len(chunks)
    processed = 0
    errors = 0
    start_time = time.time()

    for batch_start in range(0, total, BATCH_SIZE):
        batch = chunks[batch_start:batch_start + BATCH_SIZE]

        # Prepare texts for embedding with context prefix
        texts = []
        for chunk in batch:
            prefix = f"Conversazione: {chunk['title']}"
            if chunk.get("date"):
                prefix += f" ({chunk['date']})"
            full = f"{prefix}\n\n{chunk['text']}"
            texts.append(full[:MAX_TEXT_CHARS])

        if dry_run:
            processed += len(batch)
            continue

        # Generate embeddings via abstraction layer
        try:
            embeddings = get_embeddings(texts)
        except Exception as e:
            print(f"\n  Embedding error: {e}")
            errors += len(batch)
            continue

        # Prepare ChromaDB data
        ids = [c["chunk_id"] for c in batch]
        documents = [c["text"][:MAX_TEXT_CHARS] for c in batch]
        metadatas = []
        for c in batch:
            metadatas.append({
                "conv_id": c.get("conv_id", "") or "",
                "title": c.get("title", "") or "",
                "date": c.get("date", "") or "",
                "create_time": c.get("create_time", 0) or 0,
                "default_model": c.get("source", ""),
                "chunk_index": c.get("chunk_index", 0),
                "char_count": c.get("char_count", 0),
                "turn_count": 0,
                "source": c.get("source", "unknown"),
            })

        try:
            collection.upsert(
                ids=ids,
                embeddings=embeddings,
                documents=documents,
                metadatas=metadatas
            )
        except Exception as e:
            print(f"\n  ChromaDB upsert error: {e}")
            errors += len(batch)
            continue

        processed += len(batch)

        if processed % 100 < BATCH_SIZE:
            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 0
            eta = (total - processed) / rate if rate > 0 else 0
            print(f"\r  [{processed:,}/{total:,}] {processed*100//total}% | "
                  f"{rate:.1f} chunk/s | ETA: {eta:.0f}s", end="", flush=True)

    if total >= 100:
        print()  # newline after progress

    return processed, errors


# ── Main ──────────────────────────────────────────────────────

def get_conversation_sources():
    """Read conversation sources from YAML config."""
    load_config()
    sources = cfg("ingest", "conversations")
    if not sources:
        return []
    if isinstance(sources, dict):
        sources = [sources]
    return sources


def main():
    parser = argparse.ArgumentParser(description="TAILOR — Unified Conversation Ingest")
    parser.add_argument("--source", type=str, help="Ingest only this source (e.g. chatgpt, claude)")
    parser.add_argument("--stats", action="store_true", help="Show stats only, no ingest")
    parser.add_argument("--dry-run", action="store_true", help="Parse + chunk, skip embed/ingest")
    args = parser.parse_args()

    print("=" * 60)
    print("TAILOR — Unified Conversation Ingest")
    print("=" * 60)

    # Load sources from YAML
    sources = get_conversation_sources()
    if not sources:
        print("\nNo conversation sources configured in YAML.")
        print("Add an 'ingest.conversations' section to config/tailor.yaml")
        print("\nExample:")
        print("  ingest:")
        print("    conversations:")
        print("      - name: chatgpt")
        print("        parser: chatgpt")
        print("        source_files:")
        print("          - conversations-*.json")
        print("        enabled: true")
        return

    # Filter by --source if specified
    if args.source:
        sources = [s for s in sources if s.get("name") == args.source]
        if not sources:
            print(f"\nSource '{args.source}' not found in config.")
            return

    # Show embedding info
    if not args.stats:
        emb_info = embedding_info()
        print(f"\nEmbedding: {emb_info['provider']} / {emb_info['model']}")

    # Setup ChromaDB
    os.makedirs(DB_DIR, exist_ok=True)
    client = chromadb.PersistentClient(path=DB_DIR)
    collection = client.get_collection(name=COLLECTION_NAME)

    if not args.stats:
        print(f"ChromaDB: {collection.count():,} chunks in collection")

    total_records = 0
    total_chunks = 0
    total_processed = 0
    total_errors = 0
    start_time = time.time()

    for source_cfg in sources:
        name = source_cfg.get("name", "unknown")
        parser_name = source_cfg.get("parser", name)
        enabled = source_cfg.get("enabled", True)

        if not enabled:
            print(f"\n[{name}] Disabled, skipping")
            continue

        print(f"\n{'─' * 40}")
        print(f"[{name}] Processing...")

        # 1. Parse
        parser_mod = get_parser(parser_name)
        if not parser_mod:
            continue

        records = parser_mod.parse(source_cfg, DATA_DIR)
        total_records += len(records)

        if not records:
            print(f"[{name}] No records to process")
            continue

        if args.stats:
            total_chars = sum(r["char_count"] for r in records)
            dates = sorted([r["date"] for r in records if r.get("date")])
            print(f"[{name}] {len(records)} records, {total_chars:,} chars")
            if dates:
                print(f"[{name}] Period: {dates[0]} -> {dates[-1]}")
            continue

        # 2. Chunk
        all_chunks = []
        for record in records:
            all_chunks.extend(chunk_record(record))

        total_chunks += len(all_chunks)
        print(f"[{name}] {len(records)} records -> {len(all_chunks):,} chunks")

        if not all_chunks:
            continue

        # Chunk size stats
        sizes = [c["char_count"] for c in all_chunks]
        avg_size = sum(sizes) / len(sizes) if sizes else 0
        print(f"[{name}] Chunk size: avg {avg_size:.0f}, min {min(sizes)}, max {max(sizes)}")

        if args.dry_run:
            print(f"[{name}] Dry run — skipping embed/ingest")
            continue

        # 3. Delete old chunks for this source, then ingest
        deleted = delete_source_chunks(collection, name)
        if deleted > 0:
            print(f"[{name}] Deleted {deleted:,} old chunks")

        processed, errors = ingest_chunks(all_chunks, collection)
        total_processed += processed
        total_errors += errors
        print(f"[{name}] Ingested: {processed:,} chunks, {errors} errors")

    # Summary
    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"DONE in {elapsed:.1f}s")
    print(f"  Records parsed:  {total_records:,}")
    print(f"  Chunks created:  {total_chunks:,}")

    if not args.stats and not args.dry_run:
        print(f"  Chunks ingested: {total_processed:,}")
        print(f"  Errors:          {total_errors}")
        print(f"  KB total:        {collection.count():,} chunks")


if __name__ == "__main__":
    main()
