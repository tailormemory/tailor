#!/usr/bin/env python3
"""
TAILOR — Garbage Collection
Removes from KB chunks and doc_summary of files that no longer exist on disk.

PREREQUISITO: il server MCP deve essere SPENTO.

Uso:
  python scripts/maintenance/garbage_collect.py              # Esegui pulizia
  python scripts/maintenance/garbage_collect.py --dry-run    # Show what it would do
  python scripts/maintenance/garbage_collect.py --stats      # Solo statistiche
"""

import os, sys, json, argparse
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_DIR = os.path.join(BASE_DIR, "db")
REGISTRY_FILE = os.path.join(DB_DIR, "doc_registry.json")
LOG_PATH = os.path.join(BASE_DIR, "logs", "garbage_collect.log")
COLLECTION_NAME = "tailor_kb"


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")


def find_orphans():
    """Find files in the registry that no longer exist on disk."""
    with open(REGISTRY_FILE) as f:
        registry = json.load(f)

    orphans = []
    for path, info in registry.items():
        if not os.path.exists(path):
            filename = os.path.basename(path)
            rel = path.replace(CLOUD_LOCAL_ROOT + "/", "") if CLOUD_LOCAL_ROOT else path
            folder = rel.split("/")[0] if "/" in rel else "Unknown"
            orphans.append({
                "path": path,
                "filename": filename,
                "folder": folder,
                "file_path_rel": rel,
                "chunks": info.get("chunks", 0),
            })

    return registry, orphans


def remove_orphan_chunks(collection, orphan):
    """Rimuove i chunk e il doc_summary di un file orfano da ChromaDB."""
    removed = 0
    rel_path = orphan["file_path_rel"]
    filename = orphan["filename"]
    folder = orphan["folder"]

    # 1. Trova chunk per file_path
    try:
        results = collection.get(
            where={"file_path": rel_path},
            include=["metadatas"],
            limit=100
        )
        if results and results["ids"]:
            collection.delete(ids=results["ids"])
            removed += len(results["ids"])
    except Exception:
        pass

    # 2. Trova doc_summary per title + folder
    try:
        results = collection.get(
            where={"$and": [
                {"title": filename},
                {"folder": folder},
                {"category": "doc_summary"}
            ]},
            include=["metadatas"],
            limit=10
        )
        if results and results["ids"]:
            collection.delete(ids=results["ids"])
            removed += len(results["ids"])
    except Exception:
        pass

    # 3. Fallback: search by title + source=document (without folder match)
    if removed == 0:
        try:
            results = collection.get(
                where={"$and": [
                    {"title": filename},
                    {"source": "document"}
                ]},
                include=["metadatas"],
                limit=100
            )
            if results and results["ids"]:
                collection.delete(ids=results["ids"])
                removed += len(results["ids"])
        except Exception:
            pass

    return removed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--stats", action="store_true")
    args = parser.parse_args()

    registry, orphans = find_orphans()
    total_chunks = sum(o["chunks"] for o in orphans)

    log(f"Registry: {len(registry)} file | Orfani: {len(orphans)} file (~{total_chunks} chunk)")

    if args.stats:
        by_folder = {}
        for o in orphans:
            by_folder[o["folder"]] = by_folder.get(o["folder"], 0) + 1
        for folder, count in sorted(by_folder.items(), key=lambda x: -x[1]):
            print(f"  {count:4d}  {folder}")
        return

    if not orphans:
        log("No orphans found.")
        return

    if args.dry_run:
        for o in orphans[:20]:
            print(f"  [DEL] {o['folder']} | {o['filename']} ({o['chunks']} chunks)")
        if len(orphans) > 20:
            print(f"  ... e altri {len(orphans) - 20}")
        print(f"\nTotale: {len(orphans)} file, ~{total_chunks} chunk da rimuovere")
        return

    # ── Pulizia ──
    import chromadb
    client = chromadb.PersistentClient(path=DB_DIR)
    collection = client.get_collection(COLLECTION_NAME)

    removed_total = 0
    cleaned_paths = []

    for i, o in enumerate(orphans):
        removed = remove_orphan_chunks(collection, o)
        removed_total += removed
        cleaned_paths.append(o["path"])
        if (i + 1) % 10 == 0 or removed > 0:
            log(f"[{i+1}/{len(orphans)}] {o['filename']}: {removed} chunk rimossi")

    # Aggiorna registry
    for path in cleaned_paths:
        if path in registry:
            del registry[path]

    with open(REGISTRY_FILE, "w") as f:
        json.dump(registry, f, indent=2, ensure_ascii=False)

    del client
    log(f"DONE: {len(cleaned_paths)} file rimossi dal registry, {removed_total} chunk eliminati da ChromaDB")


if __name__ == "__main__":
    main()
