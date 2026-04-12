"""
rebuild_db.py — Ricostruisce la collection ChromaDB da zero.

Legge testi e metadata da SQLite (DB corrotto per HNSW/FTS),
genera nuovi embedding via Ollama, scrive in una nuova collection pulita.

Uso:
    cd /path/to/tailor && source .venv/bin/activate
    python3 scripts/rebuild_db.py              # Rebuild completo
    python3 scripts/rebuild_db.py --dry-run    # Solo conta, non scrive
    python3 scripts/rebuild_db.py --resume     # Riprende da checkpoint

Tempo stimato: ~50-60 minuti per 100k chunk (Ollama locale).
"""

import json
import os
import sqlite3
import sys
import time
import requests
import chromadb

# ============================================================
# CONFIGURAZIONE
# ============================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)
from scripts.lib.config import get as cfg
OLLAMA_URL = os.environ.get("OLLAMA_URL", cfg("ollama", "base_url") or "http://localhost:11434") + "/api/embed"
EMBED_MODEL = cfg("embedding", "model") or "nomic-embed-text"

DB_DIR = os.path.join(BASE_DIR, "db")
SQLITE_PATH = os.path.join(DB_DIR, "chroma.sqlite3.corrupted")

OLD_COLLECTION = "tailor_kb"
NEW_COLLECTION = "tailor_kb_new"

# Segment ID del METADATA segment della vecchia collection
# (filter to read only chunks from the original collection, not the new one)
OLD_METADATA_SEGMENT = "d400905a-c07d-4c44-8eb5-f9cc51c3ebb3"

READ_BATCH = 1000       # Chunk letti da SQLite per iterazione
WRITE_BATCH = 100       # Chunk scritti in ChromaDB per batch
EMBED_BATCH = 50        # Testi inviati a Ollama per chiamata
CHECKPOINT_EVERY = 1000 # Save progress every N chunks

CHECKPOINT_FILE = os.path.join(DB_DIR, "checkpoints", "rebuild_checkpoint.json")

# ============================================================
# FUNZIONI
# ============================================================

def get_embedding_batch(texts: list[str]) -> list[list[float]]:
    """Generate embeddings for a batch of texts via Ollama."""
    # Tronca a 4000 chars come fa il server MCP
    truncated = [t[:4000] for t in texts]
    payload = {
        "model": EMBED_MODEL,
        "input": truncated
    }
    r = requests.post(OLLAMA_URL, json=payload, timeout=120)
    r.raise_for_status()
    return r.json()["embeddings"]


def load_checkpoint() -> int:
    """Load the last processed offset from checkpoint."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            data = json.load(f)
            return data.get("last_offset", 0)
    return 0


def save_checkpoint(offset: int):
    """Salva il progresso."""
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"last_offset": offset, "timestamp": time.time()}, f)


def read_all_from_sqlite(conn, start_offset=0):
    """
    Generate batches of (embedding_id, document, metadata_dict) reading from SQLite.
    
    Legge in ordine di e.id (stabile), con offset per resume.
    """
    cursor = conn.cursor()
    
    offset = start_offset
    while True:
        # Read batch of embedding_id + document (only from old collection)
        cursor.execute("""
            SELECT e.id, e.embedding_id, fts.c0
            FROM embeddings e
            JOIN embedding_fulltext_search_content fts ON e.id = fts.id
            WHERE e.segment_id = ?
              AND fts.c0 IS NOT NULL
            ORDER BY e.id
            LIMIT ? OFFSET ?
        """, (OLD_METADATA_SEGMENT, READ_BATCH, offset))
        
        rows = cursor.fetchall()
        if not rows:
            break
        
        # For each row, load metadata
        batch = []
        for numeric_id, embedding_id, document in rows:
            # Read metadata for this chunk
            cursor.execute("""
                SELECT key, string_value, int_value, float_value, bool_value
                FROM embedding_metadata
                WHERE id = ?
            """, (numeric_id,))
            
            meta = {}
            for key, str_val, int_val, float_val, bool_val in cursor.fetchall():
                if key == "chroma:document":
                    continue  # Skip internal key
                if str_val is not None:
                    meta[key] = str_val
                elif int_val is not None:
                    meta[key] = int_val
                elif float_val is not None:
                    meta[key] = float_val
                elif bool_val is not None:
                    meta[key] = bool(bool_val)
            
            batch.append((embedding_id, document, meta))
        
        yield batch, offset
        offset += len(rows)


def main():
    dry_run = "--dry-run" in sys.argv
    resume = "--resume" in sys.argv
    
    print("=" * 60)
    print("TAILOR DB REBUILD")
    print("=" * 60)
    print(f"SQLite: {SQLITE_PATH}")
    print(f"DB dir: {DB_DIR}")
    print(f"Dry run: {dry_run}")
    print(f"Resume: {resume}")
    print()
    
    # Verifica Ollama
    if not dry_run:
        try:
            test = requests.post(OLLAMA_URL, json={"model": EMBED_MODEL, "input": ["test"]}, timeout=10)
            test.raise_for_status()
            print("✅ Ollama raggiungibile, modello OK")
        except Exception as e:
            print(f"❌ Ollama non raggiungibile: {e}")
            print("   Assicurati che Ollama sia attivo: brew services start ollama")
            sys.exit(1)
    
    # Apri SQLite in read-only
    conn = sqlite3.connect(f"file:{SQLITE_PATH}?mode=ro", uri=True)
    
    # Conta totale
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM embeddings WHERE segment_id = ?", (OLD_METADATA_SEGMENT,))
    total = cursor.fetchone()[0]
    print(f"Chunk totali da processare: {total:,}")
    
    if dry_run:
        # Show sample
        print("\nSample primi 3 chunk:")
        for batch, offset in read_all_from_sqlite(conn):
            for emb_id, doc, meta in batch[:3]:
                print(f"  ID: {emb_id}")
                print(f"  Doc: {doc[:80]}...")
                print(f"  Meta keys: {list(meta.keys())}")
                print(f"  Source: {meta.get('source', 'N/A')}")
                print()
            break
        conn.close()
        print("Dry run completato.")
        return
    
    # Determina offset di partenza
    start_offset = 0
    if resume:
        start_offset = load_checkpoint()
        if start_offset > 0:
            print(f"⏩ Ripresa da checkpoint: offset {start_offset:,}")
        else:
            print("No checkpoint found, starting from scratch.")
    
    # Crea nuova collection (o riprendi se esiste)
    client = chromadb.PersistentClient(path=DB_DIR)
    
    if not resume or start_offset == 0:
        # Cancella collection temporanea se esiste
        try:
            client.delete_collection(NEW_COLLECTION)
            print(f"🗑️  Collection '{NEW_COLLECTION}' precedente eliminata")
        except Exception:
            pass
        new_col = client.create_collection(
            name=NEW_COLLECTION,
            metadata={"hnsw:space": "cosine"}
        )
        print(f"✅ Collection '{NEW_COLLECTION}' creata")
    else:
        new_col = client.get_collection(name=NEW_COLLECTION)
        print(f"✅ Collection '{NEW_COLLECTION}' ripresa (count: {new_col.count():,})")
    
    # Process in batches
    processed = start_offset
    errors = 0
    start_time = time.time()
    last_report = start_time
    
    write_buffer_ids = []
    write_buffer_embeddings = []
    write_buffer_documents = []
    write_buffer_metadatas = []
    
    embed_buffer_texts = []
    embed_buffer_indices = []  # Indici nel write_buffer corrispondenti
    
    def flush_write_buffer():
        """Scrive il buffer accumulato in ChromaDB."""
        nonlocal write_buffer_ids, write_buffer_embeddings, write_buffer_documents, write_buffer_metadatas
        if not write_buffer_ids:
            return
        new_col.add(
            ids=write_buffer_ids,
            embeddings=write_buffer_embeddings,
            documents=write_buffer_documents,
            metadatas=write_buffer_metadatas
        )
        write_buffer_ids = []
        write_buffer_embeddings = []
        write_buffer_documents = []
        write_buffer_metadatas = []
    
    for batch, offset in read_all_from_sqlite(conn, start_offset):
        for embedding_id, document, meta in batch:
            # Accumula per embedding batch
            write_buffer_ids.append(embedding_id)
            write_buffer_documents.append(document[:4000])
            write_buffer_metadatas.append(meta)
            embed_buffer_texts.append(document)
            
            # Quando abbiamo abbastanza testi, genera embedding
            if len(embed_buffer_texts) >= EMBED_BATCH:
                try:
                    embeddings = get_embedding_batch(embed_buffer_texts)
                    write_buffer_embeddings.extend(embeddings)
                    embed_buffer_texts = []
                except Exception as e:
                    # Riprova uno alla volta
                    print(f"\n⚠️  Batch embedding error, retrying individually: {e}")
                    for txt in embed_buffer_texts:
                        try:
                            emb = get_embedding_batch([txt])
                            write_buffer_embeddings.extend(emb)
                        except Exception as e2:
                            print(f"  ❌ Skip chunk: {e2}")
                            # Rimuovi l'ultimo entry dal write buffer
                            write_buffer_ids.pop()
                            write_buffer_documents.pop()
                            write_buffer_metadatas.pop()
                            errors += 1
                    embed_buffer_texts = []
            
            # When write buffer is full, flush
            if len(write_buffer_ids) >= WRITE_BATCH and len(write_buffer_ids) == len(write_buffer_embeddings):
                try:
                    flush_write_buffer()
                except Exception as e:
                    print(f"\n❌ Batch write error: {e}")
                    errors += 1
                    # Svuota i buffer per non bloccarsi
                    write_buffer_ids = []
                    write_buffer_embeddings = []
                    write_buffer_documents = []
                    write_buffer_metadatas = []
            
            processed += 1
        
        # Checkpoint periodico
        if processed % CHECKPOINT_EVERY < READ_BATCH:
            save_checkpoint(processed)
        
        # Report progresso
        now = time.time()
        if now - last_report >= 10:  # Ogni 10 secondi
            elapsed = now - start_time
            rate = (processed - start_offset) / elapsed if elapsed > 0 else 0
            remaining = (total - processed) / rate if rate > 0 else 0
            pct = processed / total * 100
            print(
                f"  [{pct:5.1f}%] {processed:,}/{total:,} chunk | "
                f"{rate:.1f}/s | ETA {remaining/60:.0f}min | "
                f"errori: {errors}"
            )
            last_report = now
    
    # Flush residui embed buffer
    if embed_buffer_texts:
        try:
            embeddings = get_embedding_batch(embed_buffer_texts)
            write_buffer_embeddings.extend(embeddings)
            embed_buffer_texts = []
        except Exception as e:
            print(f"\n⚠️  Final embedding error: {e}")
            errors += len(embed_buffer_texts)
    
    # Flush residui write buffer
    if write_buffer_ids and len(write_buffer_ids) == len(write_buffer_embeddings):
        try:
            flush_write_buffer()
        except Exception as e:
            print(f"\n❌ Final write error: {e}")
            errors += len(write_buffer_ids)
    
    conn.close()
    save_checkpoint(processed)
    
    # Report finale
    elapsed = time.time() - start_time
    new_count = new_col.count()
    
    print()
    print("=" * 60)
    print("REBUILD COMPLETATO")
    print("=" * 60)
    print(f"Chunk processati: {processed:,}")
    print(f"Chunk nella nuova collection: {new_count:,}")
    print(f"Errori: {errors}")
    print(f"Tempo: {elapsed/60:.1f} minuti")
    print(f"Rate: {(processed - start_offset)/elapsed:.1f} chunk/s")
    
    if new_count < total * 0.99:
        print(f"\n⚠️  WARNING: new collection has {new_count:,} chunks vs {total:,} original.")
        print("   Difference > 1%. Check errors before proceeding.")
        print("   NON rinomino automaticamente.")
        return
    
    # Rinomina: elimina vecchia, rinomina nuova
    print(f"\n🔄 Rinomina: {NEW_COLLECTION} → {OLD_COLLECTION}")
    
    try:
        client.delete_collection(OLD_COLLECTION)
        print(f"  ✅ '{OLD_COLLECTION}' eliminata")
    except Exception as e:
        print(f"  ⚠️  Error deleting old collection: {e}")
    
    # ChromaDB non ha rename — creiamo la collection col nome giusto
    # and copy the data. But with 100k chunks this is not practical.
    # Alternativa: modifichiamo direttamente il nome in SQLite.
    
    # Chiudi il client ChromaDB prima di modificare SQLite
    del new_col
    del client
    
    # Rinomina via SQLite diretto
    rename_conn = sqlite3.connect(SQLITE_PATH)
    rename_cursor = rename_conn.cursor()
    
    # Trova l'ID della nuova collection
    rename_cursor.execute("SELECT id FROM collections WHERE name = ?", (NEW_COLLECTION,))
    row = rename_cursor.fetchone()
    if row:
        rename_cursor.execute("UPDATE collections SET name = ? WHERE name = ?", (OLD_COLLECTION, NEW_COLLECTION))
        rename_conn.commit()
        print(f"  ✅ Rinominata '{NEW_COLLECTION}' → '{OLD_COLLECTION}' in SQLite")
    else:
        print(f"  ❌ Collection '{NEW_COLLECTION}' non trovata in SQLite!")
    
    rename_conn.close()
    
    # Pulisci checkpoint
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("  🗑️  Checkpoint rimosso")
    
    print()
    print("✅ REBUILD COMPLETATO CON SUCCESSO")
    print(f"   Collection '{OLD_COLLECTION}': {new_count:,} chunk")
    print()
    print("Prossimi step:")
    print("  1. Verifica: python3 -c \"import chromadb; c=chromadb.PersistentClient(path='db'); col=c.get_collection('tailor_kb'); print(col.count())\"")
    print("  2. Riavvia MCP: sudo launchctl load /Library/LaunchDaemons/com.tailor.mcp.plist")


if __name__ == "__main__":
    main()