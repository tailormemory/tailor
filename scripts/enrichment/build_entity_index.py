"""
build_entity_index.py — Builds a SQLite index for fast entity search.

Reads 'entities' and 'entity_types' metadata from ChromaDB KB (via direct SQLite)
e li scrive in una tabella indicizzata per lookup O(1).

Uso:
    cd /path/to/tailor && source .venv/bin/activate
    python3 scripts/build_entity_index.py           # Build completo
    python3 scripts/build_entity_index.py --stats   # Show index statistics

Output: db/entity_index.sqlite3
"""

import json
import os
import sqlite3
import sys
import time

# ============================================================
# CONFIGURAZIONE
# ============================================================

DB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "db")
CHROMA_DB = os.path.join(DB_DIR, "chroma.sqlite3")
ENTITY_DB = os.path.join(DB_DIR, "entity_index.sqlite3")

# Segment ID del METADATA segment della collection tailor_kb
# Aggiornare se la collection viene ricreata
METADATA_SEGMENT = None  # Auto-detect

BATCH = 1000


def get_metadata_segment(conn):
    """Trova il segment_id METADATA della collection tailor_kb."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT s.id 
        FROM segments s 
        JOIN collections c ON s.collection = c.id 
        WHERE c.name = 'tailor_kb' AND s.scope = 'METADATA'
    """)
    row = cursor.fetchone()
    if not row:
        # Fallback: try different join (depends on ChromaDB version)
        cursor.execute("SELECT id FROM collections WHERE name = 'tailor_kb'")
        col_row = cursor.fetchone()
        if col_row:
            cursor.execute("SELECT id FROM segments WHERE collection = ? AND scope = 'METADATA'", (col_row[0],))
            row = cursor.fetchone()
    if not row:
        print("❌ Collection tailor_kb non trovata!")
        sys.exit(1)
    return row[0]


def build_index():
    """Build the entity index from scratch."""
    print("=" * 60)
    print("BUILD ENTITY INDEX")
    print("=" * 60)
    
    # Apri ChromaDB in read-only
    chroma_conn = sqlite3.connect(f"file:{CHROMA_DB}?mode=ro", uri=True)
    segment_id = get_metadata_segment(chroma_conn)
    print(f"Metadata segment: {segment_id}")
    
    # Count chunks with entities
    cursor = chroma_conn.cursor()
    cursor.execute("""
        SELECT COUNT(DISTINCT em.id)
        FROM embedding_metadata em
        JOIN embeddings e ON em.id = e.id
        WHERE em.key = 'entities_extracted' AND em.int_value = 1
        AND e.segment_id = ?
    """, (segment_id,))
    total_with_entities = cursor.fetchone()[0]
    print(f"Chunks with extracted entities: {total_with_entities:,}")
    
    # Crea/ricrea il DB indice
    if os.path.exists(ENTITY_DB):
        os.remove(ENTITY_DB)
    
    entity_conn = sqlite3.connect(ENTITY_DB)
    entity_cursor = entity_conn.cursor()
    
    # Schema
    entity_cursor.execute("""
        CREATE TABLE entity_index (
            entity TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            chunk_id TEXT NOT NULL
        )
    """)
    
    # Read in batches: for each chunk with entities, read embedding_id + entities + entity_types
    cursor.execute("""
        SELECT e.embedding_id, em_ent.string_value, em_types.string_value
        FROM embeddings e
        JOIN embedding_metadata em_ent ON e.id = em_ent.id AND em_ent.key = 'entities'
        JOIN embedding_metadata em_types ON e.id = em_types.id AND em_types.key = 'entity_types'
        JOIN embedding_metadata em_ext ON e.id = em_ext.id AND em_ext.key = 'entities_extracted' AND em_ext.int_value = 1
        WHERE e.segment_id = ?
        AND em_ent.string_value != '[]'
    """, (segment_id,))
    
    total_entities = 0
    total_chunks = 0
    insert_buffer = []
    
    start = time.time()
    
    while True:
        rows = cursor.fetchmany(BATCH)
        if not rows:
            break
        
        for chunk_id, entities_json, types_json in rows:
            try:
                entities = json.loads(entities_json)
                types = json.loads(types_json)
            except (json.JSONDecodeError, TypeError):
                continue
            
            total_chunks += 1
            
            for entity, etype in zip(entities, types):
                # Normalizza: strip spazi, lowercase per l'indice
                entity_clean = entity.strip()
                if not entity_clean:
                    continue
                insert_buffer.append((entity_clean, etype, chunk_id))
                total_entities += 1
            
            # Flush buffer every 10k entries
            if len(insert_buffer) >= 10000:
                entity_cursor.executemany(
                    "INSERT INTO entity_index (entity, entity_type, chunk_id) VALUES (?, ?, ?)",
                    insert_buffer
                )
                insert_buffer = []
    
    # Flush residui
    if insert_buffer:
        entity_cursor.executemany(
            "INSERT INTO entity_index (entity, entity_type, chunk_id) VALUES (?, ?, ?)",
            insert_buffer
        )
    
    # Crea indici
    print("Creazione indici...")
    entity_cursor.execute("CREATE INDEX idx_entity_lower ON entity_index (entity COLLATE NOCASE)")
    entity_cursor.execute("CREATE INDEX idx_entity_type ON entity_index (entity_type)")
    entity_cursor.execute("CREATE INDEX idx_chunk_id ON entity_index (chunk_id)")
    
    entity_conn.commit()
    
    elapsed = time.time() - start
    
    print()
    print("=" * 60)
    print("BUILD COMPLETATO")
    print("=" * 60)
    print(f"Chunk processati: {total_chunks:,}")
    print(f"Entities indexed: {total_entities:,}")
    print(f"Tempo: {elapsed:.1f}s")
    print(f"File: {ENTITY_DB}")
    print(f"Dimensione: {os.path.getsize(ENTITY_DB) / 1024 / 1024:.1f} MB")
    
    # Stats rapide
    show_stats(entity_conn)
    
    entity_conn.close()
    chroma_conn.close()


def show_stats(conn=None):
    """Show entity index statistics."""
    if conn is None:
        if not os.path.exists(ENTITY_DB):
            print("❌ Index not found. Run first: python3 scripts/build_entity_index.py")
            return
        conn = sqlite3.connect(f"file:{ENTITY_DB}?mode=ro", uri=True)
        close_conn = True
    else:
        close_conn = False
    
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM entity_index")
    total = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT entity) FROM entity_index")
    unique_entities = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT chunk_id) FROM entity_index")
    unique_chunks = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT entity_type, COUNT(*) as cnt 
        FROM entity_index 
        GROUP BY entity_type 
        ORDER BY cnt DESC
    """)
    type_counts = cursor.fetchall()
    
    cursor.execute("""
        SELECT entity, COUNT(*) as cnt 
        FROM entity_index 
        GROUP BY entity COLLATE NOCASE
        ORDER BY cnt DESC 
        LIMIT 15
    """)
    top_entities = cursor.fetchall()
    
    print(f"\n=== Entity Index Stats ===")
    print(f"Righe totali: {total:,}")
    print(f"Unique entities: {unique_entities:,}")
    print(f"Chunk coperti: {unique_chunks:,}")
    print(f"\nPer tipo:")
    for etype, cnt in type_counts:
        print(f"  {etype}: {cnt:,}")
    print(f"\nTop 15 entities:")
    for entity, cnt in top_entities:
        print(f"  {entity}: {cnt} chunk")
    
    # Speed test
    cursor.execute("SELECT entity FROM entity_index LIMIT 1")
    test_entity = cursor.fetchone()[0]
    t0 = time.time()
    cursor.execute("SELECT chunk_id FROM entity_index WHERE entity = ? COLLATE NOCASE", (test_entity,))
    _ = cursor.fetchall()
    t1 = time.time()
    print(f"\nQuery test ('{test_entity}'): {(t1-t0)*1000:.1f}ms")
    
    if close_conn:
        conn.close()


if __name__ == "__main__":
    if "--stats" in sys.argv:
        show_stats()
    else:
        build_index()