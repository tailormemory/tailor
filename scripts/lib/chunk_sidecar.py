"""
chunk_sidecar — invalidazione dei sidecar sqlite legati ai chunk di un documento.

Motivo: i chunk_id sono deterministici (`doc_<hash12>_chunk_<idx>`). Quando un
file viene ri-ingestato con lo stesso hash — o quando cambia il testo prodotto a
parità di indice (es. nuovo estrattore) — i chunk_id vengono RIUSATI. I sidecar
che sono chiavati su chunk_id restano quindi agganciati a testo che non esiste
più:

  - `facts.extraction_log` ha `chunk_id` come PRIMARY KEY e viene usato come
    gate di skip dall'estrattore notturno → il chunk nuovo non verrebbe MAI
    ri-estratto.
  - `facts.facts` e `entity_index.entity_index` conserverebbero fatti/entità
    derivati dal testo vecchio, senza modo di accorgersene.

Chiamare `invalidate_chunk_sidecars()` accanto alla delete Chroma dei chunk
vecchi rimette i sidecar in pari.
"""

import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_DIR = os.path.join(BASE_DIR, "db")
FACTS_DB_PATH = os.path.join(DB_DIR, "facts.sqlite3")
ENTITY_DB_PATH = os.path.join(DB_DIR, "entity_index.sqlite3")

# (db_path_key, table, colonna chunk_id) — schema verificato sulle DB live.
_SIDECAR_TARGETS = (
    ("facts", "extraction_log", "chunk_id"),
    ("facts", "facts", "chunk_id"),
    ("entity", "entity_index", "chunk_id"),
)


def _like_escape(value):
    """Neutralizza i wildcard LIKE (`%`, `_`) — i chunk_id contengono `_`."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _table_exists(conn, table):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def invalidate_chunk_sidecars(chunk_id_prefix, facts_db=None, entity_db=None, verbose=False):
    """Rimuove le righe sidecar dei chunk `<prefix>_chunk_%`.

    Args:
        chunk_id_prefix: prefisso conv_id, es. "doc_a1b2c3d4e5f6" (senza `_chunk_`).
        facts_db / entity_db: override path (test).
        verbose: stampa il riepilogo.

    Returns:
        dict {"extraction_log": n, "facts": n, "entity_index": n} — chiavi assenti
        se la DB o la tabella non esistono.
    """
    prefix = (chunk_id_prefix or "").strip()
    if not prefix:
        return {}

    # Il prefisso arriva sia come "doc_<hash>" sia già con il suffisso: normalizza.
    if prefix.endswith("_chunk_"):
        prefix = prefix[: -len("_chunk_")]
    pattern = _like_escape(f"{prefix}_chunk_") + "%"

    paths = {
        "facts": facts_db or FACTS_DB_PATH,
        "entity": entity_db or ENTITY_DB_PATH,
    }

    deleted = {}
    for db_key in ("facts", "entity"):
        db_path = paths[db_key]
        if not db_path or not os.path.exists(db_path):
            continue
        targets = [t for t in _SIDECAR_TARGETS if t[0] == db_key]
        try:
            conn = sqlite3.connect(db_path, timeout=30)
            try:
                # NIENTE journal_mode=WAL: e' un cambio PERSISTENTE del db.
                # entity_index.sqlite3 e' in journal_mode=delete e migrarlo non
                # e' mandato di questo helper (backup gzip del file principale
                # senza -wal, writer esistenti). facts e' gia' WAL -> no-op.
                conn.execute("PRAGMA busy_timeout=30000")
                for _, table, col in targets:
                    if not _table_exists(conn, table):
                        continue
                    cur = conn.execute(
                        f"DELETE FROM {table} WHERE {col} LIKE ? ESCAPE '\\'", (pattern,)
                    )
                    deleted[table] = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
                conn.commit()
            finally:
                conn.close()
        except sqlite3.Error as e:
            # Non-fatale: l'ingest deve proseguire, i sidecar si riallineano al
            # prossimo giro notturno.
            print(f"    WARNING sidecar invalidation ({db_key}): {e}")

    if verbose and deleted:
        summary = ", ".join(f"{k}={v}" for k, v in deleted.items() if v)
        if summary:
            print(f"    Sidecar invalidati: {summary}")

    return deleted
