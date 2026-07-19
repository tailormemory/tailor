"""
invalidate_chunk_sidecars — pulizia di extraction_log / facts / entity_index
sui chunk_id di un documento ri-ingestato.
"""

import os
import sqlite3
import sys

import pytest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "lib"))

from chunk_sidecar import invalidate_chunk_sidecars  # noqa: E402

# Schema allineato alle DB live (db/facts.sqlite3, db/entity_index.sqlite3).
FACTS_SCHEMA = """
CREATE TABLE facts (
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
);
CREATE TABLE extraction_log (
    chunk_id TEXT PRIMARY KEY,
    facts_count INTEGER DEFAULT 0,
    model TEXT DEFAULT '',
    extracted_at TEXT NOT NULL
);
CREATE INDEX idx_facts_chunk ON facts(chunk_id);
"""

ENTITY_SCHEMA = """
CREATE TABLE entity_index (
    entity TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    chunk_id TEXT NOT NULL
);
CREATE INDEX idx_chunk_id ON entity_index (chunk_id);
"""

TARGET = "doc_aaaaaaaaaaaa"
OTHER = "doc_bbbbbbbbbbbb"


@pytest.fixture
def dbs(tmp_path):
    facts_db = tmp_path / "facts.sqlite3"
    entity_db = tmp_path / "entity_index.sqlite3"

    conn = sqlite3.connect(facts_db)
    conn.executescript(FACTS_SCHEMA)
    for prefix in (TARGET, OTHER):
        for i in range(3):
            cid = f"{prefix}_chunk_{i:04d}"
            conn.execute("INSERT INTO extraction_log VALUES (?,?,?,?)", (cid, 2, "m", "now"))
            conn.execute(
                "INSERT INTO facts (chunk_id, fact, created_at) VALUES (?,?,?)",
                (cid, f"fatto {i}", "now"))
            conn.execute(
                "INSERT INTO facts (chunk_id, fact, created_at) VALUES (?,?,?)",
                (cid, f"altro fatto {i}", "now"))
    # Chunk di sessione, estraneo ai documenti: non deve essere toccato.
    conn.execute("INSERT INTO extraction_log VALUES (?,?,?,?)", ("sess_x_chunk_0001", 1, "m", "now"))
    conn.commit()
    conn.close()

    conn = sqlite3.connect(entity_db)
    conn.executescript(ENTITY_SCHEMA)
    for prefix in (TARGET, OTHER):
        for i in range(3):
            conn.execute("INSERT INTO entity_index VALUES (?,?,?)",
                         ("Emiliano", "person", f"{prefix}_chunk_{i:04d}"))
    conn.commit()
    conn.close()

    return str(facts_db), str(entity_db)


def _count(db, table, prefix):
    conn = sqlite3.connect(db)
    n = conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE chunk_id LIKE ?", (f"{prefix}%",)
    ).fetchone()[0]
    conn.close()
    return n


def test_removes_only_target_document(dbs):
    facts_db, entity_db = dbs
    deleted = invalidate_chunk_sidecars(TARGET, facts_db=facts_db, entity_db=entity_db)

    assert deleted == {"extraction_log": 3, "facts": 6, "entity_index": 3}
    assert _count(facts_db, "extraction_log", TARGET) == 0
    assert _count(facts_db, "facts", TARGET) == 0
    assert _count(entity_db, "entity_index", TARGET) == 0
    # Gli altri documenti restano intatti.
    assert _count(facts_db, "extraction_log", OTHER) == 3
    assert _count(facts_db, "facts", OTHER) == 6
    assert _count(entity_db, "entity_index", OTHER) == 3
    assert _count(facts_db, "extraction_log", "sess_") == 1


def test_underscore_is_not_a_wildcard(dbs):
    """`_` è wildcard LIKE: senza ESCAPE `doc_aaa...` matcherebbe `docXaaa...`."""
    facts_db, entity_db = dbs
    conn = sqlite3.connect(facts_db)
    conn.execute("INSERT INTO extraction_log VALUES (?,?,?,?)",
                 ("docXaaaaaaaaaaaaXchunkX0001", 1, "m", "now"))
    conn.commit()
    conn.close()

    invalidate_chunk_sidecars(TARGET, facts_db=facts_db, entity_db=entity_db)
    assert _count(facts_db, "extraction_log", "docX") == 1


def test_accepts_prefix_with_chunk_suffix(dbs):
    facts_db, entity_db = dbs
    deleted = invalidate_chunk_sidecars(f"{TARGET}_chunk_", facts_db=facts_db, entity_db=entity_db)
    assert deleted["extraction_log"] == 3


def test_empty_prefix_is_noop(dbs):
    facts_db, entity_db = dbs
    assert invalidate_chunk_sidecars("", facts_db=facts_db, entity_db=entity_db) == {}
    assert invalidate_chunk_sidecars(None, facts_db=facts_db, entity_db=entity_db) == {}
    assert _count(facts_db, "extraction_log", TARGET) == 3


def test_missing_db_is_noop(tmp_path):
    missing = str(tmp_path / "nope.sqlite3")
    assert invalidate_chunk_sidecars(TARGET, facts_db=missing, entity_db=missing) == {}


def test_missing_table_is_skipped(tmp_path):
    """DB presente ma schema parziale: non deve sollevare."""
    db = tmp_path / "partial.sqlite3"
    conn = sqlite3.connect(db)
    conn.executescript(
        "CREATE TABLE extraction_log (chunk_id TEXT PRIMARY KEY, facts_count INTEGER,"
        " model TEXT, extracted_at TEXT);")
    conn.execute("INSERT INTO extraction_log VALUES (?,?,?,?)",
                 (f"{TARGET}_chunk_0001", 1, "m", "now"))
    conn.commit()
    conn.close()

    deleted = invalidate_chunk_sidecars(TARGET, facts_db=str(db), entity_db=str(db))
    assert deleted["extraction_log"] == 1
    assert "facts" not in deleted


def test_no_matching_rows(dbs):
    facts_db, entity_db = dbs
    deleted = invalidate_chunk_sidecars("doc_cccccccccccc", facts_db=facts_db, entity_db=entity_db)
    assert deleted == {"extraction_log": 0, "facts": 0, "entity_index": 0}
