"""
_pre_upsert_cleanup — pulizia KB + sidecar prima di ogni upsert.

Copre il caso scoperto dal path `new_files` (usato da `--full`): file gia' in
KB con lo STESSO hash, ri-processato da un estrattore che produce boundary
diversi. Senza cleanup i chunk_id vengono riusati con testo nuovo, e
extraction_log fa da gate di skip permanente per il nightly.
"""

import os
import sqlite3
import sys

import pytest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "ingest"))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "lib"))

import chunk_sidecar  # noqa: E402
import ingest_docs  # noqa: E402

CURRENT = "a" * 64
OLD = "b" * 64


class FakeCollection:
    """Minimo indispensabile: get(where=conv_id) + delete(ids)."""

    def __init__(self, ids_by_conv=None, fail_on=None):
        self.ids_by_conv = dict(ids_by_conv or {})
        self.fail_on = fail_on
        self.deleted = []

    def get(self, where=None, include=None):
        conv = where["conv_id"]
        if self.fail_on == conv:
            raise RuntimeError("chroma down")
        return {"ids": list(self.ids_by_conv.get(conv, []))}

    def delete(self, ids=None):
        self.deleted.extend(ids)
        for conv, existing in self.ids_by_conv.items():
            self.ids_by_conv[conv] = [i for i in existing if i not in ids]


@pytest.fixture
def sidecar_dbs(tmp_path, monkeypatch):
    facts_db = tmp_path / "facts.sqlite3"
    entity_db = tmp_path / "entity_index.sqlite3"

    conn = sqlite3.connect(facts_db)
    conn.executescript(
        "CREATE TABLE extraction_log (chunk_id TEXT PRIMARY KEY, facts_count INTEGER,"
        " model TEXT, extracted_at TEXT);"
        "CREATE TABLE facts (id INTEGER PRIMARY KEY AUTOINCREMENT, chunk_id TEXT NOT NULL,"
        " fact TEXT NOT NULL, created_at TEXT NOT NULL);")
    for h in (CURRENT[:12], OLD[:12]):
        for i in range(2):
            cid = f"doc_{h}_chunk_{i:04d}"
            conn.execute("INSERT INTO extraction_log VALUES (?,?,?,?)", (cid, 1, "m", "now"))
            conn.execute("INSERT INTO facts (chunk_id, fact, created_at) VALUES (?,?,?)",
                         (cid, "vecchio fatto", "now"))
    conn.commit()
    conn.close()

    conn = sqlite3.connect(entity_db)
    conn.executescript(
        "CREATE TABLE entity_index (entity TEXT, entity_type TEXT, chunk_id TEXT NOT NULL);")
    for h in (CURRENT[:12], OLD[:12]):
        conn.execute("INSERT INTO entity_index VALUES (?,?,?)",
                     ("X", "person", f"doc_{h}_chunk_0000"))
    conn.commit()
    conn.close()

    monkeypatch.setattr(chunk_sidecar, "FACTS_DB_PATH", str(facts_db))
    monkeypatch.setattr(chunk_sidecar, "ENTITY_DB_PATH", str(entity_db))
    return str(facts_db), str(entity_db)


def _count(db, table, prefix):
    conn = sqlite3.connect(db)
    n = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE chunk_id LIKE ?",
                     (f"{prefix}%",)).fetchone()[0]
    conn.close()
    return n


def test_new_file_same_hash_still_cleaned(sidecar_dbs):
    """Il caso HIGH: `--full` su un file gia' in KB, hash invariato.

    Il registry contiene lo stesso hash (quindi nessun "old_hash diverso"), ma
    delete e invalidation devono comunque scattare.
    """
    facts_db, entity_db = sidecar_dbs
    conv = f"doc_{CURRENT[:12]}"
    collection = FakeCollection({conv: [f"{conv}_chunk_0000", f"{conv}_chunk_0001"]})
    f = {"filepath": "/docs/x.xlsx", "hash": CURRENT}
    registry = {"/docs/x.xlsx": {"hash": CURRENT}}

    ingest_docs._pre_upsert_cleanup(collection, f, registry)

    assert sorted(collection.deleted) == [f"{conv}_chunk_0000", f"{conv}_chunk_0001"]
    assert _count(facts_db, "extraction_log", conv) == 0
    assert _count(facts_db, "facts", conv) == 0
    assert _count(entity_db, "entity_index", conv) == 0


def test_new_file_absent_from_kb_is_noop(sidecar_dbs):
    """File davvero nuovo: nessun chunk da rimuovere, nessun errore."""
    facts_db, _ = sidecar_dbs
    collection = FakeCollection()
    f = {"filepath": "/docs/nuovo.xlsx", "hash": "c" * 64}

    ingest_docs._pre_upsert_cleanup(collection, f, {})

    assert collection.deleted == []
    # I documenti estranei restano intatti.
    assert _count(facts_db, "extraction_log", f"doc_{CURRENT[:12]}") == 2


def test_modified_file_cleans_both_hashes(sidecar_dbs):
    facts_db, entity_db = sidecar_dbs
    cur_conv, old_conv = f"doc_{CURRENT[:12]}", f"doc_{OLD[:12]}"
    collection = FakeCollection({
        cur_conv: [f"{cur_conv}_chunk_0000"],
        old_conv: [f"{old_conv}_chunk_0000"],
    })
    f = {"filepath": "/docs/x.xlsx", "hash": CURRENT}
    registry = {"/docs/x.xlsx": {"hash": OLD}}

    ingest_docs._pre_upsert_cleanup(collection, f, registry)

    assert sorted(collection.deleted) == [f"{cur_conv}_chunk_0000", f"{old_conv}_chunk_0000"]
    for conv in (cur_conv, old_conv):
        assert _count(facts_db, "extraction_log", conv) == 0
        assert _count(entity_db, "entity_index", conv) == 0


def test_same_hash_not_processed_twice(sidecar_dbs):
    """old_hash == hash corrente: una sola passata, non due."""
    conv = f"doc_{CURRENT[:12]}"
    collection = FakeCollection({conv: [f"{conv}_chunk_0000"]})
    f = {"filepath": "/docs/x.xlsx", "hash": CURRENT}

    ingest_docs._pre_upsert_cleanup(collection, f, {"/docs/x.xlsx": {"hash": CURRENT}})

    assert collection.deleted == [f"{conv}_chunk_0000"]


def test_delete_failure_propagates(sidecar_dbs):
    """Fail-loud: Chroma giu' -> eccezione, il caller salta il file."""
    conv = f"doc_{CURRENT[:12]}"
    collection = FakeCollection({conv: [f"{conv}_chunk_0000"]}, fail_on=conv)
    f = {"filepath": "/docs/x.xlsx", "hash": CURRENT}

    with pytest.raises(RuntimeError, match="chroma down"):
        ingest_docs._pre_upsert_cleanup(collection, f, {})


def test_sidecar_failure_propagates(tmp_path, monkeypatch):
    """Fail-loud anche sui sidecar: extraction_log non ripulito = chunk
    invisibile al nightly per sempre."""
    corrupt = tmp_path / "facts.sqlite3"
    corrupt.write_bytes(b"non sqlite" * 50)
    monkeypatch.setattr(chunk_sidecar, "FACTS_DB_PATH", str(corrupt))
    monkeypatch.setattr(chunk_sidecar, "ENTITY_DB_PATH", str(tmp_path / "assente.sqlite3"))

    collection = FakeCollection()
    f = {"filepath": "/docs/x.xlsx", "hash": CURRENT}

    with pytest.raises(sqlite3.DatabaseError):
        ingest_docs._pre_upsert_cleanup(collection, f, {})
