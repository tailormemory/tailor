"""
Ordine del ciclo di processing: extract -> cleanup -> upsert.

Un documento gia' indicizzato deve sparire dalla KB SOLO se sostituito da
un'estrazione riuscita. Se l'estrazione fallisce o produce vuoto, chunk e
sidecar restano intatti e il file viene ritentato al giro successivo.

Il test guida `main()` stubbando le dipendenze esterne (scan, chroma,
estrattore, registry su disco): l'ordine delle operazioni e' proprio la cosa
che i test unitari sui singoli helper non possono osservare.
"""

import json
import os
import sqlite3
import sys

import pytest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "ingest"))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "lib"))

import chunk_sidecar  # noqa: E402
import ingest_docs  # noqa: E402

HASH_OLD = "b" * 64
HASH_NEW = "c" * 64
CONV_OLD = f"doc_{HASH_OLD[:12]}"
CONV_NEW = f"doc_{HASH_NEW[:12]}"


class FakeCollection:
    def __init__(self, ids_by_conv):
        self.ids_by_conv = {k: list(v) for k, v in ids_by_conv.items()}
        self.upserted = []

    def get(self, where=None, include=None):
        return {"ids": list(self.ids_by_conv.get(where["conv_id"], []))}

    def delete(self, ids=None):
        for conv, existing in self.ids_by_conv.items():
            self.ids_by_conv[conv] = [i for i in existing if i not in ids]

    def count(self):
        return sum(len(v) for v in self.ids_by_conv.values())


@pytest.fixture
def env(tmp_path, monkeypatch):
    """File gia' in KB (hash vecchio) + sidecar popolati + registry su disco."""
    facts_db = tmp_path / "facts.sqlite3"
    conn = sqlite3.connect(facts_db)
    conn.executescript(
        "CREATE TABLE extraction_log (chunk_id TEXT PRIMARY KEY, facts_count INTEGER,"
        " model TEXT, extracted_at TEXT);"
        "CREATE TABLE facts (id INTEGER PRIMARY KEY AUTOINCREMENT, chunk_id TEXT NOT NULL,"
        " fact TEXT NOT NULL, created_at TEXT NOT NULL);")
    for i in range(2):
        cid = f"{CONV_OLD}_chunk_{i:04d}"
        conn.execute("INSERT INTO extraction_log VALUES (?,?,?,?)", (cid, 1, "m", "now"))
        conn.execute("INSERT INTO facts (chunk_id, fact, created_at) VALUES (?,?,?)",
                     (cid, "fatto vecchio", "now"))
    conn.commit()
    conn.close()

    entity_db = tmp_path / "entity_index.sqlite3"
    conn = sqlite3.connect(entity_db)
    conn.executescript(
        "CREATE TABLE entity_index (entity TEXT, entity_type TEXT, chunk_id TEXT NOT NULL);")
    conn.execute("INSERT INTO entity_index VALUES (?,?,?)",
                 ("X", "person", f"{CONV_OLD}_chunk_0000"))
    conn.commit()
    conn.close()

    monkeypatch.setattr(chunk_sidecar, "FACTS_DB_PATH", str(facts_db))
    monkeypatch.setattr(chunk_sidecar, "ENTITY_DB_PATH", str(entity_db))

    registry_file = tmp_path / "registry.json"
    registry_file.write_text(json.dumps({
        "/docs/x.pdf": {"hash": HASH_OLD, "filename": "x.pdf", "chunks": 2},
    }), encoding="utf-8")
    monkeypatch.setattr(ingest_docs, "REGISTRY_FILE", str(registry_file))
    monkeypatch.setattr(ingest_docs, "LOCK_FILE", str(tmp_path / "ingest.lock"))

    collection = FakeCollection({CONV_OLD: [f"{CONV_OLD}_chunk_0000", f"{CONV_OLD}_chunk_0001"]})

    class FakeClient:
        def __init__(self, path=None):
            pass

        def get_collection(self, name=None):
            return collection

    monkeypatch.setattr(ingest_docs.chromadb, "PersistentClient", FakeClient)
    monkeypatch.setattr(ingest_docs, "WATCH_FOLDERS", [])
    monkeypatch.setattr(ingest_docs, "scan_folders", lambda: [{
        "filepath": "/docs/x.pdf", "filename": "x.pdf", "rel_path": "x.pdf",
        "extension": ".pdf", "size_kb": 10.0, "modified_date": "2026-07-19",
        "modified_ts": 0, "category": "",
    }])
    monkeypatch.setattr(ingest_docs, "file_hash", lambda path: HASH_NEW)
    monkeypatch.setattr(sys, "argv", ["ingest_docs.py"])

    return {
        "facts_db": str(facts_db), "entity_db": str(entity_db),
        "registry_file": registry_file, "collection": collection,
    }


def _count(db, table, prefix):
    conn = sqlite3.connect(db)
    n = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE chunk_id LIKE ?",
                     (f"{prefix}%",)).fetchone()[0]
    conn.close()
    return n


def _assert_old_state_intact(env):
    assert env["collection"].ids_by_conv[CONV_OLD] == [
        f"{CONV_OLD}_chunk_0000", f"{CONV_OLD}_chunk_0001"]
    assert _count(env["facts_db"], "extraction_log", CONV_OLD) == 2
    assert _count(env["facts_db"], "facts", CONV_OLD) == 2
    assert _count(env["entity_db"], "entity_index", CONV_OLD) == 1


def test_empty_extraction_leaves_kb_and_sidecars_intact(env, monkeypatch):
    monkeypatch.setattr(ingest_docs, "extract_text", lambda path, doctype=None: [])
    monkeypatch.setattr(ingest_docs, "ingest_chunks",
                        lambda *a, **k: pytest.fail("upsert non deve avvenire"))

    ingest_docs.main()

    _assert_old_state_intact(env)
    reg = json.loads(env["registry_file"].read_text(encoding="utf-8"))["/docs/x.pdf"]
    assert reg["status"] == "failed"
    assert reg["fail_count"] == 1
    # L'hash NUOVO non va registrato: altrimenti il file risulterebbe
    # "unchanged" al giro dopo e non verrebbe mai ritentato.
    assert reg["hash"] == HASH_OLD


def test_failed_extraction_leaves_kb_and_sidecars_intact(env, monkeypatch):
    def boom(path, doctype=None):
        raise RuntimeError("estrattore esploso")

    monkeypatch.setattr(ingest_docs, "extract_text", boom)
    monkeypatch.setattr(ingest_docs, "ingest_chunks",
                        lambda *a, **k: pytest.fail("upsert non deve avvenire"))

    with pytest.raises(RuntimeError, match="estrattore esploso"):
        ingest_docs.main()

    _assert_old_state_intact(env)


def test_failed_file_is_retried_next_run(env, monkeypatch):
    """Dopo un fallimento il file deve tornare processabile, non 'unchanged'."""
    monkeypatch.setattr(ingest_docs, "extract_text", lambda path, doctype=None: [])
    monkeypatch.setattr(ingest_docs, "ingest_chunks", lambda *a, **k: (0, 0))
    ingest_docs.main()

    registry = json.loads(env["registry_file"].read_text(encoding="utf-8"))
    files = ingest_docs.scan_folders()
    new, modified, unchanged = ingest_docs.classify_files(files, registry)

    assert unchanged == []
    assert len(new) + len(modified) == 1


def test_partial_upsert_is_registered_as_failed(env, monkeypatch):
    """Upsert parziale: il registry NON deve registrare successo, altrimenti
    il file risulta 'unchanged' e il documento monco si cristallizza in KB."""
    monkeypatch.setattr(ingest_docs, "extract_text", lambda path, doctype=None: [
        {"text": "sezione uno", "metadata": {"page": 1}},
        {"text": "sezione due", "metadata": {"page": 2}},
    ])
    monkeypatch.setattr(ingest_docs, "embed_chunks", lambda chunks: ([{
        "ids": [c["chunk_id"] for c in chunks],
        "embeddings": [[0.0]] * len(chunks),
        "documents": [c["text"] for c in chunks],
        "metadatas": [c["metadata"] for c in chunks],
    }], 0))
    # Un chunk su due passa.
    monkeypatch.setattr(ingest_docs, "ingest_chunks", lambda c, b, run_id=None: (1, 1))

    with pytest.raises(SystemExit):  # total_errors > 0 -> exit non-zero
        ingest_docs.main()

    reg = json.loads(env["registry_file"].read_text(encoding="utf-8"))["/docs/x.pdf"]
    assert reg["status"] == "failed"
    assert reg["hash"] == HASH_OLD          # hash dell'ultima ingest riuscita
    assert reg["fail_count"] == 1
    assert "partial_upsert" in reg["last_error"]

    # E al giro dopo il file torna processabile.
    files = ingest_docs.scan_folders()
    _, modified, unchanged = ingest_docs.classify_files(
        files, json.loads(env["registry_file"].read_text(encoding="utf-8")))
    assert unchanged == []
    assert len(modified) == 1


def test_embedding_failure_leaves_kb_intact(env, monkeypatch):
    """Embedding prima del cleanup: se fallisce, la KB non e' stata toccata."""
    monkeypatch.setattr(ingest_docs, "extract_text", lambda path, doctype=None: [
        {"text": "contenuto", "metadata": {"page": 1}}])
    monkeypatch.setattr(ingest_docs, "embed_chunks", lambda chunks: ([], len(chunks)))
    monkeypatch.setattr(ingest_docs, "ingest_chunks",
                        lambda *a, **k: pytest.fail("upsert non deve avvenire"))

    with pytest.raises(SystemExit):
        ingest_docs.main()

    _assert_old_state_intact(env)
    reg = json.loads(env["registry_file"].read_text(encoding="utf-8"))["/docs/x.pdf"]
    assert reg["status"] == "failed"
    assert reg["hash"] == HASH_OLD


def test_single_instance_lock(tmp_path):
    """Seconda istanza: esce, non attende."""
    lock = str(tmp_path / "ingest.lock")

    first = ingest_docs.acquire_single_instance_lock(lock)
    assert first is not None

    second = ingest_docs.acquire_single_instance_lock(lock)
    assert second is None, "la seconda istanza deve fallire l'acquisizione"

    # Rilasciato il primo, una nuova istanza puo' partire.
    first.close()
    third = ingest_docs.acquire_single_instance_lock(lock)
    assert third is not None
    third.close()


def test_main_exits_when_lock_held(env, monkeypatch, capsys):
    """main() con lock gia' preso: exit non-zero, niente scan ne' scrittura."""
    held = ingest_docs.acquire_single_instance_lock(ingest_docs.LOCK_FILE)
    assert held is not None
    monkeypatch.setattr(ingest_docs, "scan_folders",
                        lambda: pytest.fail("non deve nemmeno scansionare"))

    with pytest.raises(SystemExit) as exc:
        ingest_docs.main()

    assert exc.value.code == 1
    assert "gia' in esecuzione" in capsys.readouterr().out
    held.close()


def test_successful_extraction_replaces_document(env, monkeypatch):
    """Il contraltare: con estrazione valida il vecchio SPARISCE davvero."""
    monkeypatch.setattr(ingest_docs, "extract_text", lambda path, doctype=None: [
        {"text": "contenuto nuovo", "metadata": {"page": 1}}])

    seen = {}

    def fake_ingest(collection, batches, run_id=None):
        # Al momento dell'upsert i chunk vecchi devono essere GIA' spariti.
        seen["old_ids_at_upsert"] = list(collection.ids_by_conv.get(CONV_OLD, []))
        ids = [i for b in batches for i in b["ids"]]
        collection.upserted.extend(ids)
        return len(ids), 0

    monkeypatch.setattr(ingest_docs, "ingest_chunks", fake_ingest)
    monkeypatch.setattr(ingest_docs, "embed_chunks", lambda chunks: ([{
        "ids": [c["chunk_id"] for c in chunks],
        "embeddings": [[0.0]] * len(chunks),
        "documents": [c["text"] for c in chunks],
        "metadatas": [c["metadata"] for c in chunks],
    }], 0))

    ingest_docs.main()

    assert seen["old_ids_at_upsert"] == []          # delete PRIMA dell'upsert
    assert env["collection"].upserted                # e l'upsert e' avvenuto
    assert _count(env["facts_db"], "extraction_log", CONV_OLD) == 0
    assert _count(env["entity_db"], "entity_index", CONV_OLD) == 0
    reg = json.loads(env["registry_file"].read_text(encoding="utf-8"))["/docs/x.pdf"]
    assert reg["hash"] == HASH_NEW
    assert "status" not in reg
