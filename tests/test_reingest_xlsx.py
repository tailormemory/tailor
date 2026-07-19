"""Guardie del runner scripts/maintenance/reingest_xlsx.py.

Due invarianti, entrambe verificate sugli EFFETTI (registry, checkpoint,
chiamate a upsert), non sugli exit code:

  (a) race entity_index rilevata DOPO il cleanup -> il file non arriva mai
      all'upsert, finisce in mark_file_failed e NON viene checkpointato;
  (b) il checkpoint e' (path, hash): hash stantio o riga legacy solo-path
      valgono come non-match e il file viene ri-processato.

Nessun test tocca db/: registry, checkpoint e log sono ridiretti su tmp_path e
chromadb e' sostituito da un doppio. Un PersistentClient vero su db/ mentre
gira il MCP e' esattamente il SIGSEGV che il runner esiste per evitare.
"""
from __future__ import annotations

import os
import sys
import types

import pytest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "maintenance"))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "ingest"))

import ingest_docs as ing                      # noqa: E402
import reingest_xlsx as rx                     # noqa: E402


SECTIONS = [{"text": "[Sheet: S]\nHeader: A | B\nx: B=1",
             "metadata": {"sheet": "S", "total_sheets": 1, "row_count": 1,
                          "prechunked": True}}]


def _make_xlsx(tmp_path, name="f.xlsx", content=b"payload"):
    """File finto: extract_excel e' sempre sostituito, conta solo l'hash."""
    p = tmp_path / name
    p.write_bytes(content)
    return str(p)


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    """Isola registry, checkpoint, log e watch folder su tmp_path."""
    saved: dict = {}
    monkeypatch.setattr(rx, "CHECKPOINT_PATH", str(tmp_path / "ck.done"))
    monkeypatch.setattr(rx, "LOGS_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr(ing, "WATCH_FOLDERS", [str(tmp_path)])
    monkeypatch.setattr(ing, "save_registry", lambda reg: saved.update(reg))
    return types.SimpleNamespace(tmp=tmp_path, saved=saved)


# ============================================================================
# (a) RACE entity_index DOPO IL CLEANUP
# ============================================================================
@pytest.fixture
def exec_env(sandbox, monkeypatch):
    """Monta un --execute con un solo file nel piano e tutto il resto finto."""
    path = _make_xlsx(sandbox.tmp)
    f = rx._build_file_info(path, str(sandbox.tmp), "h" * 64)
    plan = [(f, SECTIONS)]

    upserts: list = []
    monkeypatch.setattr(rx, "maintenance_state", lambda: (True, "test"))
    monkeypatch.setattr(rx, "_build_entity_index_running", lambda: (False, ""))
    monkeypatch.setattr(ing, "acquire_single_instance_lock",
                        lambda *a, **k: open(os.devnull))
    monkeypatch.setattr(ing, "load_registry", lambda: {})
    monkeypatch.setattr(rx, "classify",
                        lambda *a, **k: (plan, _empty_buckets(),
                                         {"chunk": 1, "chars": 1, "sezioni": 1,
                                          "candidati": 1}))
    monkeypatch.setattr(ing, "embed_chunks",
                        lambda chunks: ([{"ids": [c["chunk_id"] for c in chunks],
                                          "embeddings": [[0.0]] * len(chunks),
                                          "documents": [c["text"] for c in chunks],
                                          "metadatas": [c["metadata"] for c in chunks]}], 0))
    monkeypatch.setattr(ing, "_pre_upsert_cleanup",
                        lambda coll, f, reg: None)

    def _ingest(coll, batches, run_id=None):
        upserts.extend(batches)
        return sum(len(b["ids"]) for b in batches), 0

    monkeypatch.setattr(ing, "ingest_chunks", _ingest)

    fake_chromadb = types.ModuleType("chromadb")
    fake_chromadb.PersistentClient = lambda path: types.SimpleNamespace(
        get_collection=lambda name: object())
    monkeypatch.setitem(sys.modules, "chromadb", fake_chromadb)

    return types.SimpleNamespace(f=f, path=path, upserts=upserts,
                                 saved=sandbox.saved, tmp=sandbox.tmp)


def _empty_buckets() -> dict:
    b = {c: [] for c in ("header_aware", "fallback_only", "checkpoint")}
    for c in rx.WARN_CLASSES + rx.CHECKPOINT_WARN:
        b[c] = []
    return b


def test_entity_index_race_dopo_cleanup_aborta_senza_upsert(exec_env, monkeypatch):
    """Rebuild concorrente DURANTE il cleanup: nessun upsert, file failed.

    L'inode cambia quando parte il cleanup, non a una N-esima chiamata: legare
    il test al numero di check lo renderebbe falso al primo check aggiunto, e
    la finestra da coprire e' proprio "il rebuild e' avvenuto mentre
    invalidavo i sidecar".
    """
    state = {"inode": 1}
    monkeypatch.setattr(rx, "_entity_index_fingerprint",
                        lambda: (True, state["inode"]))

    def _cleanup_con_rebuild(coll, f, reg):
        state["inode"] = 999      # build_entity_index.py ricrea il db qui

    monkeypatch.setattr(ing, "_pre_upsert_cleanup", _cleanup_con_rebuild)

    rc = rx.execute(limit_files=None)

    assert rc == 3, "una race entity_index deve abortire il run"
    assert exec_env.upserts == [], "nessun upsert dopo una race post-cleanup"

    entry = exec_env.saved[exec_env.path]
    assert entry["status"] == "failed"
    assert entry["last_error"] == "entity_index_race"
    # L'hash NON e' quello corrente: il file deve restare ri-processabile.
    assert entry["hash"] == ""

    assert not os.path.exists(rx.CHECKPOINT_PATH) or \
        exec_env.path not in open(rx.CHECKPOINT_PATH).read(), \
        "un file colpito dalla race non va checkpointato"


def test_run_pulito_upserta_e_checkpointa_path_e_hash(exec_env, monkeypatch):
    """Controprova: senza race, upsert + checkpoint nel formato path\\thash."""
    monkeypatch.setattr(rx, "_entity_index_fingerprint", lambda: (True, 1))

    rc = rx.execute(limit_files=None)

    assert rc == 0
    assert len(exec_env.upserts) == 1
    assert exec_env.saved[exec_env.path]["hash"] == exec_env.f["hash"]
    assert open(rx.CHECKPOINT_PATH).read().strip() == \
        f"{exec_env.path}\t{exec_env.f['hash']}"


# ============================================================================
# (b) CHECKPOINT (path, hash)
# ============================================================================
def _classify_one(path, registry_hash, done):
    return rx.classify({path: {"hash": registry_hash}}, done, verbose=False)


@pytest.fixture
def classify_env(sandbox, monkeypatch):
    monkeypatch.setattr(ing, "extract_excel", lambda p: SECTIONS)
    path = _make_xlsx(sandbox.tmp)
    return types.SimpleNamespace(path=path, tmp=sandbox.tmp,
                                 hash=ing.file_hash(path))


def test_checkpoint_hash_corrente_salta(classify_env):
    plan, buckets, _ = _classify_one(
        classify_env.path, classify_env.hash,
        {classify_env.path: classify_env.hash})

    assert plan == [], "hash coincidente -> gia' fatto, si salta"
    assert len(buckets["checkpoint"]) == 1
    assert buckets["checkpoint_non_match"] == []


def test_checkpoint_hash_stantio_riprocessa(classify_env):
    plan, buckets, _ = _classify_one(
        classify_env.path, classify_env.hash,
        {classify_env.path: "0" * 64})

    assert len(plan) == 1, "hash diverso -> il contenuto attuale non e' mai stato fatto"
    assert buckets["checkpoint"] == []
    assert "hash cambiato" in buckets["checkpoint_non_match"][0][1]


def test_checkpoint_legacy_solo_path_riprocessa_con_warning(classify_env):
    """Riga senza \\t: non prova il contenuto, quindi non e' un match."""
    plan, buckets, _ = _classify_one(
        classify_env.path, classify_env.hash, {classify_env.path: ""})

    assert len(plan) == 1
    assert buckets["checkpoint"] == []
    assert "legacy" in buckets["checkpoint_non_match"][0][1]


def test_load_checkpoint_parsa_entrambi_i_formati(sandbox, monkeypatch):
    with open(rx.CHECKPOINT_PATH, "w") as fh:
        fh.write("/a/b.xlsx\tdeadbeef\n")
        fh.write("/legacy/c.xlsx\n")
        fh.write("\n")

    assert rx.load_checkpoint() == {"/a/b.xlsx": "deadbeef",
                                    "/legacy/c.xlsx": ""}
