"""
Durabilita' del registry e del lockfile.

Due failure mode silenziosi: un `--status` concorrente che legge un
doc_registry.json troncato a meta' write, e una seconda istanza che azzera il
PID scritto dalla prima pur senza ottenere il lock.
"""

import json
import os
import sys

import pytest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "ingest"))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "lib"))

import ingest_docs  # noqa: E402

ORIGINAL = {"/docs/x.pdf": {"hash": "a" * 64, "filename": "x.pdf", "chunks": 3}}


@pytest.fixture
def registry_file(tmp_path, monkeypatch):
    path = tmp_path / "doc_registry.json"
    path.write_text(json.dumps(ORIGINAL, indent=2), encoding="utf-8")
    monkeypatch.setattr(ingest_docs, "REGISTRY_FILE", str(path))
    return path


# --- registry ---------------------------------------------------------------

def test_save_registry_roundtrip(registry_file):
    nuovo = {"/docs/y.pdf": {"hash": "b" * 64, "filename": "y.pdf", "chunks": 7}}
    ingest_docs.save_registry(nuovo)

    assert json.loads(registry_file.read_text(encoding="utf-8")) == nuovo
    assert not os.path.exists(str(registry_file) + ".tmp")


def test_interrupted_save_leaves_original_intact(registry_file, monkeypatch):
    """Crash a meta' scrittura: il file originale deve restare leggibile."""
    real_dump = json.dump

    def dump_then_die(obj, fp, **kwargs):
        real_dump({"parziale": True}, fp, **kwargs)
        fp.flush()
        raise OSError("disco pieno a meta' write")

    monkeypatch.setattr(ingest_docs.json, "dump", dump_then_die)

    with pytest.raises(OSError, match="disco pieno"):
        ingest_docs.save_registry({"/docs/nuovo.pdf": {"hash": "c" * 64}})

    # Il registry originale e' intatto e ancora parsabile.
    assert json.loads(registry_file.read_text(encoding="utf-8")) == ORIGINAL
    # E nessun .tmp parziale lasciato in giro.
    assert not os.path.exists(str(registry_file) + ".tmp")


def test_reader_never_sees_partial_file(registry_file):
    """os.replace e' atomico: un reader lock-free vede sempre un JSON intero.

    Si legge il file a ogni step della serializzazione; nessuna lettura deve
    fallire il parse.
    """
    grande = {f"/docs/f{i}.pdf": {"hash": str(i) * 64, "filename": f"f{i}.pdf"}
              for i in range(200)}

    real_dump = json.dump
    letture = []

    def dump_and_peek(obj, fp, **kwargs):
        real_dump(obj, fp, **kwargs)
        fp.flush()
        # Mentre il tmp e' ancora aperto e incompleto sul path definitivo non
        # e' successo nulla: il reader vede ancora il vecchio.
        letture.append(json.loads(registry_file.read_text(encoding="utf-8")))

    json.dump = dump_and_peek
    try:
        ingest_docs.save_registry(grande)
    finally:
        json.dump = real_dump

    assert letture == [ORIGINAL]
    assert json.loads(registry_file.read_text(encoding="utf-8")) == grande


# --- lockfile ---------------------------------------------------------------

def test_failed_acquire_preserves_pid_of_holder(tmp_path):
    """La seconda istanza non deve azzerare il PID scritto dalla prima."""
    lock = str(tmp_path / "ingest.lock")

    first = ingest_docs.acquire_single_instance_lock(lock)
    assert first is not None
    contenuto_prima = open(lock, encoding="utf-8").read()
    assert contenuto_prima.strip() == str(os.getpid())

    second = ingest_docs.acquire_single_instance_lock(lock)
    assert second is None

    contenuto_dopo = open(lock, encoding="utf-8").read()
    assert contenuto_dopo == contenuto_prima, "il PID del detentore e' stato troncato"

    first.close()


def test_lock_content_replaced_not_appended(tmp_path):
    """Acquisizioni successive riscrivono il PID, non lo accodano ('a+')."""
    lock = str(tmp_path / "ingest.lock")

    first = ingest_docs.acquire_single_instance_lock(lock)
    first.close()
    second = ingest_docs.acquire_single_instance_lock(lock)
    assert second is not None
    second.close()

    assert open(lock, encoding="utf-8").read() == f"{os.getpid()}\n"


def test_lock_created_when_missing(tmp_path):
    lock = str(tmp_path / "assente.lock")
    assert not os.path.exists(lock)

    fd = ingest_docs.acquire_single_instance_lock(lock)
    assert fd is not None
    assert os.path.exists(lock)
    fd.close()
