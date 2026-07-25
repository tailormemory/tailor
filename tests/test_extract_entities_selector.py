"""Selettore di scripts/enrichment/extract_entities.py.

Fissa la convenzione dichiarata nella docstring del modulo — "processa tutto
cio' che non ha entities_extracted == 1" — su due meccanismi distinti, perche'
sono due i punti che possono nascondere un chunk:

1. il post-filtro sui metadata in get_chunks_to_process (campo assente / 0 / 1);
2. il checkpoint su file, che filtra PER ID in main().

Il caso 2 e' quello che ha morso: gli id dei chunk sono deterministici, quindi
una re-ingestion che riscrive i metadata (campo entities_extracted sparito)
lascia l'id nel checkpoint di un run vecchio. Misura del 25/07/2026: 10.807
chunk senza il campo, 10.797 gia' presenti nel checkpoint da 206.503 id — coda
effettiva 10. prune_stale_checkpoint pota quegli id: la KB e' la fonte di
verita', il checkpoint e' solo un acceleratore di resume.

Nessuna rete, nessun chromadb reale: la collection e' un fake che rispetta la
sola superficie usata dal selettore (count + get paginata).
"""

from __future__ import annotations

import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from scripts.enrichment import extract_entities as ee  # noqa: E402


# ============================================================
# FAKE COLLECTION
# ============================================================


class FakeCollection:
    """Superficie minima consumata da get_chunks_to_process: count() + get()
    paginata con include=[documents, metadatas, embeddings]."""

    def __init__(self, chunks):
        self._chunks = list(chunks)

    def count(self):
        return len(self._chunks)

    def get(self, include=None, limit=None, offset=0):
        page = self._chunks[offset:offset + (limit or len(self._chunks))]
        return {
            "ids": [c["id"] for c in page],
            "documents": [c.get("text", "testo") for c in page],
            "metadatas": [c["meta"] for c in page],
            "embeddings": [c.get("emb", [0.0]) for c in page],
        }


def _collection():
    """Tre chunk, uno per stato del campo: assente / 0 / 1."""
    return FakeCollection([
        {"id": "assente", "meta": {"source": "document"}},
        {"id": "zero", "meta": {"source": "document", "entities_extracted": 0}},
        {"id": "uno", "meta": {"source": "document", "entities_extracted": 1}},
    ])


# ============================================================
# 1. POST-FILTRO SUI METADATA
# ============================================================


@pytest.mark.parametrize("meta, atteso", [
    ({}, True),                                  # campo assente
    ({"entities_extracted": 0}, True),           # scritto dai runner di re-ingest
    ({"entities_extracted": 1}, False),          # estratto: unico caso escluso
])
def test_needs_extraction_semantica(meta, atteso):
    assert ee._needs_extraction(meta) is atteso


def test_selezione_incrementale_campo_zero_e_assente(capsys):
    to_process = ee.get_chunks_to_process(_collection())

    assert [c["id"] for c in to_process] == ["assente", "zero"]
    # il chunk gia' estratto non e' scartato in silenzio: e' contato
    assert "Already processed: 1" in capsys.readouterr().out


def test_full_mode_ignora_il_campo(capsys):
    to_process = ee.get_chunks_to_process(_collection(), full_mode=True)

    assert [c["id"] for c in to_process] == ["assente", "zero", "uno"]
    assert "Already processed: 0" in capsys.readouterr().out


def test_source_filter_ortogonale_al_campo():
    coll = FakeCollection([
        {"id": "doc", "meta": {"source": "document"}},
        {"id": "mail", "meta": {"source": "email"}},
    ])

    to_process = ee.get_chunks_to_process(coll, source_filter="email")

    assert [c["id"] for c in to_process] == ["mail"]


# ============================================================
# 2. CHECKPOINT STALE
# ============================================================


def test_prune_stale_checkpoint_pota_solo_gli_id_da_processare():
    kept, stale = ee.prune_stale_checkpoint(
        ["gia-estratto", "stale-1", "stale-2"],
        {"stale-1", "stale-2", "mai-visto"},
    )

    assert kept == {"gia-estratto"}
    assert stale == {"stale-1", "stale-2"}


def test_checkpoint_stale_non_nasconde_i_chunk_non_estratti():
    """Il caso reale: id gia' nel checkpoint, metadata che dicono non estratto."""
    chunks = ee.get_chunks_to_process(_collection())
    checkpoint_ids = {"assente", "zero", "uno"}      # run vecchio: li aveva visti tutti

    kept, stale = ee.prune_stale_checkpoint(checkpoint_ids, {c["id"] for c in chunks})
    # stessa espressione di main() dopo il pruning
    remaining = [c for c in chunks if c["id"] not in kept]

    assert stale == {"assente", "zero"}
    assert [c["id"] for c in remaining] == ["assente", "zero"]
    # l'id del chunk gia' estratto resta nel checkpoint: pruning mirato, non reset
    assert kept == {"uno"}


def test_prune_stale_checkpoint_non_muta_l_input():
    checkpoint_ids = {"a", "b"}

    kept, _ = ee.prune_stale_checkpoint(checkpoint_ids, {"a"})

    assert checkpoint_ids == {"a", "b"}
    assert kept == {"b"}
