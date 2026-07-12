"""Unit tests per il Livello A del design B2b (cosine scoring dei candidati entity).

Due blocchi:
1. `_cosine_relevance` — helper puro, testabile in isolamento (identici → 1.0,
   ortogonali → 0.0, opposti → clamp 0.0, non normalizzati → cos corretto, fallback → 0.0).
2. `_hybrid_collect` su query SENZA entity → STEP 3 non entra → `seen_ids` bit-identico
   al pre-patch (solo semantic), e `collection.get()` NON viene mai chiamato.

Nessun ChromaDB/Ollama reale: `get_embedding` e l'accessor collection sono monkeypatchati.
"""

from __future__ import annotations

import math
import os
import sys

import numpy as np
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

import mcp_server  # noqa: E402
from mcp_server import _cosine_relevance  # noqa: E402


# ── 1. helper puro _cosine_relevance ──────────────────────────────────────────

def test_vettori_identici_danno_uno():
    v = [0.1, 0.2, 0.3, -0.4]
    assert _cosine_relevance(v, v) == pytest.approx(1.0, abs=1e-9)


def test_vettori_ortogonali_danno_zero():
    assert _cosine_relevance([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0, abs=1e-9)


def test_vettori_opposti_clampati_a_zero():
    # cos = -1 → clamp a 0.0 (mai negativo, stessa scala di max(0, 1-dist))
    assert _cosine_relevance([1.0, 2.0, 3.0], [-1.0, -2.0, -3.0]) == 0.0


def test_non_normalizzati_cos_corretto():
    # [1,1] vs [2,0]: cos = 2 / (sqrt(2) * 2) = 1/sqrt(2) ≈ 0.7071.
    # La normalizzazione esplicita deve dare questo, NON il dot nudo (=2).
    got = _cosine_relevance([1.0, 1.0], [2.0, 0.0])
    assert got == pytest.approx(1.0 / math.sqrt(2), abs=1e-9)


def test_scala_invariante_alla_norma():
    # Riscalare un vettore non cambia il coseno.
    a = [0.3, -0.7, 0.5]
    b = [1.2, 0.4, -0.9]
    base = _cosine_relevance(a, b)
    scaled = _cosine_relevance([10 * x for x in a], [0.01 * x for x in b])
    assert scaled == pytest.approx(base, abs=1e-9)


def test_input_ndarray_come_da_chroma():
    # c_vec arriva da Chroma come numpy.ndarray: deve funzionare senza truth-test nudo.
    q = [1.0, 0.0, 0.0]
    c = np.asarray([1.0, 0.0, 0.0], dtype=float)
    assert _cosine_relevance(q, c) == pytest.approx(1.0, abs=1e-9)


@pytest.mark.parametrize("c_vec", [
    None,                              # vettore assente
    np.asarray([], dtype=float),       # vettore vuoto
    np.asarray([1.0, 2.0], dtype=float),  # dimensione inattesa (q ha 3 dim)
    np.zeros(3, dtype=float),          # norma nulla
])
def test_fallback_zero_mai_solleva(c_vec):
    q = [1.0, 2.0, 3.0]
    got = _cosine_relevance(q, c_vec)
    assert got == 0.0  # fallback esplicito, MAI 0.5, MAI eccezione


def test_query_none_fallback():
    assert _cosine_relevance(None, [1.0, 2.0]) == 0.0


# ── 2. _hybrid_collect: query senza entity → pool bit-identico ────────────────

class _StubCollection:
    """Collection minimale: query() ritorna semantic canned, get() è vietato."""

    def __init__(self, sem_results):
        self._sem = sem_results
        self.get_called = False

    def query(self, **kwargs):
        return self._sem

    def get(self, **kwargs):
        self.get_called = True
        raise AssertionError("collection.get() non deve essere chiamato senza entity")


@pytest.fixture
def _no_entity_env(monkeypatch):
    """get_embedding fisso + collection stub; entity DB neutralizzato via query lowercase."""
    stub_sem = {
        "documents": [["testo alpha", "testo beta"]],
        "metadatas": [[{"date": "2020-01-01"}, {"date": "2021-06-06"}]],
        "distances": [[0.1, 0.4]],
        "ids": [["id_alpha", "id_beta"]],
    }
    stub = _StubCollection(stub_sem)
    monkeypatch.setattr(mcp_server, "get_embedding", lambda q: [0.5, 0.5, 0.5])
    monkeypatch.setattr(mcp_server, "get_collection", lambda: stub)
    return stub, stub_sem


def test_query_senza_entity_pool_bit_identico(_no_entity_env):
    stub, stub_sem = _no_entity_env
    # query tutta minuscola → nessun candidato entity → STEP 3 non entra
    query = "quanto costa il rinnovo annuale del contratto"

    out = mcp_server._hybrid_collect(
        query, n_results=5, source_filter="",
        include_superseded=True, diag=False,
    )

    # get() mai chiamato: il ramo entity non ha toccato nulla
    assert stub.get_called is False

    # seen_ids identico a quello prodotto dal solo ramo semantic (invariato dal patch)
    expected = {}
    for doc, meta, dist, cid in zip(
        stub_sem["documents"][0], stub_sem["metadatas"][0],
        stub_sem["distances"][0], stub_sem["ids"][0],
    ):
        expected[cid] = {
            "doc": doc, "meta": meta, "score": max(0, 1 - dist),
            "source_type": "semantic", "date": meta.get("date", ""),
        }

    assert out["seen_ids"] == expected
    # nessun candidato entity materializzato
    assert all(v["source_type"] == "semantic" for v in out["seen_ids"].values())
