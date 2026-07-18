"""source_filter applicato verbatim, senza whitelist hardcoded.

Regressione per il bug: la whitelist `("document", "chatgpt", "claude", "email")`
sui rami semantic (kb_search e _hybrid_collect STEP 1) faceva cadere il filtro
per qualunque source fuori elenco (`gemini`, `telegram`, ...) → la query partiva
SENZA `where`, restituendo risultati NON filtrati invece che zero. Falso positivo
silenzioso, e opposto al ramo lessicale che filtra qualunque valore (STEP 4).

Semantica attesa, uniforme sui 3 rami: source non vuoto → filtro applicato
verbatim; source ignoto → zero risultati, mai fallthrough.

Nessun ChromaDB/Ollama reale: get_embedding e gli accessor collection sono
monkeypatchati (stesso pattern di test_cosine_relevance.py).
"""

from __future__ import annotations

import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

import mcp_server  # noqa: E402


class _RecordingCollection:
    """query() registra i kwargs ricevuti e ritorna un risultato vuoto.

    Il vuoto basta: l'asserzione e' sui kwargs passati a Chroma, non sull'output.
    """

    def __init__(self):
        self.query_kwargs = []

    def query(self, **kwargs):
        self.query_kwargs.append(kwargs)
        return {"documents": [[]], "metadatas": [[]], "distances": [[]], "ids": [[]]}

    def get(self, **kwargs):
        raise AssertionError("collection.get() non atteso in questo test")


@pytest.fixture
def _recording_env(monkeypatch, tmp_path):
    """Collection registrante + DB_DIR isolato (niente entity_index / lexical_index)."""
    stub = _RecordingCollection()
    monkeypatch.setattr(mcp_server, "DB_DIR", str(tmp_path))
    monkeypatch.setattr(mcp_server, "get_embedding", lambda q: [0.5, 0.5, 0.5])
    monkeypatch.setattr(mcp_server, "get_collection", lambda: stub)
    monkeypatch.setattr(mcp_server, "_get_collection_readonly", lambda: stub)
    return stub


# ── source fuori dalla whitelist storica → filtro comunque applicato ──────────

@pytest.mark.parametrize("source", ["gemini", "telegram"])
def test_kb_search_filtra_source_fuori_whitelist_storica(_recording_env, source):
    stub = _recording_env

    mcp_server.kb_search("rinnovo contratto", n_results=5, source_filter=source)

    assert len(stub.query_kwargs) == 1
    # il punto: where PRESENTE. Pre-fix mancava del tutto → risultati unfiltered.
    assert stub.query_kwargs[0].get("where") == {"source": source}


@pytest.mark.parametrize("source", ["gemini", "telegram"])
def test_hybrid_collect_filtra_source_fuori_whitelist_storica(_recording_env, source):
    stub = _recording_env

    # query tutta minuscola → nessun candidato entity → isola il ramo semantic
    mcp_server._hybrid_collect(
        "rinnovo contratto annuale", n_results=5, source_filter=source,
        include_superseded=True, diag=False,
    )

    assert len(stub.query_kwargs) == 1
    assert stub.query_kwargs[0].get("where") == {"source": source}


# ── source nella whitelist storica → invariato (nessuna regressione) ──────────

@pytest.mark.parametrize("source", ["document", "chatgpt", "claude", "email"])
def test_kb_search_source_noti_invariati(_recording_env, source):
    stub = _recording_env

    mcp_server.kb_search("rinnovo contratto", n_results=5, source_filter=source)

    assert stub.query_kwargs[0].get("where") == {"source": source}


@pytest.mark.parametrize("source", ["document", "chatgpt", "claude", "email"])
def test_hybrid_collect_source_noti_invariati(_recording_env, source):
    stub = _recording_env

    mcp_server._hybrid_collect(
        "rinnovo contratto annuale", n_results=5, source_filter=source,
        include_superseded=True, diag=False,
    )

    assert stub.query_kwargs[0].get("where") == {"source": source}


# ── source vuoto → nessun where (ricerca su tutta la KB) ──────────────────────

def test_kb_search_source_vuoto_nessun_where(_recording_env):
    stub = _recording_env

    mcp_server.kb_search("rinnovo contratto", n_results=5, source_filter="")

    assert "where" not in stub.query_kwargs[0]


def test_hybrid_collect_source_vuoto_nessun_where(_recording_env):
    stub = _recording_env

    mcp_server._hybrid_collect(
        "rinnovo contratto annuale", n_results=5, source_filter="",
        include_superseded=True, diag=False,
    )

    assert "where" not in stub.query_kwargs[0]
