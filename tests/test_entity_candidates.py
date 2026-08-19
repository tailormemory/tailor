"""Stoplist sui candidati entity di _hybrid_collect STEP 2.

Il problema misurato il 19/08 sulle 12 query di eval/retrieval_corpus.jsonl: una
query che apre con una parola-funzione ("Mi dai i valori...", "Su cosa verte...",
"Come ho gestito...") la vede capitalizzata PER POSIZIONE e la promuove a
candidato entity. Da lì `LIKE '%Mi%' COLLATE NOCASE` matcha "$1 million",
"#MicrodosiBitcoin" e simili, e satura il cap di 100 chunk con rumore puro —
sottraendo slot ai candidati veri. 4 query su 12 producevano almeno un
candidato-stopword (Mi ×2, Su, Come).

Il trigramma è il caso peggiore: la sua condizione richiede solo w1 capitalizzato
(w2/w3 no), quindi genera anche "Mi dai i".

Nessun ChromaDB / entity_index / embedding reale: la collection e get_embedding
sono stub, e sqlite3.connect è sostituito da una connessione finta che REGISTRA
il parametro LIKE di ogni lookup invece di eseguirlo. Le asserzioni sono così sui
candidati che il codice di produzione interroga davvero, non su una copia della
logica di estrazione re-implementata nel test (che potrebbe divergere in
silenzio).
"""

from __future__ import annotations

import os
import sqlite3
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

import mcp_server  # noqa: E402


# ── doppi ────────────────────────────────────────────────────────────────────

class _StubCollection:
    """Risultato semantico vuoto: qui interessa solo il ramo entity (STEP 2)."""

    def query(self, **kwargs):
        return {"documents": [[]], "metadatas": [[]], "distances": [[]], "ids": [[]]}

    def get(self, **kwargs):
        return {"documents": [], "metadatas": [], "ids": []}


class _RecordingCursor:
    def __init__(self, sink):
        self._sink = sink

    def execute(self, sql, params=()):
        # Solo i lookup sull'entity index; il parametro è '%<candidato>%'.
        if "entity_index" in sql and params:
            self._sink.append(str(params[0]).strip("%"))
        return self

    def fetchall(self):
        return []

    def fetchone(self):
        return None


class _RecordingConnection:
    def __init__(self, sink):
        self._sink = sink

    def cursor(self):
        return _RecordingCursor(self._sink)

    def execute(self, sql, params=()):
        return _RecordingCursor(self._sink).execute(sql, params)

    def close(self):
        pass


@pytest.fixture
def candidates_of(monkeypatch, tmp_path):
    """Ritorna una callable query -> set dei candidati interrogati sull'index."""
    # entity_index.sqlite3 vuoto: serve solo a far passare os.path.exists().
    (tmp_path / "entity_index.sqlite3").write_bytes(b"")

    stub = _StubCollection()
    monkeypatch.setattr(mcp_server, "DB_DIR", str(tmp_path))
    monkeypatch.setattr(mcp_server, "get_embedding", lambda q: [0.1, 0.2, 0.3])
    monkeypatch.setattr(mcp_server, "get_collection", lambda: stub)
    monkeypatch.setattr(mcp_server, "_get_collection_readonly", lambda: stub)

    sink: list[str] = []
    monkeypatch.setattr(sqlite3, "connect", lambda *a, **k: _RecordingConnection(sink))

    def _run(query: str) -> set[str]:
        sink.clear()
        mcp_server._hybrid_collect(
            query, n_results=5, source_filter="",
            include_superseded=True, diag=False,
        )
        return set(sink)

    return _run


# ── i 4 casi del corpus ──────────────────────────────────────────────────────

def test_mi_unigramma_e_trigramma_scartati(candidates_of):
    got = candidates_of("Mi dai i valori delle analisi del sangue del 2025?")

    assert "Mi" not in got
    # Il trigramma: w1 capitalizzato basta a generarlo, la stoplist lo taglia.
    assert "Mi dai i" not in got


def test_su_scartato_entita_vere_conservate(candidates_of):
    got = candidates_of(
        "Su cosa verte il potenziale accordo fra YOUniversal e Blazemedia"
    )

    assert "Su" not in got
    assert "YOUniversal" in got
    assert "Blazemedia" in got


def test_come_scartato_nome_persona_conservato(candidates_of):
    got = candidates_of("Come ho gestito la redundancy di Alex Persehais?")

    assert "Come" not in got
    assert "Alex Persehais" in got


def test_sigla_tre_caratteri_sopravvive(candidates_of):
    """Presidio sulla soglia `len(clean) >= 2`: NON va alzata.

    Entità legittime corte esistono (AMG, IBM, BCE). Lo strumento contro il
    rumore è la stoplist, non una soglia più aggressiva sulla lunghezza — che
    ucciderebbe le sigle insieme alle stopword.
    """
    got = candidates_of("perché è saltato l accordo con AMG")

    assert "AMG" in got


# ── presidi collaterali ──────────────────────────────────────────────────────

def test_stopword_dentro_al_candidato_non_lo_uccide(candidates_of):
    """Il filtro guarda SOLO il primo token.

    "Yahoo da Nexify" contiene "da" in posizione 2: deve sopravvivere, altrimenti
    la stoplist smetterebbe di essere chirurgica e taglierebbe i multi-gramma
    legittimi.
    """
    got = candidates_of(
        "Riassumi il processo do novation del contratto di Yahoo "
        "da Nexify a YOUniversal"
    )

    assert "Riassumi" not in got
    assert "Riassumi il processo" not in got
    assert "Yahoo da Nexify" in got
    assert "Nexify a YOUniversal" in got


def test_query_senza_stopword_iniziale_invariata(candidates_of):
    """Nessuna regressione sulle query già pulite."""
    got = candidates_of("chi ha l'usufrutto sulla casa di Ninfa")

    assert got == {"Ninfa"}


# ── mutation check ───────────────────────────────────────────────────────────

def test_mutation_senza_stoplist_i_casi_del_corpus_rientrano(candidates_of, monkeypatch):
    """Svuotando la stoplist i tre casi devono tornare a fallire.

    Senza questo, un refactor che rende il filtro un no-op (stoplist svuotata,
    confronto sul token sbagliato, filtro spostato dopo il lookup) passerebbe
    inosservato: i test sopra asseriscono assenze, e un'assenza si ottiene anche
    per caso. Qui si verifica che siano le stopword a produrla.
    """
    monkeypatch.setattr(mcp_server, "ENTITY_CANDIDATE_STOPWORDS", frozenset())

    assert "Mi" in candidates_of("Mi dai i valori delle analisi del sangue del 2025?")
    assert "Mi dai i" in candidates_of(
        "Mi dai i valori delle analisi del sangue del 2025?"
    )
    assert "Su" in candidates_of(
        "Su cosa verte il potenziale accordo fra YOUniversal e Blazemedia"
    )
    assert "Come" in candidates_of("Come ho gestito la redundancy di Alex Persehais?")


# ── FIX A: il cap non deve più tagliare il materiale recente ─────────────────

def test_lookup_entity_ordina_per_chunk_id_desc(monkeypatch, tmp_path):
    """chunk_id cresce col tempo: con ASC il LIMIT 100 scartava i chunk recenti."""
    (tmp_path / "entity_index.sqlite3").write_bytes(b"")

    stub = _StubCollection()
    monkeypatch.setattr(mcp_server, "DB_DIR", str(tmp_path))
    monkeypatch.setattr(mcp_server, "get_embedding", lambda q: [0.1, 0.2, 0.3])
    monkeypatch.setattr(mcp_server, "get_collection", lambda: stub)
    monkeypatch.setattr(mcp_server, "_get_collection_readonly", lambda: stub)

    statements: list[str] = []

    class _SqlCursor(_RecordingCursor):
        def execute(self, sql, params=()):
            statements.append(sql)
            return self

    class _SqlConnection(_RecordingConnection):
        def cursor(self):
            return _SqlCursor([])

    monkeypatch.setattr(sqlite3, "connect", lambda *a, **k: _SqlConnection([]))

    mcp_server._hybrid_collect(
        "che contesto avevamo con Visymo", n_results=5, source_filter="",
        include_superseded=True, diag=False,
    )

    entity_sql = [s for s in statements if "entity_index" in s]
    assert entity_sql, "nessun lookup sull'entity index: il test non prova nulla"
    for sql in entity_sql:
        assert "ORDER BY chunk_id DESC" in sql
