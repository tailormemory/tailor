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


# ── FIX B: cap SQL per-candidato + diagnostico di saturazione ────────────────
#
# Il ramo entity ha DUE tetti distinti e per mesi ne e' stato misurato uno solo:
#   - ENTITY_SQL_CANDIDATE_CAP → LIMIT della SELECT, per SINGOLO candidato
#   - ENTITY_FETCH_CAP         → slice aggregato post-dedup, prima del get() Chroma
# unique_lost_to_cap (ora unique_lost_to_fetch_cap) misura SOLO il secondo: con il
# LIMIT letterale a 100 riportava 0 mentre la SELECT scartava due terzi dei
# candidati. Da qui due diagnosi sbagliate. I test sotto presidiano che il LIMIT
# resti legato alla costante e che la saturazione per-candidato sia visibile.

import re as _re_test  # noqa: E402


class _EntityFetchStub:
    """query() semantico vuoto; get() ritorna array allineati sugli id richiesti."""

    def __init__(self):
        self.requested_ids = []

    def query(self, **kwargs):
        return {"documents": [[]], "metadatas": [[]], "distances": [[]], "ids": [[]]}

    def get(self, ids=None, include=None, **kwargs):
        ids = list(ids or [])
        self.requested_ids.extend(ids)
        return {
            "ids": ids,
            "documents": [f"doc-{c}" for c in ids],
            "metadatas": [{"date": ""} for _ in ids],
            "embeddings": [[0.5, 0.5, 0.5] for _ in ids],
        }


class _SpyCursor:
    """Cursor reale, ma registra (sql, params) di ogni execute."""

    def __init__(self, cursor, log):
        self._cursor = cursor
        self._log = log

    def execute(self, sql, params=()):
        self._log.append((sql, tuple(params)))
        self._cursor.execute(sql, params)
        return self

    def fetchall(self):
        return self._cursor.fetchall()

    def fetchone(self):
        return self._cursor.fetchone()


class _SpyConnection:
    def __init__(self, conn, log):
        self._conn = conn
        self._log = log

    def cursor(self):
        return _SpyCursor(self._conn.cursor(), self._log)

    def close(self):
        self._conn.close()


def _build_index(path, rows):
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE entity_index (entity TEXT, chunk_id TEXT)")
    conn.executemany("INSERT INTO entity_index VALUES (?, ?)", rows)
    conn.commit()
    conn.close()


@pytest.fixture
def sql_cap_env(monkeypatch, tmp_path):
    """entity_index REALE (5 chunk distinti per 'Ninfa', uno ripetuto) + spy SQL.

    La SELECT gira davvero su SQLite: il LIMIT e il COUNT(DISTINCT) sono valutati
    dal motore, non simulati — cosi' matched_count/returned_count sono i numeri
    veri e non una copia della logica re-implementata nel test.
    """
    db = tmp_path / "entity_index.sqlite3"
    _build_index(str(db), [
        ("Ninfa", "c1"), ("Ninfa", "c2"), ("Ninfa", "c3"),
        ("Ninfa", "c4"), ("Ninfa", "c5"),
        ("Ninfa", "c5"),  # duplicato: COUNT(DISTINCT) deve dire 5, non 6
    ])

    stub = _EntityFetchStub()
    monkeypatch.setattr(mcp_server, "DB_DIR", str(tmp_path))
    monkeypatch.setattr(mcp_server, "get_embedding", lambda q: [0.5, 0.5, 0.5])
    monkeypatch.setattr(mcp_server, "get_collection", lambda: stub)
    monkeypatch.setattr(mcp_server, "_get_collection_readonly", lambda: stub)

    log: list[tuple[str, tuple]] = []
    real_connect = sqlite3.connect
    monkeypatch.setattr(
        sqlite3, "connect",
        lambda *a, **k: _SpyConnection(real_connect(*a, **k), log),
    )

    def _run(query="Ninfa", diag=True):
        log.clear()
        stub.requested_ids.clear()
        return mcp_server._hybrid_collect(
            query, n_results=5, source_filter="",
            include_superseded=True, diag=diag,
        )

    return _run, log, stub


def _lookups(log):
    return [(s, p) for (s, p) in log if "entity_index" in s and "SELECT DISTINCT" in s]


def _counts(log):
    return [(s, p) for (s, p) in log if "entity_index" in s and "COUNT(" in s]


def test_limit_e_parametrizzato_non_letterale(sql_cap_env, monkeypatch):
    """Il LIMIT arriva come bind param dalla costante, non come numero nella SQL."""
    run, log, _ = sql_cap_env
    monkeypatch.setattr(mcp_server, "ENTITY_SQL_CANDIDATE_CAP", 3)
    run()

    found = _lookups(log)
    assert found, "nessun lookup sull'entity index: il test non prova nulla"
    for sql, params in found:
        assert "LIMIT ?" in sql
        # nessun letterale residuo (il vecchio "LIMIT 100")
        assert _re_test.search(r"LIMIT\s+\d", sql) is None
        assert params[-1] == 3
        # invarianti che il fix non deve toccare
        assert "ORDER BY chunk_id DESC" in sql
        assert params[0] == "%Ninfa%"


def test_mutation_cambiare_la_costante_cambia_il_limit(sql_cap_env, monkeypatch):
    """Presidio anti-hardcode: due valori diversi → due LIMIT diversi.

    Senza questo, un LIMIT reinserito come letterale che per caso coincide con la
    costante passerebbe il test sopra sul solo `params[-1]`.
    """
    run, log, _ = sql_cap_env

    monkeypatch.setattr(mcp_server, "ENTITY_SQL_CANDIDATE_CAP", 2)
    run()
    assert [p[-1] for _, p in _lookups(log)] == [2]

    monkeypatch.setattr(mcp_server, "ENTITY_SQL_CANDIDATE_CAP", 4)
    run()
    assert [p[-1] for _, p in _lookups(log)] == [4]


def test_candidato_saturo_compare_con_lost_corretto(sql_cap_env, monkeypatch):
    run, log, _ = sql_cap_env
    monkeypatch.setattr(mcp_server, "ENTITY_SQL_CANDIDATE_CAP", 3)

    out = run()
    pre = out["stages"]["entity_candidates_pre_cap"]

    assert pre["entity_sql_cap"] == 3
    assert pre["entity_sql_cap_saturated"] == [{
        "entity": "Ninfa",
        "matched_count": 5,     # COUNT(DISTINCT): il c5 ripetuto non conta due volte
        "returned_count": 3,
        "lost": 2,
    }]
    # il COUNT si paga una sola volta, solo per il candidato saturo
    assert len(_counts(log)) == 1


def test_candidato_non_saturo_lista_vuota_e_nessun_count(sql_cap_env, monkeypatch):
    """Il caso normale non deve pagare la query in piu'."""
    run, log, _ = sql_cap_env
    monkeypatch.setattr(mcp_server, "ENTITY_SQL_CANDIDATE_CAP", 10)

    out = run()
    pre = out["stages"]["entity_candidates_pre_cap"]

    assert pre["entity_sql_cap_saturated"] == []
    assert pre["entity_sql_cap"] == 10
    assert _counts(log) == []
    assert len(_lookups(log)) == 1


def test_cap_esatto_senza_perdita_non_e_saturazione(sql_cap_env, monkeypatch):
    """righe == cap ma matched == returned: il COUNT gira, la lista resta vuota.

    E' il confine: `matched_count > returned_count` e' la condizione, non
    `len(righe) == cap`.
    """
    run, log, _ = sql_cap_env
    monkeypatch.setattr(mcp_server, "ENTITY_SQL_CANDIDATE_CAP", 5)

    out = run()
    pre = out["stages"]["entity_candidates_pre_cap"]

    assert len(_counts(log)) == 1        # il COUNT viene eseguito...
    assert pre["entity_sql_cap_saturated"] == []  # ...ma non c'e' nulla di perso


def test_unique_lost_to_fetch_cap_misura_entity_fetch_cap(sql_cap_env, monkeypatch):
    """Il campo rinominato resta agganciato al cap AGGREGATO, non a quello SQL."""
    run, log, stub = sql_cap_env
    monkeypatch.setattr(mcp_server, "ENTITY_SQL_CANDIDATE_CAP", 10)  # non satura
    monkeypatch.setattr(mcp_server, "ENTITY_FETCH_CAP", 2)

    out = run()
    pre = out["stages"]["entity_candidates_pre_cap"]

    assert pre["unique_count"] == 5
    assert pre["unique_lost_to_fetch_cap"] == 3          # 5 unici - 2 di cap
    assert "unique_lost_to_cap" not in pre               # vecchio nome rimosso
    assert pre["entity_sql_cap_saturated"] == []         # l'altro cap non c'entra
    assert stub.requested_ids == ["c5", "c4"]            # slice effettivo, DESC

    monkeypatch.setattr(mcp_server, "ENTITY_FETCH_CAP", 5)
    pre2 = run()["stages"]["entity_candidates_pre_cap"]
    assert pre2["unique_lost_to_fetch_cap"] == 0


def test_default_costanti_allineati_alla_misura_del_corpus():
    """700/700: sotto questo valore la copertura misurata torna a 11/21."""
    assert mcp_server.ENTITY_SQL_CANDIDATE_CAP == 700
    assert mcp_server.ENTITY_FETCH_CAP == 700
