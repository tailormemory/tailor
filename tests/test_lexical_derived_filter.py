"""Esclusione dei chunk DERIVATI dal ramo lessicale di `_hybrid_collect`.

Il ramo lessicale ordina per bm25, che premia la frequenza dei termini: i
derivati (riassunti di conversazione, riassunti di documento, log delle
sessioni live) ripetono le parole della domanda molte piu' volte della fonte
primaria e la scavalcano. Qui si verifica che il filtro esista, che stia
DENTRO la SQL (prima di ORDER BY/LIMIT, altrimenti brucia lex_limit) e che non
tocchi nulla al di fuori del ramo lessicale.

Niente ChromaDB/Ollama reali: `get_embedding` e gli accessor collection sono
monkeypatchati; i due sidecar sqlite sono file veri in tmp_path, con lo stesso
schema del reconciler.
"""

from __future__ import annotations

import os
import sqlite3
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

import mcp_server  # noqa: E402
from mcp_server import (  # noqa: E402
    LEXICAL_DERIVED_CHUNK_PREFIXES,
    _is_derived_chunk,
    _lexical_not_derived_sql,
)


# ── harness ───────────────────────────────────────────────────────────────────

class _StubCollection:
    """Collection minimale: `query()` canned per il semantic, `get()` per id."""

    def __init__(self, docs, sem_ids=()):
        self._docs = docs  # chunk_id -> (document, metadata)
        self._sem_ids = list(sem_ids)
        self.get_calls = []

    def query(self, **kwargs):
        ids = self._sem_ids
        return {
            "documents": [[self._docs[c][0] for c in ids]],
            "metadatas": [[self._docs[c][1] for c in ids]],
            "distances": [[0.1] * len(ids)],
            "ids": [ids],
        }

    def get(self, ids=None, include=None):
        self.get_calls.append(list(ids or []))
        hit = [c for c in (ids or []) if c in self._docs]
        out = {
            "ids": hit,
            "documents": [self._docs[c][0] for c in hit],
            "metadatas": [self._docs[c][1] for c in hit],
        }
        # Il ramo entity chiede anche gli embeddings e scorda l'intero batch
        # su eccezione (try/except): senza la chiave il test fallirebbe in
        # silenzio invece di misurare il filtro.
        if include and "embeddings" in include:
            out["embeddings"] = [[0.5, 0.5, 0.5] for _ in hit]
        return out


def _build_index(db_dir, rows, entities=()):
    """Scrive lexical_index.sqlite3 (schema del reconciler) e ritorna i doc.

    rows: [(chunk_id, document, source)] — l'ordine bm25 lo decide il testo.
    entities: [(entity, chunk_id)] → crea anche entity_index.sqlite3.
    """
    conn = sqlite3.connect(os.path.join(db_dir, "lexical_index.sqlite3"))
    conn.executescript(
        """
        CREATE VIRTUAL TABLE lexical_fts USING fts5(
            chunk_id UNINDEXED, document, title, folder, doc_type,
            email_from, file_path, tokenize='unicode61 remove_diacritics 1'
        );
        CREATE TABLE lexical_meta (
            chunk_id TEXT PRIMARY KEY, source TEXT, fingerprint TEXT NOT NULL
        );
        """
    )
    docs = {}
    for cid, document, source in rows:
        conn.execute(
            "INSERT INTO lexical_fts VALUES (?,?,'','','','','')", (cid, document)
        )
        conn.execute(
            "INSERT INTO lexical_meta VALUES (?,?,'fp')", (cid, source)
        )
        docs[cid] = (document, {"source": source, "date": "2026-01-01"})
    conn.commit()
    conn.close()

    if entities:
        ent = sqlite3.connect(os.path.join(db_dir, "entity_index.sqlite3"))
        ent.execute("CREATE TABLE entity_index (entity TEXT, chunk_id TEXT)")
        ent.executemany("INSERT INTO entity_index VALUES (?,?)", entities)
        ent.commit()
        ent.close()
    return docs


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Ritorna `setup(rows, sem_ids=..., entities=...)` → (stub, collect)."""
    def setup(rows, sem_ids=(), entities=()):
        docs = _build_index(str(tmp_path), rows, entities)
        stub = _StubCollection(docs, sem_ids)
        monkeypatch.setattr(mcp_server, "DB_DIR", str(tmp_path))
        monkeypatch.setattr(mcp_server, "get_embedding", lambda q: [0.5, 0.5, 0.5])
        monkeypatch.setattr(mcp_server, "get_collection", lambda: stub)
        monkeypatch.setattr(mcp_server, "_get_collection_readonly", lambda: stub)
        return stub
    return setup


def _lexical_ids(out):
    return {c for c, v in out["seen_ids"].items() if v["source_type"] == "lexical"}


def _collect(query, **kw):
    kw.setdefault("n_results", 5)
    kw.setdefault("source_filter", "")
    kw.setdefault("include_superseded", True)
    kw.setdefault("diag", False)
    return mcp_server._hybrid_collect(query, **kw)


# ── helper SQL puro ───────────────────────────────────────────────────────────

def test_prefissi_congelati():
    assert LEXICAL_DERIVED_CHUNK_PREFIXES == (
        "conv_summary_", "doc_summary_", "live_"
    )


def test_helper_sql_e_congiunzione_di_not_glob():
    sql, params = _lexical_not_derived_sql("chunk_id")
    assert sql == (
        "chunk_id NOT GLOB ? AND chunk_id NOT GLOB ? AND chunk_id NOT GLOB ?"
    )
    assert params == ["conv_summary_*", "doc_summary_*", "live_*"]


def test_helper_sql_qualifica_la_colonna_passata():
    sql, _ = _lexical_not_derived_sql("f.chunk_id")
    assert sql.startswith("f.chunk_id NOT GLOB ?")


def test_helper_usa_glob_e_mai_like():
    # LIKE senza ESCAPE tratterebbe `_` come wildcard di un carattere.
    sql, _ = _lexical_not_derived_sql("chunk_id")
    assert "GLOB" in sql and "LIKE" not in sql


def test_is_derived_chunk_coincide_col_glob():
    for cid in ("conv_summary_1", "doc_summary_1", "live_claude_1",
                "live_gemini_1", "conv_summary_conv_summary_1"):
        assert _is_derived_chunk(cid)
    for cid in ("email_abc_chunk_0000", "doc_xyz_chunk_0000",
                "claude_session_1", "claude_memories_chunk_0001",
                "convXsummary_foo", "liveZfoo",
                "f47ac10b-58cc-4372-a567-0e02b2c3d479"):
        assert not _is_derived_chunk(cid)


# ── il filtro nel ramo lessicale ──────────────────────────────────────────────

def test_i_tre_prefissi_sono_scartati(env):
    env([
        ("conv_summary_1", "persehais persehais", "claude"),
        ("doc_summary_1", "persehais persehais", "claude"),
        ("live_claude_1", "persehais persehais", "claude"),
        ("email_abc_chunk_0000", "persehais", "email"),
    ])
    assert _lexical_ids(_collect("persehais")) == {"email_abc_chunk_0000"}


def test_live_copre_tutti_i_provider(env):
    # `live_` e non `live_claude_`: live_gemini e live_chatgpt sono derivati
    # allo stesso titolo (948 / 104 / 29 chunk censiti).
    env([
        ("live_claude_1", "persehais", "claude"),
        ("live_gemini_1", "persehais", "gemini"),
        ("live_chatgpt_1", "persehais", "chatgpt"),
        ("email_abc_chunk_0000", "persehais", "email"),
    ])
    assert _lexical_ids(_collect("persehais")) == {"email_abc_chunk_0000"}


def test_derivati_di_secondo_grado_esclusi(env):
    # Il prefisso e' un prefisso, non un pattern esatto: un riassunto di
    # riassunto resta derivato.
    env([
        ("conv_summary_conv_summary_1", "persehais", "claude"),
        ("doc_summary_doc_summary_1", "persehais", "claude"),
        ("live_live_claude_1", "persehais", "claude"),
        ("email_abc_chunk_0000", "persehais", "email"),
    ])
    assert _lexical_ids(_collect("persehais")) == {"email_abc_chunk_0000"}


def test_primari_con_bm25_identico_sopravvivono(env):
    # Stesso identico testo → stesso bm25: a discriminare e' solo il prefisso.
    testo = "persehais ridondanza"
    env([
        ("conv_summary_1", testo, "claude"),
        ("email_abc_chunk_0000", testo, "email"),
        ("doc_xyz_chunk_0000", testo, "doc"),
    ])
    assert _lexical_ids(_collect("persehais ridondanza")) == {
        "email_abc_chunk_0000", "doc_xyz_chunk_0000"
    }


def test_filtro_applicato_prima_del_cap(env):
    # 40 derivati con tf altissima (bm25 migliore) + 1 email con una sola
    # occorrenza: con n_results=5 il cap e' 15, quindi post-fetch l'email
    # sarebbe stata tagliata. Il filtro in SQL le lascia lo slot.
    rows = [(f"live_claude_{i:03d}", "persehais " * 20, "claude") for i in range(40)]
    rows.append(("email_abc_chunk_0000", "persehais", "email"))
    env(rows)

    out = _collect("persehais", n_results=5)
    assert "email_abc_chunk_0000" in _lexical_ids(out)
    assert _lexical_ids(out) == {"email_abc_chunk_0000"}


def test_anti_wildcard_underscore_non_e_jolly(env):
    # `_` e' wildcard in LIKE, non in GLOB: `convXsummary_foo` NON e' derivato.
    env([
        ("convXsummary_foo", "persehais", "claude"),
        ("docYsummary_foo", "persehais", "claude"),
        ("liveZfoo", "persehais", "claude"),
    ])
    assert _lexical_ids(_collect("persehais")) == {
        "convXsummary_foo", "docYsummary_foo", "liveZfoo"
    }


def test_prefissi_claude_restano_primari(env):
    env([
        ("claude_session_20260101", "persehais", "claude"),
        ("claude_memories_chunk_0001", "persehais", "claude"),
        ("claude_altro_1", "persehais", "claude"),
        ("f47ac10b-58cc-4372-a567-0e02b2c3d479", "persehais", "doc"),
        ("live_claude_1", "persehais", "claude"),
    ])
    assert _lexical_ids(_collect("persehais")) == {
        "claude_session_20260101",
        "claude_memories_chunk_0001",
        "claude_altro_1",
        "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    }


def test_composizione_con_source_filter(env):
    # Il ramo con JOIN su lexical_meta deve filtrare ENTRAMBI: source e derivati.
    env([
        ("live_claude_1", "persehais", "email"),
        ("email_abc_chunk_0000", "persehais", "email"),
        ("email_def_chunk_0000", "persehais", "gemini"),
        ("conv_summary_1", "persehais", "email"),
    ])
    out = _collect("persehais", source_filter="email")
    assert _lexical_ids(out) == {"email_abc_chunk_0000"}


# ── perimetro: il filtro NON esce dal ramo lessicale ──────────────────────────

def test_derivato_da_semantic_resta_nel_pool(env):
    # Il dedup e' invariato: un derivato che arriva da semantic resta, e il
    # ramo lessicale non lo rimuove ne' lo declassa.
    env(
        [("conv_summary_1", "persehais", "claude"),
         ("email_abc_chunk_0000", "persehais", "email")],
        sem_ids=["conv_summary_1"],
    )
    out = _collect("persehais")
    assert out["seen_ids"]["conv_summary_1"]["source_type"] == "semantic"
    assert _lexical_ids(out) == {"email_abc_chunk_0000"}


def test_derivato_da_entity_resta_nel_pool(env):
    # 'Persehais' capitalizzato → candidato entity. Il ramo entity deve poter
    # restituire un live_* che il lessicale scarterebbe.
    env(
        [("live_claude_1", "persehais", "claude"),
         ("email_abc_chunk_0000", "persehais", "email")],
        entities=[("Persehais", "live_claude_1")],
    )
    out = _collect("Persehais")
    assert out["seen_ids"]["live_claude_1"]["source_type"] == "entity"
    assert _lexical_ids(out) == {"email_abc_chunk_0000"}


# ── diagnostica ───────────────────────────────────────────────────────────────

# `lexical_derived_dropped` conta i derivati nella FINESTRA COMBINATA (i primi
# lex_limit senza filtro), non i primi lex_limit fra i soli derivati. I due
# numeri coincidono finche' tutto sta dentro la finestra — per questo i casi
# che discriminano hanno piu' derivati di lex_limit.
#
# `_docs` genera bm25 controllato: un documento corto col termine ranka meglio
# di uno lungo che lo cita una volta sola fra il riempitivo.

_CORTO = "persehais"
_LUNGO = "persehais " + " ".join(f"riempitivo{i}" for i in range(60))


def test_diag_count_solo_ammessi_e_contatore_dropped(env):
    # Caso piccolo: 3 derivati + 1 primario, tutti dentro la finestra.
    env([
        ("conv_summary_1", _CORTO, "claude"),
        ("doc_summary_1", _CORTO, "claude"),
        ("live_claude_1", _CORTO, "claude"),
        ("email_abc_chunk_0000", _CORTO, "email"),
    ])
    stage = _collect("persehais", diag=True)["stages"]["lexical_candidates"]
    assert stage["count"] == 1                       # solo gli ammessi
    assert stage["lexical_derived_dropped"] == 3
    assert stage["error"] is None


def test_diag_dropped_zero_se_i_derivati_non_entravano_in_finestra(env):
    # 20 derivati a bm25 PEGGIORE di 15 primari, lex_limit=15: senza filtro la
    # finestra sarebbe stata tutta primari → il filtro non ha liberato niente.
    # E' il caso che smaschera la semantica sbagliata: contando i top-15 fra i
    # soli derivati verrebbe 15 invece di 0.
    rows = [(f"live_claude_{i:03d}", _LUNGO, "claude") for i in range(20)]
    rows += [(f"email_{i:03d}_chunk_0000", _CORTO, "email") for i in range(15)]
    env(rows)

    stage = _collect("persehais", n_results=5, diag=True)[
        "stages"]["lexical_candidates"]
    assert stage["count"] == 15                      # lex_limit pieno di primari
    assert stage["lexical_derived_dropped"] == 0


def test_diag_dropped_conta_solo_gli_slot_realmente_liberati(env):
    # Stessi 20 derivati scadenti, ma solo 10 primari: la finestra da 15 ne
    # avrebbe presi 10 + 5 derivati → 5 slot liberati, non 15.
    rows = [(f"live_claude_{i:03d}", _LUNGO, "claude") for i in range(20)]
    rows += [(f"email_{i:03d}_chunk_0000", _CORTO, "email") for i in range(10)]
    env(rows)

    stage = _collect("persehais", n_results=5, diag=True)[
        "stages"]["lexical_candidates"]
    assert stage["count"] == 10
    assert stage["lexical_derived_dropped"] == 5


def test_diag_dropped_satura_quando_i_derivati_dominano(env):
    # 20 derivati a bm25 MIGLIORE e un solo primario scadente: la finestra
    # sarebbe stata tutta derivata → 15 slot liberati (= lex_limit).
    rows = [(f"live_claude_{i:03d}", _CORTO, "claude") for i in range(20)]
    rows.append(("email_abc_chunk_0000", _LUNGO, "email"))
    env(rows)

    stage = _collect("persehais", n_results=5, diag=True)[
        "stages"]["lexical_candidates"]
    assert stage["count"] == 1
    assert stage["lexical_derived_dropped"] == 15


def test_diag_dropped_zero_senza_derivati(env):
    env([("email_abc_chunk_0000", _CORTO, "email")])
    stage = _collect("persehais", diag=True)["stages"]["lexical_candidates"]
    assert stage["count"] == 1
    assert stage["lexical_derived_dropped"] == 0


def test_diag_dropped_conta_anche_con_source_filter(env):
    env([
        ("live_claude_1", "persehais", "email"),
        ("conv_summary_1", "persehais", "gemini"),   # fuori source → non contato
        ("email_abc_chunk_0000", "persehais", "email"),
    ])
    stage = _collect("persehais", source_filter="email", diag=True)[
        "stages"]["lexical_candidates"]
    assert stage["count"] == 1
    assert stage["lexical_derived_dropped"] == 1
