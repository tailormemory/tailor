"""Test del contratto unico di embedding.

Il test piu' importante e' `test_source_and_meta_do_not_change_output`:
e' il test di regressione che impedisce a chiunque di reintrodurre un
ramo su `source`/`meta`. Se un domani qualcuno aggiunge `if source == ...`,
questo test rompe.
"""

import copy

import pytest

from scripts.lib.embedding_contract import MAX_EMBED_CHARS, embedding_text


# --- purezza: idempotenza + assenza di side effect ---------------------------

def test_idempotent_same_input_same_output():
    doc = "contenuto di prova"
    a = embedding_text("gmail", {"k": "v"}, doc)
    b = embedding_text("gmail", {"k": "v"}, doc)
    assert a == b == doc


def test_no_side_effect_on_meta():
    meta = {"k": "v", "nested": {"x": 1}}
    snapshot = copy.deepcopy(meta)
    embedding_text("session", meta, "qualsiasi documento")
    assert meta == snapshot  # meta non mutato


def test_no_side_effect_on_document_identity():
    doc = "documento immutabile"
    embedding_text("live", {}, doc)
    assert doc == "documento immutabile"


# --- troncamento esatto ------------------------------------------------------

def test_truncation_exact_boundary():
    doc = "x" * (MAX_EMBED_CHARS + 500)
    out = embedding_text("s", {}, doc)
    assert len(out) == MAX_EMBED_CHARS
    assert out == "x" * MAX_EMBED_CHARS


def test_exactly_max_is_unchanged():
    doc = "y" * MAX_EMBED_CHARS
    out = embedding_text("s", {}, doc)
    assert out == doc
    assert len(out) == MAX_EMBED_CHARS


def test_below_max_is_unchanged():
    doc = "z" * (MAX_EMBED_CHARS - 1)
    out = embedding_text("s", {}, doc)
    assert out == doc
    assert len(out) == MAX_EMBED_CHARS - 1


# --- REGRESSIONE: source/meta non alterano l'output --------------------------

def test_source_and_meta_do_not_change_output():
    """Il test di regressione piu' importante.

    Stesso documento, `source` diversi e `meta` ricchi/vari: output identico.
    Impedisce la reintroduzione di un ramo su source/meta.
    """
    doc = "un documento qualunque, abbastanza lungo da essere realistico " * 10

    sources = [
        "gmail",
        "session",
        "live",
        "kb_add",
        "telegram",
        "",
        "SOURCE_SCONOSCIUTO_FUTURO",
    ]
    metas = [
        {},
        {"thread_id": "abc", "from": "x@y.z", "labels": ["a", "b"]},
        {"nested": {"deep": {"deeper": [1, 2, 3]}}, "n": 42, "flag": True},
        {"source_hint": "gmail", "prefix": "Da: X\nOggetto: Y\n"},
        None,
    ]

    baseline = embedding_text(sources[0], metas[0], doc)
    for s in sources:
        for m in metas:
            assert embedding_text(s, m, doc) == baseline == doc[:MAX_EMBED_CHARS]


# --- document vuoto / None-safe ----------------------------------------------

def test_empty_document():
    assert embedding_text("s", {}, "") == ""


def test_none_document_is_safe():
    assert embedding_text("s", {}, None) == ""


@pytest.mark.parametrize("bad_source", [None, "", 123])
def test_source_type_irrelevant(bad_source):
    doc = "resiliente al tipo di source"
    assert embedding_text(bad_source, {}, doc) == doc
