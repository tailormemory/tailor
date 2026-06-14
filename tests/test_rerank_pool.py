"""Unit tests for mcp_server._compose_rerank_pool.

The pool composition (STEP 4 sort + slice di kb_hybrid_search) è stata
estratta in una funzione pura su liste/dict, così da poterla testare senza
ChromaDB. Questo commit NON cambia il ranking: i test fissano il
comportamento ATTUALE (semantic prima di entity, score DESC, date ASC) come
baseline di regressione per B2b-core.
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from mcp_server import _compose_rerank_pool  # noqa: E402


def _item(key: str, source_type: str, score: float, date: str = "") -> tuple:
    """Costruisce una tupla (key, dict) come quelle in seen_ids.items()."""
    return (key, {
        "source_type": source_type,
        "score": score,
        "date": date,
        "doc": f"text-{key}",
    })


# ── ordinamento: solo semantic ─────────────────────────────────


def test_only_semantic_sorted_by_score_desc():
    items = [
        _item("a", "semantic", 0.3),
        _item("b", "semantic", 0.9),
        _item("c", "semantic", 0.6),
    ]
    pool = _compose_rerank_pool(items, n_results=10)
    assert [k for k, _ in pool] == ["b", "c", "a"]


def test_only_semantic_tiebreak_date_ascending():
    # Stesso score → tie-break su date ASC (comportamento attuale).
    items = [
        _item("new", "semantic", 0.5, date="2024-01-01"),
        _item("old", "semantic", 0.5, date="2020-01-01"),
    ]
    pool = _compose_rerank_pool(items, n_results=10)
    assert [k for k, _ in pool] == ["old", "new"]


# ── ordinamento: mix semantic + entity ─────────────────────────


def test_entity_always_after_semantic_even_with_higher_score():
    # entity score 0.5 > semantic score 0.1, ma semantic resta PRIMA:
    # è il comportamento attuale (questo commit non cambia la quota).
    items = [
        _item("ent", "entity", 0.5, date="2022-01-01"),
        _item("sem", "semantic", 0.1, date="2024-01-01"),
    ]
    pool = _compose_rerank_pool(items, n_results=10)
    assert [k for k, _ in pool] == ["sem", "ent"]


def test_mixed_block_ordering():
    items = [
        _item("e1", "entity", 0.5, date="2023-05-05"),
        _item("s1", "semantic", 0.8),
        _item("e2", "entity", 0.5, date="2021-01-01"),
        _item("s2", "semantic", 0.4),
    ]
    pool = _compose_rerank_pool(items, n_results=10)
    # semantic per score DESC, poi entity per date ASC
    assert [k for k, _ in pool] == ["s1", "s2", "e2", "e1"]


# ── determinismo ───────────────────────────────────────────────


def test_deterministic_across_repeated_runs():
    items = [
        _item("e1", "entity", 0.5, date="2022-03-03"),
        _item("s1", "semantic", 0.7),
        _item("e2", "entity", 0.5, date="2022-01-01"),
        _item("s2", "semantic", 0.7, date="2024-01-01"),
        _item("s3", "semantic", 0.7, date="2023-01-01"),
    ]
    first = _compose_rerank_pool(items, n_results=10)
    for _ in range(5):
        assert _compose_rerank_pool(items, n_results=10) == first


def test_deterministic_regardless_of_input_order():
    # Chiavi di sort tutte distinte → l'output non dipende dall'ordine
    # d'ingresso (dict/set insertion order). Stesso set, due permutazioni.
    base = [
        _item("a", "semantic", 0.9),
        _item("b", "semantic", 0.5),
        _item("c", "entity", 0.5, date="2020-01-01"),
        _item("d", "entity", 0.5, date="2021-01-01"),
    ]
    perm = [base[2], base[0], base[3], base[1]]
    assert _compose_rerank_pool(base, 10) == _compose_rerank_pool(perm, 10)


# ── equivalenza con l'algoritmo inline del parent ──────────────


def _inline_pool(items: list, n_results: int) -> list:
    """Replica ESATTA del codice inline pre-refactor (STEP 4 del parent):
    items.sort(key=..., reverse=False); items[:min(len, n*3)]."""
    items = list(items)  # copia: items.sort muta in place
    items.sort(key=lambda x: (
        0 if x[1]["source_type"] == "semantic" else 1,
        -x[1]["score"],
        x[1]["date"] if x[1]["date"] else ""
    ), reverse=False)
    return items[:min(len(items), n_results * 3)]


def test_equivalent_to_inline_algorithm_with_identical_keys():
    # Chiavi di sort COMPLETAMENTE uguali (stesso score/date/source_type):
    # _compose_rerank_pool deve restituire l'IDENTICO output dell'inline,
    # incluso l'ordine delle tuple a parità di chiave (stabilità del sort).
    items = [
        _item("a", "semantic", 0.5, date="2022-01-01"),
        _item("b", "semantic", 0.5, date="2022-01-01"),
        _item("c", "semantic", 0.5, date="2022-01-01"),
        _item("d", "semantic", 0.5, date="2022-01-01"),
    ]
    for n in (3, 5, 10):
        assert _compose_rerank_pool(items, n) == _inline_pool(items, n)


def test_equivalent_to_inline_algorithm_mix_and_duplicate_keys():
    # Mix semantic+entity con chiavi di sort duplicate dentro ciascun blocco.
    items = [
        _item("s1", "semantic", 0.7, date="2024-01-01"),
        _item("s2", "semantic", 0.7, date="2024-01-01"),   # chiave == s1
        _item("e1", "entity", 0.5, date="2022-01-01"),
        _item("e2", "entity", 0.5, date="2022-01-01"),     # chiave == e1
        _item("s3", "semantic", 0.3),
        _item("e3", "entity", 0.5, date="2021-01-01"),
    ]
    for n in (2, 3, 5, 10):
        assert _compose_rerank_pool(items, n) == _inline_pool(items, n)


# ── dimensione pool = min(len, n_results*3) ────────────────────


def test_pool_size_caps_at_n_results_times_three():
    items = [_item(f"s{i}", "semantic", 1.0 - i * 0.001) for i in range(40)]
    for n in (3, 5, 10):
        pool = _compose_rerank_pool(items, n_results=n)
        assert len(pool) == min(len(items), n * 3)


def test_pool_size_when_fewer_items_than_cap():
    items = [_item(f"s{i}", "semantic", 1.0 - i * 0.1) for i in range(4)]
    pool = _compose_rerank_pool(items, n_results=10)  # cap 30 > 4
    assert len(pool) == 4
