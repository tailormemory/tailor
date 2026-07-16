"""Unit tests for mcp_server._compose_rerank_pool.

La composizione del pool (STEP 4 di kb_hybrid_search) vive in una funzione
pura su liste/dict, testabile senza ChromaDB.

Policy ATTUALE = ADDITIVA (B2b-core + ramo lessicale A1):
pool = semantic[:n*3] + entity[:n] + lexical[:n].
Gli entity-hit validi (score 0.5 fisso) hanno una quota propria e non vengono
più tagliati fuori dal pool del cross-encoder dai semantic. Idem i lexical-hit
(score = -rank bm25, scala DIVERSA dal cosine → la quota separata è anche ciò
che evita il confronto cross-scala). Query senza entity né lexical → pool =
semantic[:n*3], identico al pre-fix (zero regressione).
Tie-break date ASC invariato (policy separata, fuori scope).

Copre anche _lexical_match_expr (intent-parse B1, funzione pura).
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from mcp_server import _compose_rerank_pool, _lexical_match_expr  # noqa: E402
from fts5_sanitize import INDEXED_COLUMNS  # noqa: E402  (via sys.path di mcp_server)


def _item(key: str, source_type: str, score: float, date: str = "") -> tuple:
    """Costruisce una tupla (key, dict) come quelle in seen_ids.items()."""
    return (key, {
        "source_type": source_type,
        "score": score,
        "date": date,
        "doc": f"text-{key}",
    })


def _keys(pool: list) -> list:
    return [k for k, _ in pool]


# ── ordinamento: solo semantic ─────────────────────────────────


def test_only_semantic_sorted_by_score_desc():
    items = [
        _item("a", "semantic", 0.3),
        _item("b", "semantic", 0.9),
        _item("c", "semantic", 0.6),
    ]
    pool = _compose_rerank_pool(items, n_results=10)
    assert _keys(pool) == ["b", "c", "a"]


def test_only_semantic_tiebreak_date_ascending():
    # Stesso score → tie-break su date ASC (invariato).
    items = [
        _item("new", "semantic", 0.5, date="2024-01-01"),
        _item("old", "semantic", 0.5, date="2020-01-01"),
    ]
    pool = _compose_rerank_pool(items, n_results=10)
    assert _keys(pool) == ["old", "new"]


# ── ordinamento: mix semantic + entity ─────────────────────────


def test_semantic_block_precedes_entity_block():
    # I semantic occupano la loro quota per primi, gli entity sono AGGIUNTI dopo.
    items = [
        _item("ent", "entity", 0.5, date="2022-01-01"),
        _item("sem", "semantic", 0.1, date="2024-01-01"),
    ]
    pool = _compose_rerank_pool(items, n_results=10)
    assert _keys(pool) == ["sem", "ent"]


def test_mixed_block_ordering():
    items = [
        _item("e1", "entity", 0.5, date="2023-05-05"),
        _item("s1", "semantic", 0.8),
        _item("e2", "entity", 0.5, date="2021-01-01"),
        _item("s2", "semantic", 0.4),
    ]
    pool = _compose_rerank_pool(items, n_results=10)
    # semantic per score DESC, poi blocco entity per date ASC
    assert _keys(pool) == ["s1", "s2", "e2", "e1"]


# ── quota additiva: entity entrano anche con ≥3n semantic ──────


def test_entity_present_even_with_full_semantic_quota():
    # 3n semantic forti riempiono la quota semantic; gli entity entrano LO STESSO
    # grazie alla quota separata. Pre-fix (slice unico) li avrebbe esclusi.
    n = 5
    semantic = [_item(f"s{i}", "semantic", 0.9 - i * 0.01) for i in range(3 * n)]
    entity = [_item("ent_keep", "entity", 0.5, date="2022-01-01")]
    pool = _compose_rerank_pool(semantic + entity, n_results=n)
    assert "ent_keep" in _keys(pool)
    # tutti i 3n semantic ci sono ancora (nessuno espulso)
    for i in range(3 * n):
        assert f"s{i}" in _keys(pool)


def test_no_semantic_evicted_by_entities():
    n = 4
    semantic = [_item(f"s{i}", "semantic", 0.9 - i * 0.01) for i in range(3 * n)]
    entity = [_item(f"e{j}", "entity", 0.5, date=f"20{20 + j:02d}-01-01") for j in range(10)]
    pool = _compose_rerank_pool(semantic + entity, n_results=n)
    sem_in_pool = [k for k in _keys(pool) if k.startswith("s")]
    assert len(sem_in_pool) == 3 * n  # i primi 3n semantic restano tutti


# ── ACCETTAZIONE (gate B2b-core): Atto_Ninfa entra nel pool ────


def _inline_pool(items: list, n_results: int) -> list:
    """Replica ESATTA dell'algoritmo PRE-fix (tutti semantic poi entity, slice
    unico): items.sort(key bucket 0/1, -score, date); items[:min(len, n*3)].
    Serve a dimostrare che il chunk veniva escluso prima del fix."""
    items = list(items)
    items.sort(key=lambda x: (
        0 if x[1]["source_type"] == "semantic" else 1,
        -x[1]["score"],
        x[1]["date"] if x[1]["date"] else ""
    ), reverse=False)
    return items[:min(len(items), n_results * 3)]


def test_acceptance_atto_ninfa_enters_pool_at_chat_default():
    # Scenario reale: 35 semantic forti + l'entity-hit valido Atto_Ninfa (2022),
    # globalmente "profondo" perché tutti i semantic lo precedono nel sort unico.
    NINFA = "doc_611765bb6025_chunk_0003"
    semantic = [_item(f"s{i}", "semantic", 0.95 - i * 0.01) for i in range(35)]
    entity = [
        _item("e_other_a", "entity", 0.5, date="2024-06-01"),
        _item("e_other_b", "entity", 0.5, date="2023-09-01"),
        _item(NINFA, "entity", 0.5, date="2022-01-01"),
        _item("e_other_c", "entity", 0.5, date="2025-02-01"),
    ]
    items = semantic + entity
    n = 5  # default reale della chat

    # PRE-fix: il chunk NON entrava nel pool (slice unico [:15] = solo semantic).
    assert NINFA not in _keys(_inline_pool(items, n))
    # POST-fix: la quota entity[:n] lo fa entrare nel pool del cross-encoder.
    assert NINFA in _keys(_compose_rerank_pool(items, n))


# ── dedup difensivo ────────────────────────────────────────────


def test_dedup_key_present_as_both_semantic_and_entity():
    # Difesa: stessa key in entrambi i flussi → una sola occorrenza (semantic).
    items = [
        _item("dup", "semantic", 0.7),
        _item("dup", "entity", 0.5, date="2022-01-01"),
        _item("s1", "semantic", 0.9),
    ]
    pool = _compose_rerank_pool(items, n_results=10)
    keys = _keys(pool)
    assert keys.count("dup") == 1
    # la prima occorrenza è quella semantic (mantiene score 0.7)
    dup_val = dict(pool)["dup"]
    assert dup_val["source_type"] == "semantic"


# ── dimensioni pool: semantic[:3n] + entity[:n] ────────────────


def test_pool_sizes_additive_quota():
    n_entities = 8
    for n in (3, 5, 10):
        semantic = [_item(f"s{i}", "semantic", 0.9 - i * 0.001) for i in range(40)]
        entity = [_item(f"e{j}", "entity", 0.5, date=f"20{10 + j:02d}-01-01") for j in range(n_entities)]
        pool = _compose_rerank_pool(semantic + entity, n_results=n)
        expected = min(40, n * 3) + min(n_entities, n)
        assert len(pool) == expected


def test_semantic_only_pool_identical_to_prefix():
    # Invariante: senza entity il pool è IDENTICO all'algoritmo pre-fix.
    items = [_item(f"s{i}", "semantic", 1.0 - i * 0.001) for i in range(40)]
    for n in (3, 5, 10):
        assert _compose_rerank_pool(items, n) == _inline_pool(items, n)
        assert len(_compose_rerank_pool(items, n)) == min(len(items), n * 3)


def test_pool_size_when_fewer_items_than_cap():
    items = [_item(f"s{i}", "semantic", 1.0 - i * 0.1) for i in range(4)]
    pool = _compose_rerank_pool(items, n_results=10)  # cap 30 > 4, nessun entity
    assert len(pool) == 4


def test_short_entity_list_takes_what_exists():
    # Meno entity della quota → prende quelli che ci sono, niente errori.
    semantic = [_item(f"s{i}", "semantic", 0.9 - i * 0.01) for i in range(3)]
    entity = [_item("e1", "entity", 0.5, date="2022-01-01")]
    pool = _compose_rerank_pool(semantic + entity, n_results=5)
    assert len(pool) == 3 + 1


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
    # Chiavi di sort tutte distinte → output indipendente dall'ordine d'ingresso.
    base = [
        _item("a", "semantic", 0.9),
        _item("b", "semantic", 0.5),
        _item("c", "entity", 0.5, date="2020-01-01"),
        _item("d", "entity", 0.5, date="2021-01-01"),
    ]
    perm = [base[2], base[0], base[3], base[1]]
    assert _compose_rerank_pool(base, 10) == _compose_rerank_pool(perm, 10)


# ── ramo lessicale: fetta additiva lexical[:n] ─────────────────
#
# Lo score dei lexical è -rank bm25: il rank di FTS5 è negativo e più-negativo =
# più rilevante, quindi -rank è positivo-decrescente e ordina correttamente sotto
# la stessa _sort_key (-score, date) degli altri flussi.


def test_lexical_block_follows_semantic_and_entity():
    items = [
        _item("lex", "lexical", 8.1, date="2022-01-01"),
        _item("ent", "entity", 0.5, date="2022-01-01"),
        _item("sem", "semantic", 0.1),
    ]
    pool = _compose_rerank_pool(items, n_results=10)
    assert _keys(pool) == ["sem", "ent", "lex"]


def test_lexical_sorted_by_score_desc():
    # rank bm25 -0.5 / -9.9 / -3.2 → score 0.5 / 9.9 / 3.2 → ordine l2, l3, l1.
    items = [
        _item("l1", "lexical", 0.5),
        _item("l2", "lexical", 9.9),
        _item("l3", "lexical", 3.2),
    ]
    pool = _compose_rerank_pool(items, n_results=10)
    assert _keys(pool) == ["l2", "l3", "l1"]


def test_lexical_tiebreak_date_ascending():
    items = [
        _item("new", "lexical", 4.0, date="2024-01-01"),
        _item("old", "lexical", 4.0, date="2020-01-01"),
    ]
    pool = _compose_rerank_pool(items, n_results=10)
    assert _keys(pool) == ["old", "new"]


def test_lexical_present_even_with_full_semantic_and_entity_quotas():
    # Quota separata: il gold lessicale entra anche con 3n semantic forti e n
    # entity già in pool. È il punto del ramo A1 — il match esatto che il
    # vettore manca non deve essere spinto fuori dal cross-encoder.
    n = 5
    semantic = [_item(f"s{i}", "semantic", 0.9 - i * 0.01) for i in range(3 * n)]
    entity = [_item(f"e{j}", "entity", 0.5, date=f"20{20 + j:02d}-01-01") for j in range(n)]
    lexical = [_item("lex_gold", "lexical", 2.5, date="2022-01-01")]
    pool = _compose_rerank_pool(semantic + entity + lexical, n_results=n)
    assert "lex_gold" in _keys(pool)
    # nessun semantic né entity espulso dal nuovo flusso
    assert len([k for k in _keys(pool) if k.startswith("s")]) == 3 * n
    assert len([k for k in _keys(pool) if k.startswith("e")]) == n


def test_lexical_quota_capped_at_n():
    n = 3
    lexical = [_item(f"l{i}", "lexical", 10.0 - i) for i in range(10)]
    pool = _compose_rerank_pool(lexical, n_results=n)
    assert _keys(pool) == ["l0", "l1", "l2"]  # i migliori n per score


def test_pool_sizes_with_three_streams():
    for n in (3, 5, 10):
        semantic = [_item(f"s{i}", "semantic", 0.9 - i * 0.001) for i in range(40)]
        entity = [_item(f"e{j}", "entity", 0.5, date=f"20{10 + j:02d}-01-01") for j in range(8)]
        lexical = [_item(f"l{k}", "lexical", 9.0 - k * 0.1) for k in range(6)]
        pool = _compose_rerank_pool(semantic + entity + lexical, n_results=n)
        assert len(pool) == min(40, n * 3) + min(8, n) + min(6, n)


def test_semantic_only_pool_unchanged_by_lexical_support():
    # NON-REGRESSIONE: senza item lexical il pool è identico all'algoritmo pre-fix
    # (e quindi al comportamento pre-ramo-lessicale). Il ramo è puramente additivo.
    items = [_item(f"s{i}", "semantic", 1.0 - i * 0.001) for i in range(40)]
    for n in (3, 5, 10):
        assert _compose_rerank_pool(items, n) == _inline_pool(items, n)


def test_dedup_first_wins_across_three_streams():
    # I5 (sources[]) è DEFERITO all'unità 2 (RRF): un cid trovato da più rami
    # resta first-wins, niente lista sources.
    items = [
        _item("dup", "semantic", 0.7),
        _item("dup", "entity", 0.5, date="2022-01-01"),
        _item("dup", "lexical", 9.0),
        _item("s1", "semantic", 0.9),
    ]
    pool = _compose_rerank_pool(items, n_results=10)
    assert _keys(pool).count("dup") == 1
    assert dict(pool)["dup"]["source_type"] == "semantic"


def test_dedup_entity_wins_over_lexical():
    items = [
        _item("dup", "entity", 0.5, date="2022-01-01"),
        _item("dup", "lexical", 9.0),
    ]
    pool = _compose_rerank_pool(items, n_results=10)
    assert _keys(pool) == ["dup"]
    assert dict(pool)["dup"]["source_type"] == "entity"


# ── intent-parse (B1): _lexical_match_expr ─────────────────────


def test_intent_parse_known_column_prefix_scopes_terms():
    # Prefisso nell'allowlist → ramo column. L'email è tokenizzata (terms, non
    # phrase) e ogni token esce quotato: mai barewords.
    assert _lexical_match_expr("email_from:gianluca@x.com") == \
        'email_from:("gianluca" "x" "com")'


def test_intent_parse_column_prefix_is_case_insensitive():
    assert _lexical_match_expr("EMAIL_FROM:gianluca") == 'email_from:("gianluca")'


def test_intent_parse_natural_query_goes_to_unscoped_terms():
    # Nessuna forma `col:valore` → terms unscoped su tutte le colonne indicizzate.
    assert _lexical_match_expr("mail di gianluca") == '"mail" "di" "gianluca"'


def test_intent_parse_unknown_prefix_falls_back_to_terms():
    # "foo" non è in INDEXED_COLUMNS → nessuno scope. Il ':' non è alfanumerico:
    # il tokenizer lo tratta da separatore e non finisce nei token.
    assert _lexical_match_expr("foo:bar") == '"foo" "bar"'


def test_intent_parse_all_indexed_columns_are_routable():
    for col in INDEXED_COLUMNS:
        assert _lexical_match_expr(f"{col}:valore") == f'{col}:("valore")'


def test_intent_parse_empty_when_no_alphanumeric_tokens():
    # MATCH '' è invalido in FTS5 → l'expr vuota è il segnale di "salta il ramo".
    assert _lexical_match_expr("") == ""
    assert _lexical_match_expr("   ") == ""
    assert _lexical_match_expr("!!! ---") == ""


def test_intent_parse_column_with_empty_value_is_empty():
    # Mai emettere un 'col:' nudo, che sarebbe sintassi FTS5 rotta.
    assert _lexical_match_expr("email_from:!!!") == ""


def test_intent_parse_fts5_operators_are_neutralized():
    # Le keyword FTS5 escono quotate = stringhe, non sintassi.
    assert _lexical_match_expr("usufrutto AND ninfa") == '"usufrutto" "AND" "ninfa"'


# ── intent-parse: il ':' interno al VALORE non rompe il parse ──
#
# `^(\w+):(.+)$` è greedy solo sul primo ':': group(1) è \w+ (niente ':'),
# quindi lo split avviene sul PRIMO ':' e (.+) cattura tutto il resto, ':' inclusi.
# Il sanitizer poi tokenizza il valore e ogni ':' residuo cade come separatore.


def test_intent_parse_windows_path_value_with_drive_colon():
    # 'C:\foo\bar' — il ':' del drive letter sta nel VALORE, non è un secondo
    # separatore col:valore.
    assert _lexical_match_expr("file_path:C:\\foo\\bar") == 'file_path:("C" "foo" "bar")'


def test_intent_parse_url_value_with_multiple_colons():
    assert _lexical_match_expr("file_path:s3://bucket/a:b") == \
        'file_path:("s3" "bucket" "a" "b")'


def test_intent_parse_leading_colon_is_not_a_column_prefix():
    # ':foo' — group(1) sarebbe vuoto, ma \w+ richiede ≥1 char → nessun match →
    # terms unscoped. Non solleva, non emette un ':' nudo.
    assert _lexical_match_expr(":foo") == '"foo"'


def test_intent_parse_url_scheme_is_not_a_column():
    # 'http' non è in INDEXED_COLUMNS → nessuno scope, terms unscoped.
    assert _lexical_match_expr("http://x") == '"http" "x"'
