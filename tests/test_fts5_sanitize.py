"""Regression gate per scripts/lib/fts5_sanitize.py.

Tutto string-assertion puro tranne i test marcati che aprono una FTS5
in-memory per provare che il MATCH reale non esploda.
"""

import sqlite3

import pytest

from scripts.lib.fts5_sanitize import (
    INDEXED_COLUMNS,
    sanitize_column_phrase,
    sanitize_column_terms,
    sanitize_phrase,
    sanitize_terms,
)


@pytest.fixture()
def fts():
    """FTS5 in-memory con una riga realistica."""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE VIRTUAL TABLE docs USING fts5("
        "document, title, folder, doc_type, email_from, file_path)"
    )
    conn.execute(
        "INSERT INTO docs VALUES (?, ?, ?, ?, ?, ?)",
        (
            "Preventivo per il cliente, contatto gianluca@example.com",
            "Preventivo 2026",
            "clienti",
            "preventivo",
            "gianluca@example.com",
            "/Users/jarvis/tailor/docs/preventivi/2026.pdf",
        ),
    )
    conn.commit()
    yield conn
    conn.close()


def _match(conn, expr):
    return conn.execute("SELECT rowid FROM docs WHERE docs MATCH ?", (expr,)).fetchall()


# --- sanitize_terms -------------------------------------------------------


def test_terms_basic_and_implicito():
    assert sanitize_terms("alfa beta gamma") == '"alfa" "beta" "gamma"'


def test_terms_email_non_conserva_prefisso_colonna():
    # 'email_from:' NON deve sopravvivere come sintassi di colonna.
    out = sanitize_terms("email_from:gianluca@example.com")
    assert out == '"email" "from" "gianluca" "example" "com"'
    assert "email_from:" not in out


def test_terms_keyword_fts5_quotate_non_operatori():
    assert sanitize_terms("AND OR NOT NEAR") == '"AND" "OR" "NOT" "NEAR"'


def test_terms_doppio_apice_raddoppiato():
    assert sanitize_terms('a"b') == '"a" "b"'


def test_terms_underscore_e_trattino_sono_separatori():
    assert sanitize_terms("foo_bar-baz") == '"foo" "bar" "baz"'


def test_terms_accenti_italiani_preservati():
    assert sanitize_terms("città perché è più") == '"città" "perché" "è" "più"'


def test_terms_apostrofo_separa():
    assert sanitize_terms("l'anagrafica dell'utente") == (
        '"l" "anagrafica" "dell" "utente"'
    )


def test_terms_solo_separatori_emoji_punteggiatura_vuoto():
    # Il caller salta il MATCH quando l'output è "" — una MATCH con stringa
    # vuota è syntax error in FTS5.
    for junk in ["   ", "!!! ... ---", "🎉🚀", "___", "()*:"]:
        assert sanitize_terms(junk) == "", junk


def test_terms_input_vuoto_o_none():
    assert sanitize_terms("") == ""
    assert sanitize_terms(None) == ""


def test_terms_control_char_e_nul_separano():
    # Nessuno strip: i control char cadono fuori dai run alfanumerici, quindi
    # sono boundary di token e non collassano 'foo\x00bar' in 'foobar'.
    out = sanitize_terms("foo\x00bar\x01 baz\x1f")
    assert "\x00" not in out
    assert out == '"foo" "bar" "baz"'


def test_terms_newline_e_tab_separano():
    assert sanitize_terms("foo\nbar\tbaz") == '"foo" "bar" "baz"'


def test_terms_pua_e_separatore():
    # [^\W_]+ non copre la categoria PUA (Co): \uE000 e' droppato come
    # separatore, mentre unicode61 lo tokenizzerebbe. Approssimazione nota,
    # accettabile per email/path/doc.
    assert sanitize_terms("foo\uE000bar") == '"foo" "bar"'
    assert sanitize_terms("\uE000") == ""
    # In phrase il PUA e' solo un char qualunque dentro il valore quotato.
    assert sanitize_phrase("foo\uE000bar") == '"foo\uE000bar"'


def test_terms_wildcard_e_parentesi_neutralizzati():
    assert sanitize_terms("foo* (bar)") == '"foo" "bar"'


# --- sanitize_phrase ------------------------------------------------------


def test_phrase_email_forma_frase():
    assert sanitize_phrase("gianluca@example.com") == '"gianluca@example.com"'


def test_phrase_doppio_apice_raddoppiato():
    assert sanitize_phrase('a"b') == '"a""b"'


def test_phrase_non_tokenizza():
    assert sanitize_phrase("mario rossi") == '"mario rossi"'


def test_phrase_control_char_e_nul_rimossi():
    out = sanitize_phrase("gianluca\x00@exa\x07mple.com")
    assert "\x00" not in out
    assert out == '"gianluca@example.com"'


def test_phrase_senza_alfanumerici_vuoto():
    for junk in ["   ", "!!! ...", "🎉", "___", '"""']:
        assert sanitize_phrase(junk) == "", junk


def test_phrase_input_vuoto_o_none():
    assert sanitize_phrase("") == ""
    assert sanitize_phrase(None) == ""


def test_phrase_accenti_e_trattini():
    assert sanitize_phrase("città-stato perché") == '"città-stato perché"'


# --- sanitize_column_terms / sanitize_column_phrase -----------------------


def test_column_terms_email_from():
    assert sanitize_column_terms("email_from", "gianluca@x.com") == (
        'email_from:("gianluca" "x" "com")'
    )


def test_column_terms_colonna_ignota_fallback_unscoped():
    out = sanitize_column_terms("colonna_ignota", "foo")
    assert out == '"foo"'
    assert "colonna_ignota:" not in out


def test_column_terms_inner_vuoto():
    assert sanitize_column_terms("document", "!!!") == ""
    assert sanitize_column_terms("colonna_ignota", "!!!") == ""


def test_column_phrase_forma():
    assert sanitize_column_phrase("title", "a b") == 'title:("a b")'


def test_column_phrase_colonna_ignota_fallback():
    out = sanitize_column_phrase("foobar", "qualcosa altro")
    assert out == '"qualcosa altro"'
    assert "foobar:" not in out


def test_column_phrase_inner_vuoto():
    assert sanitize_column_phrase("email_from", "   ") == ""


def test_column_terms_case_insensitive():
    assert sanitize_column_terms("Email_From", "gianluca@x.com") == (
        'email_from:("gianluca" "x" "com")'
    )
    assert sanitize_column_terms("DOCUMENT", "foo") == 'document:("foo")'


def test_column_phrase_case_insensitive():
    assert sanitize_column_phrase("FILE_PATH", "a/b") == 'file_path:("a/b")'
    assert sanitize_column_phrase("Title", "a b") == 'title:("a b")'


def test_column_normalizzata_e_canonica_non_l_input():
    # L'output usa il nome dell'allowlist, non la forma passata dal caller.
    assert sanitize_column_terms("EmAiL_FrOm", "x").startswith("email_from:")


def test_column_case_insensitive_match_reale(fts):
    assert _match(fts, sanitize_column_phrase("EMAIL_FROM", "gianluca@example.com")) == [
        (1,)
    ]


def test_allowlist_contenuto():
    assert INDEXED_COLUMNS == frozenset(
        {"document", "title", "folder", "doc_type", "email_from", "file_path"}
    )


# --- MATCH reale su FTS5 in-memory ---------------------------------------


def test_phrase_email_match_reale_non_esplode(fts):
    expr = sanitize_phrase("gianluca@example.com")
    assert _match(fts, expr) == [(1,)]


def test_column_phrase_email_match_reale(fts):
    expr = sanitize_column_phrase("email_from", "gianluca@example.com")
    assert _match(fts, expr) == [(1,)]


def test_terms_prefisso_colonna_distrutto_non_interpretato(fts):
    # 'email_from:' diventa due parole normali: il MATCH non esplode e non
    # viene scopato sulla colonna. Non matcha perche' 'email'/'from' non sono
    # nel contenuto indicizzato (stanno solo nel *nome* della colonna).
    expr = sanitize_terms("email_from:gianluca@example.com")
    assert _match(fts, expr) == []
    # Senza il prefisso, gli stessi token matchano: la riga c'e'.
    assert _match(fts, sanitize_terms("gianluca@example.com")) == [(1,)]


def test_keyword_quotate_non_matchano_come_operatori(fts):
    # "AND" "OR" "NOT" "NEAR" cercati come parole: la riga non li contiene.
    assert _match(fts, sanitize_terms("AND OR NOT NEAR")) == []


def test_nul_non_solleva_unterminated_string(fts):
    expr = sanitize_phrase("gianluca\x00@example.com")
    assert _match(fts, expr) == [(1,)]


def test_file_path_phrase_vs_terms_rigidita_diversa(fts):
    path = "docs/preventivi/2026.pdf"
    phrase_expr = sanitize_column_phrase("file_path", path)
    terms_expr = sanitize_column_terms("file_path", path)

    assert phrase_expr == 'file_path:("docs/preventivi/2026.pdf")'
    assert terms_expr == 'file_path:("docs" "preventivi" "2026" "pdf")'
    assert phrase_expr != terms_expr

    # Entrambe matchano il path adiacente reale.
    assert _match(fts, phrase_expr) == [(1,)]
    assert _match(fts, terms_expr) == [(1,)]

    # Ma con i segmenti fuori ordine solo terms sopravvive: la phrase
    # richiede adiacenza.
    scrambled = "2026.pdf/preventivi/docs"
    assert _match(fts, sanitize_column_phrase("file_path", scrambled)) == []
    assert _match(fts, sanitize_column_terms("file_path", scrambled)) == [(1,)]


def test_wildcard_non_espande_in_match_reale(fts):
    # 'prevent*' quotato è la parola letterale 'prevent', che non esiste.
    assert _match(fts, sanitize_terms("prevent*")) == []
    assert _match(fts, sanitize_terms("preventivo")) == [(1,)]
