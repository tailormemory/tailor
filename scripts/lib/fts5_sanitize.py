"""Sanitizzazione query FTS5 — funzioni pure, zero DB.

Invariante: mai emettere bareword. Ogni token/valore esce tra doppie virgolette,
con `"` interni raddoppiati. Il quoting neutralizza `@ . : - / ( ) *` e le
keyword `AND OR NOT NEAR` come sintassi: FTS5 le tratta come stringhe.

In `sanitize_phrase` i NUL e i control char non stampabili sono rimossi prima
del quoting: `\\x00` rompe FTS5 con "unterminated string". `sanitize_terms` non
ne ha bisogno — la tokenizzazione isola i run alfanumerici, quindi un control
char è un boundary e non finisce mai dentro un token.

`[^\\W_]+` è approssimazione deliberata di unicode61 — non copre la categoria
PUA (Co), che unicode61 tokenizza: un codepoint PUA qui è separatore e viene
droppato. Accettabile per email/path/doc, non per codepoint esotici.
"""

import re

INDEXED_COLUMNS = frozenset(
    {"document", "title", "folder", "doc_type", "email_from", "file_path"}
)

# Run alfanumerici Unicode, `_` escluso (FTS5 lo tratta come non-token).
_TOKEN_RE = re.compile(r"[^\W_]+", re.UNICODE)

# NUL + control char C0/C1 non stampabili. Usato solo da sanitize_phrase, che
# quota la stringa intera e altrimenti li renderebbe letterali.
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f-\x9f]")

_ALNUM_RE = re.compile(r"[^\W_]", re.UNICODE)


def _quote(value):
    """Quota `value` per FTS5 raddoppiando i `"` interni."""
    return '"' + value.replace('"', '""') + '"'


def _strip_control(value):
    """Rimuove NUL e control char non stampabili."""
    return _CONTROL_RE.sub("", value)


def sanitize_terms(text):
    """Tokenizza e quota ogni token; join con spazio = AND implicito.

    'a b c' -> '"a" "b" "c"'. Nessun token alfanumerico -> ''.

    NUL-safe senza strip: i control char cadono fuori dai run alfanumerici,
    quindi separano i token invece di collassarli.
    """
    if not text:
        return ""
    tokens = _TOKEN_RE.findall(text)
    return " ".join(_quote(t) for t in tokens)


def sanitize_phrase(value):
    """Quota l'INTERA stringa come frase adiacente, senza pre-tokenizzare.

    'gianluca@example.com' -> '"gianluca@example.com"'.
    Nessun carattere alfanumerico dopo la pulizia -> ''.
    """
    if not value:
        return ""
    cleaned = _strip_control(value).strip()
    if not _ALNUM_RE.search(cleaned):
        return ""
    return _quote(cleaned)


def sanitize_column_terms(col, value):
    """Terms scopati su colonna: 'col:("a" "b")'.

    Il nome colonna è normalizzato lowercase (l'allowlist è la forma canonica).
    Colonna ignota -> fallback unscoped (mai 'col:' nudo). Inner vuoto -> ''.
    """
    inner = sanitize_terms(value)
    if not inner:
        return ""
    key = col.lower() if col else ""
    if key not in INDEXED_COLUMNS:
        return inner
    return key + ":(" + inner + ")"


def sanitize_column_phrase(col, value):
    """Phrase scopata su colonna: 'col:("a b")'.

    Il nome colonna è normalizzato lowercase (l'allowlist è la forma canonica).
    Colonna ignota -> fallback unscoped. Inner vuoto -> ''.
    """
    inner = sanitize_phrase(value)
    if not inner:
        return ""
    key = col.lower() if col else ""
    if key not in INDEXED_COLUMNS:
        return inner
    return key + ":(" + inner + ")"
