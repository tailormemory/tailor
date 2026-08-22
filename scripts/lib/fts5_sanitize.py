"""Sanitizzazione query FTS5 — funzioni pure, zero DB.

`sanitize_terms` (AND implicito) e `sanitize_terms_or` (OR esplicito,
unscoped, con stoplist) sono due funzioni distinte di proposito: vedi il
docstring di `sanitize_terms_or`.

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
import unicodedata

INDEXED_COLUMNS = frozenset(
    {"document", "title", "folder", "doc_type", "email_from", "file_path"}
)

# Run alfanumerici Unicode, `_` escluso (FTS5 lo tratta come non-token).
_TOKEN_RE = re.compile(r"[^\W_]+", re.UNICODE)

# NUL + control char C0/C1 non stampabili. Usato solo da sanitize_phrase, che
# quota la stringa intera e altrimenti li renderebbe letterali.
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f-\x9f]")

_ALNUM_RE = re.compile(r"[^\W_]", re.UNICODE)

# Stopword italiane per il ramo unscoped-OR. Forma NORMALIZZATA (lowercase,
# senza diacritici): il confronto passa sempre da `_normalize`, quindi
# `Perché`/`perche`/`PERCHE` collassano tutti su `perche`.
#
# LETTERALE e CONGELATA: e' la lista con cui il ramo lexical e' stato misurato
# (recall@n 5/13, mediana 38 ms). Cambiarla invalida il confronto pre/post —
# si tocca solo insieme a una nuova misura offline.
_STOPWORDS = frozenset("""
a ad agli ai al all alla alle allo anche c che chi ci come con cosa cui da dai
dal dall dalla dalle dallo degli dei del dell della delle dello di do e ed era
essere fa fare fra gli ha hai hanno ho i il in io la le lei li lo loro ma me mi
ne negli nei nel nell nella nelle nello noi non o per perche piu quale quali
quando quanta quante quanti quanto qual si sia siamo sono su sui sul sull sulla
sulle sullo ti tra tu tuo un una uno vi
""".split())


def _normalize(token):
    """Forma di confronto per la stoplist: lowercase + NFD senza combining marks.

    Specchia il tokenizer dell'indice (`unicode61 remove_diacritics 1`): la' i
    diacritici cadono in fase di indicizzazione, qui devono cadere in fase di
    filtro, altrimenti `perche'` e `piu'` scritti con accento passerebbero il
    controllo e resterebbero nell'OR come termini "informativi".
    """
    decomposed = unicodedata.normalize("NFD", token.lower())
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch))


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


def sanitize_terms_or(text):
    """Tokenizza, scarta le stopword, dedup e joina con OR esplicito. UNSCOPED.

    'valori delle analisi' -> '"valori" OR "analisi"'.

    Esiste separata da `sanitize_terms` (AND implicito) e non come suo flag:
    il ramo `col:valore` DEVE restare in AND. Un OR ereditato da
    `sanitize_column_terms` renderebbe `email_from:gianluca@x.com` un match su
    qualunque indirizzo che contenga "com".

    - Stoplist confrontata sulla forma normalizzata (`_normalize`), non sul
      token grezzo: cadono anche `Perche'`, `Piu`, `NON`.
    - Dedup sulla stessa forma normalizzata: `piu' piu valore` non produce OR
      ridondanti (vince la prima occorrenza, il quoting e' della forma
      originale — l'indice normalizza comunque).
    - Il quoting resta quello di `_quote`: mai barewords, quindi le keyword
      FTS5 digitate dall'utente (AND, OR, NEAR) escono come stringhe.

    Query interamente stopword -> '' (nessun fallback ai token originali: un
    OR di sole stopword tocca ~124k righe con top-3 di puro rumore, il
    chiamante deve saltare il ramo). Nessun token alfanumerico -> ''.
    """
    if not text:
        return ""
    seen = set()
    quoted = []
    for token in _TOKEN_RE.findall(text):
        norm = _normalize(token)
        if norm in _STOPWORDS or norm in seen:
            continue
        seen.add(norm)
        quoted.append(_quote(token))
    return " OR ".join(quoted)


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
