"""Tests: classificazione dei chunk in load_chunks() (extract_facts_nightly).

load_chunks() non è solo un SELECT: è il punto dove un chunk viene deciso
NON-COVERABLE e la decisione viene PERSISTITA in extraction_log come
`model = 'skipped_empty' | 'skipped_short' | 'skipped_code'`. Quelle righe
escono poi dal denominatore di coverage (kb_stats in mcp_server, get_kb_stats
in scripts/lib/tool_executor) e dal selettore di rientro della notte dopo.

L'ordine dei tre filtri è quindi semantica, non stile:

    1. not text or is_empty_chunk(text)          -> skipped_empty
    2. len(text.strip()) < MIN_FACT_CHUNK_CHARS  -> skipped_short
    3. is_code_heavy(text)                       -> skipped_code

Una griglia xlsx vuota è corta *e* vuota: sale il primo filtro che la
intercetta, e deve essere `empty`. Chi domani sposta `is_code_heavy` sopra il
filtro di lunghezza cambia la classificazione di chunk reali senza che nulla
fallisca — questi test esistono per farlo fallire.

Approccio: load_chunks() REALE su sqlite temporanei, con CHROMA_DB_PATH e
FACTS_DB_PATH monkeypatchati a livello modulo. Testare i soli predicati
(is_empty_chunk / is_code_heavy) non coprirebbe il bug che ha originato il
fix: non era "questo testo è corto?", era "load_chunks marca in
extraction_log il chunk che scarta?" — i chunk < 50 char uscivano con un
`continue` secco, senza lasciare traccia, e rientravano nel denominatore ogni
notte (floor permanente ~1.119 in coda).
"""

from __future__ import annotations

import os
import sqlite3
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from scripts.enrichment import extract_facts_nightly as efn  # noqa: E402


# ── fixture di testo ─────────────────────────────────────────────────────
# Ogni costante è scelta per attivare UN solo filtro; i commenti dicono
# perché non attiva gli altri.

# alnum ratio 0 → is_empty_chunk. È anche < 50 char: deve vincere `empty`.
GRID_TEXT = "| | | | |\n| | | |\n| | | | |"

# 18 char, alnum ratio alto (non empty), 1 riga sola (non code-heavy).
SHORT_TEXT = "Fattura 42 pagata."

# 4 righe, 3 matchano _CODE_LINE_RE (0.75 > 0.6), > 50 char, alnum alto.
CODE_TEXT = (
    'function initApp() {\n'
    '  const root = document.getElementById("app");\n'
    '  root.addEventListener("click", handler);\n'
    '}'
)

# Prosa normale, > 50 char, nessun filtro attivo → deve finire in coda.
NORMAL_TEXT = (
    "Riunione del 12 marzo con il consulente fiscale: confermato il "
    "versamento IVA trimestrale entro il 16 aprile, importo 4.250 euro."
)

# Chunk già lavorato in una notte precedente da un backend reale.
DONE_TEXT = NORMAL_TEXT + " Verbale archiviato."


CHROMA_SCHEMA = """
CREATE TABLE embeddings (
    id INTEGER PRIMARY KEY,
    segment_id TEXT NOT NULL,
    embedding_id TEXT NOT NULL,
    seq_id BLOB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (segment_id, embedding_id)
);
CREATE TABLE embedding_metadata (
    id INTEGER REFERENCES embeddings(id),
    key TEXT NOT NULL,
    string_value TEXT,
    int_value INTEGER,
    float_value REAL, bool_value INTEGER,
    PRIMARY KEY (id, key)
);
CREATE TABLE 'embedding_fulltext_search_content'(id INTEGER PRIMARY KEY, c0);
"""
# NB: in produzione embedding_fulltext_search_content è la shadow table di un
# indice FTS5, ma la query di load_chunks la tratta come tabella normale
# (JOIN su id, lettura di c0). Qui è una tabella plain con le stesse due
# colonne: nessun indice FTS5 da costruire.

FACTS_SCHEMA = """
CREATE TABLE extraction_log (
    chunk_id TEXT PRIMARY KEY,
    facts_count INTEGER DEFAULT 0,
    model TEXT DEFAULT '',
    extracted_at TEXT NOT NULL
);
"""

# (embedding_id, testo). L'ordine è l'ordine di inserimento, che è anche
# l'ordine in cui load_chunks li incontra.
FIXTURE_CHUNKS = [
    ("chunk_none", None),
    ("chunk_grid", GRID_TEXT),
    ("chunk_short", SHORT_TEXT),
    ("chunk_code", CODE_TEXT),
    ("chunk_normal", NORMAL_TEXT),
    ("chunk_done", DONE_TEXT),
]

EXPECTED_MODELS = {
    "chunk_none": "skipped_empty",
    "chunk_grid": "skipped_empty",
    "chunk_short": "skipped_short",
    "chunk_code": "skipped_code",
    "chunk_done": "anthropic",   # pre-esistente, non deve essere toccato
    # chunk_normal: nessuna riga attesa
}


@pytest.fixture
def kb(tmp_path, monkeypatch):
    """Chroma + facts temporanei, path patchati sul modulo.

    load_chunks() legge CHROMA_DB_PATH / FACTS_DB_PATH come globali del
    modulo a ogni chiamata: monkeypatch.setattr basta, nessun refactor della
    funzione. Niente tocca db/ di produzione.
    """
    chroma_path = tmp_path / "chroma.sqlite3"
    facts_path = tmp_path / "facts.sqlite3"

    cc = sqlite3.connect(chroma_path)
    cc.executescript(CHROMA_SCHEMA)
    for rowid, (eid, text) in enumerate(FIXTURE_CHUNKS, start=1):
        cc.execute(
            "INSERT INTO embeddings (id, segment_id, embedding_id, seq_id) "
            "VALUES (?, ?, ?, ?)",
            (rowid, "seg-test", eid, b"\x00"),
        )
        # Almeno una fra 'date' e 'source' è OBBLIGATORIA: la query filtra
        # `WHERE m.key IN ('date','source')` sul JOIN, e un chunk senza
        # nessuna delle due sparisce dal result set — il test validerebbe il
        # nulla. Qui mettiamo entrambe.
        cc.execute(
            "INSERT INTO embedding_metadata (id, key, string_value) VALUES (?, 'date', ?)",
            (rowid, "2026-08-17"),
        )
        cc.execute(
            "INSERT INTO embedding_metadata (id, key, string_value) VALUES (?, 'source', ?)",
            (rowid, "document"),
        )
        cc.execute(
            "INSERT INTO embedding_fulltext_search_content (id, c0) VALUES (?, ?)",
            (rowid, text),
        )
    cc.commit()
    cc.close()

    fc = sqlite3.connect(facts_path)
    fc.executescript(FACTS_SCHEMA)
    fc.execute(
        "INSERT INTO extraction_log (chunk_id, facts_count, model, extracted_at) "
        "VALUES (?, ?, ?, ?)",
        ("chunk_done", 3, "anthropic", "2026-08-16T02:00:00"),
    )
    fc.commit()
    fc.close()

    monkeypatch.setattr(efn, "CHROMA_DB_PATH", str(chroma_path))
    monkeypatch.setattr(efn, "FACTS_DB_PATH", str(facts_path))
    return facts_path


def _log(facts_path):
    """{chunk_id: (model, facts_count)} da extraction_log."""
    conn = sqlite3.connect(f"file:{facts_path}?mode=ro", uri=True)
    rows = conn.execute(
        "SELECT chunk_id, model, facts_count FROM extraction_log").fetchall()
    conn.close()
    return {r[0]: (r[1], r[2]) for r in rows}


# ── coda ─────────────────────────────────────────────────────────────────

def test_only_extractable_chunk_is_queued(kb):
    """Un solo chunk è estraibile: gli altri cinque sono scarti o già fatti."""
    chunks = efn.load_chunks()
    assert [c["id"] for c in chunks] == ["chunk_normal"]
    assert chunks[0]["text"] == NORMAL_TEXT
    assert chunks[0]["source"] == "document"
    assert chunks[0]["date"] == "2026-08-17"


# ── classificazione ──────────────────────────────────────────────────────

@pytest.mark.parametrize("chunk_id,model", sorted(EXPECTED_MODELS.items()))
def test_chunk_classification(kb, chunk_id, model):
    """Ogni scarto finisce in extraction_log con il proprio `model`.

    chunk_none (text NULL) è un CAMBIO DI COMPORTAMENTO deliberato: prima
    usciva con `continue` senza lasciare traccia, ora è persistito come
    skipped_empty. Inchiodato qui apposta.
    """
    efn.load_chunks()
    assert _log(kb).get(chunk_id, (None, None))[0] == model


def test_grid_is_empty_not_short(kb):
    """La griglia vuota è corta E vuota: deve vincere `empty`.

    È l'invariante che il riordino dei filtri protegge — se qualcuno rimette
    il filtro di lunghezza sopra is_empty_chunk, questo diventa
    'skipped_short' e `skipped_short` smette di significare "testo presente
    ma troppo corto".
    """
    efn.load_chunks()
    assert len(GRID_TEXT.strip()) < efn.MIN_FACT_CHUNK_CHARS  # davvero ambiguo
    assert _log(kb)["chunk_grid"][0] == "skipped_empty"


def test_code_is_code_not_short(kb):
    """Il chunk code-heavy supera la soglia di lunghezza: resta `skipped_code`."""
    efn.load_chunks()
    assert len(CODE_TEXT.strip()) >= efn.MIN_FACT_CHUNK_CHARS
    assert _log(kb)["chunk_code"][0] == "skipped_code"


def test_extractable_chunk_is_not_marked(kb):
    """Il chunk normale non deve avere riga: marcarlo lo escluderebbe per sempre."""
    efn.load_chunks()
    assert "chunk_normal" not in _log(kb)


def test_skipped_rows_have_zero_facts(kb):
    """Gli scarti sono marcati con facts_count = 0, non con un conteggio finto."""
    efn.load_chunks()
    log = _log(kb)
    for cid in ("chunk_none", "chunk_grid", "chunk_short", "chunk_code"):
        assert log[cid][1] == 0


# ── selettore di rientro ─────────────────────────────────────────────────

def test_already_extracted_chunk_is_untouched(kb):
    """Un chunk con model reale è escluso dal selettore e non ri-marcato."""
    before = _log(kb)["chunk_done"]
    efn.load_chunks()
    assert _log(kb)["chunk_done"] == before


def test_second_run_is_idempotent(kb):
    """Seconda notte: gli scarti sono già in log, nessun duplicato, nessun
    model riscritto (INSERT OR IGNORE + PRIMARY KEY su chunk_id).

    Il chunk normale resta in coda: non è marcato, quindi rientra — è il
    comportamento voluto, non un leak.
    """
    first_chunks = efn.load_chunks()
    snapshot = _log(kb)

    second_chunks = efn.load_chunks()
    assert [c["id"] for c in second_chunks] == [c["id"] for c in first_chunks]
    assert _log(kb) == snapshot
    assert len(snapshot) == len(EXPECTED_MODELS)
