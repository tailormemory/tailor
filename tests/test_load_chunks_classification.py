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

# Chunk estraibili quanto NORMAL_TEXT, ma con un tentativo fallito alle
# spalle: servono a esercitare il selettore di rientro
#   WHERE model = 'failed_persistent' OR model NOT LIKE 'failed_%'
# (extract_facts_nightly:769). Il testo NON deve attivare nessun filtro,
# altrimenti il test misurerebbe lo scarto invece del rientro.
RETRY_1_TEXT = (
    "Contratto di fornitura firmato il 3 febbraio con scadenza annuale: "
    "canone 1.200 euro al mese, rinnovo tacito salvo disdetta a 60 giorni."
)
RETRY_2_TEXT = (
    "Nota spese di aprile approvata dall'amministrazione: trasferta a "
    "Milano, 340 euro di rimborso chilometrico piu' 95 euro di pernottamento."
)
PERSISTENT_TEXT = (
    "Tabella assicurativa multilingua con codici polizza e massimali, "
    "illeggibile per l'LLM dopo tre tentativi consecutivi a vuoto."
)


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
    ("chunk_failed_1", RETRY_1_TEXT),
    ("chunk_failed_2", RETRY_2_TEXT),
    ("chunk_failed_persistent", PERSISTENT_TEXT),
]

EXPECTED_MODELS = {
    "chunk_none": "skipped_empty",
    "chunk_grid": "skipped_empty",
    "chunk_short": "skipped_short",
    "chunk_code": "skipped_code",
    "chunk_done": "anthropic",   # pre-esistente, non deve essere toccato
    # I tre failed_* sono pre-esistenti come chunk_done: load_chunks non
    # riscrive mai una riga già presente, nemmeno quando rimette il chunk in
    # coda. Il model resta quello dell'ultimo tentativo.
    "chunk_failed_1": "failed_1",
    "chunk_failed_2": "failed_2",
    "chunk_failed_persistent": "failed_persistent",
    # chunk_normal: nessuna riga attesa
}

# Chunk estraibili attesi in coda: chunk_normal (mai visto) + i due
# transienti che rientrano. chunk_done e chunk_failed_persistent restano
# fuori.
EXPECTED_QUEUE = ["chunk_failed_1", "chunk_failed_2", "chunk_normal"]


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
    fc.executemany(
        "INSERT INTO extraction_log (chunk_id, facts_count, model, extracted_at) "
        "VALUES (?, ?, ?, ?)",
        [
            ("chunk_done", 3, "anthropic", "2026-08-16T02:00:00"),
            # 1o e 2o tentativo a vuoto: transienti, devono rientrare.
            ("chunk_failed_1", 0, "failed_1", "2026-08-16T02:00:00"),
            ("chunk_failed_2", 0, "failed_2", "2026-08-16T02:00:00"),
            # 3 tentativi a vuoto: permanente, deve restare fuori.
            ("chunk_failed_persistent", 0, "failed_persistent", "2026-08-16T02:00:00"),
        ],
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

def test_only_extractable_chunks_are_queued(kb):
    """In coda solo gli estraibili: scarti, già-fatti e permanenti restano fuori.

    Confronto su `sorted`: l'ordine del result set dipende dal GROUP BY di
    SQLite, non dall'ordine di inserimento — inchiodarlo renderebbe il test
    fragile senza coprire nulla in più.
    """
    chunks = efn.load_chunks()
    assert sorted(c["id"] for c in chunks) == EXPECTED_QUEUE
    by_id = {c["id"]: c for c in chunks}
    assert by_id["chunk_normal"]["text"] == NORMAL_TEXT
    assert by_id["chunk_normal"]["source"] == "document"
    assert by_id["chunk_normal"]["date"] == "2026-08-17"


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

def test_transient_failures_are_requeued(kb):
    """`failed_1` / `failed_2` rientrano: il retry ha 3 tentativi totali.

    Ramo `model NOT LIKE \'failed_%\'` del selettore (extract_facts_nightly:769):
    la riga in extraction_log esiste, ma NON esclude il chunk. In produzione
    non è mai stato esercitato da dati reali (13 failed_persistent, 0
    failed_1/failed_2), quindi un domani chi semplifica quella WHERE in
    `model IS NOT NULL` non romperebbe nessun test — questo lo rompe.
    """
    chunks = efn.load_chunks()
    queued = {c["id"]: c for c in chunks}
    assert "chunk_failed_1" in queued
    assert "chunk_failed_2" in queued
    assert queued["chunk_failed_1"]["text"] == RETRY_1_TEXT
    assert queued["chunk_failed_2"]["text"] == RETRY_2_TEXT


def test_persistent_failure_stays_excluded(kb):
    """`failed_persistent` resta fuori: è il freno al retry loop infinito.

    Il ramo `model = \'failed_persistent\'` è l'unico `failed_*` che esclude.
    Senza, i chunk che l'LLM non sa parsare (mojibake, tabelle assicurative
    multilingua — indagine 2026-05-25) rientrerebbero ogni notte per sempre.
    """
    chunks = efn.load_chunks()
    assert "chunk_failed_persistent" not in [c["id"] for c in chunks]
    # e la riga non viene ri-marcata come scarto: resta failed_persistent
    assert _log(kb)["chunk_failed_persistent"] == ("failed_persistent", 0)


def test_requeued_chunk_log_row_is_not_rewritten(kb):
    """Il rientro non tocca extraction_log: il model resta failed_N.

    load_chunks scrive solo gli scarti. Se un domani marcasse anche i
    rientri, il chunk uscirebbe dal denominatore di coverage pur non avendo
    mai prodotto fatti.
    """
    before = {k: v for k, v in _log(kb).items() if k.startswith("chunk_failed_")}
    efn.load_chunks()
    after = {k: v for k, v in _log(kb).items() if k.startswith("chunk_failed_")}
    assert after == before


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
