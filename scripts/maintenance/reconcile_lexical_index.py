"""
reconcile_lexical_index.py — Riallinea db/lexical_index.sqlite3 alla KB.

P6 step 2. Job periodico (nightly): legge la KB in sola lettura e riconcilia
l'indice lessicale dedicato confrontando **set di id + fingerprint per riga**.
Il primo run su indice vuoto *è* il bootstrap: nessun codice separato.

Invarianti rispettate (design P6 §2bis):
    I6 — legge SOLO la collection `tailor_kb_v2` e il suo segmento METADATA,
         auto-detectato come `build_entity_index.py:36`.
    I7 — `source` vive in una colonna NON-FTS del sidecar `lexical_meta`,
         interrogabile per il filtro pre-query del ramo lessicale.

L'indice è un **accessorio, non una fonte** (I1): rigenerabile da zero. Questo
job non tocca la KB (`mode=ro&immutable=1`, nessun PersistentClient) e non è
agganciato a nessun writer.

Uso:
    cd /path/to/tailor && .venv/bin/python3 -m scripts.maintenance.reconcile_lexical_index
    .venv/bin/python3 -m scripts.maintenance.reconcile_lexical_index --dry-run
    .venv/bin/python3 -m scripts.maintenance.reconcile_lexical_index --stats

Exit code non-zero se le righe riconciliate superano `--max-churn` in un run
non-bootstrap: una divergenza di massa non è normale, è un sintomo. Fail-loud,
niente apply silenzioso.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sqlite3
import sys
import time
from dataclasses import dataclass, field

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "lib"))
from telegram_notify import send_telegram  # noqa: E402

# ============================================================
# CONFIGURAZIONE
# ============================================================

DB_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "db"
)
CHROMA_DB = os.path.join(DB_DIR, "chroma.sqlite3")
LEXICAL_DB = os.path.join(DB_DIR, "lexical_index.sqlite3")

COLLECTION_NAME = "tailor_kb_v2"
# `collections` ha UNIQUE (name, database_id): il nome è unico solo dentro un
# database. Qualificare evita che un futuro multi-tenant risolva il segmento
# sbagliato (Codex #1).
DATABASE_NAME = "default_database"

# Sentinel di fingerprint per le righe la cui proiezione FTS diverge dal
# sidecar. NON è un sha256 (contiene `!`, non è esadecimale) -> non può
# collidere con una fingerprint vera, quindi forza sempre un rebuild.
FTS_DIVERGENT = "!fts-divergent"

# Chiavi lette da embedding_metadata. `chroma:document` è il corpo nudo
# (troncato a 4000 char dal contratto embedding P1), non il full-text.
DOCUMENT_KEY = "chroma:document"
# `file_path` porta la gerarchia di cartelle ("Case/Viale Ippocrate/Rate
# mutuo/..."): unicode61 spezza su slash E underscore, quindi "Ippocrate" o
# "Marche" pescano il sotto-ramo. È il caso d'uso "cerca in Case, poi restringi".
FTS_META_KEYS = ("title", "folder", "doc_type", "email_from", "file_path")
SOURCE_KEY = "source"
READ_KEYS = (DOCUMENT_KEY,) + FTS_META_KEYS + (SOURCE_KEY,)

# Ordine dei campi nella fingerprint. NON riordinare e non inserire in mezzo:
# ogni cambio di ordine invalida tutte le fingerprint su disco. Aggiungere in
# coda è additivo per la lettura, ma cambia comunque OGNI hash (il campo nuovo
# entra nel digest anche quando è vuoto) -> al primo run post-modifica è un
# update di massa che il guardiano blocca. Va gestito: o l'indice non esiste
# ancora (allora è il bootstrap, tutti insert, nessun problema), o serve un
# `--max-churn 1.0` una-tantum come escape hatch consapevole.
FINGERPRINT_FIELDS = (
    "document",
    "title",
    "folder",
    "doc_type",
    "email_from",
    "source",
    "file_path",
)

DEFAULT_MAX_CHURN = 0.20  # 20% del corpus in un run non-bootstrap
BATCH = 1000


# ============================================================
# LOGICA PURA — nessun I/O qui sotto
# ============================================================


def as_text(value) -> str:
    """Normalizza un valore di campo a stringa.

    CAVEAT tipi: i ritorni di sqlite/chroma non sono garantiti `str`. Presenza
    testata via `is None` + `len`, MAI via bool() implicito — un ndarray o un
    valore numpy in un `if value:` solleva
    `ValueError: truth value of an array is ambiguous`, oppure (peggio) un `0`
    / `""` legittimo verrebbe scambiato per assente.

    CONFINE DICHIARATO (Codex #4): il ramo bytes usa `errors="replace"`, quindi
    due sequenze byte invalide DIVERSE possono collassare sullo stesso U+FFFD e
    produrre la stessa fingerprint -> una modifica non verrebbe vista. È
    irraggiungibile sul corpus reale: i campi letti sono `string_value` di
    `embedding_metadata` (TEXT, già decodificato da sqlite), mai bytes grezzi.
    Il ramo esiste come rete di sicurezza sui tipi, non come percorso vivo. Se
    un domani una sorgente passasse bytes veri, questo va rivisto (`errors=
    "surrogateescape"` o hashare i byte grezzi).
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def compute_fingerprint(row: dict) -> str:
    """sha256 dei campi indicizzati (§7).

    I campi sono NUL-delimitati e length-prefissati: una concatenazione nuda
    collide (`title="ab", folder=""` == `title="a", folder="b"`) e le collisioni
    di fingerprint sono divergenze silenziose — esattamente ciò che il
    guardiano deve catturare.
    """
    h = hashlib.sha256()
    for name in FINGERPRINT_FIELDS:
        value = as_text(row.get(name)).encode("utf-8")
        h.update(str(len(value)).encode("ascii"))
        h.update(b"\x00")
        h.update(value)
        h.update(b"\x00")
    return h.hexdigest()


class ReconcileAborted(RuntimeError):
    """Il guardiano ha bloccato il run. Porta con sé il report.

    Sottoclasse di RuntimeError: i callsite che catturano RuntimeError (main,
    test esistenti) continuano a funzionare. Serve perché `reconcile()` solleva
    PRIMA di ritornare il report, ma il messaggio FAIL ha bisogno dei conteggi.
    """

    def __init__(self, message: str, report: dict):
        super().__init__(message)
        self.report = report


@dataclass
class ReconcilePlan:
    """Piano di riconciliazione. Puro: set di id, nessun contenuto."""

    inserted: list = field(default_factory=list)
    deleted: list = field(default_factory=list)
    updated: list = field(default_factory=list)
    unchanged: list = field(default_factory=list)

    @property
    def churn(self) -> int:
        """Righe toccate. `unchanged` non è churn."""
        return len(self.inserted) + len(self.deleted) + len(self.updated)

    def counts(self) -> dict:
        return {
            "inserted": len(self.inserted),
            "deleted": len(self.deleted),
            "updated": len(self.updated),
            "unchanged": len(self.unchanged),
        }


def build_plan(kb_fingerprints: dict, index_fingerprints: dict) -> ReconcilePlan:
    """Confronta id + fingerprint. Pura: due dict id->fingerprint in, piano out.

    - id in KB non in FTS  -> insert (nuovi chunk; su indice vuoto = bootstrap)
    - id in FTS non in KB  -> delete (chiude il buco DELETE: i writer fanno
                              delete reali che `verified_upsert` non vede)
    - id in entrambi, fingerprint diversa -> update (contenuto/metadati mutati
                              da QUALUNQUE writer — il confine writer è
                              irrilevante con la riconciliazione a fingerprint)
    """
    plan = ReconcilePlan()
    for chunk_id, fingerprint in kb_fingerprints.items():
        if chunk_id not in index_fingerprints:
            plan.inserted.append(chunk_id)
        elif index_fingerprints[chunk_id] != fingerprint:
            plan.updated.append(chunk_id)
        else:
            plan.unchanged.append(chunk_id)
    for chunk_id in index_fingerprints:
        if chunk_id not in kb_fingerprints:
            plan.deleted.append(chunk_id)
    return plan


def is_bootstrap(index_fingerprints: dict) -> bool:
    """Indice vuoto = bootstrap atteso, il guardiano non si applica."""
    return len(index_fingerprints) == 0


def check_churn(plan: ReconcilePlan, index_fingerprints: dict, max_churn_ratio: float) -> tuple:
    """Guardiano fail-loud. Pura. Ritorna (ok, message).

    Un COUNT non basta (§7): il confronto è su id+fingerprint, e qui si misura
    quante righe il piano tocca rispetto al corpus. Base = max(KB, indice) così
    un mass-delete (KB improvvisamente vuota) non divide per zero e non passa
    per "churn 0%".
    """
    if is_bootstrap(index_fingerprints):
        return True, "bootstrap (indice vuoto): guardiano non applicabile"

    corpus = max(
        len(plan.inserted) + len(plan.updated) + len(plan.unchanged),
        len(index_fingerprints),
    )
    if corpus == 0:
        return True, "corpus vuoto su entrambi i lati: nulla da riconciliare"

    ratio = plan.churn / corpus
    if ratio > max_churn_ratio:
        return False, (
            f"churn {plan.churn}/{corpus} = {ratio:.1%} > soglia {max_churn_ratio:.1%} "
            f"({plan.counts()}) — divergenza di massa, NON procedo"
        )
    return True, f"churn {plan.churn}/{corpus} = {ratio:.1%} entro la soglia {max_churn_ratio:.1%}"


# ============================================================
# I/O — lettura KB (read-only) e scrittura sidecar
# ============================================================


def get_metadata_segment(conn) -> str:
    """Segment_id METADATA della collection tailor_kb_v2 (I6).

    Qualificato per **database** (`default_database`), come fa il codice Chroma:
    `collections` ha `UNIQUE (name, database_id)`, quindi il nome è unico solo
    DENTRO un database. Filtrare il solo `name` (come `build_entity_index.py:36`)
    è ambiguo il giorno che esiste un secondo database/tenant: si prenderebbe il
    segmento sbagliato in silenzio.

    Il join per database è però version-dependent (`databases` / `database_id`
    non esistono in ogni versione ChromaDB) -> resta il fallback non qualificato.
    In ENTRAMBI i rami la risoluzione è fail-loud: `fetchall()` + abort se i
    risultati sono più di uno, mai un `fetchone()` cieco che sceglie a caso.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT s.id
            FROM segments s
            JOIN collections c ON s.collection = c.id
            JOIN databases d ON c.database_id = d.id
            WHERE c.name = ? AND d.name = ? AND s.scope = 'METADATA'
            """,
            (COLLECTION_NAME, DATABASE_NAME),
        )
        rows = cursor.fetchall()
        qualifier = f"database {DATABASE_NAME}"
    except sqlite3.OperationalError:
        # Schema senza `databases`/`database_id`: versione ChromaDB diversa.
        cursor.execute(
            """
            SELECT s.id
            FROM segments s
            JOIN collections c ON s.collection = c.id
            WHERE c.name = ? AND s.scope = 'METADATA'
            """,
            (COLLECTION_NAME,),
        )
        rows = cursor.fetchall()
        qualifier = "senza qualifica di database (schema legacy)"

    if len(rows) > 1:
        raise RuntimeError(
            f"Segmento METADATA ambiguo per {COLLECTION_NAME} ({qualifier}): "
            f"{len(rows)} risultati {[r[0] for r in rows]} — "
            f"non scelgo a caso, risolvi la collection duplicata"
        )
    if len(rows) == 0:
        raise RuntimeError(
            f"Collection {COLLECTION_NAME} / segmento METADATA non trovati ({qualifier})"
        )
    return rows[0][0]


def open_kb_readonly(chroma_path: str):
    """Apre la KB in sola lettura. `immutable=1`: niente WAL, niente lock,
    nessuna scrittura possibile — la KB non si tocca (design §2). Niente
    PersistentClient: istanziarlo caricherebbe l'HNSW e i writer chromadb.
    """
    return sqlite3.connect(f"file:{chroma_path}?mode=ro&immutable=1", uri=True)


def read_kb_rows(conn, segment_id: str) -> dict:
    """Legge document + metadati dalla KB. Ritorna {chunk_id: row-dict}.

    Una riga per (id, key): si pivota in memoria. I chunk senza `folder` /
    `email_from` (metadati assenti, non vuoti) entrano comunque — as_text()
    li normalizza a "" e restano cercabili via `title`.
    """
    cursor = conn.cursor()
    placeholders = ",".join("?" for _ in READ_KEYS)
    cursor.execute(
        f"""
        SELECT e.embedding_id, em.key, em.string_value
        FROM embeddings e
        JOIN embedding_metadata em ON em.id = e.id
        WHERE e.segment_id = ?
          AND em.key IN ({placeholders})
        """,
        (segment_id,) + READ_KEYS,
    )

    rows = {}
    while True:
        batch = cursor.fetchmany(BATCH)
        if len(batch) == 0:
            break
        for chunk_id, key, value in batch:
            if chunk_id is None:
                continue
            row = rows.get(chunk_id)
            if row is None:
                row = {name: "" for name in FINGERPRINT_FIELDS}
                rows[chunk_id] = row
            name = "document" if key == DOCUMENT_KEY else key
            row[name] = as_text(value)
    return rows


def fingerprints_of(rows: dict) -> dict:
    return {chunk_id: compute_fingerprint(row) for chunk_id, row in rows.items()}


def ensure_schema(conn) -> None:
    """Crea lo schema del sidecar se assente. Idempotente.

    `lexical_fts`  — FTS5, tokenizer LESSICALE (`unicode61 remove_diacritics`),
                     non trigram: confini di parola veri. `chunk_id` UNINDEXED:
                     è la chiave di ritorno, non un termine cercabile (I1 — la
                     FTS è un indice di ID, mai una fonte di contenuto).
                     `file_path` è indicizzato: unicode61 tratta `/` e `_` come
                     separatori, quindi ogni segmento della gerarchia diventa
                     un token cercabile.
    `lexical_meta` — sidecar NON-FTS: `source` per il filtro pre-query (I7),
                     `fingerprint` per la riconciliazione (§7).
    """
    conn.executescript(
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS lexical_fts USING fts5(
            chunk_id UNINDEXED,
            document,
            title,
            folder,
            doc_type,
            email_from,
            file_path,
            tokenize='unicode61 remove_diacritics 1'
        );
        CREATE TABLE IF NOT EXISTS lexical_meta (
            chunk_id TEXT PRIMARY KEY,
            source TEXT,
            fingerprint TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_lexical_meta_source ON lexical_meta (source);
        """
    )


def open_index(path: str):
    """Apre l'indice in scrittura, creando file + schema se assenti."""
    conn = sqlite3.connect(path)
    ensure_schema(conn)
    return conn


def open_index_for_plan(path: str, dry_run: bool):
    """Connessione all'indice per calcolare il piano. `None` = tratta l'indice
    come vuoto senza aprire nulla.

    Un `--dry-run` è READ-ONLY (Codex #3): prima `open_index()` girava PRIMA del
    check `dry_run`, quindi un dry-run su un path inesistente CREAVA file e
    schema — un comando che dichiara di non scrivere non deve lasciare tracce,
    o non è ispezionabile e diventa un modo per creare l'indice per sbaglio.

    - dry-run + file assente -> None: il piano è quello di un bootstrap.
    - dry-run + file presente -> `mode=ro`: sqlite rifiuta la scrittura, non la
      previene solo per convenzione.
    """
    if dry_run:
        if not os.path.exists(path):
            return None
        return sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    return open_index(path)


def read_index_state(conn) -> dict:
    """{chunk_id: fingerprint} dal sidecar, riconciliato con la FTS.

    Il sidecar da solo NON basta (Codex #2): il reconciler confrontava KB <->
    `lexical_meta` e dava per scontato che `lexical_fts` fosse la sua proiezione
    fedele. Se la FTS perde o duplica una riga mentre il sidecar resta coerente,
    il piano direbbe "unchanged, churn 0%" mentre il retrieval è sbagliato — un
    guardiano che mente è peggio di nessun guardiano.

    Qui la FTS è la seconda fonte di verità del confronto: un chunk_id vale la
    sua fingerprint SOLO se ha ESATTAMENTE una riga in `lexical_fts`. Ogni altra
    cardinalità (0 = riga persa, >1 = duplicata) prende il sentinel
    `FTS_DIVERGENT`, che non combacia con nessuna fingerprint reale -> il chunk
    finisce in `updated` e viene ricostruito (delete-all + insert), invece di
    passare per unchanged.

    Anche gli orfani FTS (chunk_id in `lexical_fts` ma non in `lexical_meta`)
    entrano col sentinel: se l'id è ancora in KB -> update (rebuild), se non
    c'è più -> delete. Entrambi i rami di `build_plan` li puliscono senza
    codice dedicato.

    Nota: la divergenza NON è silenziosa neanche quando è di massa — quei chunk
    contano come churn, quindi sopra soglia il guardiano aborta comunque.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT chunk_id, fingerprint FROM lexical_meta")
    fingerprints = {chunk_id: fingerprint for chunk_id, fingerprint in cursor.fetchall()}

    # `chunk_id` è UNINDEXED: il GROUP BY è UNA scansione della %_content, non
    # un lookup FTS. Costo misurato su 161k righe: dentro il rumore (run pulito
    # 3,7s prima e dopo l'aggiunta). NON sostituirlo con un LEFT JOIN
    # meta<->fts: senza indice su `chunk_id` degenera in O(n²) e su questo
    # corpus non termina in tempo utile (verificato).
    cursor.execute("SELECT chunk_id, COUNT(*) FROM lexical_fts GROUP BY chunk_id")
    fts_counts = {chunk_id: count for chunk_id, count in cursor.fetchall()}

    for chunk_id in fingerprints:
        if fts_counts.get(chunk_id, 0) != 1:
            fingerprints[chunk_id] = FTS_DIVERGENT
    for chunk_id in fts_counts:
        if chunk_id not in fingerprints:
            fingerprints[chunk_id] = FTS_DIVERGENT

    return fingerprints


def _delete_rows(conn, chunk_ids) -> None:
    """Cancella da FTS + sidecar. `chunk_id` è UNINDEXED: il DELETE va per
    valore di colonna, non via MATCH.
    """
    conn.executemany("DELETE FROM lexical_fts WHERE chunk_id = ?", [(c,) for c in chunk_ids])
    conn.executemany("DELETE FROM lexical_meta WHERE chunk_id = ?", [(c,) for c in chunk_ids])


def _insert_rows(conn, chunk_ids, rows, fingerprints) -> None:
    """Insert in FTS + sidecar. I valori passano SEMPRE per parametri bind:
    `@`, path, virgolette nel document sono dati, non sintassi FTS5 — il
    parser FTS5 non li vede in scrittura. (La sanitizzazione della *query*
    è §9.6, altro step.)
    """
    conn.executemany(
        """
        INSERT INTO lexical_fts (chunk_id, document, title, folder, doc_type, email_from, file_path)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                chunk_id,
                as_text(rows[chunk_id].get("document")),
                as_text(rows[chunk_id].get("title")),
                as_text(rows[chunk_id].get("folder")),
                as_text(rows[chunk_id].get("doc_type")),
                as_text(rows[chunk_id].get("email_from")),
                as_text(rows[chunk_id].get("file_path")),
            )
            for chunk_id in chunk_ids
        ],
    )
    conn.executemany(
        "INSERT OR REPLACE INTO lexical_meta (chunk_id, source, fingerprint) VALUES (?, ?, ?)",
        [
            (chunk_id, as_text(rows[chunk_id].get("source")), fingerprints[chunk_id])
            for chunk_id in chunk_ids
        ],
    )


def apply_plan(conn, plan: ReconcilePlan, rows: dict, fingerprints: dict) -> None:
    """Applica il piano in UNA transazione sul solo file indice.

    L'update è delete+insert: una FTS5 esterna non ha UPDATE per colonna
    affidabile, e delete+insert garantisce che non resti un duplicato della
    riga vecchia (il caso "fingerprint cambiata -> update, non duplicato").
    Nessuna transazione cross-file con Chroma: è il motivo per cui il design
    ha scelto la riconciliazione periodica invece del sync inline (§7).
    """
    with conn:
        if len(plan.deleted) > 0:
            _delete_rows(conn, plan.deleted)
        if len(plan.updated) > 0:
            _delete_rows(conn, plan.updated)
            _insert_rows(conn, plan.updated, rows, fingerprints)
        if len(plan.inserted) > 0:
            _insert_rows(conn, plan.inserted, rows, fingerprints)


# ============================================================
# NOTIFICA — testo puro qui, invio (I/O) sotto
# ============================================================


def build_notification(report, error: str = None) -> str:
    """Testo del messaggio Telegram, o `None` se non c'è niente da dire. Pura.

    Un job schedulato che può abortire da solo DEVE gridare, o diventa il triage
    fermo 60 notti. Ma un messaggio ogni notte è un messaggio che si ignora — la
    notifica quindi è per eccezione, non per abitudine:

    - ABORT -> FAIL: l'evento che non deve passare inosservato.
    - churn > 0 su indice non-bootstrap -> INFO: un incrementale sano è pochi
      chunk/notte; migliaia sotto il 20% è un sintomo da vedere PRIMA che
      diventi il 20% e blocchi il job.
    - run pulito (churn 0) o bootstrap -> None: silenzio.
    """
    if error is not None:
        lines = [
            "❌ *TAILOR lexical reconciler — FAIL*",
            "",
            "Il guardiano ha abortito il run: l'indice NON è stato modificato "
            "(nessun apply parziale).",
            "",
            f"`{error}`",
        ]
        if report is not None:
            lines += [
                "",
                f"inserted {report['inserted']:,} · deleted {report['deleted']:,} · "
                f"updated {report['updated']:,} · unchanged {report['unchanged']:,}",
                f"KB {report['kb_rows']:,} · indice {report['index_rows_before']:,}",
            ]
            if report["fts_divergent"] > 0:
                lines.append(f"FTS divergente: {report['fts_divergent']:,} righe")
        lines += ["", "L'indice resta com'era: il retrieval lessicale serve dati stale."]
        return "\n".join(lines)

    if report["bootstrap"]:
        return None  # il primo popolamento è atteso, non è un evento
    if report["churn"] == 0:
        return None  # run pulito: silenzio

    lines = [
        "ℹ️ *TAILOR lexical reconciler*",
        "",
        f"{report['churn']:,} righe riconciliate su {report['kb_rows']:,} in KB "
        f"({report['guard']}).",
        "",
        f"inserted {report['inserted']:,} · deleted {report['deleted']:,} · "
        f"updated {report['updated']:,}",
    ]
    if report["fts_divergent"] > 0:
        lines.append(
            f"⚠️ {report['fts_divergent']:,} righe con FTS divergente dal sidecar, ricostruite"
        )
    return "\n".join(lines)


def notify(text: str, enabled: bool = True) -> bool:
    """Manda `text` se c'è qualcosa da mandare. Non solleva MAI.

    `send_telegram` è già best-effort, ma il try/except resta: l'indice conta
    più della notifica, e un errore inatteso nel path di notifica NON deve
    trasformare un reconcile riuscito in un exit non-zero (né un abort corretto
    in un traceback che ne nasconde il motivo).
    """
    if not enabled or text is None:
        return False
    try:
        return send_telegram(text)
    except Exception as e:  # noqa: BLE001 — deliberato: nessuna eccezione passa
        print(f"WARN: notifica Telegram fallita: {type(e).__name__}: {e}", file=sys.stderr)
        return False


# ============================================================
# ORCHESTRAZIONE
# ============================================================


def reconcile(chroma_path: str, index_path: str, max_churn_ratio: float, dry_run: bool = False) -> dict:
    """Riconcilia l'indice alla KB. Ritorna il report.

    Solleva RuntimeError se il guardiano scatta: il piano NON viene applicato.
    """
    kb_conn = open_kb_readonly(chroma_path)
    try:
        segment_id = get_metadata_segment(kb_conn)
        rows = read_kb_rows(kb_conn, segment_id)
    finally:
        kb_conn.close()

    kb_fingerprints = fingerprints_of(rows)

    index_conn = open_index_for_plan(index_path, dry_run)
    try:
        if index_conn is None:
            index_fingerprints = {}  # dry-run su indice inesistente
        else:
            index_fingerprints = read_index_state(index_conn)
        plan = build_plan(kb_fingerprints, index_fingerprints)
        bootstrap = is_bootstrap(index_fingerprints)
        ok, message = check_churn(plan, index_fingerprints, max_churn_ratio)

        report = plan.counts()
        report.update(
            {
                "kb_rows": len(kb_fingerprints),
                "index_rows_before": len(index_fingerprints),
                "bootstrap": bootstrap,
                "churn": plan.churn,
                "fts_divergent": sum(
                    1 for fp in index_fingerprints.values() if fp == FTS_DIVERGENT
                ),
                "guard": message,
                "applied": False,
                "segment_id": segment_id,
            }
        )

        if not ok:
            raise ReconcileAborted(message, report)

        if not dry_run:
            apply_plan(index_conn, plan, rows, kb_fingerprints)
            report["applied"] = True
        return report
    finally:
        if index_conn is not None:
            index_conn.close()


def show_stats(index_path: str) -> int:
    if not os.path.exists(index_path):
        print(f"❌ Indice non trovato: {index_path}")
        return 1
    conn = sqlite3.connect(f"file:{index_path}?mode=ro", uri=True)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM lexical_meta")
        meta_rows = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM lexical_fts")
        fts_rows = cursor.fetchone()[0]
        print(f"File:        {index_path}")
        print(f"Dimensione:  {os.path.getsize(index_path) / 1024 / 1024:.1f} MB")
        print(f"Righe FTS:   {fts_rows:,}")
        print(f"Righe meta:  {meta_rows:,}")
        if fts_rows != meta_rows:
            print(f"⚠️  FTS e sidecar divergono ({fts_rows} vs {meta_rows})")
        cursor.execute("SELECT source, COUNT(*) FROM lexical_meta GROUP BY source ORDER BY 2 DESC")
        print("\nPer source:")
        for source, count in cursor.fetchall():
            print(f"  {source or '(vuoto)':<30} {count:>8,}")
    finally:
        conn.close()
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Riconcilia l'indice lessicale P6 alla KB")
    parser.add_argument("--chroma-db", default=CHROMA_DB, help="path di chroma.sqlite3 (read-only)")
    parser.add_argument("--index-db", default=LEXICAL_DB, help="path di lexical_index.sqlite3")
    parser.add_argument(
        "--max-churn",
        type=float,
        default=DEFAULT_MAX_CHURN,
        help=f"soglia fail-loud, frazione del corpus (default {DEFAULT_MAX_CHURN})",
    )
    parser.add_argument("--dry-run", action="store_true", help="calcola il piano, non applica")
    parser.add_argument("--stats", action="store_true", help="statistiche dell'indice, poi esci")
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="non mandare Telegram (default: notifica attiva su abort e churn anomalo). "
        "--dry-run implica --quiet: non cambia nulla, quindi non c'è nessun evento.",
    )
    args = parser.parse_args(argv)

    if args.stats:
        return show_stats(args.index_db)

    # Un dry-run non muta niente -> non c'è evento da notificare, e chi lo lancia
    # sta guardando il terminale.
    notify_enabled = not args.quiet and not args.dry_run

    start = time.time()
    try:
        report = reconcile(args.chroma_db, args.index_db, args.max_churn, dry_run=args.dry_run)
    except ReconcileAborted as exc:
        print(f"❌ RECONCILE ABORTITO: {exc}", file=sys.stderr)
        notify(build_notification(exc.report, error=str(exc)), notify_enabled)
        return 1
    except RuntimeError as exc:
        # Segmento ambiguo / collection assente: aborta anche questo, e da
        # schedulato sarebbe muto quanto il guardiano. Grida pure lui.
        print(f"❌ RECONCILE ABORTITO: {exc}", file=sys.stderr)
        notify(build_notification(None, error=str(exc)), notify_enabled)
        return 1

    elapsed = time.time() - start
    mode = "DRY-RUN" if args.dry_run else ("BOOTSTRAP" if report["bootstrap"] else "RECONCILE")
    print("=" * 60)
    print(f"{mode} LEXICAL INDEX")
    print("=" * 60)
    print(f"Segmento METADATA:  {report['segment_id']}")
    print(f"Righe KB:           {report['kb_rows']:,}")
    print(f"Righe indice prima: {report['index_rows_before']:,}")
    print(f"  inserted:         {report['inserted']:,}")
    print(f"  deleted:          {report['deleted']:,}")
    print(f"  updated:          {report['updated']:,}")
    print(f"  unchanged:        {report['unchanged']:,}")
    if report["fts_divergent"] > 0:
        print(f"⚠️  FTS divergente:  {report['fts_divergent']:,} righe da ricostruire")
    print(f"Guardiano:          {report['guard']}")
    print(f"Applicato:          {report['applied']}")
    print(f"Tempo:              {elapsed:.1f}s")

    notify(build_notification(report), notify_enabled)
    return 0


if __name__ == "__main__":
    sys.exit(main())
