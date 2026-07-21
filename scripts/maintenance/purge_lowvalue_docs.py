#!/usr/bin/env python3
"""Purge dei documenti contabili riga-per-riga dalla KB (+ quarantena derivati).

================================================================================
CONTESTO
================================================================================
21 documenti contabili (intercompany, invoicing report, journal/general ledger,
fatturato storico) occupano 51.090 chunk = il 21% della KB. Sono righe di
partita doppia: come contenuto recuperabile valgono zero, ma dominano il
retrieval per massa. Escono dall'indice.

I FILE RESTANO SU DISCO E SU ONEDRIVE. Esce solo l'indicizzazione, ed e'
reversibile: togliere i path dalla denylist e lanciare un ingest --full li
riporta dentro.

Perimetro deciso da Emiliano, incluse le copie sotto "Italian Tax Police
Check" / "Documenti alla GdF": scelta esplicita, non ereditata dal pattern.

================================================================================
DENYLIST — PATH ESATTI, NON PATTERN
================================================================================
config/ingest_denylist.txt elenca 29 path ASSOLUTI: i 21 gia' in registry piu'
8 copie mai indicizzate (deduplicate per hash all'ingest, perche' il gemello
era gia' in registry).

Gli 8 fuori registry sono il motivo per cui la denylist e' load-bearing e non
housekeeping: tolta l'entry del gemello, al primo giro successivo quelle copie
diventano "nuove" e rientrano in KB, annullando il purge in silenzio.

Il confronto e' ESATTO (path normalizzato), non substring e non glob. Una
regola substring in IGNORE_PATTERNS (che e' `any(p in fname)`) cresce da sola e
prima o poi mangia un file che serviva; qui un documento nuovo con nome simile
NON e' escluso automaticamente e va aggiunto a mano. E' il comportamento
voluto. Il caricamento vive in ingest_docs.load_denylist() e il filtro in
scan_folders(), che stampa il contatore dei file esclusi.

================================================================================
COSA VIENE CANCELLATO — TRE LIVELLI
================================================================================
1. CHUNK Chroma, per `conv_id = doc_<hash12>`, SENZA limit e con verifica del
   count post-delete. Il pattern di garbage_collect.py NON e' riusato: quello
   fa `limit=100` (su "IC - Marzo 2016.xlsx", 13.113 chunk, ne cancellerebbe
   100) dentro un `except Exception: pass` (e tacerebbe). Di quel file si
   riusa SOLO la forma del `where` per i doc_summary (riga 92).

2. SIDECAR, via invalidate_chunk_sidecars: facts, extraction_log, entity_index
   dei chunk cancellati. extraction_log e' il gate di skip del nightly: una
   riga non cancellata significa un chunk mai piu' ri-estratto.

3. DOC_SUMMARY, per `title + folder + category='doc_summary'`. Il loro
   `conv_id` e' `doc_summary_<data-batch>`, NON `doc_<hash>`: la delete per
   conv_id non li prenderebbe. Il campo `extends` sarebbe il link preciso ma
   ce l'ha solo un terzo dei summary e NESSUNO dei 22 in perimetro, quindi
   l'aggancio e' per titolo. Verificato sui dati: 22 summary, folder combacia
   22/22 con infer_folder() del doc, zero collisioni di basename in registry.
   (`intracompany--2015-12-31.xlsx` ne ha 2: 22 summary per 21 doc.)

================================================================================
LAYER DERIVATO — QUARANTENA, NON CANCELLAZIONE
================================================================================
I derived facts hanno `chunk_id = "derived_<ts>_<n>"`, che NON matcha mai
`doc_<hash>_chunk_%`: invalidate_chunk_sidecars non li tocca. E non sono
inerti — mcp_server.py:1742 li serve come "Inferenze derivate" filtrando su
`relation_type='derived' AND entity_tags LIKE ?`, cioe' per ENTITA', senza
passare da chunk_id. Lasciati stare, continuerebbero a essere serviti
all'utente con l'evidenza cancellata da sotto.

Sono pero' la distillazione proprio dei dati che stiamo buttando: cancellarli
butterebbe l'unica sintesi rimasta. Quindi QUARANTENA, reversibile:

  * `relation_type`: 'derived' -> 'derived_purged_source'. Basta questo a
    toglierli dal retrieval (la query filtra `= 'derived'`), e una singola
    UPDATE inversa li riabilita.
  * `derived_from` riscritto con un marker onesto invece che con id morti:
    {"purged": "<data>", "docs": [<hash12>...], "kept_refs": [<id vivi>]}.
    La provenienza diventa "derivato da documenti poi rimossi" — vera —
    invece di puntatori a righe che non esistono piu'.

Inoltre `superseded_by` = NULL per i facts il cui superseder viene cancellato:
il retrieval esclude `superseded_by IS NOT NULL`, quindi resterebbero
invisibili per sempre, superati da qualcosa che non c'e' piu'.

`judged_pairs` orfani: LASCIATI. Sono un memo cache di fact_supersession e
`facts.id` e' AUTOINCREMENT, quindi gli id non vengono riusati e una riga
orfana non puo' produrre un match sbagliato, solo morto. Cleanup futuro.

ORDINE OBBLIGATO: il piano sui derivati si calcola PRIMA di cancellare i
facts (il join parte da derived_from -> id dei facts vittima). Il piano viene
persistito su disco prima della prima delete e riusato al resume: ricalcolarlo
a meta' purge, con parte dei facts gia' spariti, darebbe una lista di
contaminati piu' corta e lascerebbe derivati sporchi in circolazione.

================================================================================
SEQUENZA OPERATIVA (di Emiliano — lo script non tocca daemon/launchctl)
================================================================================
    1. maintenance ON:  kill -USR1 <mcp_pid>
       (freeze implicito: il nightly e' fuori finestra e l'auto-flush e'
        gated dal lock)
    2. backup COMPLETO: bash scripts/maintenance/backup_db.sh
       verificare che copra chroma.sqlite3 + hnsw, doc_registry.json,
       facts.sqlite3, entity_index.sqlite3, indice lessicale
    3. dry-run:         ./.venv/bin/python scripts/maintenance/purge_lowvalue_docs.py
       -> conferma UMANA del perimetro a video
    4. execute:         ./.venv/bin/python scripts/maintenance/purge_lowvalue_docs.py --execute
    5. reconciler:      ./.venv/bin/python scripts/maintenance/reconcile_lexical_index.py \\
                          --max-churn 1.0
       churn atteso 20,87% (51.112 / 244.939): sopra la soglia di default del
       20%, quindi il run supervisionato con soglia alzata e' NECESSARIO, non
       prudenziale. Il denominatore e' max(KB_dopo, indice_prima) = l'indice
       pre-purge.
    6. maintenance OFF: kill -USR2 <mcp_pid>
    7. persist durevole: sudo launchctl kickstart system/com.tailor.mcp
       (graceful, senza -k: e' lo shutdown pulito a persistere l'HNSW e a
        drenare embeddings_queue dopo 51k delete)

================================================================================
VINCOLI
================================================================================
  * --dry-run (default): READ-ONLY, MAI un PersistentClient. Legge Chroma via
    sqlite mode=ro e facts.sqlite3 mode=ro.
  * --execute: gate maintenance PID-validato (avvio, pre-client,
    pre-get_collection, prima di OGNI delete) + guardia entity_index
    (pgrep build_entity_index.py + inode), entrambi importati e non
    duplicati. Lock condiviso ingest_docs.lock: si tocca il registry.
  * CHECKPOINT `path\\thash` per-doc, scritto solo dopo un save_registry
    riuscito. Le fasi summary e derivati hanno i propri marker sentinella.
  * AUDIT finale con i conteggi attesi hardcodati: divergenza = exit non-zero.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime

# --- path setup ---
_HERE = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(_HERE))            # repo root
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "ingest"))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "lib"))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "maintenance"))

import ingest_docs as ing                                          # noqa: E402
from chunk_sidecar import invalidate_chunk_sidecars                # noqa: E402

# Gate maintenance PID-validato: unica fonte, mai duplicata.
from backfill_email_youniversal import (                           # noqa: E402
    MAINTENANCE_LOCK,
    MaintenanceLost,
    maintenance_state,
)
# Guardia entity_index: stessa ragione, gia' scritta e testata.
from reingest_xlsx import (                                        # noqa: E402
    EntityIndexBusy,
    _assert_entity_index_stable,
    _build_entity_index_running,
    _entity_index_fingerprint,
)

LOGS_DIR = os.path.join(BASE_DIR, "logs")
CHECKPOINT_PATH = os.path.join(LOGS_DIR, "purge_lowvalue_docs.done")
DERIVED_PLAN_PATH = os.path.join(LOGS_DIR, "purge_lowvalue_derived_plan.json")
FACTS_DB = os.path.join(ing.DB_DIR, "facts.sqlite3")
CHROMA_SQLITE = os.path.join(ing.DB_DIR, "chroma.sqlite3")

# Marker di provenienza scritto nei derivati messi in quarantena. Costante e
# non datetime.now(): il valore deve restare identico tra il primo run e un
# eventuale resume, altrimenti lo stesso purge lascia marker con date diverse.
PURGE_DATE = "2026-07-20"
QUARANTINE_RELATION = "derived_purged_source"

# Versione del formato del piano derivati. Da alzare a ogni cambio di campi:
# un piano scritto da un'altra versione viene rifiutato invece di essere
# interpretato con lo schema sbagliato.
PLAN_SCHEMA_VERSION = 1

# Sentinelle di fase nel checkpoint (le fasi 2 e 3 non sono per-documento).
SENTINEL_SUMMARIES = "__SUMMARIES__"
SENTINEL_DERIVED = "__DERIVED__"

# ATTESI — misurati sulla KB del 2026-07-20 a maintenance ferma. Servono da
# audit: una divergenza significa che la KB si e' mossa sotto i piedi (nightly
# entrato in finestra, ingest concorrente) e il risultato va guardato a mano.
EXPECTED = {
    "docs": 21,
    "doc_chunks": 51_090,
    "summaries": 22,
    "total_deleted": 51_112,
    "kb_before": 244_939,
    "kb_after": 193_827,
    "derived_quarantined": 19_856,
    "superseded_reset": 924,
}


# ============================================================================
# PERIMETRO
# ============================================================================
def resolve_perimeter(registry: dict, done: dict[str, str] | None = None):
    """(perimetro, off_registry) dalla denylist. Nessun pattern: path esatti.

    perimetro: [(filepath, hash12)] dei documenti INDICIZZATI — quelli ancora
    in registry PIU' quelli gia' purgati, che dal registry sono spariti ma
    restano nel checkpoint col loro hash.

    Includere i gia'-purgati non e' un dettaglio: il purge CANCELLA l'entry di
    registry, quindi un perimetro ricavato dal solo registry si accorcia a
    ogni documento fatto. Il piano derivati verrebbe rifiutato dalla
    validazione al primo resume (21 hash nel piano, 18 adesso) e l'audit
    finale guarderebbe solo i documenti rimasti, dichiarando verde un purge
    incompleto. Il checkpoint e' cio' che rende il perimetro stabile.

    off_registry: path denylistati mai indicizzati (le copie deduplicate per
    hash all'ingest). Niente da cancellare: stanno in denylist per non
    rientrare domani. Un path gia' purgato NON finisce qui — lo distingue la
    presenza nel checkpoint.
    """
    done = done or {}
    denylist = ing.load_denylist()
    reg_norm = {os.path.normpath(p): p for p in registry}
    done_norm = {os.path.normpath(p): (p, h) for p, h in done.items()}
    perimeter, off_reg = [], []
    for path in sorted(denylist):
        real = reg_norm.get(path)
        if real is not None:
            perimeter.append((real, (registry[real].get("hash") or "")[:12]))
            continue
        purged = done_norm.get(path)
        if purged is not None:                # gia' fatto in un run precedente
            perimeter.append(purged)
            continue
        off_reg.append(path)
    return perimeter, off_reg


def _ro(db_path: str) -> sqlite3.Connection:
    """Sola lettura coordinata coi writer: mode=ro (mai immutable=1)."""
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    con.execute("PRAGMA busy_timeout=30000")
    return con


def kb_count() -> int:
    """Chunk totali in Chroma. Connessione APERTA E CHIUSA subito.

    Tenerla aperta per la durata del run significherebbe un lettore sqlite
    sullo stesso file su cui il PersistentClient sta cancellando 51k righe:
    lock condivisi che si incrociano con le sue transazioni, per un numero che
    serve due volte in tutto.
    """
    con = _ro(CHROMA_SQLITE)
    try:
        return con.execute("SELECT COUNT(*) FROM embeddings").fetchone()[0]
    finally:
        con.close()


def count_chunks_ro(con: sqlite3.Connection, h12: str) -> int:
    return con.execute(
        "SELECT COUNT(*) FROM embeddings WHERE embedding_id LIKE ? ESCAPE '\\'",
        (f"doc\\_{h12}\\_chunk\\_%",),
    ).fetchone()[0]


def find_summaries_ro(con: sqlite3.Connection, titles: set[str]) -> list[dict]:
    """doc_summary il cui title e' in `titles`. Ritorna [{id,title,folder}]."""
    meta: dict[str, dict] = {}
    for eid, k, v in con.execute(
        """SELECT e.embedding_id, m.key,
                  COALESCE(m.string_value, m.int_value, m.float_value)
           FROM embeddings e JOIN embedding_metadata m ON m.id = e.id
           WHERE e.embedding_id LIKE 'doc\\_summary\\_%' ESCAPE '\\'"""
    ):
        meta.setdefault(eid, {})[k] = v
    out = []
    for eid, m in meta.items():
        if m.get("title") in titles:
            out.append({"id": eid, "title": m.get("title"),
                        "folder": m.get("folder") or ""})
    return sorted(out, key=lambda d: (d["title"], d["id"]))


# ============================================================================
# PIANO SUL LAYER DERIVATO — calcolato PRIMA di ogni delete
# ============================================================================
def build_derived_plan(hashes: list[str], kb_before: int = 0,
                       expected_deleted: int = 0) -> dict:
    """Chi va in quarantena e quali superseded_by vanno resettati.

    Il join parte dagli id dei facts vittima, quindi DEVE girare prima che
    invalidate_chunk_sidecars li cancelli. Vedi docstring del modulo.
    """
    con = _ro(FACTS_DB)
    try:
        victim: dict[int, str] = {}          # fact_id -> hash12 del doc
        for h in hashes:
            for (fid,) in con.execute(
                "SELECT id FROM facts WHERE chunk_id LIKE ? ESCAPE '\\'",
                (f"doc\\_{h}\\_chunk\\_%",),
            ):
                victim[fid] = h

        quarantine = []
        for fid, df in con.execute(
            """SELECT id, derived_from FROM facts
               WHERE relation_type = 'derived'
                 AND derived_from IS NOT NULL AND derived_from NOT IN ('', '[]')"""
        ):
            try:
                refs = json.loads(df)
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(refs, list):
                continue
            hit = [x for x in refs if isinstance(x, int) and x in victim]
            if not hit:
                continue
            quarantine.append({
                "id": fid,
                "docs": sorted({victim[x] for x in hit}),
                "kept_refs": [x for x in refs
                              if isinstance(x, int) and x not in victim],
                "lost": len(hit),
            })

        superseded = [fid for (fid, sb) in con.execute(
            "SELECT id, superseded_by FROM facts WHERE superseded_by IS NOT NULL")
            if sb in victim]
    finally:
        con.close()

    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "purge_date": PURGE_DATE,
        # Misurati all'istante del piano, cioe' prima di qualsiasi delete.
        # L'audit finale confronta la KB con QUESTI, non con una costante
        # scritta a mano: le costanti EXPECTED invecchiano a ogni nightly
        # (i derivati crescono), e un audit ancorato a loro dichiarerebbe
        # fallito un purge riuscito solo perche' e' passata una notte.
        "kb_before": kb_before,
        "expected_deleted": expected_deleted,
        "hashes": sorted(hashes),
        "victim_facts": len(victim),
        "quarantine": quarantine,
        "superseded_reset": sorted(superseded),
    }


class PlanMismatch(RuntimeError):
    """Il piano su disco non appartiene a QUESTO purge -> stop, mai indovinare."""


def validate_plan(plan: dict, hashes: list[str]) -> None:
    """Il piano persistito descrive lo stesso purge che stiamo eseguendo?

    Il piano vince sul ricalcolo (e' il suo scopo), quindi se appartiene a un
    altro purge lo applicheremmo alla cieca: quarantena su fatti sbagliati,
    superseded_by resettati a caso, e nessuno se ne accorge perche' il file
    "c'era". Tre controlli, tutti su cose che cambiano se il purge e' un altro:
    schema, perimetro, data.
    """
    problems = []
    got_v = plan.get("schema_version")
    if got_v != PLAN_SCHEMA_VERSION:
        problems.append(
            f"schema_version={got_v!r}, atteso {PLAN_SCHEMA_VERSION!r} "
            f"(piano scritto da un'altra versione dello script)")
    got_h = set(plan.get("hashes") or [])
    want_h = set(hashes)
    if got_h != want_h:
        only_plan = sorted(got_h - want_h)
        only_now = sorted(want_h - got_h)
        problems.append(
            f"perimetro diverso: {len(got_h)} hash nel piano vs {len(want_h)} "
            f"adesso"
            + (f"; solo nel piano: {only_plan[:5]}" if only_plan else "")
            + (f"; solo adesso: {only_now[:5]}" if only_now else ""))
    got_d = plan.get("purge_date")
    if got_d != PURGE_DATE:
        problems.append(f"purge_date={got_d!r}, atteso {PURGE_DATE!r}")
    if problems:
        raise PlanMismatch(
            "il piano derivati su disco NON corrisponde a questo purge:\n"
            + "".join(f"    - {p}\n" for p in problems)
            + f"\n  file: {DERIVED_PLAN_PATH}\n"
            f"  Cosa verificare, in ordine:\n"
            f"    1. e' il residuo di un purge PRECEDENTE gia' chiuso?\n"
            f"       -> confronta `purge_date` nel file con {PURGE_DATE}\n"
            f"    2. la denylist e' cambiata dopo che il piano e' stato\n"
            f"       scritto? -> il perimetro non e' piu' quello, e i\n"
            f"       documenti aggiunti/tolti non sono nella quarantena\n"
            f"    3. c'e' un purge interrotto a meta' da riprendere?\n"
            f"       -> allora NON toccare il file: rimetti la denylist come\n"
            f"          era e rilancia lo stesso comando\n"
            f"  Solo se hai stabilito che e' un residuo da buttare:\n"
            f"    rm {DERIVED_PLAN_PATH}\n"
            f"  Cancellarlo durante un purge interrotto significa perdere la\n"
            f"  lista dei derivati contaminati: il ricalcolo su facts gia'\n"
            f"  potati ne troverebbe zero.")


def load_or_build_derived_plan(hashes: list[str], log=print, kb_before: int = 0,
                               expected_deleted: int = 0) -> dict:
    """Il piano persistito vince sempre sul ricalcolo. Vedi docstring."""
    if os.path.exists(DERIVED_PLAN_PATH):
        with open(DERIVED_PLAN_PATH) as fh:
            plan = json.load(fh)
        validate_plan(plan, hashes)          # fail-loud PRIMA di riusarlo
        log(f"[derived] piano PERSISTITO riusato ({DERIVED_PLAN_PATH}): "
            f"{len(plan['quarantine']):,} da quarantinare, "
            f"{len(plan['superseded_reset']):,} superseded_by da resettare "
            f"(schema v{plan.get('schema_version')}, perimetro e data OK)")
        return plan
    plan = build_derived_plan(hashes, kb_before, expected_deleted)
    os.makedirs(LOGS_DIR, exist_ok=True)
    tmp = DERIVED_PLAN_PATH + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(plan, fh)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, DERIVED_PLAN_PATH)
    log(f"[derived] piano CALCOLATO e persistito ({DERIVED_PLAN_PATH})")
    return plan


def apply_derived_plan(plan: dict, log=print) -> tuple[int, int, int]:
    """Quarantena + reset superseded_by. Idempotente (UPDATE per id).

    Ritorna (quarantinati, resettati, superseded_pendenti). Il terzo valore e'
    il criterio di completezza della parte superseded: quanti id del piano
    ESISTONO ancora con superseded_by valorizzato. Non si puo' usare il
    rowcount, perche' la maggior parte di quegli id sono facts dei chunk
    purgati e la fase sidecar li ha gia' cancellati: una riga che non c'e' piu'
    non ha puntatori pendenti, e pretendere rowcount == len(piano)
    significherebbe aspettare per sempre un lavoro che non esiste.
    """
    con = sqlite3.connect(FACTS_DB, timeout=30)
    try:
        con.execute("PRAGMA busy_timeout=30000")
        quarantined = 0
        for item in plan["quarantine"]:
            marker = json.dumps({
                "purged": plan.get("purge_date", PURGE_DATE),
                "docs": item["docs"],
                "kept_refs": item["kept_refs"],
            })
            cur = con.execute(
                "UPDATE facts SET relation_type = ?, derived_from = ? WHERE id = ?",
                (QUARANTINE_RELATION, marker, item["id"]))
            quarantined += cur.rowcount or 0
        reset = 0
        for fid in plan["superseded_reset"]:
            cur = con.execute(
                "UPDATE facts SET superseded_by = NULL, superseded_at = '' "
                "WHERE id = ?", (fid,))
            reset += cur.rowcount or 0
        con.commit()

        # Quanti degli id del piano esistono ancora, e quanti restano da
        # sistemare. Serve a spiegare il numero: `reset` da solo sembra un
        # fallimento (25 su 1.353) quando invece e' il risultato giusto.
        s_ids = plan["superseded_reset"]
        esistenti = pendenti = 0
        for i in range(0, len(s_ids), 500):
            batch = s_ids[i:i + 500]
            ph = ",".join("?" * len(batch))
            esistenti += con.execute(
                f"SELECT COUNT(*) FROM facts WHERE id IN ({ph})",
                batch).fetchone()[0]
            pendenti += con.execute(
                f"SELECT COUNT(*) FROM facts WHERE id IN ({ph}) "
                f"AND superseded_by IS NOT NULL", batch).fetchone()[0]
    finally:
        con.close()
    log(f"[derived] quarantena applicata: relation_type -> "
        f"{QUARANTINE_RELATION} su {quarantined:,} facts")
    log(f"[derived] superseded_by resettati: {reset:,} su {esistenti:,} "
        f"esistenti ({len(s_ids) - esistenti:,} del piano gia' eliminati come "
        f"vittime della delete facts)")
    return quarantined, reset, pendenti


# ============================================================================
# CHECKPOINT
# ============================================================================
def load_checkpoint() -> dict[str, str]:
    """path -> hash. Stesso formato del runner reingest_xlsx."""
    if not os.path.exists(CHECKPOINT_PATH):
        return {}
    done: dict[str, str] = {}
    with open(CHECKPOINT_PATH) as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line.strip():
                continue
            path, _, h = line.partition("\t")
            done[path.strip()] = h.strip()
    return done


def _assert_maintenance(stage: str) -> None:
    ok, why = maintenance_state()
    if not ok:
        raise MaintenanceLost(f"{stage}: {why}")


# ============================================================================
# DRY-RUN
# ============================================================================
def dry_run(show: int) -> int:
    print("=" * 78)
    print("PURGE DOCUMENTI CONTABILI — DRY-RUN (READ-ONLY, zero scritture)")
    print("=" * 78)
    print(f"[denylist] {ing.DENYLIST_FILE}")
    registry = ing.load_registry()
    done = load_checkpoint()
    in_reg, off_reg = resolve_perimeter(registry, done)
    print(f"[denylist] {len(in_reg) + len(off_reg)} path "
          f"({len(in_reg)} indicizzati, {len(off_reg)} copie mai indicizzate)")

    if done:
        print(f"[ckpt]     {len(done)} voci a checkpoint ({CHECKPOINT_PATH})")

    kb_before = kb_count()
    con = _ro(CHROMA_SQLITE)
    try:
        rows = []
        total = 0
        for path, h in in_reg:
            n = count_chunks_ro(con, h)
            total += n
            # `registry[path]` diretto era un KeyError su ogni doc gia' purgato:
            # il perimetro li include (dal checkpoint) ma dal registry sono
            # spariti — cioe' il dry-run esplodeva esattamente nello stato in
            # cui serve di piu', a purge interrotto a meta'. L'hash lo porta
            # gia' il perimetro; dal registry si prende solo il conteggio, che
            # e' informativo e puo' non esserci.
            reg_entry = registry.get(path)
            rows.append((path, h, n,
                         reg_entry.get("chunks", 0) if reg_entry else None))
        titles = {os.path.basename(p) for p, _ in in_reg}
        summaries = find_summaries_ro(con, titles)
    finally:
        con.close()

    print("\n" + "=" * 78)
    print(f"DOCUMENTI DA PURGARE ({len(rows)}) — chunk REALI da Chroma")
    print("=" * 78)
    for path, h, n, reg_n in rows:
        if reg_n is None:
            flag = "   [gia' purgato: fuori registry, da checkpoint]"
        elif n != reg_n:
            flag = f"   [registry dice {reg_n:,}]"
        else:
            flag = ""
        print(f"  {n:>7,} ch  doc_{h}  {os.path.basename(path)}{flag}")
        print(f"              {os.path.dirname(path)}")

    print(f"\n{'=' * 78}\nDOC_SUMMARY DA PURGARE ({len(summaries)})\n{'=' * 78}")
    for s in summaries:
        print(f"  {s['id']}  title={s['title']!r}  folder={s['folder']!r}")

    print(f"\n{'=' * 78}\nCOPIE MAI INDICIZZATE ({len(off_reg)}) — "
          f"niente da cancellare\n{'=' * 78}")
    print("  (in denylist per non rientrare al prossimo ingest)")
    for p in off_reg[:show]:
        print(f"  {os.path.basename(p)}")
        print(f"      {os.path.dirname(p)}")
    if len(off_reg) > show:
        print(f"  ... e altri {len(off_reg) - show}")

    print(f"\n{'=' * 78}\nLAYER DERIVATO\n{'=' * 78}")
    plan = build_derived_plan([h for _, h in in_reg])
    print(f"facts cancellati dai sidecar     : {plan['victim_facts']:,}")
    print(f"derived da mettere in quarantena : {len(plan['quarantine']):,}"
          f"  (relation_type -> {QUARANTINE_RELATION})")
    print(f"riferimenti derived_from rotti   : "
          f"{sum(q['lost'] for q in plan['quarantine']):,}")
    print(f"superseded_by da resettare       : "
          f"{len(plan['superseded_reset']):,}")
    if plan["quarantine"]:
        print("\n  esempio di marker che verrebbe scritto:")
        q = plan["quarantine"][0]
        print(f"    fact id={q['id']} -> " + json.dumps({
            "purged": PURGE_DATE, "docs": q["docs"],
            "kept_refs": q["kept_refs"][:5]}))

    deleted = total + len(summaries)
    kb_after = kb_before - deleted
    churn = deleted / max(kb_after, kb_before)
    print(f"\n{'=' * 78}\nTOTALI\n{'=' * 78}")
    print(f"KB prima          : {kb_before:,}")
    print(f"chunk documenti   : {total:,}")
    print(f"doc_summary       : {len(summaries):,}")
    print(f"TOTALE DELETE     : {deleted:,}")
    print(f"KB dopo           : {kb_after:,}")
    print(f"registry: -{len(in_reg)} entry  ({len(registry):,} -> "
          f"{len(registry) - len(in_reg):,})")
    print(f"\nchurn reconciler  : {deleted:,}/max({kb_after:,}, {kb_before:,}) "
          f"= {churn:.2%}")
    if churn > 0.20:
        print(f"  -> SOPRA la soglia di default (20%): il run dopo il purge "
              f"richiede --max-churn 1.0 supervisionato")

    _audit_expected(deleted, total, len(summaries), kb_after, len(in_reg),
                    len(plan["quarantine"]), len(plan["superseded_reset"]),
                    kb_before, label="ATTESI (confronto dry-run)")

    ok, why = maintenance_state()
    print(f"\n[gate]  maintenance mode adesso: {'OK' if ok else 'NO'} — {why}")
    busy, bwhy = _build_entity_index_running()
    print(f"[gate]  build_entity_index: {'BUSY — ' + bwhy if busy else 'fermo'}")
    print("[dry-run] nessuna scrittura. Fine.")
    return 0


def audit_state(in_reg, plan, log=print) -> int:
    """AUDIT DI STATO: interroga i DB, non i contatori del processo.

    I contatori (docs_ok, chunks_deleted, quarantined...) dicono cosa ha fatto
    QUESTO processo. Non e' la domanda giusta: dopo un abort e un resume il
    lavoro e' spalmato su due o piu' run, e ognuno vede solo la propria fetta —
    un audit a contatori griderebbe "divergenza" su un purge perfettamente
    riuscito, oppure (peggio) direbbe "tutto ok" perche' i suoi 3 documenti
    sono andati bene mentre gli altri 18 non li ha nemmeno guardati.

    Qui si chiede allo STATO: i chunk ci sono ancora? il registry e' pulito?
    i derivati del piano sono tutti in quarantena? La risposta e' la stessa
    indipendentemente da chi ha fatto cosa e in quanti run.

    Ritorna il numero di controlli falliti.
    """
    log(f"\n{'=' * 78}\nAUDIT DI STATO (interroga i DB, non i contatori)\n"
        f"{'=' * 78}")
    failed = 0

    def check(nome: str, got, want) -> None:
        nonlocal failed
        ok = got == want
        if not ok:
            failed += 1
        got_s = f"{got:,}" if isinstance(got, int) else str(got)
        want_s = f"{want:,}" if isinstance(want, int) else str(want)
        log(f"  {nome:<38} {got_s:>10}   "
            + ("OK" if ok else f"FALLITO (atteso {want_s})"))

    hashes = [h for _, h in in_reg]
    paths = [p for p, _ in in_reg]

    # --- Chroma: chunk e summary residui ---
    con = _ro(CHROMA_SQLITE)
    try:
        residui = sum(count_chunks_ro(con, h) for h in hashes)
        check("chunk residui dei doc purgati", residui, 0)
        titles = {os.path.basename(p) for p in paths}
        check("doc_summary residui del perimetro",
              len(find_summaries_ro(con, titles)), 0)
        kb = con.execute("SELECT COUNT(*) FROM embeddings").fetchone()[0]
    finally:
        con.close()
    # Atteso ricavato dal PIANO (misurato pre-delete in questo purge), non da
    # EXPECTED: quelle costanti sono la baseline del giorno della decisione e
    # invecchiano a ogni nightly.
    want_kb = plan.get("kb_before", 0) - plan.get("expected_deleted", 0)
    if want_kb > 0:
        check("chunk totali in KB", kb, want_kb)
    else:
        log(f"  {'chunk totali in KB':<38} {kb:>10,}   "
            f"(piano senza baseline: controllo saltato)")

    # --- Registry: nessuna delle 21 entry deve essere sopravvissuta ---
    registry = ing.load_registry()
    check("entry registry dei doc purgati",
          sum(1 for p in paths if p in registry), 0)

    # --- facts: TUTTI gli id del piano, non "quanti ne ho toccati io" ---
    # I due set del piano hanno controlli ASIMMETRICI, e non e' una svista.
    #
    # QUARANTENA — si verifica anche l'ESISTENZA. I derivati hanno chunk_id
    # "derived_<ts>_<n>", che non matcha mai "doc_<hash>_chunk_%": la delete
    # dei sidecar non puo' toccarli. Se uno di loro sparisce e' una perdita di
    # dati vera, e senza il check passerebbe inosservata — "nessuna riga in
    # stato sbagliato" e' soddisfatto anche da zero righe. Il contratto qui e'
    # quarantena, non delete.
    #
    # SUPERSEDED — l'esistenza NON si verifica. Quegli id sono facts di chunk
    # qualunque, e in larga maggioranza dei chunk purgati: la fase sidecar li
    # cancella per costruzione (nel purge del 2026-07-21: 1.328 su 1.353).
    # Pretenderli tutti presenti vorrebbe dire dichiarare fallito un purge
    # riuscito. Quel che conta e' che nessuno dei sopravvissuti resti appeso a
    # un superseder cancellato, ed e' esattamente il controllo qui sotto.
    con = _ro(FACTS_DB)
    try:
        def _count(ids: list, extra_sql: str = "", extra_args: list = None):
            tot = 0
            for i in range(0, len(ids), 500):
                batch = ids[i:i + 500]
                ph = ",".join("?" * len(batch))
                tot += con.execute(
                    f"SELECT COUNT(*) FROM facts WHERE id IN ({ph}){extra_sql}",
                    batch + (extra_args or [])).fetchone()[0]
            return tot

        q_ids = [q["id"] for q in plan["quarantine"]]
        check("derivati del piano ancora esistenti",
              _count(q_ids), len(q_ids))
        check("derivati del piano NON in quarantena",
              _count(q_ids, " AND relation_type != ?", [QUARANTINE_RELATION]), 0)

        s_ids = plan["superseded_reset"]
        vivi = _count(s_ids)
        log(f"  {'superseded del piano ancora esistenti':<38} {vivi:>10,}   "
            f"(informativo: {len(s_ids) - vivi:,} eliminati come vittime)")
        check("superseded_by del piano non resettati",
              _count(s_ids, " AND superseded_by IS NOT NULL"), 0)
    finally:
        con.close()

    if failed:
        log(f"\n  {failed} controlli FALLITI: il purge NON e' completo.\n"
            f"  Rilanciare lo stesso comando (e' resumable) e ri-guardare\n"
            f"  l'audit. Se un controllo resta rosso dopo un rilancio pulito,\n"
            f"  fermarsi: NON lanciare il reconciler e NON riaprire il MCP.")
    else:
        log("\n  tutti i controlli di stato verdi: il purge e' completo.")
    return failed


def _audit_expected(deleted, doc_chunks, summaries, kb_after, docs,
                    quarantined, superseded, kb_before,
                    label="ATTESI") -> int:
    """Confronto coi conteggi misurati il giorno del piano (solo dry-run).

    Serve a far vedere all'operatore che la KB e' ancora quella su cui il
    perimetro e' stato deciso. NON e' l'audit post-purge: quello e'
    audit_state(), che non dipende da quanti run sono serviti.
    """
    got = {
        "docs": docs, "doc_chunks": doc_chunks, "summaries": summaries,
        "total_deleted": deleted, "kb_before": kb_before, "kb_after": kb_after,
        "derived_quarantined": quarantined, "superseded_reset": superseded,
    }
    print(f"\n{'=' * 78}\n{label}\n{'=' * 78}")
    diverged = 0
    for k, exp in EXPECTED.items():
        v = got[k]
        mark = "OK" if v == exp else f"DIVERGENZA (atteso {exp:,})"
        if v != exp:
            diverged += 1
        print(f"  {k:<22} {v:>10,}   {mark}")
    if diverged:
        print(f"\n  {diverged} divergenze: la KB si e' mossa rispetto alla "
              f"misura del {PURGE_DATE}.\n  Guardare a mano prima di "
              f"proseguire (nightly in finestra? ingest concorrente?).")
    return diverged


# ============================================================================
# EXECUTE
# ============================================================================
def execute(limit_docs: int | None) -> int:
    ok, why = maintenance_state()
    if not ok:
        sys.stderr.write(
            f"\nRIFIUTO --execute: MCP NON in maintenance mode ({why}).\n"
            f"  Entra in maintenance: kill -USR1 <mcp_pid>\n"
            f"  (il lock da solo non basta: serve che il MCP abbia RILASCIATO\n"
            f"   il client ChromaDB, cosa che fa solo nel handler SIGUSR1)\n"
            f"  Poi backup COMPLETO: bash scripts/maintenance/backup_db.sh\n")
        return 2
    print(f"[gate]  {why}")

    busy, bwhy = _build_entity_index_running()
    if busy:
        sys.stderr.write(f"\nRIFIUTO --execute: {bwhy}.\n")
        return 2
    entity_baseline = _entity_index_fingerprint()

    lock_fd = ing.acquire_single_instance_lock()
    if lock_fd is None:
        sys.stderr.write(
            f"\nRIFIUTO --execute: un altro ingest e' in esecuzione "
            f"({ing.LOCK_FILE}).\n")
        return 2

    os.makedirs(LOGS_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(LOGS_DIR, f"purge_lowvalue_docs_{ts}.log")
    logf = open(log_path, "a")

    def log(msg: str) -> None:
        print(msg, flush=True)
        logf.write(msg + "\n")
        logf.flush()

    log(f"[execute] start {ts}  log={log_path}")
    log(f"[lock]    {ing.LOCK_FILE} acquisito (pid {os.getpid()})")

    registry = ing.load_registry()
    done = load_checkpoint()
    in_reg, off_reg = resolve_perimeter(registry, done)
    log(f"[denylist] perimetro {len(in_reg)} doc, {len(off_reg)} copie "
        f"mai indicizzate (nulla da cancellare)")
    log(f"[ckpt]    {len(done)} voci a checkpoint -> resume")

    # Fotografia PRE-delete: quanti chunk ci sono adesso e quanti ne devono
    # sparire. Va presa prima del piano perche' e' il piano a conservarla, ed
    # e' contro questa — non contro una costante — che l'audit finale misura.
    kb_before = kb_count()
    con = _ro(CHROMA_SQLITE)
    try:
        expected_deleted = sum(count_chunks_ro(con, h) for _, h in in_reg)
        expected_deleted += len(find_summaries_ro(
            con, {os.path.basename(p) for p, _ in in_reg}))
    finally:
        con.close()
    log(f"[kb]      chunk prima del purge: {kb_before:,}  "
        f"(attesi in uscita: {expected_deleted:,})")

    # PIANO DERIVATI PRIMA DI QUALSIASI DELETE. Persistito: al resume si riusa
    # quello, mai un ricalcolo su facts gia' potati.
    try:
        derived_plan = load_or_build_derived_plan(
            [h for _, h in in_reg], log, kb_before, expected_deleted)
    except PlanMismatch as e:
        sys.stderr.write(f"\nABORT: {e}\n")
        log(f"[ABORT] piano derivati non valido: {e}")
        logf.close()
        return 4

    # CHECKPOINT: l'hash e' un vincolo, non un promemoria. Un path gia' a
    # checkpoint con hash DIVERSO significa che il file su disco non e' quello
    # che avevamo purgato — in un'operazione distruttiva quel caso non si
    # salta in silenzio (si perderebbe la delete del contenuto nuovo) e non si
    # purga alla cieca: si ferma tutto e lo guarda una persona.
    todo = []
    for path, h in in_reg:
        if path not in done:
            todo.append((path, h))
            continue
        if done[path] == h:
            continue                          # gia' fatto, stesso contenuto
        sys.stderr.write(
            f"\nABORT: checkpoint incoerente per\n  {path}\n"
            f"  checkpoint: {done[path] or '(vuoto)'}\n"
            f"  adesso    : {h}\n"
            f"  Il file risulta gia' purgato ma con un contenuto diverso da\n"
            f"  quello attuale. Le possibilita':\n"
            f"    - il file e' stato modificato/sostituito dopo il purge\n"
            f"      -> i chunk del contenuto NUOVO sono in KB e vanno tolti\n"
            f"    - il checkpoint e' di un purge precedente ({CHECKPOINT_PATH})\n"
            f"  In nessuno dei due casi ha senso saltarlo o rifarlo in\n"
            f"  automatico: decidere a mano.\n")
        log(f"[ABORT] checkpoint incoerente su {path}: "
            f"ckpt={done[path]!r} adesso={h!r}")
        logf.close()
        return 4

    if limit_docs:
        todo = todo[:limit_docs]
    log(f"[plan]    {len(todo)} documenti da purgare in questo run")

    try:
        _assert_maintenance("prima di PersistentClient")
        _assert_entity_index_stable(entity_baseline, "prima di PersistentClient")
        import chromadb
        client = chromadb.PersistentClient(path=ing.DB_DIR)
        _assert_maintenance("prima di get_collection")
        collection = client.get_collection(name=ing.COLLECTION_NAME)
    except (MaintenanceLost, EntityIndexBusy) as e:
        sys.stderr.write(f"\nABORT: {e}\n  NESSUNA scrittura (client mai "
                         f"aperto).\n")
        log(f"[ABORT] {e}")
        logf.close()
        return 3

    ckpt = open(CHECKPOINT_PATH, "a")
    docs_ok = chunks_deleted = summaries_deleted = 0
    quarantined = superseded_reset = 0
    errors = 0

    try:
        # ---------- FASE 1: chunk + sidecar + registry, per documento -------
        for idx, (path, h) in enumerate(todo):
            log(f"\n[{idx + 1}/{len(todo)}] {os.path.basename(path)}  doc_{h}")
            _assert_maintenance(f"prima della delete di doc_{h}")
            _assert_entity_index_stable(entity_baseline,
                                        f"prima della delete di doc_{h}")

            # get SENZA limit: e' l'errore di garbage_collect.py, e su un file
            # da 13k chunk lascerebbe indietro tutto tranne i primi 100.
            res = collection.get(where={"conv_id": f"doc_{h}"}, include=[])
            ids = res.get("ids") or []
            if ids:
                collection.delete(ids=ids)

            # VERIFICA post-delete: che la delete sia avvenuta lo deve dire
            # una rilettura, non l'assenza di eccezioni.
            left = collection.get(where={"conv_id": f"doc_{h}"}, include=[])
            n_left = len(left.get("ids") or [])
            if n_left:
                errors += 1
                log(f"  ERRORE: {n_left} chunk ancora presenti dopo la delete "
                    f"— documento SALTATO, registry intatto")
                continue
            log(f"  cancellati {len(ids):,} chunk (residui: 0)")
            chunks_deleted += len(ids)

            # Sidecar: fail-loud, propaga e ferma il run.
            sc = invalidate_chunk_sidecars(f"doc_{h}", verbose=False)
            detail = ", ".join(f"{k}={v}" for k, v in sc.items()) or "nessuna riga"
            log(f"  sidecar invalidati: {detail}")

            _assert_entity_index_stable(entity_baseline, "dopo il cleanup")

            registry.pop(path, None)
            ing.save_registry(registry)
            ckpt.write(f"{path}\t{h}\n")
            ckpt.flush()
            os.fsync(ckpt.fileno())
            docs_ok += 1

        # ---------- FASE 2: doc_summary ------------------------------------
        if SENTINEL_SUMMARIES in done:
            log("\n[summary] gia' fatta (sentinella a checkpoint), salto")
        elif limit_docs:
            log("\n[summary] SALTATA: --limit-docs e' uno smoke, i summary si "
                "cancellano solo nel run completo")
        else:
            log("\n[summary] cancellazione doc_summary per title+folder+category")
            _assert_maintenance("prima della delete dei summary")
            titles = {os.path.basename(p) for p, _ in in_reg}
            folders = {os.path.basename(p): ing.infer_folder(p)
                       for p, _ in in_reg}
            summary_residui = 0
            for t in sorted(titles):
                res = collection.get(where={"$and": [
                    {"title": t},
                    {"folder": folders[t]},
                    {"category": "doc_summary"},
                ]}, include=[])
                sids = res.get("ids") or []
                if not sids:
                    continue
                collection.delete(ids=sids)
                left = collection.get(where={"$and": [
                    {"title": t},
                    {"folder": folders[t]},
                    {"category": "doc_summary"},
                ]}, include=[])
                if left.get("ids"):
                    errors += 1
                    summary_residui += len(left["ids"])
                    log(f"  ERRORE: summary di {t!r} ancora presenti dopo la "
                        f"delete")
                    continue
                summaries_deleted += len(sids)
                log(f"  {len(sids)} summary  title={t!r}")
            # La sentinella sigilla una fase COMPLETA, mai una TENTATA: e' un
            # "non rifarlo", e scriverla con dei residui vuol dire che ogni
            # resume salta l'unica fase che potrebbe ripararli — audit rosso
            # per sempre, senza che nessun rilancio possa cambiarlo.
            if summary_residui:
                log(f"  [summary] {summary_residui} residui: sentinella NON "
                    f"scritta, la fase verra' rieseguita al prossimo run")
            else:
                ckpt.write(f"{SENTINEL_SUMMARIES}\tdone\n")
                ckpt.flush()
                os.fsync(ckpt.fileno())

        # ---------- FASE 3: quarantena derivati ----------------------------
        if SENTINEL_DERIVED in done:
            log("\n[derived] gia' fatta (sentinella a checkpoint), salto")
        elif limit_docs:
            log("\n[derived] SALTATA: con --limit-docs i facts vittima sono "
                "solo una parte, la quarantena andrebbe incompleta")
        else:
            log("\n[derived] applico la quarantena")
            quarantined, superseded_reset, sup_pendenti = apply_derived_plan(
                derived_plan, log)
            # Sigillo = fase completa. I due rami hanno criteri DIVERSI perche'
            # sono asimmetrici rispetto alla delete facts:
            #   * quarantena: i derivati hanno chunk_id "derived_*", che non
            #     matcha mai "doc_<hash>_chunk_%". Non possono essere vittime,
            #     quindi devono esserci tutti e rowcount == len(piano) e' il
            #     criterio giusto.
            #   * superseded: quegli id sono facts di chunk qualsiasi, spesso
            #     dei chunk purgati stessi, e la fase sidecar li ha cancellati
            #     prima che arrivassimo qui. Il criterio e' "nessun id del
            #     piano ANCORA ESISTENTE ha superseded_by valorizzato": un id
            #     sparito non ha niente da resettare.
            attesi_q = len(derived_plan["quarantine"])
            if quarantined != attesi_q or sup_pendenti:
                errors += 1
                log(f"  [derived] INCOMPLETA: quarantena {quarantined:,}/"
                    f"{attesi_q:,}, superseded ancora pendenti "
                    f"{sup_pendenti:,} — sentinella NON scritta, la fase "
                    f"verra' rieseguita")
            else:
                ckpt.write(f"{SENTINEL_DERIVED}\tdone\n")
                ckpt.flush()
                os.fsync(ckpt.fileno())

    except (MaintenanceLost, EntityIndexBusy) as e:
        sys.stderr.write(
            f"\nABORT: {e}\n"
            f"  lock maintenance atteso in {MAINTENANCE_LOCK}\n"
            f"  fatti fin qui: doc={docs_ok} chunk={chunks_deleted:,}\n"
            f"  checkpoint: {CHECKPOINT_PATH}\n"
            f"  piano derivati: {DERIVED_PLAN_PATH} (NON cancellarlo: il "
            f"resume lo riusa)\n"
            f"  risolvi la causa e rilancia lo stesso comando.\n")
        log(f"[ABORT] {e} — doc={docs_ok} chunk={chunks_deleted}")
        return 3
    finally:
        ckpt.close()

    kb_after = kb_count()
    # Contatori di PROCESSO: cosa ha fatto questo run. Informativi — il
    # giudizio sul purge lo da' audit_state(), che guarda i DB.
    log(f"\n[progress] questo run: doc={docs_ok} chunk={chunks_deleted:,} "
        f"summary={summaries_deleted} quarantena={quarantined:,} "
        f"superseded_reset={superseded_reset:,} errori={errors}")
    log(f"[kb]      {kb_before:,} -> {kb_after:,}")
    log(f"[execute] log: {log_path}")

    if limit_docs:
        # Uno smoke lascia la KB a META': summary non cancellati, derivati non
        # quarantinati, gran parte dei documenti ancora dentro. Far vedere qui
        # la sequenza di chiusura sarebbe un invito a eseguirla, e il
        # reconciler su uno stato parziale riscriverebbe l'indice lessicale
        # allineandolo a una KB che non e' quella finale.
        log("[audit]   saltato: run parziale (--limit-docs)")
        logf.close()
        print(f"\n{'=' * 78}")
        print("RUN PARZIALE (smoke) — LA KB E' IN UNO STATO INTERMEDIO")
        print("=" * 78)
        print("  NON lanciare il reconciler.")
        print("  NON riaprire il MCP (niente kill -USR2, niente kickstart).")
        print("  NON seguire i next step di chiusura: qui non sono stampati")
        print("  apposta.")
        print("\n  Rilanciare senza --limit-docs per completare il purge:")
        print("    ./.venv/bin/python scripts/maintenance/purge_lowvalue_docs.py"
              " --execute")
        return 1 if errors else 0

    failed = audit_state(in_reg, derived_plan, log)
    logf.close()

    if failed or errors:
        print(f"\n{'=' * 78}")
        print("PURGE INCOMPLETO — NON PROSEGUIRE")
        print("=" * 78)
        print("  NON lanciare il reconciler, NON riaprire il MCP.")
        print("  Rilanciare lo stesso comando (e' resumable) e ri-guardare"
              " l'audit.")
        return 1

    print("\n[next] reconciler:  ./.venv/bin/python "
          "scripts/maintenance/reconcile_lexical_index.py --max-churn 1.0")
    print("[next] maintenance OFF: kill -USR2 <mcp_pid>")
    print("[next] persist:     sudo launchctl kickstart system/com.tailor.mcp"
          "   (graceful, senza -k)")
    return 0


# ============================================================================
def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", default=True,
                   help="censimento READ-ONLY (default)")
    g.add_argument("--execute", action="store_true",
                   help="purge (richiede maintenance mode)")
    ap.add_argument("--limit-docs", type=int, default=None,
                    help="smoke: purga al massimo N documenti. Salta le fasi "
                         "summary e derivati, che hanno senso solo complete")
    ap.add_argument("--show", type=int, default=10, metavar="N",
                    help="dry-run: quanti path elencare per lista (default 10)")
    args = ap.parse_args()

    if args.execute:
        return execute(args.limit_docs)
    return dry_run(args.show)


if __name__ == "__main__":
    sys.exit(main())
