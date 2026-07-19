#!/usr/bin/env python3
"""Re-ingest degli Excel storici per applicare l'estrazione header-aware.

================================================================================
CONTESTO
================================================================================
Gli .xlsx/.xls ingeriti prima di d9faf2d sono in KB con l'estrazione legacy: un
unico blocco `[Sheet: X]\\nriga | riga | riga` per foglio, spezzato poi dal
chunker generico a meta' riga e con overlap. L'estrattore attuale
(`_xl_sheet_sections`) rileva l'header e produce sezioni PRE-CHUNKED — righe
intere, header ripetuto su ogni blocco, nessun overlap.

Questo runner ri-processa SOLO i file per cui l'estrattore odierno produce
davvero sezioni header-aware. Un file che ricade nel fallback legacy verrebbe
ri-scritto identico a se stesso: e' lavoro (ed embedding) a zero valore, quindi
si salta.

Sorgente della lista: db/doc_registry.json (estensioni .xlsx/.xls). Non si
riscansionano le cartelle: l'universo di interesse e' cio' che e' GIA' in KB.

================================================================================
CLASSIFICAZIONE — SI LEGGE L'ESTRAZIONE, NON I CHUNK
================================================================================
Il flag `prechunked` vive in `sections[*]["metadata"]` e viene CONSUMATO da
`chunk_document` (`sec_meta.pop("prechunked")`): nei chunk finali non compare, e
nemmeno in KB. Classificare guardando i chunk darebbe quindi sempre
"fallback_only". La classe si decide sull'output di `extract_excel()`:

  header_aware   almeno una sezione con metadata.prechunked -> DA RIFARE
  fallback_only  nessuna sezione prechunked -> skip benigno (output identico)

Un file header-aware si ri-ingerisce INTERO, mai per-foglio: `chunk_index` in
`chunk_document` e' file-wide e continua a incrementare tra un foglio e
l'altro. Riscrivere i chunk di un solo foglio produrrebbe id sovrapposti a
quelli di altri fogli.

================================================================================
CLASSI DI SKIP — QUALI SONO WARNING E QUALI NO
================================================================================
  fallback_only   skip BENIGNO: fuori scope per costruzione.
  assente         il path del registry non esiste piu' su disco. WARNING:
                  significa registry e disco divergenti (file spostato o
                  cancellato senza che il registry se ne accorgesse).
  modified        hash su disco != hash nel registry. WARNING ESPLICITO, non
                  skip benigno: il file e' cambiato e il daily lo deve ancora
                  processare. Ri-ingerirlo QUI vorrebbe dire fare il lavoro del
                  daily con uno scope diverso da quello dichiarato; la classe
                  header-aware/fallback viene comunque riportata per diagnosi.
  fuori_watch     path del registry non sotto nessuna WATCH_FOLDERS attuale.
                  WARNING: non e' ricostruibile un file_info identico a quello
                  che scan_folders() produrrebbe (vedi sotto), quindi si salta.
  escluso_da_scan path sotto una watch folder ma che scan_folders() scarterebbe
                  (IGNORE_FOLDERS / IGNORE_PATTERNS / estensione non
                  supportata). WARNING: e' un residuo di una config precedente.
  estrazione_vuota  extract_excel() non ritorna sezioni. WARNING, e il registry
                  NON viene toccato: il file non entra mai nel piano, quindi
                  non e' un tentativo di re-ingest fallito e non deve gonfiare
                  il `fail_count` che il daily usa per il permanently-failed a
                  3 tentativi (vedi "mark_file_failed" sotto).

================================================================================
RIUSO DI ingest_docs COME LIBRERIA (mai come script)
================================================================================
`extract_excel`, `chunk_document`, `embed_chunks`, `_pre_upsert_cleanup`,
`ingest_chunks`, `mark_file_failed`, `save_registry`, `load_registry`,
`file_hash`, `acquire_single_instance_lock` sono importati da
scripts/ingest/ingest_docs.py: i chunk prodotti qui devono essere byte-identici
a quelli che il daily produrrebbe.

I globali di ingest_docs (WATCH_FOLDERS, DB_DIR, REGISTRY_FILE, LOCK_FILE,
COLLECTION_NAME, BATCH_SIZE, TARGET_CHUNK_CHARS...) sono letti, MAI riassegnati:
sono config import-time e sovrascriverli qui farebbe divergere silenziosamente
questo runner dal daily.

`file_info` e' ricostruito con gli STESSI campi e le STESSE formule di
`scan_folders()` (rel_path via commonpath su tutte le WATCH_FOLDERS, category =
primo sottolivello rispetto alla watch folder che contiene il file, modified_ts
/ modified_date / size_kb da os.stat). Non e' pedanteria: `rel_path` finisce in
`meta["file_path"]` e `category` in `meta["category"]`, quindi metadati diversi
= contenuto indicizzato diverso da quello del daily.

LOCK: si prende `ingest_docs.lock`, lo STESSO del daily (`LOCK_FILE`), perche'
questo runner e' a tutti gli effetti un write path di ingest: due write path
concorrenti si sovrascrivono il registry a vicenda (read-modify-write non
atomico) e interlacciano delete e upsert sugli stessi chunk_id.

================================================================================
MAINTENANCE GATE (--execute)
================================================================================
`maintenance_state()` e' importata da backfill_email_youniversal.py: e' la
versione PID-validata (lock presente + PID vivo + PID che e' davvero il MCP,
anti PID-reuse). Duplicarla qui la farebbe divergere dall'originale, ed e'
logica in cui un falso positivo = due PersistentClient sullo stesso path =
SIGSEGV Rust non catchabile. Il gate e' verificato:

  * all'avvio di --execute;
  * PRIMA di aprire PersistentClient (la classificazione estrae ~450 file e
    dura minuti: la maintenance puo' cadere proprio li');
  * prima di get_collection;
  * prima di OGNI upsert (per batch, non per file).

GUARDIA entity_index: `_pre_upsert_cleanup` -> `invalidate_chunk_sidecars`
scrive su db/entity_index.sqlite3, mentre scripts/enrichment/build_entity_index.py
fa `os.remove(ENTITY_DB)` e lo ricostruisce da zero. Le due cose insieme
significano DELETE su un file gia' scollegato, cioe' invalidazione persa in
silenzio. Quindi si aborta se build_entity_index.py risulta attivo (pgrep) o se
entity_index.sqlite3 sparisce / cambia inode tra l'avvio e un cleanup.

Il controllo e' ripetuto SUBITO DOPO il cleanup, prima del primo upsert: la
finestra pericolosa e' proprio quella tra invalidazione e riscrittura, e il
check pre-cleanup da solo non la copre. Se scatta li', il file ha gia' subito la
delete Chroma ma potrebbe aver perso l'invalidazione dei sidecar: non e'
registrabile come successo -> mark_file_failed("entity_index_race"), niente
checkpoint, abort del run. Al resume viene rifatto da capo, il che e' sicuro
perche' il cleanup e' idempotente e i chunk_id sono deterministici sull'hash.

================================================================================
VINCOLI
================================================================================
  * --dry-run (default): READ-ONLY, MAI un PersistentClient. Legge registry e
    file su disco, estrae e chunka in RAM per contare. Ollama toccato solo con
    --probe-embed.
  * --execute: sequenza per-file extract -> chunk -> embed -> cleanup -> upsert
    -> registry -> checkpoint. L'embedding sta PRIMA del cleanup (come nel
    daily): se fallisce, la KB non e' stata toccata e il documento vecchio resta
    intero.
  * REGISTRY salvato per-file (save_registry e' atomica: tmp + os.replace), non
    a fine run: un'interruzione a meta' non deve buttare via i file gia' fatti.
  * CHECKPOINT `path\\thash`, scritto SOLO dopo un save_registry riuscito e solo
    con errors == 0 and processed == len(chunks). Un upsert parziale non e' un
    successo: va a mark_file_failed e viene ritentato. Al resume il match e' su
    (path, contenuto): un file cambiato dopo il checkpoint viene rifatto invece
    di essere saltato per sempre, e le righe legacy solo-path valgono come
    non-match (con warning).
  * mark_file_failed SOLO su file davvero tentati (embedding / cleanup / upsert
    parziale). Mai su assenti, fuori_watch, fallback_only o estrazione vuota:
    quei file non sono un tentativo fallito, e gonfiare il loro fail_count li
    porterebbe verso il permanently-failed del daily per un lavoro che il daily
    non ha nemmeno chiesto.
  * ENRICHMENT: i chunk riscritti perdono le entita' (i sidecar vengono
    invalidati dal cleanup, come nel daily). Li ri-raccogliera' il normale giro
    di scripts/enrichment/extract_entities.py.

Sequenza operativa (di Emiliano — lo script non tocca daemon/launchctl):

    1. dry-run:          ./.venv/bin/python scripts/maintenance/reingest_xlsx.py
    2. maintenance ON:   kill -USR1 <mcp_pid>
    3. backup:           bash scripts/maintenance/backup_db.sh
    4. smoke:            ./.venv/bin/python scripts/maintenance/reingest_xlsx.py \\
                           --execute --limit-files 3
    5. run:              ./.venv/bin/python scripts/maintenance/reingest_xlsx.py --execute
    6. maintenance OFF:  kill -USR2 <mcp_pid>
    7. persist durevole: sudo launchctl kickstart -k system/com.tailor.mcp
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime

# --- path setup ---
_HERE = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(_HERE))            # repo root
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "ingest"))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "maintenance"))

# ingest_docs USATO COME LIBRERIA (vedi docstring): mai main().
import ingest_docs as ing                                          # noqa: E402

# Gate maintenance PID-validato: unica fonte, mai duplicata.
from backfill_email_youniversal import (                           # noqa: E402
    MAINTENANCE_LOCK,
    MaintenanceLost,
    maintenance_state,
)

LOGS_DIR = os.path.join(BASE_DIR, "logs")
CHECKPOINT_PATH = os.path.join(LOGS_DIR, "reingest_xlsx.done")
ENTITY_DB = os.path.join(ing.DB_DIR, "entity_index.sqlite3")

XL_EXTENSIONS = (".xlsx", ".xls")

# rate di embedding assunto per la stima a dry-run; --probe-embed lo misura.
ASSUMED_EMBED_RATE = 20.0   # chunk/s

# classi che NON sono skip benigni: vanno stampate come warning.
WARN_CLASSES = ("assente", "modified", "fuori_watch", "escluso_da_scan",
                "estrazione_vuota")
# warning che NON sono uno skip: il file prosegue e viene ri-processato.
CHECKPOINT_WARN = ("checkpoint_non_match",)


# ============================================================================
# GUARDIA entity_index (collisione con build_entity_index.py)
# ============================================================================
class EntityIndexBusy(RuntimeError):
    """build_entity_index.py attivo o entity_index.sqlite3 ricreato sotto i piedi."""


def _entity_index_fingerprint() -> tuple[bool, int]:
    """(esiste, inode). L'inode distingue 'stesso file' da 'ricreato da zero'."""
    try:
        st = os.stat(ENTITY_DB)
    except FileNotFoundError:
        return False, 0
    return True, st.st_ino


def _build_entity_index_running() -> tuple[bool, str]:
    """pgrep su build_entity_index.py. Non validabile == occupato.

    Se `pgrep` stesso fallisce non possiamo escludere il rebuild concorrente, e
    l'errore da preferire e' il falso positivo (mi fermo per niente) invece di
    un'invalidazione sidecar persa in silenzio.
    """
    try:
        r = subprocess.run(["pgrep", "-f", "build_entity_index.py"],
                           capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.SubprocessError) as e:
        return True, f"pgrep non eseguibile ({e}): rebuild NON escludibile"
    if r.returncode == 0:
        pids = " ".join(r.stdout.split())
        return True, f"build_entity_index.py attivo (pid {pids})"
    if r.returncode != 1:                      # 1 = nessun match, tutto ok
        return True, f"pgrep rc={r.returncode}: rebuild NON escludibile"
    return False, ""


def _assert_entity_index_stable(baseline: tuple[bool, int], stage: str) -> None:
    busy, why = _build_entity_index_running()
    if busy:
        raise EntityIndexBusy(f"{stage}: {why}")
    now = _entity_index_fingerprint()
    if now != baseline:
        raise EntityIndexBusy(
            f"{stage}: entity_index.sqlite3 cambiato sotto i piedi "
            f"(era exists={baseline[0]} inode={baseline[1]}, "
            f"ora exists={now[0]} inode={now[1]}) — rebuild concorrente")


def _assert_maintenance(stage: str) -> None:
    ok, why = maintenance_state()
    if not ok:
        raise MaintenanceLost(f"{stage}: {why}")


# ============================================================================
# RICOSTRUZIONE file_info — identica a scan_folders()
# ============================================================================
def _contains(folder: str, filepath: str) -> bool:
    folder = os.path.normpath(folder)
    try:
        return os.path.commonpath([folder, os.path.normpath(filepath)]) == folder
    except ValueError:                 # path relativi/drive diversi
        return False


def _watch_folder_for(filepath: str) -> str | None:
    """Prima watch folder che contiene il file, nell'ordine di WATCH_FOLDERS.

    "Prima" e non "ultima": con watch folder annidate os.walk emette il file una
    volta per cartella, ma classify_files() deduplica per hash e tiene la PRIMA
    occorrenza. La category va quindi calcolata sulla stessa.
    """
    for folder in ing.WATCH_FOLDERS:
        if _contains(folder, filepath):
            return folder
    return None


def _excluded_by_scan(filepath: str, folder: str) -> str:
    """Motivo per cui scan_folders() scarterebbe il file, "" se lo terrebbe."""
    fname = os.path.basename(filepath)
    if any(p in fname for p in ing.IGNORE_PATTERNS):
        return "IGNORE_PATTERNS"
    ext = os.path.splitext(fname)[1].lower()
    if ext not in ing.SUPPORTED_EXTENSIONS:
        return f"estensione {ext} non in SUPPORTED_EXTENSIONS"
    # os.walk pota le dir in IGNORE_FOLDERS: basta un componente intermedio.
    parts = os.path.relpath(filepath, folder).split(os.sep)[:-1]
    for p in parts:
        if p in ing.IGNORE_FOLDERS:
            return f"IGNORE_FOLDERS ({p})"
    return ""


def _build_file_info(filepath: str, folder: str, fhash: str) -> dict:
    """Stessi campi e stesse formule di scan_folders(). Vedi docstring."""
    rel_path = os.path.relpath(
        filepath, os.path.commonpath(list(ing.WATCH_FOLDERS) + [filepath]))
    parts = os.path.relpath(filepath, folder).split(os.sep)
    category = parts[0] if len(parts) > 1 else ""
    try:
        stat = os.stat(filepath)
        mod_ts = stat.st_mtime
        mod_date = time.strftime("%Y-%m-%d", time.localtime(mod_ts))
        size_kb = stat.st_size / 1024
    except Exception:
        mod_ts, mod_date, size_kb = 0, "", 0
    return {
        "filepath": filepath,
        "filename": os.path.basename(filepath),
        "rel_path": rel_path,
        "extension": os.path.splitext(filepath)[1].lower(),
        "category": category,
        "modified_ts": mod_ts,
        "modified_date": mod_date,
        "size_kb": size_kb,
        "hash": fhash,
    }


# ============================================================================
# CLASSIFICAZIONE
# ============================================================================
def load_checkpoint() -> dict[str, str]:
    """path -> hash del contenuto al momento del checkpoint.

    Il formato e' `path\\thash`: un checkpoint solo-path direbbe "questo path
    e' stato fatto", che non e' la domanda giusta. Se il file cambia dopo il
    checkpoint, il re-ingest da fare e' quello del contenuto NUOVO, e un match
    per path lo salterebbe per sempre.

    Le righe legacy solo-path (checkpoint scritti prima di questo formato)
    mappano a "": non provano il contenuto, quindi valgono come NON-match e il
    file viene ri-processato — con warning, perche' e' lavoro rifatto e
    l'operatore deve sapere perche'.
    """
    if not os.path.exists(CHECKPOINT_PATH):
        return {}
    done: dict[str, str] = {}
    with open(CHECKPOINT_PATH) as f:
        for line in f:
            line = line.rstrip("\n")
            if not line.strip():
                continue
            path, _, h = line.partition("\t")
            done[path.strip()] = h.strip()
    return done


def _is_header_aware(sections: list) -> bool:
    """Il flag vive nell'ESTRAZIONE: chunk_document lo pop-pa. Vedi docstring."""
    return any(s.get("metadata", {}).get("prechunked") for s in sections)


def classify(registry: dict, done: dict[str, str], limit_files: int | None = None,
             verbose: bool = True):
    """Ritorna (plan, buckets, stats).

    plan: [(file_info, sections)] dei soli header_aware da rifare.
    buckets: classe -> lista di (filepath, dettaglio) per il report.
    """
    plan: list[tuple[dict, list]] = []
    buckets: dict[str, list] = {c: [] for c in
                                ("header_aware", "fallback_only", "checkpoint")}
    for c in WARN_CLASSES + CHECKPOINT_WARN:
        buckets[c] = []
    stats = {"candidati": 0, "chunk": 0, "chars": 0, "sezioni": 0}

    candidates = sorted(
        p for p in registry
        if os.path.splitext(p)[1].lower() in XL_EXTENSIONS)
    stats["candidati"] = len(candidates)

    for idx, filepath in enumerate(candidates):
        if verbose and idx and idx % 50 == 0:
            print(f"  ...{idx}/{len(candidates)} classificati "
                  f"({len(plan)} da rifare)", flush=True)

        folder = _watch_folder_for(filepath)
        if folder is None:
            buckets["fuori_watch"].append((filepath, "nessuna WATCH_FOLDERS"))
            continue
        why = _excluded_by_scan(filepath, folder)
        if why:
            buckets["escluso_da_scan"].append((filepath, why))
            continue
        if not os.path.exists(filepath):
            buckets["assente"].append((filepath, "non esiste su disco"))
            continue

        try:
            fhash = ing.file_hash(filepath)
        except OSError as e:
            buckets["assente"].append((filepath, f"illeggibile: {e}"))
            continue

        # Checkpoint DOPO l'hash: il match e' su (path, contenuto), non sul
        # path. Costa una sha256 in piu' per file gia' fatto, ma evita di
        # saltare per sempre un file cambiato dopo il checkpoint. Sta comunque
        # PRIMA dell'estrazione, che e' la parte cara.
        if filepath in done:
            ck_hash = done[filepath]
            if ck_hash == fhash:
                buckets["checkpoint"].append((filepath, ""))
                continue
            buckets["checkpoint_non_match"].append(
                (filepath, "riga legacy senza hash" if not ck_hash
                 else f"hash cambiato dal checkpoint ({ck_hash[:12]} -> "
                      f"{fhash[:12]})"))
            # nessun continue: il file prosegue la classifica e viene rifatto.

        # Estrazione: unica fonte della classificazione.
        try:
            sections = ing.extract_excel(filepath)
        except Exception as e:
            buckets["estrazione_vuota"].append((filepath, f"eccezione: {e}"))
            continue
        if not sections:
            buckets["estrazione_vuota"].append((filepath, "0 sezioni"))
            continue
        klass = "header_aware" if _is_header_aware(sections) else "fallback_only"

        reg_hash = registry.get(filepath, {}).get("hash", "") or ""
        if reg_hash != fhash:
            # Fuori scope, ma NON un skip benigno: e' roba del daily.
            buckets["modified"].append(
                (filepath, f"hash disco != registry (classe: {klass})"))
            continue

        if klass == "fallback_only":
            buckets["fallback_only"].append((filepath, ""))
            continue

        f = _build_file_info(filepath, folder, fhash)
        chunks = ing.chunk_document(sections, f)
        buckets["header_aware"].append(
            (filepath, f"{len(sections)} sez -> {len(chunks)} chunk"))
        stats["sezioni"] += len(sections)
        stats["chunk"] += len(chunks)
        stats["chars"] += sum(len(s["text"]) for s in sections)
        plan.append((f, sections))
        if limit_files and len(plan) >= limit_files:
            break

    return plan, buckets, stats


# ============================================================================
# DRY-RUN
# ============================================================================
def probe_embed_rate(plan, n: int) -> float | None:
    """Misura il rate reale su n chunk (solo Ollama, zero Chroma, zero KB)."""
    chunks = []
    for f, sections in plan:
        chunks.extend(ing.chunk_document(sections, f))
        if len(chunks) >= n:
            break
    chunks = chunks[:n]
    if not chunks:
        return None
    t0 = time.time()
    batches, errors = ing.embed_chunks(chunks)
    el = time.time() - t0
    if errors or not batches:
        print(f"  [probe] embedding fallito su {errors} chunk")
        return None
    return len(chunks) / el if el else None


def dry_run(limit_files: int | None, probe: int, show: int) -> int:
    print("=" * 78)
    print("RE-INGEST XLSX HEADER-AWARE — DRY-RUN (READ-ONLY, zero scritture)")
    print("=" * 78)
    print(f"[registry] {ing.REGISTRY_FILE}")
    print(f"[watch]    {len(ing.WATCH_FOLDERS)} cartelle")

    # Probe del lock condiviso: si acquisisce e si RILASCIA subito. Tenerlo per
    # tutta la classificazione (minuti) bloccherebbe il daily per un dry-run che
    # non scrive nulla.
    lock_fd = ing.acquire_single_instance_lock()
    if lock_fd is None:
        print(f"[lock]     ATTENZIONE: {ing.LOCK_FILE} occupato — un ingest e' "
              f"in corso, i numeri qui sotto possono spostarsi.")
    else:
        lock_fd.close()
        print(f"[lock]     {ing.LOCK_FILE} libero")

    registry = ing.load_registry()
    done = load_checkpoint()
    if done:
        print(f"[ckpt]     {len(done):,} file gia' a checkpoint ({CHECKPOINT_PATH})")

    print("\n[scan]     classificazione (estrazione reale di ogni file)...",
          flush=True)
    t0 = time.time()
    plan, buckets, st = classify(registry, done, limit_files)
    el = time.time() - t0

    print("\n" + "=" * 78)
    print("CLASSIFICA")
    print("=" * 78)
    print(f"candidati in registry (.xlsx/.xls) : {st['candidati']:,}")
    print(f"  header_aware  DA RIFARE          : {len(buckets['header_aware']):,}")
    print(f"  fallback_only skip benigno       : {len(buckets['fallback_only']):,}")
    print(f"  gia' a checkpoint                : {len(buckets['checkpoint']):,}")
    for c in WARN_CLASSES:
        print(f"  {c:<30} : {len(buckets[c]):,}"
              + ("   <-- WARNING" if buckets[c] else ""))
    n_ck = len(buckets["checkpoint_non_match"])
    print(f"  checkpoint NON match (rifatti)   : {n_ck:,}"
          + ("   <-- WARNING" if n_ck else ""))

    for c in WARN_CLASSES + CHECKPOINT_WARN:
        if not buckets[c]:
            continue
        print(f"\n--- WARNING [{c}] ({len(buckets[c])}) ---")
        for p, d in buckets[c][:show]:
            print(f"  {os.path.basename(p)}" + (f"  — {d}" if d else ""))
            print(f"      {p}")
        if len(buckets[c]) > show:
            print(f"  ... e altri {len(buckets[c]) - show} "
                  f"(usa --show {len(buckets[c])} per vederli tutti)")

    print("\n" + "=" * 78)
    print("PIANO")
    print("=" * 78)
    print(f"file da re-ingerire  : {len(plan):,}")
    print(f"sezioni estratte     : {st['sezioni']:,}")
    print(f"CHUNK EFFETTIVI      : {st['chunk']:,}  (chunk_document reale, non stima)")
    print(f"chars estratti       : {st['chars']:,}")
    if plan:
        print(f"media chunk/file     : {st['chunk'] / len(plan):.1f}")
    print(f"tempo classificazione: {el / 60:.1f} min")

    if plan:
        print("\n--- PRIMI FILE DEL PIANO ---")
        for p, d in buckets["header_aware"][:show]:
            print(f"  {os.path.basename(p)}  — {d}")

    rate = None
    if probe and plan:
        print(f"\n[probe] misuro il rate su {probe} chunk reali "
              f"(solo Ollama, nessuna scrittura)...", flush=True)
        rate = probe_embed_rate(plan, probe)
        if rate:
            print(f"[probe] rate misurato: {rate:.1f} chunk/s")
    if rate is None:
        rate = ASSUMED_EMBED_RATE
        print(f"\n[stima] rate assunto: {rate:.1f} chunk/s "
              f"(usa --probe-embed N per misurarlo)")
    print(f"STIMA TEMPO EMBED+UPSERT: {st['chunk'] / rate / 60:.1f} min "
          f"({st['chunk']:,} chunk / {rate:.1f} chunk/s)")

    ok, why = maintenance_state()
    print(f"\n[gate]  maintenance mode adesso: {'OK' if ok else 'NO'} — {why}")
    busy, bwhy = _build_entity_index_running()
    print(f"[gate]  build_entity_index: {'BUSY — ' + bwhy if busy else 'fermo'}")
    print("[dry-run] nessuna scrittura. Fine.")
    return 0


# ============================================================================
# EXECUTE
# ============================================================================
def execute(limit_files: int | None) -> int:
    ok, why = maintenance_state()
    if not ok:
        sys.stderr.write(
            f"\nRIFIUTO --execute: MCP NON in maintenance mode ({why}).\n"
            f"  Entra in maintenance: kill -USR1 <mcp_pid>\n"
            f"  (il lock da solo non basta: serve che il MCP abbia RILASCIATO\n"
            f"   il client ChromaDB, cosa che fa solo nel handler SIGUSR1)\n"
            f"  Poi backup: bash scripts/maintenance/backup_db.sh\n")
        return 2
    print(f"[gate]  {why}")

    busy, bwhy = _build_entity_index_running()
    if busy:
        sys.stderr.write(
            f"\nRIFIUTO --execute: {bwhy}.\n"
            f"  Il cleanup pre-upsert invalida i sidecar in "
            f"entity_index.sqlite3, che build_entity_index.py rimuove e\n"
            f"  ricostruisce: le invalidazioni finirebbero in un file gia'\n"
            f"  scollegato, perse in silenzio.\n")
        return 2
    entity_baseline = _entity_index_fingerprint()

    # Lock CONDIVISO col daily: questo runner e' un write path di ingest.
    # lock_fd va tenuto referenziato per tutta la durata della funzione — se il
    # file object viene raccolto, l'OS rilascia il lock.
    lock_fd = ing.acquire_single_instance_lock()
    if lock_fd is None:
        sys.stderr.write(
            f"\nRIFIUTO --execute: un altro ingest e' in esecuzione "
            f"({ing.LOCK_FILE}).\n")
        return 2

    os.makedirs(LOGS_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(LOGS_DIR, f"reingest_xlsx_{ts}.log")
    logf = open(log_path, "a")

    def log(msg: str) -> None:
        print(msg, flush=True)
        logf.write(msg + "\n")
        logf.flush()

    log(f"[execute] start {ts}  log={log_path}")
    log(f"[lock]    {ing.LOCK_FILE} acquisito (pid {os.getpid()})")

    registry = ing.load_registry()
    done = load_checkpoint()
    log(f"[ckpt]    {len(done):,} file gia' fatti -> resume")
    log("[scan]    classificazione (estrazione reale di ogni file)...")
    plan, buckets, st = classify(registry, done, limit_files)
    log(f"[scan]    header_aware={len(buckets['header_aware'])} "
        f"fallback_only={len(buckets['fallback_only'])} "
        f"checkpoint={len(buckets['checkpoint'])}")
    for c in WARN_CLASSES + CHECKPOINT_WARN:
        if buckets[c]:
            log(f"[warn]    {c}: {len(buckets[c])}")
    log(f"[scan]    {len(plan)} file da re-ingerire, {st['chunk']} chunk")
    if not plan:
        log("[execute] niente da fare.")
        logf.close()
        return 0

    # GATE prima di APRIRE il client, non solo prima di scrivere: la
    # classificazione estrae centinaia di file e dura minuti; la maintenance
    # puo' cadere proprio li'. Aprire un secondo PersistentClient mentre il MCP
    # ha riaperto il suo E' GIA' il SIGSEGV, molto prima del primo upsert.
    try:
        _assert_maintenance("prima di PersistentClient")
        _assert_entity_index_stable(entity_baseline, "prima di PersistentClient")
        import chromadb
        client = chromadb.PersistentClient(path=ing.DB_DIR)
        _assert_maintenance("prima di get_collection")
        collection = client.get_collection(name=ing.COLLECTION_NAME)
    except (MaintenanceLost, EntityIndexBusy) as e:
        sys.stderr.write(
            f"\nABORT: {e}\n"
            f"  lock maintenance atteso in {MAINTENANCE_LOCK}\n"
            f"  NESSUNA scrittura effettuata (client mai aperto).\n")
        log(f"[ABORT] {e}")
        logf.close()
        return 3

    ckpt = open(CHECKPOINT_PATH, "a")
    run_id = ing.make_run_id("document")
    files_ok = chunks_ok = total_errors = 0
    t0 = time.time()

    try:
        for idx, (f, sections) in enumerate(plan):
            log(f"\n[{idx + 1}/{len(plan)}] {f['filename']} "
                f"({f['size_kb']:.0f} KB)")

            chunks = ing.chunk_document(sections, f)
            total_chars = sum(len(s["text"]) for s in sections)
            log(f"  {len(sections)} sezioni ({total_chars:,} chars) -> "
                f"{len(chunks)} chunk")

            # Embedding PRIMA del cleanup (come nel daily): e' la fase lunga e
            # quella che fallisce piu' spesso. Se fallisce qui, la KB non e'
            # stata toccata e il documento vecchio resta intero.
            batches, embed_errors = ing.embed_chunks(chunks)
            if embed_errors or not batches:
                total_errors += embed_errors or 1
                fail_count = ing.mark_file_failed(registry, f, "embedding_failed")
                ing.save_registry(registry)
                log(f"  Embedding fallito, KB intatta, file SALTATO "
                    f"(fail_count={fail_count})")
                continue

            # Da qui la sequenza e' atomica per-file. Cleanup fallito = file
            # saltato: fare upsert dopo una delete non verificata lascia in KB
            # un mix di chunk vecchi e nuovi sotto gli stessi id.
            _assert_maintenance("prima del cleanup")
            _assert_entity_index_stable(entity_baseline, "prima del cleanup")
            try:
                ing._pre_upsert_cleanup(collection, f, registry)
            except Exception as e:
                total_errors += 1
                ing.mark_file_failed(registry, f, f"cleanup_failed: {e}")
                ing.save_registry(registry)
                log(f"  ERRORE cleanup pre-upsert, file SALTATO: {e}")
                continue

            # Il cleanup e' appena avvenuto: la delete Chroma e' andata, ma
            # l'invalidazione dei sidecar puo' essere finita su un
            # entity_index.sqlite3 gia' scollegato da un rebuild concorrente —
            # cioe' persa in silenzio, lasciando in KB entita' che puntano a
            # chunk_id che sto per riscrivere con testo diverso. Il file NON e'
            # in uno stato registrabile come successo: mark_file_failed e
            # abort. Al resume viene rifatto da capo, e va bene: il cleanup e'
            # idempotente (delete di chunk gia' spariti = no-op) e i chunk_id
            # sono deterministici sull'hash.
            try:
                _assert_entity_index_stable(entity_baseline, "dopo cleanup")
            except EntityIndexBusy:
                total_errors += 1
                fail_count = ing.mark_file_failed(
                    registry, f, "entity_index_race")
                ing.save_registry(registry)
                log(f"  RACE entity_index dopo il cleanup: file NON "
                    f"checkpointato (fail_count={fail_count}), abort del run")
                raise

            # Un batch per chiamata: il gate va verificato prima di OGNI upsert,
            # non una volta per file.
            processed = errors = 0
            for b in batches:
                _assert_maintenance(f"prima di upsert ({len(b['ids'])} chunk)")
                p, e = ing.ingest_chunks(collection, [b], run_id=run_id)
                processed += p
                errors += e
            chunks_ok += processed
            total_errors += errors
            log(f"  Ingestati {processed} chunk"
                + (f" ({errors} errori)" if errors else ""))

            # Successo SOLO se il documento e' integro: un upsert parziale
            # registrato come riuscito renderebbe il file "unchanged" al giro
            # dopo, cristallizzando in KB un documento monco.
            if errors == 0 and processed == len(chunks):
                registry[f["filepath"]] = {
                    "hash": f["hash"],
                    "filename": f["filename"],
                    "date_ingested": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "chunks": len(chunks),
                    "chars": total_chars,
                    "sections": len(sections),
                }
                ing.save_registry(registry)     # atomica, per-file
                # CHECKPOINT solo DOPO un save_registry riuscito: se il registry
                # non e' su disco, il file non e' fatto — e al resume deve
                # essere rifatto, non saltato.
                ckpt.write(f"{f['filepath']}\t{f['hash']}\n")
                ckpt.flush()
                os.fsync(ckpt.fileno())
                files_ok += 1
            else:
                fail_count = ing.mark_file_failed(
                    registry, f,
                    f"partial_upsert: {processed}/{len(chunks)} chunk")
                ing.save_registry(registry)
                log(f"  ATTENZIONE upsert parziale ({processed}/{len(chunks)}): "
                    f"registrato come FALLITO, niente checkpoint "
                    f"(fail_count={fail_count})")

            el = time.time() - t0
            rate = chunks_ok / el if el else 0
            eta = (st["chunk"] - chunks_ok) / rate / 60 if rate else 0
            log(f"  ...file={files_ok}/{len(plan)} "
                f"chunk={chunks_ok}/{st['chunk']} "
                f"{rate:.1f} chunk/s ETA {eta:.1f}m")

    except (MaintenanceLost, EntityIndexBusy) as e:
        sys.stderr.write(
            f"\nABORT: {e}\n"
            f"  lock maintenance atteso in {MAINTENANCE_LOCK}\n"
            f"  fatti fin qui: file={files_ok} chunk={chunks_ok}\n"
            f"  checkpoint: {CHECKPOINT_PATH}\n"
            f"  risolvi la causa e rilancia lo stesso comando: il resume\n"
            f"  riparte dall'ultimo file completo.\n")
        log(f"[ABORT] {e} — file={files_ok} chunk={chunks_ok}")
        return 3
    finally:
        ckpt.close()
        log(f"\n[execute] SUMMARY file={files_ok}/{len(plan)} "
            f"chunk={chunks_ok} errori={total_errors}")
        log(f"[execute] log: {log_path}")
        logf.close()

    print("[execute] enrichment: i sidecar invalidati verranno ricostruiti da"
          " extract_entities.py / build_entity_index.py")
    print("[execute] persist durevole: kickstart -k del MCP (operatore).")
    return 1 if total_errors else 0


# ============================================================================
def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", default=True,
                   help="classificazione READ-ONLY (default)")
    g.add_argument("--execute", action="store_true",
                   help="re-ingest (richiede maintenance mode)")
    ap.add_argument("--limit-files", type=int, default=None,
                    help="processa al massimo N file header_aware (smoke)")
    ap.add_argument("--probe-embed", type=int, default=0, metavar="N",
                    help="dry-run: misura il rate reale embeddando N chunk "
                         "(solo Ollama, nessuna scrittura)")
    ap.add_argument("--show", type=int, default=10, metavar="N",
                    help="dry-run: quanti path elencare per classe (default 10)")
    args = ap.parse_args()

    if args.execute:
        return execute(args.limit_files)
    return dry_run(args.limit_files, args.probe_embed, args.show)


if __name__ == "__main__":
    sys.exit(main())
