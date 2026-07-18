#!/usr/bin/env python3
"""Backfill delle email youniversal storiche perse dal thread-collapse pre-34995f0.

================================================================================
CONTESTO
================================================================================
Prima di 34995f0 `chunk_gmail.dedup_by_thread` teneva UNA SOLA email per thread
(la piu' recente): tutti gli altri messaggi dello stesso thread non sono mai
arrivati in KB. Oggi la funzione e' pass-through (idempotenza garantita a valle
dal chunk_id stabile `email_<id>_chunk_NNNN` + upsert Chroma), ma lo storico
gia' ingerito resta monco. Questo script recupera i messaggi mancanti dalla
sorgente di triage youniversal, senza ri-toccare quelli gia' presenti.

Sorgente: data/gmail_triage_youniversal.jsonl (campo `useful` gia' valorizzato
dal triage LLM: qui NON si ri-triagia nulla).

================================================================================
RIUSO DI chunk_gmail COME LIBRERIA (mai come script)
================================================================================
`should_keep`, `strip_quoted`, `chunk_email` (chunking + metadata + data ISO)
sono importati da scripts/gmail/chunk_gmail.py per garantire che i chunk di
backfill siano byte-identici a quelli che il daily produrrebbe.

`chunk_gmail` NON va MAI invocato come script in questo contesto: il suo
`process()` riscrive data/chunks_gmail.jsonl in modalita' "w" e interferirebbe
col pipeline daily. L'import e' privo di side effect sul filesystem (a
load-time legge solo config).

Il campo `embed_prefix` prodotto da `chunk_email` e' deliberatamente IGNORATO:
il testo da embeddare passa dal contratto P1
(scripts/lib/embedding_contract.py::embedding_text -> documento nudo troncato).

================================================================================
CRITERIO DI SKIP — UNIONE DI DUE FONTI INDIPENDENTI
================================================================================
Un email_id e' considerato GIA' PRESENTE, e quindi saltato integralmente (tutti
i suoi chunk), se compare in almeno una di:

  a) metadata: embedding_metadata.key='email_id' sui chunk con source='email'
  b) embedding_id: parsing RIGOROSO  ^email_(.+)_chunk_\\d{4}$

Le due fonti sono ridondanti per costruzione ma divergono se un ingest passato
ha scritto l'una senza l'altra: l'unione e' l'unico criterio conservativo.
Summary e derivati (conv_summary_*, doc_summary_*) sono IGNORATI da (b): non
sono chunk raw e la loro presenza non prova che il raw esista.

================================================================================
MAINTENANCE GATE (--execute)
================================================================================
Il contratto operativo NON e' "il file maintenance.lock esiste": e' che il MCP
abbia effettivamente RILASCIATO il client ChromaDB, cosa che fa solo nel
handler SIGUSR1 (mcp_server.py::_enter_maintenance, che chiude il client e poi
scrive il lock col proprio PID). Due PersistentClient simultanei sullo stesso
path = SIGSEGV Rust non catchabile, quindi:

  * il lock viene verificato all'avvio E prima di OGNI upsert;
  * il PID scritto nel lock viene validato (kill -0): un lock il cui PID e'
    morto e' STALE — il MCP che aveva rilasciato Chroma non c'e' piu' e uno
    nuovo potrebbe averlo riaperto. Si rifiuta di partire / si aborta.

Sequenza operativa (di Emiliano — lo script non tocca daemon/launchctl):

    1. maintenance ON:   kill -USR1 <mcp_pid>
    2. backup:           bash scripts/maintenance/backup_db.sh
    3. backfill:         ./.venv/bin/python \\
                           scripts/maintenance/backfill_email_youniversal.py --execute
    4. maintenance OFF:  kill -USR2 <mcp_pid>
    5. persist durevole: sudo launchctl kickstart -k system/com.tailor.mcp

================================================================================
VINCOLI
================================================================================
  * --dry-run (default): READ-ONLY. sqlite via mode=ro, MAI PersistentClient.
    Nessuna scrittura su db/. Ollama toccato solo con --probe-embed.
  * --execute: scrive SOLO su Chroma, sul proprio checkpoint e sul proprio log.
    Indice lessicale NON toccato (riallineamento a parte, se servira').
  * CHECKPOINT per email_id, scritto SOLO dopo l'upsert riuscito di TUTTI i
    chunk di quell'email — mai per-chunk. Un'email interrotta a meta' viene
    ri-processata da capo al resume (i chunk hanno id stabile: l'upsert e'
    idempotente).
  * ENRICHMENT fuori scope: i chunk sono scritti con entities_extracted=0 e
    verranno raccolti dal normale giro di scripts/enrichment/extract_entities.py
    (che processa tutto cio' che non ha entities_extracted == 1).
  * Re-fetch di corpi troncati: fuori scope.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime

# --- path setup ---
_HERE = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(_HERE))            # repo root
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "lib"))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "gmail"))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts"))

from embedding import get_embeddings, info as embedding_info      # noqa: E402
from embedding_contract import embedding_text                     # noqa: E402

# chunk_gmail USATO COME LIBRERIA (vedi docstring): mai process().
from chunk_gmail import should_keep, strip_quoted, chunk_email     # noqa: E402

from repair_hnsw_index import MAINTENANCE_LOCK                     # noqa: E402

# ----------------------------------------------------------------------------
DB_DIR = os.path.join(BASE_DIR, "db")
SQLITE_PATH = os.path.join(DB_DIR, "chroma.sqlite3")
COLLECTION_NAME = "tailor_kb_v2"
DATA_DIR = os.path.join(BASE_DIR, "data")
LOGS_DIR = os.path.join(BASE_DIR, "logs")

SOURCE_JSONL = os.environ.get(
    "BACKFILL_SOURCE_JSONL",
    os.path.join(DATA_DIR, "gmail_triage_youniversal.jsonl"),
)
CHECKPOINT_PATH = os.path.join(LOGS_DIR, "backfill_email_youniversal.done")

# chunk per upsert (e per batch di embedding)
BATCH = 64
# rate di embedding assunto per la stima a dry-run; --probe-embed lo misura.
ASSUMED_EMBED_RATE = 20.0   # chunk/s

# parsing rigoroso degli embedding_id raw email (fonte b del criterio di skip)
RAW_EMAIL_EID = re.compile(r"^email_(.+)_chunk_\d{4}$")


# ============================================================================
# MAINTENANCE GATE
# ============================================================================
class MaintenanceLost(RuntimeError):
    """Lock sparito o diventato stale a meta' run -> abort immediato."""


def maintenance_state() -> tuple[bool, str]:
    """(ok, motivo). ok=True solo con lock presente e PID vivo.

    Il PID nel lock e' quello del MCP che ha eseguito _enter_maintenance, cioe'
    che ha CHIUSO il client Chroma. Se quel processo non esiste piu', il lock e'
    un residuo: il MCP corrente (altro PID) puo' avere Chroma aperto.
    """
    if not os.path.exists(MAINTENANCE_LOCK):
        return False, f"nessun lock in {MAINTENANCE_LOCK}"
    try:
        with open(MAINTENANCE_LOCK) as f:
            raw = f.read().strip()
    except OSError as e:
        return False, f"lock illeggibile: {e}"
    if not raw:
        # lock senza PID: formato legacy, non validabile -> non si indovina.
        return False, "lock privo di PID (formato inatteso, impossibile validare)"
    try:
        pid = int(raw.split()[0])
    except ValueError:
        return False, f"PID non parsabile nel lock: {raw[:40]!r}"
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False, (f"lock STALE: PID {pid} non esiste piu' "
                       f"(il MCP che rilascio' Chroma e' morto)")
    except PermissionError:
        pass   # esiste ma di altro utente: vivo, va bene
    return True, f"lock valido (MCP pid={pid})"


def _assert_maintenance(stage: str) -> None:
    ok, why = maintenance_state()
    if not ok:
        raise MaintenanceLost(f"{stage}: {why}")


# ============================================================================
# CENSIMENTO ID GIA' PRESENTI (sqlite READ-ONLY)
# ============================================================================
def ro_connect() -> sqlite3.Connection:
    """Sola lettura COORDINATA con i writer (lezione P6).

    `mode=ro` e NON `immutable=1`: il censimento gira a KB viva (il MCP in
    KeepAlive puo' scrivere in qualsiasi istante, e il dry-run per contratto
    non richiede maintenance mode). `immutable=1` salterebbe lock e journal e
    su un file scrivibile in journal_mode=delete produrrebbe torn read /
    SQLITE_CORRUPT — cioe' un censimento di skip SILENZIOSAMENTE sbagliato, che
    qui significa ri-scrivere o saltare email. `mode=ro` prende lock condivisi.
    busy_timeout: attende invece di errorare se un commit MCP tiene EXCLUSIVE
    nell'istante della lettura. Riferimento: reconcile_lexical_index.py:295-307.
    """
    conn = sqlite3.connect(f"file:{SQLITE_PATH}?mode=ro", uri=True)
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def load_existing_email_ids() -> tuple[set[str], set[str]]:
    """(fonte_a, fonte_b) — vedi docstring, criterio di skip = unione."""
    con = ro_connect()
    try:
        src_a = {r[0] for r in con.execute("""
            SELECT m.string_value
            FROM embedding_metadata m
            JOIN embedding_metadata s
              ON s.id = m.id AND s.key = 'source' AND s.string_value = 'email'
            WHERE m.key = 'email_id' AND m.string_value IS NOT NULL
        """)}
        src_b = set()
        for (eid,) in con.execute("SELECT embedding_id FROM embeddings"):
            m = RAW_EMAIL_EID.match(eid or "")
            if m:
                src_b.add(m.group(1))
    finally:
        con.close()
    return src_a, src_b


def load_checkpoint() -> set[str]:
    if not os.path.exists(CHECKPOINT_PATH):
        return set()
    with open(CHECKPOINT_PATH) as f:
        return {l.strip() for l in f if l.strip()}


# ============================================================================
# SCANSIONE SORGENTE
# ============================================================================
def iter_source(path: str):
    """Streaming del jsonl (618MB: mai caricato in memoria)."""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def scan(src_a: set[str], src_b: set[str], done: set[str],
         limit_emails: int | None = None):
    """Percorre la sorgente e produce il piano di backfill.

    Ritorna (plan, stats). `plan` = lista di (email_id, chunks) con i chunk
    EFFETTIVI post strip+chunking (non stime: si chiama davvero chunk_email).
    """
    plan = []
    st = {
        "righe": 0, "non_useful_o_filtrate": 0,
        "skip_gia_presenti": 0, "skip_solo_a": 0, "skip_solo_b": 0,
        "skip_entrambe": 0, "skip_checkpoint": 0,
        "vuote_dopo_strip": 0, "duplicate_in_sorgente": 0,
        "nuove": 0, "chunk": 0, "chars": 0,
    }
    seen_in_source = set()
    for email in iter_source(SOURCE_JSONL):
        st["righe"] += 1
        if not should_keep(email):
            st["non_useful_o_filtrate"] += 1
            continue
        eid = email.get("id") or ""
        if not eid:
            st["non_useful_o_filtrate"] += 1
            continue
        if eid in seen_in_source:
            st["duplicate_in_sorgente"] += 1
            continue
        seen_in_source.add(eid)

        in_a, in_b = eid in src_a, eid in src_b
        if in_a or in_b:
            st["skip_gia_presenti"] += 1
            if in_a and in_b:
                st["skip_entrambe"] += 1
            elif in_a:
                st["skip_solo_a"] += 1
            else:
                st["skip_solo_b"] += 1
            continue
        if eid in done:
            st["skip_checkpoint"] += 1
            continue

        stripped = strip_quoted(email.get("body", "") or "")
        if not stripped.strip():
            st["vuote_dopo_strip"] += 1
            continue
        chunks = chunk_email(email, stripped)
        if not chunks:
            st["vuote_dopo_strip"] += 1
            continue

        st["nuove"] += 1
        st["chunk"] += len(chunks)
        st["chars"] += sum(len(c["text"]) for c in chunks)
        plan.append((eid, chunks))
        if limit_emails and len(plan) >= limit_emails:
            break
    return plan, st


# ============================================================================
# DRY-RUN
# ============================================================================
def probe_embed_rate(plan, n: int) -> float | None:
    """Misura il rate reale di embedding su n chunk (solo Ollama, zero Chroma)."""
    texts = []
    for _eid, chunks in plan:
        for c in chunks:
            texts.append(embedding_text("email", c["metadata"], c["text"]))
            if len(texts) >= n:
                break
        if len(texts) >= n:
            break
    if not texts:
        return None
    t0 = time.time()
    for i in range(0, len(texts), BATCH):
        get_embeddings(texts[i:i + BATCH])
    el = time.time() - t0
    return len(texts) / el if el else None


def dry_run(limit_emails: int | None, probe: int) -> int:
    print("=" * 78)
    print("BACKFILL EMAIL YOUNIVERSAL — DRY-RUN (READ-ONLY, zero scritture)")
    print("=" * 78)
    print(f"[src]   {SOURCE_JSONL}")
    if not os.path.exists(SOURCE_JSONL):
        sys.stderr.write(f"\nERRORE: sorgente inesistente: {SOURCE_JSONL}\n")
        return 2
    print(f"        {os.path.getsize(SOURCE_JSONL) / 1024 / 1024:.0f} MB")
    print(f"[embed] {embedding_info()}")

    t0 = time.time()
    src_a, src_b = load_existing_email_ids()
    union = src_a | src_b
    print(f"[skip]  fonte a (metadata email_id, source=email) : {len(src_a):,}")
    print(f"[skip]  fonte b (embedding_id ^email_..._chunk_\\d4): {len(src_b):,}")
    print(f"[skip]  unione (criterio effettivo)               : {len(union):,}"
          f"   [delta b\\a={len(src_b - src_a):,}  a\\b={len(src_a - src_b):,}]")

    done = load_checkpoint()
    if done:
        print(f"[ckpt]  {len(done):,} email gia' a checkpoint ({CHECKPOINT_PATH})")

    print("\n[scan]  scansione sorgente + chunking reale...", flush=True)
    plan, st = scan(src_a, src_b, done, limit_emails)
    scan_el = time.time() - t0

    print("\n" + "=" * 78)
    print("REPORT")
    print("=" * 78)
    print(f"righe sorgente               : {st['righe']:,}")
    print(f"scartate (useful/filtro)     : {st['non_useful_o_filtrate']:,}")
    print(f"duplicate nella sorgente     : {st['duplicate_in_sorgente']:,}")
    print(f"SKIP gia' in KB              : {st['skip_gia_presenti']:,}")
    print(f"   di cui in ENTRAMBE le fonti : {st['skip_entrambe']:,}")
    print(f"   di cui SOLO fonte a         : {st['skip_solo_a']:,}")
    print(f"   di cui SOLO fonte b         : {st['skip_solo_b']:,}")
    print(f"SKIP da checkpoint           : {st['skip_checkpoint']:,}")
    print(f"vuote dopo strip (no chunk)  : {st['vuote_dopo_strip']:,}")
    print(f"\nMESSAGGI NUOVI da backfillare: {st['nuove']:,}")
    print(f"CHUNK EFFETTIVI (post strip+chunking, non stimati): {st['chunk']:,}")
    print(f"CHARS totali                 : {st['chars']:,}")
    if st["nuove"]:
        print(f"media chunk/email            : {st['chunk'] / st['nuove']:.2f}")
        print(f"media chars/chunk            : {st['chars'] / max(1, st['chunk']):.0f}")
    print(f"\ntempo scansione+chunking     : {scan_el / 60:.1f} min")

    rate = None
    if probe and plan:
        print(f"\n[probe] misuro il rate di embedding su {probe} chunk reali "
              f"(solo Ollama, nessuna scrittura)...", flush=True)
        rate = probe_embed_rate(plan, probe)
        if rate:
            print(f"[probe] rate misurato: {rate:.1f} chunk/s")
    if rate is None:
        rate = ASSUMED_EMBED_RATE
        print(f"\n[stima] rate assunto: {rate:.1f} chunk/s "
              f"(usa --probe-embed N per misurarlo)")
    print(f"STIMA TEMPO EMBED+UPSERT     : {st['chunk'] / rate / 60:.1f} min "
          f"({st['chunk']:,} chunk / {rate:.1f} chunk/s)")

    print("\n" + "-" * 78)
    print("PRIMI 3 MESSAGGI DEL PIANO")
    print("-" * 78)
    for eid, chunks in plan[:3]:
        m = chunks[0]["metadata"]
        print(f"\n• email_id={eid}  chunk={len(chunks)}  date={m.get('date')}")
        print(f"  subject: {str(m.get('title'))[:70]!r}")
        print(f"  from   : {str(m.get('email_from'))[:60]!r}")
        print(f"  id primo chunk: {chunks[0]['chunk_id']}")
        print(f"  testo  : {chunks[0]['text'][:150]!r}")

    ok, why = maintenance_state()
    print(f"\n[gate]  maintenance mode adesso: {'OK' if ok else 'NO'} — {why}")
    print("[dry-run] nessuna scrittura. Fine.")
    return 0


# ============================================================================
# EXECUTE
# ============================================================================
def execute(limit_emails: int | None) -> int:
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

    if not os.path.exists(SOURCE_JSONL):
        sys.stderr.write(f"\nERRORE: sorgente inesistente: {SOURCE_JSONL}\n")
        return 2

    os.makedirs(LOGS_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(LOGS_DIR, f"backfill_email_youniversal_{ts}.log")
    logf = open(log_path, "a")

    def log(msg: str) -> None:
        print(msg, flush=True)
        logf.write(msg + "\n")
        logf.flush()

    log(f"[execute] start {ts}  log={log_path}")
    log(f"[embed]   {embedding_info()}")

    src_a, src_b = load_existing_email_ids()
    done = load_checkpoint()
    log(f"[skip]    fonte a={len(src_a):,} fonte b={len(src_b):,} "
        f"unione={len(src_a | src_b):,}")
    log(f"[ckpt]    {len(done):,} email gia' fatte -> resume")

    log("[scan]    scansione sorgente + chunking...")
    plan, st = scan(src_a, src_b, done, limit_emails)
    log(f"[scan]    {st['nuove']:,} email nuove, {st['chunk']:,} chunk da scrivere")
    if not plan:
        log("[execute] niente da fare.")
        logf.close()
        return 0

    import chromadb
    client = chromadb.PersistentClient(path=DB_DIR)
    coll = client.get_collection(COLLECTION_NAME)

    ckpt = open(CHECKPOINT_PATH, "a")
    emails_ok = chunks_ok = 0
    t0 = time.time()

    def flush(buf_chunks, buf_emails) -> None:
        """Upsert dei chunk bufferizzati, poi checkpoint delle email COMPLETE.

        Il buffer contiene solo email INTERE (vedi loop): ogni email in
        buf_emails ha qui tutti i suoi chunk, quindi il checkpoint scritto dopo
        l'upsert riuscito rispetta l'invariante "mai per-chunk".
        """
        nonlocal emails_ok, chunks_ok
        for i in range(0, len(buf_chunks), BATCH):
            sub = buf_chunks[i:i + BATCH]
            texts = [embedding_text("email", c["metadata"], c["text"]) for c in sub]
            vecs = get_embeddings(texts)                      # fail-loud
            metas = []
            for c in sub:
                m = dict(c["metadata"])
                m["entities_extracted"] = 0    # enrichment fuori scope: lo
                metas.append(m)                # raccogliera' extract_entities.py
            _assert_maintenance(f"prima di coll.upsert ({len(sub)} chunk)")
            coll.upsert(
                ids=[c["chunk_id"] for c in sub],
                embeddings=vecs,
                documents=[c["text"] for c in sub],
                metadatas=metas,
            )                                                 # fail-loud
            chunks_ok += len(sub)
        for eid in buf_emails:
            ckpt.write(eid + "\n")
        ckpt.flush()
        os.fsync(ckpt.fileno())
        emails_ok += len(buf_emails)
        el = time.time() - t0
        log(f"  ...email={emails_ok}/{len(plan)} chunk={chunks_ok}/{st['chunk']} "
            f"{chunks_ok / el:.1f} chunk/s "
            f"ETA {(st['chunk'] - chunks_ok) / max(0.1, chunks_ok / el) / 60:.1f}m")

    try:
        buf_chunks: list[dict] = []
        buf_emails: list[str] = []
        for eid, chunks in plan:
            # mai spezzare un'email tra due flush: se non ci sta, si flusha
            # prima. Cosi' ogni flush contiene solo email intere e il
            # checkpoint per-email resta corretto per costruzione.
            if buf_chunks and len(buf_chunks) + len(chunks) > BATCH:
                flush(buf_chunks, buf_emails)
                buf_chunks, buf_emails = [], []
            buf_chunks.extend(chunks)
            buf_emails.append(eid)
            if len(buf_chunks) >= BATCH:
                flush(buf_chunks, buf_emails)
                buf_chunks, buf_emails = [], []
        if buf_chunks:
            flush(buf_chunks, buf_emails)
    except MaintenanceLost as e:
        sys.stderr.write(
            f"\nABORT: maintenance mode persa ({e}).\n"
            f"  lock atteso in {MAINTENANCE_LOCK}\n"
            f"  fatti fin qui: email={emails_ok} chunk={chunks_ok}\n"
            f"  checkpoint: {CHECKPOINT_PATH}\n"
            f"  rientra in maintenance (kill -USR1 <mcp_pid>) e rilancia lo\n"
            f"  stesso comando: il resume riparte dall'ultima email completa.\n")
        log(f"[ABORT] maintenance persa: {e} — email={emails_ok} chunk={chunks_ok}")
        return 3
    finally:
        ckpt.close()
        logf.close()

    print(f"\n[execute] SUMMARY email={emails_ok} chunk={chunks_ok}")
    print(f"[execute] log: {log_path}")
    print("[execute] enrichment: entities_extracted=0 -> lo raccogliera'"
          " extract_entities.py")
    print("[execute] persist durevole: kickstart -k del MCP (operatore).")
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
                   help="backfill (richiede maintenance mode)")
    ap.add_argument("--limit-emails", type=int, default=None,
                    help="processa al massimo N email nuove (test/smoke)")
    ap.add_argument("--probe-embed", type=int, default=0, metavar="N",
                    help="dry-run: misura il rate reale embeddando N chunk "
                         "(solo Ollama, nessuna scrittura)")
    args = ap.parse_args()

    if args.execute:
        return execute(args.limit_emails)
    return dry_run(args.limit_emails, args.probe_embed)


if __name__ == "__main__":
    sys.exit(main())
