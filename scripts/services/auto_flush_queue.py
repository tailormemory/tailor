#!/usr/bin/env python3
"""
TAILOR — Auto-flush del backlog queue HNSW (chromadb #6975).

Esegue NON-PRESIDIATO la procedura 5-step del runbook RUNBOOK_drift_alert.md
quando il backlog #6975 è benigno e azionabile, eliminando l'intervento manuale
dell'operatore (5 incidenti in 9 giorni → flush manuale ripetuto).

Gate di azionabilità (read-only, da `repair_hnsw_index.py --audit --json`):
    queue_total >= QUEUE_MIN (default 300)  AND  orphans == 0  AND  ghosts == 0
    AND unknown == 0
Solo questo è il caso #6975 PURO (backlog benigno nel WAL, nessuna perdita dati).
Con orphans/ghosts/unknown > 0 NON si auto-remedia: si manda Telegram di
escalation e ci si ferma (vedi tabella verdetti del runbook).

Procedura (try/finally — maintenance OFF garantito anche su eccezione):
    1. backup_db.sh                      (precondizione del tool flush: <60 min)
    2. maintenance ON   → kill -USR1 MCP (NO sudo: MCP gira come jarvis)
    3. flush            → repair_hnsw_index.py --flush-queue-backlog --json
    4. maintenance OFF  → kill -USR2 MCP (NO sudo) — SEMPRE, in finally
    5. restart MCP      → sudo -n launchctl unload+load (grant passwordless
                          /etc/sudoers.d/jarvis-mcp; unload = SIGTERM graceful)

Niente sudo interattivo: maintenance via segnali a processo same-user, restart
via grant NOPASSWD già esistente. Restart eseguito SEMPRE dopo essere entrati in
maintenance, così MCP torna a uno stato pulito (evita il rischio _collection=None
→ NoneType.upsert lasciato da una sola SIGUSR2).

Telegram a inizio (START) e fine (SUCCESS/FAIL/ESCALATION). Lock fcntl per
evitare run concorrenti. Log dedicato in logs/auto_flush.log. Skip silenzioso se
`maintenance.lock` è già presente (un'altra operazione è in corso). La race
residua con altre manutenzioni non coordinate è mitigata dagli orari fissi,
deliberatamente non sovrapposti al nightly.

Schedulazione: LaunchDaemon com.tailor.auto-flush-queue alle 08:00, 14:00 e
20:00, fuori dalla finestra nightly delle 02:00 (vedi plist proposto).

Override soglia: env AUTO_FLUSH_QUEUE_MIN, o config maintenance.auto_flush_queue_min.
Flag: --dry-run (valuta il gate, non muta), --force-start-telegram (smoke test
notifica), override path per i test.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from typing import NamedTuple

import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "lib"))
from env_loader import load_env  # noqa: E402
from telegram_notify import redact  # noqa: E402

load_env()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
VENV_PY = os.path.join(BASE_DIR, ".venv", "bin", "python3")
REPAIR_TOOL = os.path.join("scripts", "maintenance", "repair_hnsw_index.py")
BACKUP_SH = os.path.join("scripts", "maintenance", "backup_db.sh")
LOG_PATH = os.path.join(BASE_DIR, "logs", "auto_flush.log")
MAINTENANCE_LOCK = os.path.join(BASE_DIR, "maintenance.lock")
RUN_LOCK = "/tmp/tailor_auto_flush.flock"

# Cache dell'esito audit del gate, consumata da queue_depth_monitor per il segnale
# STUCK senza rieffettuare l'audit a ogni run (vedi write_audit_cache). Persistita a
# ogni run del gate (08/14/20), quindi sempre fresca entro la cadenza del gate.
AUDIT_CACHE_PATH = os.path.join(BASE_DIR, "logs", "gate_audit_cache.json")

# Restart MCP — deve combaciare ESATTAMENTE col grant in /etc/sudoers.d/jarvis-mcp:
#   jarvis ALL=(root) NOPASSWD: /bin/launchctl unload <plist>, /bin/launchctl load <plist>
LAUNCHCTL = "/bin/launchctl"
MCP_PLIST = "/Library/LaunchDaemons/com.tailor.mcp.plist"

# Gate: backlog azionabile (default = FLUSH_QUEUE_BACKLOG_MIN del runbook).
DEFAULT_QUEUE_MIN = 300

# Backup
BACKUPS_DIR = os.path.join(BASE_DIR, "backups")
BACKUP_FRESH_MAX_S = 120      # il backup verificato deve essere di QUESTO run (mtime recente)

# Tempi di attesa
MAINTENANCE_LOCK_WAIT_S = 12   # quanto attendere la comparsa/scomparsa di maintenance.lock
MCP_SETTLE_S = 3              # pausa dopo unload prima di load
MCP_HEALTH_TIMEOUT_S = 90     # cold start HNSW: polling PID nuovo + HTTP 200 post-load
MCP_HEALTH_POLL_S = 2
MCP_HEALTH_URL = "http://127.0.0.1:8787/api/auth/check"

TS_FMT = "%Y-%m-%d %H:%M:%S"

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "")


# ─────────────────────────────── logging ────────────────────────────────────

def log(level: str, message: str) -> None:
    ts = datetime.now().strftime(TS_FMT)
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    line = f"[{ts}] {level}: {message}"
    with open(LOG_PATH, "a") as f:
        f.write(line + "\n")
    print(line, flush=True)


# ─────────────────────────────── telegram ───────────────────────────────────

def send_telegram(text: str) -> bool:
    """Best-effort. Markdown con fallback a plain text su parse-error (come
    queue_depth_monitor). Un fallimento Telegram NON deve abortire la procedura."""
    if not TG_TOKEN or not TG_CHAT:
        log("WARN", "Telegram skipped: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID empty")
        return False
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    body = {"chat_id": TG_CHAT, "text": text, "parse_mode": "Markdown"}
    try:
        resp = requests.post(url, json=body, timeout=15)
    except Exception as e:
        log("WARN", redact(f"Telegram POST raised: {type(e).__name__}: {e}", TG_TOKEN))
        return False
    if resp.status_code == 200:
        return True
    if resp.status_code == 400 and "can't parse" in resp.text.lower():
        body.pop("parse_mode", None)
        try:
            resp = requests.post(url, json=body, timeout=15)
        except Exception as e:
            log("WARN", redact(f"Telegram retry POST raised: {type(e).__name__}: {e}", TG_TOKEN))
            return False
        if resp.status_code == 200:
            return True
    log("WARN", f"Telegram non-200: status={resp.status_code} "
        f"body={redact(resp.text, TG_TOKEN)[:200]}")
    return False


# ──────────────────────────── helpers / subprocess ──────────────────────────

def queue_min() -> int:
    """Soglia gate: env AUTO_FLUSH_QUEUE_MIN > config maintenance.auto_flush_queue_min > default."""
    env_val = os.environ.get("AUTO_FLUSH_QUEUE_MIN")
    if env_val:
        try:
            return int(env_val)
        except ValueError:
            log("WARN", f"AUTO_FLUSH_QUEUE_MIN non intero ({env_val!r}), uso config/default")
    try:
        sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "lib"))
        from config import get as cfg  # noqa: E402
        v = cfg("maintenance", "auto_flush_queue_min", DEFAULT_QUEUE_MIN)
        return int(v)
    except Exception:
        return DEFAULT_QUEUE_MIN


def _extract_json_objects(text: str) -> list[dict]:
    """Estrae gli oggetti JSON top-level concatenati in stdout.

    `repair_hnsw_index.py --flush-queue-backlog --json` stampa DUE oggetti:
    prima il blocco audit, poi il blocco flush. Ritorniamo tutti in ordine;
    il chiamante prende quello rilevante (primo=audit, ultimo=flush)."""
    decoder = json.JSONDecoder()
    objs: list[dict] = []
    idx = 0
    n = len(text)
    while idx < n:
        while idx < n and text[idx] in " \t\r\n":
            idx += 1
        if idx >= n:
            break
        try:
            obj, end = decoder.raw_decode(text, idx)
        except json.JSONDecodeError:
            break
        if isinstance(obj, dict):
            objs.append(obj)
        idx = end
    return objs


def run_audit() -> dict | None:
    """`repair_hnsw_index.py --audit --json --no-history` → dict audit (read-only)."""
    cmd = [VENV_PY, REPAIR_TOOL, "--audit", "--json", "--no-history"]
    try:
        proc = subprocess.run(cmd, cwd=BASE_DIR, capture_output=True, text=True, timeout=180)
    except Exception as e:
        log("ERROR", f"audit subprocess raised: {type(e).__name__}: {e}")
        return None
    objs = _extract_json_objects(proc.stdout)
    if not objs:
        log("ERROR", f"audit: nessun JSON in stdout (rc={proc.returncode}) stderr={proc.stderr[:200]}")
        return None
    return objs[0]


def write_audit_cache(audit: dict, flush: dict | None = None, path: str | None = None) -> bool:
    """Persiste l'esito del gate per queue_depth_monitor (segnali STUCK queue e STUCK_VECTOR).

    Il monitor (ogni 30 min) NON rieffettua l'audit: legge questa cache. Il gate
    gira 08/14/20 e produce SEMPRE un audit (anche su skip/escalate/dry-run), quindi
    la cache è fresca entro la cadenza del gate.

    Due chiamate per run del gate:
      * pre-flush (main, flush=None): stato dall'audit (collection_pending, queue_total,
        vector_seq, drift), flush_ran=False — sufficiente per il segnale queue STUCK.
      * post-flush (run_procedure, flush=flush_json): sovrascrive con lo stato POST
        mutazione + esito, così il monitor sa se un flush è girato e se ha RISOLTO.
        `flush_resolved` = il contratto `success` del tool (che richiede già drift
        post < DRIFT_WARNING): è la firma che separa #6975 reale (flush girato ma
        drift NON sceso → resolved=False) da transitorio benigno (resolved=True).

    Scrittura atomica (tmp+replace). Best-effort: un fallimento qui NON aborta il gate.
    """
    if path is None:
        path = AUDIT_CACHE_PATH
    try:
        post = (flush or {}).get("post") or {}
        flush_ran = isinstance(flush, dict) and isinstance(flush.get("post"), dict)
        if flush_ran:
            vector_seq = post.get("vector_seq", audit.get("vector_seq"))
            drift = post.get("drift", audit.get("drift"))
            cqp = flush.get("post_collection_queue_pending_count",
                            audit.get("collection_queue_pending_count"))
            qt = flush.get("post_queue_total", audit.get("queue_total"))
            flush_resolved = bool(flush.get("success"))
        else:
            vector_seq = audit.get("vector_seq")
            drift = audit.get("drift")
            cqp = audit.get("collection_queue_pending_count")
            qt = audit.get("queue_total")
            flush_resolved = None
        payload = {
            "ts": datetime.now().strftime(TS_FMT),
            "ts_unix": int(time.time()),
            "collection_name": audit.get("collection_name"),
            "collection_queue_pending_count": cqp,
            "queue_total": qt,
            "vector_seq": vector_seq,
            "drift": drift,
            "flush_ran": flush_ran,
            "flush_resolved": flush_resolved,
        }
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(payload, f)
        os.replace(tmp, path)
        return True
    except Exception as e:
        log("WARN", f"write_audit_cache fallito: {type(e).__name__}: {e}")
        return False


def run_backup() -> bool:
    cmd = ["/bin/bash", BACKUP_SH]
    try:
        proc = subprocess.run(cmd, cwd=BASE_DIR, capture_output=True, text=True, timeout=300)
    except Exception as e:
        log("ERROR", f"backup subprocess raised: {type(e).__name__}: {e}")
        return False
    if proc.returncode != 0:
        log("ERROR", f"backup_db.sh rc={proc.returncode} stderr={proc.stderr[:300]}")
        return False
    log("INFO", "backup_db.sh rc=0")
    return True


def _newest_fresh_backup(max_age_s: int) -> tuple[str, float] | None:
    """Path + età (s) del backup tailor_db_full_*.tar.gz più recente in backups/,
    solo se mtime entro max_age_s (deve essere di QUESTO run). None altrimenti."""
    if not os.path.isdir(BACKUPS_DIR):
        return None
    best: tuple[float, str] | None = None
    for fname in os.listdir(BACKUPS_DIR):
        if fname.startswith("tailor_db_full_") and fname.endswith(".tar.gz"):
            full = os.path.join(BACKUPS_DIR, fname)
            try:
                mt = os.path.getmtime(full)
            except OSError:
                continue
            if best is None or mt > best[0]:
                best = (mt, full)
    if best is None:
        return None
    age = time.time() - best[0]
    if age > max_age_s:
        return None
    return best[1], age


def verify_backup() -> bool:
    """backup_db.sh può ritornare 0 anche con un .gz troncato. Verifica l'integrità
    del backup fresco con `gzip -t` prima di toccare ChromaDB."""
    fresh = _newest_fresh_backup(BACKUP_FRESH_MAX_S)
    if fresh is None:
        log("ERROR", f"nessun backup tailor_db_full_*.tar.gz con mtime < {BACKUP_FRESH_MAX_S}s in {BACKUPS_DIR}")
        return False
    path, age = fresh
    try:
        proc = subprocess.run(["/usr/bin/gzip", "-t", path],
                              capture_output=True, text=True, timeout=120)
    except Exception as e:
        log("ERROR", f"gzip -t raised: {type(e).__name__}: {e}")
        return False
    if proc.returncode != 0:
        log("ERROR", f"gzip -t FALLITO su {os.path.basename(path)}: {proc.stderr[:200]} — backup corrotto")
        return False
    log("INFO", f"backup verificato OK: {os.path.basename(path)} ({age:.0f}s fa, gzip -t passato)")
    return True


def find_mcp_pid() -> int | None:
    try:
        proc = subprocess.run(["/usr/bin/pgrep", "-f", "mcp_server.py"],
                              capture_output=True, text=True, timeout=10)
    except Exception as e:
        log("ERROR", f"pgrep raised: {type(e).__name__}: {e}")
        return None
    pids = [int(x) for x in proc.stdout.split() if x.strip().isdigit()]
    if not pids:
        return None
    if len(pids) > 1:
        log("WARN", f"pgrep mcp_server.py ha trovato PID multipli: {pids} — abort automatico")
        return None
    return pids[0]


def _wait_lock(present: bool, timeout_s: int) -> bool:
    """Attende che maintenance.lock esista (present=True) o sparisca (present=False)."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if os.path.exists(MAINTENANCE_LOCK) == present:
            return True
        time.sleep(1)
    return os.path.exists(MAINTENANCE_LOCK) == present


def signal_maintenance_on(mcp_pid: int) -> bool:
    """Invia SIGUSR1. Ritorna True se il SEGNALE è partito (NON se il lock è
    comparso). Separare i due è critico: se USR1 parte, il chiamante DEVE
    comunque garantire USR2 in finally, anche se il lock non si materializza."""
    log("INFO", f"maintenance ON → kill -USR1 {mcp_pid}")
    try:
        os.kill(mcp_pid, __import__("signal").SIGUSR1)
    except Exception as e:
        log("ERROR", f"kill -USR1 raised: {type(e).__name__}: {e}")
        return False
    return True


def exit_maintenance(mcp_pid: int) -> bool:
    """Invia SIGUSR2 e attende che MCP rimuova maintenance.lock.

    Non rimuove mai il lock manualmente: nella race residua accettata potrebbe
    appartenere a un'altra procedura di manutenzione."""
    log("INFO", f"maintenance OFF → kill -USR2 {mcp_pid}")
    try:
        os.kill(mcp_pid, __import__("signal").SIGUSR2)
    except Exception as e:
        log("ERROR", f"kill -USR2 raised: {type(e).__name__}: {e}")
    ok = _wait_lock(present=False, timeout_s=MAINTENANCE_LOCK_WAIT_S)
    if not ok:
        log("WARN", "maintenance.lock ancora presente dopo USR2 — lasciato intatto")
    return ok


def run_flush() -> tuple[int, dict | None]:
    """Esegue il flush. Ritorna (returncode, flush_json|None).
    rc: 0 success/skipped, 2 not-in-maint, 3 no-backup, 4 not-clean/empty, 5 criteria-non-met."""
    cmd = [VENV_PY, REPAIR_TOOL, "--flush-queue-backlog", "--json",
           "--queue-backlog-min", str(queue_min())]
    try:
        proc = subprocess.run(cmd, cwd=BASE_DIR, capture_output=True, text=True, timeout=600)
    except Exception as e:
        log("ERROR", f"flush subprocess raised: {type(e).__name__}: {e}")
        return 1, None
    objs = _extract_json_objects(proc.stdout)
    flush_json = objs[-1] if objs else None
    if proc.stderr.strip():
        log("INFO", f"flush stderr: {proc.stderr[:300]}")
    return proc.returncode, flush_json


def restart_mcp(old_pid: int | None) -> str:
    """Restart graceful via grant NOPASSWD (unload=SIGTERM, attende shutdown pulito; poi load).
    sudo -n: se il grant manca fallisce subito invece di bloccarsi su prompt password.

    Ritorna: healthy, unload_failed, MCP-DOWN, health_timeout."""
    action_ok: dict[str, bool] = {}
    for action in ("unload", "load"):
        cmd = ["sudo", "-n", LAUNCHCTL, action, MCP_PLIST]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        except Exception as e:
            log("ERROR", f"launchctl {action} raised: {type(e).__name__}: {e}")
            action_ok[action] = False
            proc = None
        else:
            action_ok[action] = proc.returncode == 0
        if not action_ok[action]:
            rc = proc.returncode if proc is not None else "raise"
            stderr = proc.stderr[:200] if proc is not None else ""
            log("ERROR", f"sudo -n launchctl {action} rc={rc} "
                         f"stderr={stderr} — grant sudoers mancante?")
        else:
            log("INFO", f"launchctl {action} rc=0")
        if action == "unload":
            time.sleep(MCP_SETTLE_S)

    if not action_ok.get("load"):
        state = "MCP-DOWN" if action_ok.get("unload") else "unload_failed"
        log("ERROR", f"restart MCP stato={state}: load fallito")
        return state

    deadline = time.monotonic() + MCP_HEALTH_TIMEOUT_S
    while time.monotonic() < deadline:
        new_pid = find_mcp_pid()
        pid_ok = new_pid is not None and (old_pid is None or new_pid != old_pid)
        http_ok = False
        if pid_ok:
            try:
                http_ok = requests.get(MCP_HEALTH_URL, timeout=2).status_code == 200
            except Exception:
                pass
        if pid_ok and http_ok:
            if not action_ok.get("unload"):
                log("ERROR", f"MCP risponde con PID {new_pid}, ma unload era fallito")
                return "unload_failed"
            log("INFO", f"MCP ripartito sano: PID {old_pid} → {new_pid}, HTTP 200")
            return "healthy"
        time.sleep(MCP_HEALTH_POLL_S)

    log("ERROR", "post-restart health timeout: richiesti PID nuovo + HTTP 200")
    return "health_timeout"


# ──────────────────────────────── gate logic ────────────────────────────────


class GhostCounts(NamedTuple):
    anomalous: int
    benign: int
    blocking_unknown: int
    legacy: bool          # schema vecchio senza info benignità (conservativo)
    schema_invalid: bool  # JSON incoerente/malformato (conservativo, FORZA escalate)


def _nonneg_int(x) -> int | None:
    """int >= 0, oppure None se non convertibile/negativo. Non solleva mai."""
    try:
        v = int(x)
    except (TypeError, ValueError):
        return None
    return v if v >= 0 else None


def _ghost_counts(audit: dict) -> GhostCounts:
    """Conteggi ghost robusti a JSON incoerente.

    Tre esiti conservativi distinti:
      * schema_invalid=True → JSON malformato/incoerente → gate FORZA escalate.
      * legacy=True         → schema vecchio senza benignità → tutti anomali.
      * altrimenti          → conteggi affidabili.
    Nessun int() solleva: valore non numerico → schema_invalid, non eccezione.
    """
    def invalid() -> GhostCounts:
        # valori non-fuorvianti che escalerebbero anche se il flag fosse ignorato
        # (difesa in profondità: anomalous=1>0).
        return GhostCounts(1, 0, 1, legacy=False, schema_invalid=True)

    raw = audit.get("hnsw_only_ghosts", None)
    if not isinstance(raw, list):                      # assente o non-lista
        return invalid()
    total = len(raw)
    if any(not isinstance(g, dict) for g in raw):      # elementi non-dict
        return invalid()

    # flag per-ghost: o su TUTTI o su NESSUNO
    flags = [("benign" in g) for g in raw]
    if any(flags) and not all(flags):                  # misti
        return invalid()
    per_ghost = sum(1 for g in raw if not g.get("benign")) if (total and all(flags)) else None

    # aggregato anomalous: se presente dev'essere valido, in range, coerente
    agg = None
    if "anomalous_ghost_count" in audit:
        agg = _nonneg_int(audit["anomalous_ghost_count"])
        if agg is None or agg > total:
            return invalid()
        if per_ghost is not None and agg != per_ghost:
            return invalid()

    # blocking_unknown: se presente dev'essere valido; se assente → default
    # CONSERVATIVO = unknown_count totale (NON 0).
    if "blocking_unknown_count" in audit:
        bu = _nonneg_int(audit["blocking_unknown_count"])
        if bu is None:
            return invalid()
    else:
        bu = _nonneg_int(audit.get("unknown_count", 0))
        if bu is None:
            return invalid()

    if agg is not None:
        anomalous, legacy = agg, False
    elif per_ghost is not None:
        anomalous, legacy = per_ghost, False
    else:
        anomalous, legacy = total, True                # vecchio schema → conservativo
    benign = total - anomalous
    if anomalous < 0 or benign < 0 or anomalous + benign != total:
        return invalid()
    return GhostCounts(anomalous, benign, bu, legacy, schema_invalid=False)


def evaluate_gate(audit: dict, qmin: int) -> tuple[str, str]:
    """Funzione pura — decide l'azione dal JSON di audit.

    Ritorna (action, reason):
      "act"      → #6975 puro (ghost benigni inclusi) azionabile per queue O drift
      "skip"     → niente da fare (queue sotto soglia E drift sotto warning/assente)
      "escalate" → drift anomalo / schema_invalid (NON auto-remediabile)

    Con indice pulito (no orphans/anomali/blocking_unknown, guard schema_invalid
    intatto) si AGISCE se la queue è sopra soglia OPPURE se il seq-drift è ≥
    drift_warning. La metrica queue è `collection_queue_pending_count` (backlog
    AZIONABILE per la collection sopra il watermark), NON `queue_total` (globale,
    multi-topic: un backlog di altri topic non rende flushabile questa collection).

    Missing-field a DUE rami distinti sulla metrica queue (input primario):
      * collection_queue_pending_count ASSENTE → deploy misto (tool vecchio) →
        fallback a queue_total, status-quo safe, niente escalate (WARN in main()).
      * collection_queue_pending_count PRESENTE ma malformato/negativo (_nonneg_int
        → None) → schema_invalid → escalate (valore corrotto della metrica
        primaria ≠ incompatibilità di versione).
    Per drift/drift_warning resta il degrado inerte (assenti → ramo drift spento,
    fallback queue-only). queue_total resta SEMPRE riportato (osservabilità globale).
    """
    gc = _ghost_counts(audit)
    queue_total = _nonneg_int(audit.get("queue_total", 0))
    orphans_raw = audit.get("sql_only_orphans", [])
    orphans = len(orphans_raw) if isinstance(orphans_raw, list) else None

    # Metrica queue primaria: collection_queue_pending_count. Assente → fallback
    # queue_total. Presente-ma-malformata → cqp=None → escalate (sotto).
    cqp_present = "collection_queue_pending_count" in audit
    cqp = _nonneg_int(audit["collection_queue_pending_count"]) if cqp_present else None

    # schema_invalid / campi base non interpretabili → escalate forzato.
    # cqp malformato escala SOLO se la chiave è presente (assente = deploy misto).
    if gc.schema_invalid or queue_total is None or orphans is None or (cqp_present and cqp is None):
        return "escalate", (f"schema_invalid: audit JSON incoerente "
                            f"(collection_queue_pending_count={audit.get('collection_queue_pending_count')!r}, "
                            f"queue_total={audit.get('queue_total')!r}, "
                            f"sql_only_orphans={type(orphans_raw).__name__}) → escalate conservativo")

    # Metrica decisionale: collection_pending se presente, altrimenti fallback globale.
    queue = cqp if cqp_present else queue_total
    qmetric = (f"collection_pending={cqp} queue_total={queue_total}" if cqp_present
               else f"collection_pending=n/d→fallback queue_total={queue_total}")

    leg = " [schema legacy: benignità sconosciuta, conservativo]" if gc.legacy else ""
    if orphans > 0 or gc.anomalous > 0 or gc.blocking_unknown > 0:
        return "escalate", (f"orphans={orphans} anomalous_ghosts={gc.anomalous} "
                            f"blocking_unknown={gc.blocking_unknown} "
                            f"(benign_ghosts={gc.benign} ignorati) {qmetric}{leg}")

    # Ramo drift (additivo). _nonneg_int su entrambi i campi: assente/malformato →
    # None → ramo inerte → fallback queue-only (status quo safe, no crash).
    drift = _nonneg_int(audit.get("drift"))
    drift_warning = _nonneg_int(audit.get("drift_warning"))
    drift_actionable = (drift is not None and drift_warning is not None
                        and drift >= drift_warning)
    drift_note = (f"drift={drift}/warning={drift_warning}"
                  if drift is not None and drift_warning is not None
                  else "drift=n/d (campo assente → gate queue-only)")

    queue_actionable = queue >= qmin
    if queue_actionable or drift_actionable:
        triggers = []
        if queue_actionable:
            triggers.append(f"collection_pending={queue}>=min={qmin}" if cqp_present
                            else f"queue_total={queue}>=min={qmin}")
        if drift_actionable:
            triggers.append(f"drift={drift}>=warning={drift_warning}")
        return "act", (f"{' & '.join(triggers)}; {qmetric} {drift_note}; indice pulito "
                       f"modulo {gc.benign} ghost benigni (#6975 puro){leg}")
    return "skip", (f"{qmetric}<min={qmin}, {drift_note}, nessun drift anomalo "
                    f"(benign_ghosts={gc.benign}){leg}")


# ───────────────────────────────── procedure ────────────────────────────────

# Rifiuti STRUTTURATI del tool (rc=4 con `error` noto): stati non auto-remediabili
# che il tool DIAGNOSTICA senza mutare. Un rifiuto allowlistato = il daemon ha
# funzionato → escalation gestita (return 0 se restart sano), NON FAIL generico.
# Validazione stretta: serve la COPPIA (rc=4, error ∈ allowlist), mai rc=4 da solo.
# NON allargare: ogni altro error / rc → FAIL generico (return 1).
# `unknown_drift_types` è apply-only oggi (il flush non lo emette): sola difesa futura.
# "collection_queue_empty" è l'erede di "queue_empty" dopo lo switch del gate a
# collection_queue_pending_count: entrambi restano allowlistati durante il rolling
# deploy (daemon nuovo + tool vecchio che emette ancora "queue_empty", o viceversa).
REFUSE_ALLOWLIST = frozenset({
    "ambiguous_collection_topic", "anomalous_drift", "queue_empty",
    "collection_queue_empty", "unknown_drift_types",
})


def _classify_flush(flush_json: dict | None) -> tuple[str, int | None]:
    """Disambigua `flush_queue_backlog.skipped` (overloaded):
      - dict con skipped is True (bool)  → ("gate_skip", None)    NO-OP (queue già bassa)
      - dict con skipped int             → ("flushed", <int>)     real flush, int = upsert saltati
      - assente/None                     → ("none", None)
    Il chiamante combina con flush_rc per l'esito finale."""
    fqb = (flush_json or {}).get("flush_queue_backlog")
    if not isinstance(fqb, dict):
        return "none", None
    sk = fqb.get("skipped")
    if isinstance(sk, bool) and sk is True:
        return "gate_skip", None
    if isinstance(sk, int):
        return "flushed", sk
    return "none", None


def run_procedure(audit: dict, dry_run: bool) -> int:
    """Esegue la 5-step. Ritorna exit code (0 = success pieno)."""
    qmin = queue_min()
    queue0 = int(audit.get("queue_total", 0))
    cqp0 = audit.get("collection_queue_pending_count")   # metrica decisionale (pre); None se tool vecchio

    if dry_run:
        log("DRY_RUN", f"agirei: 5-step flush, collection_pending={cqp0} queue_total={queue0} min={qmin}")
        print(f"DRY-RUN would run 5-step flush (collection_pending={cqp0}, queue_total={queue0}, min={qmin})")
        return 0

    gc = _ghost_counts(audit)
    ghost_note = (f"orphans=0, {gc.benign} ghost benigni ammessi (compattati dal flush)"
                  if gc.benign else "orphans=0, ghosts=0")
    send_telegram(
        "🧹 *TAILOR auto-flush — START*\n"
        f"Collection pending: `{cqp0}` (min {qmin}) · queue_total globale: `{queue0}`\n"
        f"Indice pulito ({ghost_note}) → flush #6975 automatico in corso."
    )

    mcp_pid = find_mcp_pid()
    if mcp_pid is None:
        log("ERROR", "MCP non identificato univocamente — abort, niente maintenance")
        send_telegram("❌ *TAILOR auto-flush — FAIL*\nMCP assente o con PID multipli: impossibile "
                      "identificarlo univocamente. Nessuna azione eseguita.")
        return 1

    # Backup PRIMA della maintenance (il tool flush rifiuta senza backup <60 min).
    # Verifica integrità con gzip -t: backup_db.sh può ritornare 0 con un .gz troncato.
    if not run_backup() or not verify_backup():
        send_telegram("❌ *TAILOR auto-flush — FAIL*\nBackup fallito o corrotto (`gzip -t`): "
                      "flush annullato (nessuna maintenance, nessuna mutazione).")
        return 1

    signaled = False
    maint_confirmed = False
    flush_rc, flush_json = 1, None
    try:
        # Re-check race: tra l'audit e ora può essere partita un'altra manutenzione
        # (es. sync_and_ingest notturno) che ha messo lei il maintenance.lock.
        if os.path.exists(MAINTENANCE_LOCK):
            log("ABORT", "maintenance.lock comparso tra audit e flush — altra operazione in corso")
            send_telegram("⏸️ *TAILOR auto-flush — ABORT*\nUn'altra manutenzione ha preso il "
                          "`maintenance.lock` mentre stavamo per agire. Nessun flush eseguito.")
            return 0
        if not signal_maintenance_on(mcp_pid):
            send_telegram("❌ *TAILOR auto-flush — FAIL*\nInvio SIGUSR1 a MCP fallito. "
                          "Nessun flush eseguito.")
            return 1
        signaled = True
        if _wait_lock(present=True, timeout_s=MAINTENANCE_LOCK_WAIT_S):
            maint_confirmed = True
            log("INFO", "maintenance.lock presente — ChromaDB rilasciato")
            try:
                flush_rc, flush_json = run_flush()
            except Exception as e:
                # run_flush cattura già internamente; questo è difesa-in-profondità
                # perché un raise qui NON deve saltare exit_maintenance + restart.
                log("ERROR", f"flush raised inatteso: {type(e).__name__}: {e}")
                flush_rc, flush_json = 1, None
        else:
            log("WARN", "USR1 inviato ma maintenance.lock non comparso — flush SALTATO (USR2 garantito)")
    finally:
        if signaled:
            exit_maintenance(mcp_pid)

    # Restart SEMPRE dopo aver segnalato la maintenance (stato MCP pulito,
    # evita _collection=None residuo → NoneType.upsert).
    restart_state = restart_mcp(mcp_pid) if signaled else "not_attempted"
    restarted = restart_state == "healthy"

    # Aggiorna la cache con lo stato POST-flush per queue_depth_monitor (STUCK_VECTOR
    # condizione C: "flush girato + drift non sceso"). Solo se un flush ha prodotto un
    # blocco `post` (flush_json con post): altrimenti la cache pre-flush di main resta
    # valida. Best-effort.
    if isinstance(flush_json, dict) and isinstance(flush_json.get("post"), dict):
        write_audit_cache(audit, flush=flush_json)

    # ── Esito ────────────────────────────────────────────────────────────────
    if not maint_confirmed:
        log("FAIL", "maintenance non confermata (lock non comparso) — flush non eseguito")
        send_telegram(
            "❌ *TAILOR auto-flush — FAIL*\nMaintenance ON non confermata (`maintenance.lock` "
            f"non comparso). Flush non eseguito. Restart MCP: `{restart_state}`."
        )
        return 1

    kind, skipped_n = _classify_flush(flush_json)
    success = (flush_rc == 0)
    post_queue = (flush_json or {}).get("post_queue_total")
    post_cqp = (flush_json or {}).get("post_collection_queue_pending_count")
    flush_trigger = (flush_json or {}).get("flush_trigger")
    pre = (flush_json or {}).get("pre", {}) or {}
    post = (flush_json or {}).get("post", {}) or {}
    # Quintupla di strumentazione (ogni esito flush): queue_total / collection_pending
    # / drift / trigger / valori pre+post — per il forensic in logs/auto_flush.log.
    metrics5 = (f"queue_total={queue0}->{post_queue} collection_pending={cqp0}->{post_cqp} "
                f"drift={pre.get('drift')}->{post.get('drift')} trigger={flush_trigger}")

    # ── Rifiuto strutturato del tool (rc=4 + error allowlisted) ───────────────
    # Stato non auto-remediabile diagnosticato dal tool, NESSUNA mutazione: il
    # daemon ha fatto il suo lavoro. Escalation gestita (return 0 se restart sano,
    # coerente col return dell'escalation del gate iniziale — NON significa "sano",
    # significa "gestito + alert inviato"). Un restart FALLITO prevale comunque:
    # l'MCP post-SIGUSR1 ha _collection=None e DEVE ripartire pulito.
    refuse_err = flush_json.get("error") if isinstance(flush_json, dict) else None
    if flush_rc == 4 and isinstance(refuse_err, str) and refuse_err in REFUSE_ALLOWLIST:
        if not restarted:
            log("PARTIAL", f"tool refuse '{refuse_err}' (rc=4) ma restart MCP stato={restart_state}")
            send_telegram(
                "⚠️ *TAILOR auto-flush — PARTIAL / FAIL-RESTART*\n"
                f"Mutazione rifiutata dal tool (`{refuse_err}`, gestito) ma **restart MCP "
                f"FALLITO**: `{restart_state}`.\n"
                "Restart manuale: `sudo launchctl unload/load /Library/LaunchDaemons/com.tailor.mcp.plist`."
            )
            return 1
        if refuse_err in ("queue_empty", "collection_queue_empty"):
            log("ESCALATE", f"drift non flushabile ({refuse_err}, rc=4) restart={restart_state}")
            send_telegram(
                "⚠️ *TAILOR auto-flush — ESCALATION — DRIFT NON FLUSHABILE*\n"
                "Drift ≥ soglia ma nessuna op netta replayable per la collection sopra il "
                "watermark (`collection_queue_pending=0`): il WAL non ha backlog flushabile "
                "per questa collection. NON ritentare il flush — investigare i write/persist "
                "mancanti o valutare un rebuild dell'indice.\n"
                f"Restart MCP: `{restart_state}`."
            )
            return 0
        if refuse_err == "ambiguous_collection_topic":
            log("ESCALATE", f"topic WAL ambiguo (rc=4) restart={restart_state}")
            send_telegram(
                "⚠️ *TAILOR auto-flush — ESCALATION — MUTAZIONE RIFIUTATA (topic WAL ambiguo)*\n"
                "Il tool non ha risolto univocamente il topic della collection nel WAL: "
                "mutazione rifiutata, nessuna scrittura. Investigare i topic duplicati.\n"
                f"Restart MCP: `{restart_state}`."
            )
            return 0
        # anomalous_drift + unknown_drift_types (difesa futura) → stessa escalation.
        sql_only = flush_json.get("sql_only_count")
        anomalous = flush_json.get("anomalous_ghost_count")
        blocking = flush_json.get("blocking_unknown_count")
        log("ESCALATE", f"drift anomalo ({refuse_err}, rc=4) sql_only={sql_only} "
                        f"anomalous_ghosts={anomalous} blocking_unknown={blocking} "
                        f"restart={restart_state}")
        send_telegram(
            "⚠️ *TAILOR auto-flush — ESCALATION — DRIFT ANOMALO*\n"
            f"Mutazione rifiutata dal tool (`{refuse_err}`): drift non #6975-puro.\n"
            f"sql_only_orphans=`{sql_only}` anomalous_ghosts=`{anomalous}` "
            f"blocking_unknown=`{blocking}`.\n"
            "Non auto-remediabile: investigare / rebuild manuale. Vedi RUNBOOK_drift_alert.md §2.\n"
            f"Restart MCP: `{restart_state}`."
        )
        return 0

    # NO-OP: il tool ha skippato (queue scesa sotto soglia tra gate e flush)
    if success and kind == "gate_skip":
        if restarted:
            log("NOOP", "flush gate-skip: queue già sotto soglia al momento del flush (rc=0)")
            send_telegram("ℹ️ *TAILOR auto-flush — NO-OP*\nLa queue si è drenata da sola prima del "
                          "flush. Restart MCP: OK.")
            return 0
        log("PARTIAL", f"flush NO-OP ma restart MCP stato={restart_state}")
        send_telegram("⚠️ *TAILOR auto-flush — PARTIAL / FAIL-RESTART*\nLa queue si è drenata "
                      f"prima del flush, ma restart MCP fallito: `{restart_state}`.")
        return 1

    flush_contract_ok = (
        success
        and kind == "flushed"
        and (flush_json or {}).get("success") is True
    )

    # Flush reale riuscito. skipped_n>0 = alcuni upsert saltati.
    if flush_contract_ok:
        partial_note = f" ({skipped_n} upsert saltati)" if (skipped_n or 0) > 0 else ""
        if restarted:
            log("SUCCESS", f"flush {metrics5} skipped={skipped_n} restart=OK")
            send_telegram(
                "✅ *TAILOR auto-flush — SUCCESS*\n"
                f"Collection pending: `{cqp0}` → `{post_cqp}`\n"
                f"Queue total (globale): `{queue0}` → `{post_queue}`\n"
                f"Drift: `{pre.get('drift')}` → `{post.get('drift')}`\n"
                f"Vector seq Δ: `{(flush_json or {}).get('vector_seq_delta')}`{partial_note}\n"
                "Restart MCP: OK (graceful)"
            )
            return 0
        # Flush OK ma MCP non riavviato → PARTIAL (l'HNSW in-memory resta stale)
        log("PARTIAL", f"flush OK (queue {queue0}->{post_queue}) ma restart MCP stato={restart_state}")
        send_telegram(
            "⚠️ *TAILOR auto-flush — PARTIAL*\n"
            f"Flush OK (queue `{queue0}` → `{post_queue}`) ma **restart MCP FALLITO**: "
            f"`{restart_state}`.\n"
            "Restart manuale: `sudo launchctl unload/load /Library/LaunchDaemons/com.tailor.mcp.plist`."
        )
        return 1

    # rc != 0 oppure contratto JSON incompleto/malformato → fallimento.
    log("FAIL", f"flush rc={flush_rc} kind={kind} {metrics5} "
                f"json={json.dumps(flush_json or {})[:300]}")
    pbg = (flush_json or {}).get("post_benign_ghost_count")
    pag = (flush_json or {}).get("post_anomalous_ghost_count")
    ghost_note = (f"Ghost post-flush: benigni=`{pbg}` anomali=`{pag}` "
                  f"(post-condizione: entrambi devono essere 0).\n" if pbg is not None else "")
    send_telegram(
        "❌ *TAILOR auto-flush — FAIL*\n"
        f"Flush rc=`{flush_rc}` (queue `{queue0}` → `{post_queue}`).\n"
        f"{ghost_note}"
        f"Restart MCP: `{restart_state}`.\n"
        "Non ritentare alla cieca: vedi `logs/auto_flush.log` e RUNBOOK_drift_alert.md §3."
    )
    return 1


# ───────────────────────────────── main ─────────────────────────────────────

def _acquire_run_lock():
    fd = open(RUN_LOCK, "w")
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return None
    return fd  # tenere il riferimento vivo per tutta la durata del processo


def main() -> int:
    parser = argparse.ArgumentParser(description="Auto-flush del backlog queue HNSW (#6975).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Valuta il gate e logga l'azione, senza mutare nulla.")
    parser.add_argument("--force-start-telegram", action="store_true",
                        help="Manda un Telegram di test per validare il path notifica, poi esce.")
    parser.add_argument("--ignore-maintenance-lock", action="store_true",
                        help="Non skippare se maintenance.lock è presente (test).")
    parser.add_argument("--log-path", help="Override log path (test).")
    parser.add_argument("--maintenance-lock", help="Override maintenance.lock path (test).")
    args = parser.parse_args()

    global LOG_PATH, MAINTENANCE_LOCK
    if args.log_path:
        LOG_PATH = args.log_path
    if args.maintenance_lock:
        MAINTENANCE_LOCK = args.maintenance_lock

    if args.force_start_telegram:
        ok = send_telegram("🧪 TEST — TAILOR auto-flush path notifica OK (ignora questo messaggio).")
        log("FORCE_TG", f"sent={ok}")
        return 0 if ok else 1

    lock_fd = _acquire_run_lock()
    if lock_fd is None:
        log("SKIP", "altro run di auto-flush in corso (flock occupato)")
        return 0

    # Un'altra operazione di manutenzione (es. sync notturno) sta usando ChromaDB
    if os.path.exists(MAINTENANCE_LOCK) and not args.ignore_maintenance_lock:
        log("SKIP", "maintenance.lock già presente — altra operazione in corso")
        return 0

    audit = run_audit()
    if audit is None:
        log("ERROR", "audit fallito — abort (nessuna azione)")
        return 1

    # Persiste l'esito audit per queue_depth_monitor (segnale STUCK senza re-audit).
    write_audit_cache(audit)

    qmin = queue_min()
    action, reason = evaluate_gate(audit, qmin)
    log("GATE", f"action={action} {reason}")
    gc = _ghost_counts(audit)
    if gc.schema_invalid:
        log("WARN", "audit JSON incoerente/malformato (schema_invalid) → escalate conservativo. "
                    "Verificare l'output di `repair_hnsw_index.py --audit --json`.")
    elif gc.legacy:
        log("WARN", "audit JSON schema legacy: benignità ghost sconosciuta → conservativo "
                    "(tutti anomali). Allineare repair_hnsw_index.")
    if audit.get("drift") is None or audit.get("drift_warning") is None:
        log("WARN", "audit JSON senza campi drift/drift_warning (tool vecchio?) → gate "
                    "degrada a queue-only (comportamento odierno, nessuna regressione). "
                    "Aggiornare repair_hnsw_index per coprire anche il caso drift.")

    if action == "skip":
        return 0

    if action == "escalate":
        if gc.schema_invalid:
            send_telegram(
                "⚠️ *TAILOR auto-flush — ESCALATION (schema invalid)*\n"
                "Audit JSON incoerente/malformato: impossibile valutare la benignità dei ghost. "
                "Nessun flush eseguito. Verificare `repair_hnsw_index.py --audit --json` e i log."
            )
            log("ESCALATE", "schema_invalid")
            return 0
        orphans = len(audit.get("sql_only_orphans", []) or [])
        mixed = ""
        if gc.benign > 0 and gc.anomalous > 0:
            mixed = (f"\n⚠️ *{gc.benign} ghost benigni in attesa DIETRO {gc.anomalous} anomali* "
                     f"— bloccati finché gli anomali non sono risolti.")
        elif gc.benign > 0:
            mixed = f"\nℹ️ {gc.benign} ghost benigni presenti (compattati a sblocco avvenuto)."
        send_telegram(
            "⚠️ *TAILOR auto-flush — ESCALATION*\n"
            f"Drift NON auto-remediabile: orphans=`{orphans}` anomalous_ghosts=`{gc.anomalous}` "
            f"blocking_unknown=`{gc.blocking_unknown}` (benign_ghosts=`{gc.benign}`)."
            f"{mixed}\n"
            + ("⚠️ audit schema legacy: benignità sconosciuta, conservativo.\n" if gc.legacy else "")
            + "`--apply` per orphans; ghost anomali → *investigare / rebuild manuale*. "
            "Vedi RUNBOOK_drift_alert.md §2."
        )
        log("ESCALATE", reason)
        return 0

    return run_procedure(audit, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
