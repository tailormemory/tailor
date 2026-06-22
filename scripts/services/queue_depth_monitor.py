#!/usr/bin/env python3
"""
TAILOR — Queue depth monitor for chromadb embeddings_queue.

Polls `embeddings_queue` depth on a 30-min schedule (LaunchDaemon
`com.tailor.queue-depth-monitor`). Read-only SQLite, no chromadb client
instantiated — zero risk of racing the live MCP process.

Due segnali DISTINTI, allineati alla remediation reale (sdoppiamento 21/06,
prima un singolo STUCK su queue_total+età-oldest generava falsi positivi non
azionabili — il gate auto-flush decide su collection_queue_pending_count dal
12/06, commit 6be02c4, e il monitor era rimasto indietro):

- STUCK azionabile  (PAGE, CTA repair_hnsw_index.py)
    collection_queue_pending_count >= flush-watermark (la STESSA soglia del gate,
    auto_flush_queue.queue_min()) in modo continuo da >=STUCK_WINDOW_HOURS (48h).
    Legge la STESSA metrica del gate dalla cache che il gate persiste a ogni run
    (auto_flush_queue.write_audit_cache: ts + collection_pending + queue_total), NON
    più COUNT(*) sul WAL globale e SENZA rieffettuare l'audit ogni 30 min (cache
    fresca entro la cadenza del gate; staleness > CACHE_MAX_AGE_HOURS → non
    valutabile, degrado safe). Se l'auto-flush skippa legittimamente
    (collection_pending < min) STUCK NON parte → niente CTA no-op. È l'unico
    segnale page-worthy con il bottone repair.

- WAL global aging  (osservabilità, severità WARNING/CRITICAL, NESSUN CTA repair)
    queue_total globale >= WARNING_THRESHOLD (severità CRITICAL >= CRITICAL_THRESHOLD)
    E non-decrescente nella finestra WAL_AGING_WINDOW_HOURS, E nessuna collection
    sopra watermark. Triggerato su profondità+crescita, NON sull'età statica
    dell'oldest. Messaggio "WAL globale non drena, nessuna collection sopra
    watermark — indaga sync_threshold/burst", senza repair_hnsw_index.py: tiene
    osservabile il rischio chromadb-backlog (#6975, 200+ chunk = zona rischio)
    senza spacciarlo per stuck azionabile.

I due segnali sono mutuamente esclusivi per costruzione (STUCK ⟹ collection sopra
watermark; WAL aging richiede NESSUNA collection sopra watermark): al più uno
scatta per run. Snooze 6h INDIPENDENTE per segnale (chiavi "stuck" /
"wal_aging:<level>"): un WAL-aging rumoroso non silenzia una STUCK page e vv.

Snapshots written append-only to `logs/queue_depth/snapshots.jsonl`,
pruned to last 8 days. Alert state per chiave in `alert_state.json`.

Drift monitor (Path A azione 3): per ogni collection confronta
max_seq_id VECTOR vs METADATA. Rileva drift e VECTOR-stuck PRIMA che la
queue cresca oltre WARNING. Snapshot append-only in `drift_snapshots.jsonl`,
alert state per (collection, level) in `drift_alert_state.json`.
- WARNING      : drift > 800   (lazy compaction normale fino a sync_threshold=1000)
- CRITICAL     : drift > 1500  (oltre sync_threshold → problema reale)
- STUCK_VECTOR : A+C (allineato al gate, sdoppiamento 22/06). Base = vector_seq
                 invariato e meta_seq avanzato tra 2 letture. A = drift >=
                 SYNC_THRESHOLD (sotto soglia è lazy compaction, non stuck — era il FP).
                 C = l'ultimo flush del gate NON ha risolto (flush_resolved=False dalla
                 cache audit) e il vector non è avanzato → consumer davvero bloccato
                 (#6975), CTA repair. Se C non valutabile (cache assente/stale) →
                 STUCK_VECTOR_UNCONFIRMED: osservabilità, nessuna CTA. Se il flush ha
                 risolto → soppresso (transitorio pre-flush benigno).

Skips silently when `maintenance.lock` is present.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta

import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "lib"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # per import auto_flush_queue
from env_loader import load_env  # noqa: E402
# Stessa fonte del gate auto-flush: la soglia (queue_min) e la cache dell'audit
# per-collection che il gate persiste a ogni run (collection_queue_pending_count).
# Il monitor NON rieffettua l'audit: legge la cache (vedi read_collection_pending).
from auto_flush_queue import queue_min as flush_watermark, AUDIT_CACHE_PATH as GATE_AUDIT_CACHE_PATH  # noqa: E402

load_env()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "db", "chroma.sqlite3")
SNAP_DIR = os.path.join(BASE_DIR, "logs", "queue_depth")
SNAP_PATH = os.path.join(SNAP_DIR, "snapshots.jsonl")
LOG_PATH = os.path.join(SNAP_DIR, "monitor.log")
STATE_PATH = os.path.join(SNAP_DIR, "alert_state.json")
MAINTENANCE_LOCK = os.path.join(BASE_DIR, "maintenance.lock")

# Soglie del SEGNALE WAL-aging (osservabilità globale): profondità del WAL totale.
# Dopo lo sdoppiamento NON governano più STUCK (che dipende da collection-pending +
# tempo, non da queue_total). Severità WARNING/CRITICAL del solo WAL-aging.
WARNING_THRESHOLD = 1200
CRITICAL_THRESHOLD = 1800
SYNC_THRESHOLD = 1000  # chromadb default
STUCK_WINDOW_HOURS = 48     # persistenza collection-pending >= watermark per la STUCK page
WAL_AGING_WINDOW_HOURS = 6  # finestra "non-decrescente" del WAL-aging (≈ cadenza auto-flush)
SNOOZE_HOURS = 6
KEEP_DAYS = 8
# Staleness max della cache audit del gate prima di considerare collection_pending
# "non disponibile". Il gate gira 08/14/20 → gap massimo 12h (notturno 20→08); 13h
# assorbe quel gap + slack per un cron in ritardo. Oltre soglia = gate probabilmente
# fermo → STUCK non valutabile (degrado safe). Irrilevante vs finestra STUCK 48h.
CACHE_MAX_AGE_HOURS = 13

DRIFT_SNAP_PATH = os.path.join(SNAP_DIR, "drift_snapshots.jsonl")
DRIFT_STATE_PATH = os.path.join(SNAP_DIR, "drift_alert_state.json")
DRIFT_WARNING = 800   # drift > 800  → WARNING (tollerante: lazy compaction normale fino a sync_threshold=1000)
DRIFT_CRITICAL = 1500 # drift > 1500 → CRITICAL (problema reale, oltre sync_threshold)

TS_FMT = "%Y-%m-%d %H:%M:%S"

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "")


def log(level: str, message: str) -> None:
    ts = datetime.now().strftime(TS_FMT)
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "a") as f:
        f.write(f"[{ts}] {level}: {message}\n")


def read_queue_snapshot() -> dict | None:
    """Read-only SQLite query: COUNT + MIN/MAX created_at on embeddings_queue."""
    try:
        con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5)
        cur = con.cursor()
        cur.execute("SELECT COUNT(*), MIN(created_at), MAX(created_at) FROM embeddings_queue")
        n, mn, mx = cur.fetchone()
        con.close()
        return {
            "ts": datetime.now().strftime(TS_FMT),
            "queue_total": int(n),
            "oldest": mn,
            "newest": mx,
        }
    except Exception as e:
        log("ERROR", f"snapshot read failed: {type(e).__name__}: {e}")
        return None


def classify_level(queue_total: int) -> str:
    if queue_total >= CRITICAL_THRESHOLD:
        return "CRITICAL"
    if queue_total >= WARNING_THRESHOLD:
        return "WARNING"
    return "NORMAL"


def _read_gate_cache(args: argparse.Namespace) -> dict | None:
    """Cache audit del gate (auto_flush_queue.write_audit_cache → gate_audit_cache.json),
    validata per freschezza. Ritorna il dict o None se assente/illeggibile/malformata/
    priva di ts valido/stale oltre CACHE_MAX_AGE_HOURS. Sorgente unica di staleness per
    entrambi i segnali che la consumano (queue STUCK e drift STUCK_VECTOR-C).

    --skip-audit forza None (test); --audit-cache-path override (test).
    """
    if getattr(args, "skip_audit", False):
        return None
    path = getattr(args, "audit_cache_path", None) or GATE_AUDIT_CACHE_PATH
    try:
        with open(path) as f:
            cache = json.load(f)
    except (OSError, json.JSONDecodeError):
        log("WARN", f"cache audit gate assente/illeggibile ({path})")
        return None
    if not isinstance(cache, dict):
        log("WARN", "cache audit gate malformata (non-dict)")
        return None
    try:
        age = datetime.now().timestamp() - float(cache.get("ts_unix"))
    except (TypeError, ValueError):
        log("WARN", "cache audit gate senza ts_unix valido")
        return None
    if age > CACHE_MAX_AGE_HOURS * 3600:
        log("WARN", f"cache audit gate stale ({age / 3600:.1f}h > {CACHE_MAX_AGE_HOURS}h) → gate fermo?")
        return None
    return cache


def read_collection_pending(args: argparse.Namespace) -> int | None:
    """collection_queue_pending_count dalla CACHE dell'audit del gate auto-flush.

    Il gate (auto_flush_queue) persiste l'esito del proprio audit a ogni run
    (08/14/20, vedi auto_flush_queue.write_audit_cache); il monitor — che gira ogni
    30 min — NON rieffettua l'audit (eviterebbe un subprocess inutile 48×/giorno):
    legge questa cache. La finestra STUCK è 48h, una staleness di poche ore è
    irrilevante. Stessa soglia/metrica del gate → STUCK resta allineato al gate.

    Ritorna None (→ STUCK non valutabile, status quo safe, niente CTA no-op) se la
    cache è assente/stale (vedi _read_gate_cache) o senza collection-pending valido.

    Override test: --collection-pending N forza il valore (salta la cache);
    --skip-audit ritorna None senza leggere la cache; --audit-cache-path override.
    """
    if getattr(args, "collection_pending", None) is not None:
        return args.collection_pending
    cache = _read_gate_cache(args)
    if cache is None:
        return None
    raw = cache.get("collection_queue_pending_count")
    try:
        v = int(raw)
    except (TypeError, ValueError):
        log("WARN", f"cache collection_queue_pending_count assente/non-int ({raw!r}) "
                    "→ STUCK non valutabile")
        return None
    return v if v >= 0 else None


def stuck_vector_verdict(cache: dict | None, collection_name: str,
                         current_vector_seq) -> bool | None:
    """Condizione C di STUCK_VECTOR: l'ultimo flush del gate ha RISOLTO o no?

    Distingue il vero #6975 (flush girato ma drift non sceso / vector non avanzato)
    dal transitorio benigno pre-flush. Ritorna:
      * True  → CONFERMATO stuck: il gate ha flushato questa collection, il flush NON
                ha risolto (flush_resolved=False) E il vector live non è avanzato oltre
                quello cachato (current <= cached). Consumer davvero bloccato → CTA.
      * False → BENIGNO: il gate ha flushato e ha RISOLTO (resolved=True) oppure il
                vector è avanzato oltre la cache (consumer ripartito) → sopprimi.
      * None  → NON valutabile su C: cache assente/stale, di altra collection, o
                nessun flush registrato (flush_ran=False). Il chiamante ricade su A.
    """
    if not cache:
        return None
    if cache.get("collection_name") != collection_name:
        return None
    if not cache.get("flush_ran"):
        return None
    resolved = cache.get("flush_resolved")
    cached_vec = cache.get("vector_seq")
    advanced = False
    try:
        advanced = current_vector_seq > int(cached_vec)
    except (TypeError, ValueError):
        advanced = False  # cache senza vector confrontabile → non posso dire "avanzato"
    if resolved is True or advanced:
        return False
    if resolved is False:
        return True
    return None  # flush_ran ma resolved sconosciuto → non valutabile


def _ref_before_cutoff(history: list[dict], now_ts: str, window_hours: int) -> dict | None:
    """Snapshot più recente con ts <= now-window_hours, altrimenti None.

    L'ancora per le condizioni temporali "da >=window" (persistenza / non-decrescita):
    senza uno snapshot abbastanza vecchio non si può asserire la durata → None.
    """
    try:
        now_t = datetime.strptime(now_ts, TS_FMT)
    except (ValueError, KeyError):
        return None
    cutoff = now_t - timedelta(hours=window_hours)
    cands = []
    for s in history:
        try:
            t = datetime.strptime(s["ts"], TS_FMT)
        except (ValueError, KeyError):
            continue
        if t <= cutoff:
            cands.append((t, s))
    if not cands:
        return None
    cands.sort(key=lambda x: x[0])
    return cands[-1][1]


def is_actionable_stuck(collection_pending: int | None, watermark: int,
                        history: list[dict], now_ts: str, window_hours: int) -> bool:
    """STUCK azionabile = la collection è sopra il flush-watermark e NON drena da >=window.

    Allineato 1:1 al gate auto-flush: la metrica decisionale è
    collection_queue_pending_count (`collection_pending` negli snapshot), NON
    queue_total globale. Scatta SOLO se:
      1. collection_pending corrente >= watermark, E
      2. esiste uno snapshot-ancora con collection_pending presente e ts <= now-window
         il cui collection_pending era già >= watermark (backlog azionabile già da >=48h), E
      3. nessuno snapshot DENTRO la finestra [now-window, now] è sceso sotto watermark
         (mai drenato → l'auto-flush non ha rimediato per >=48h).

    Se collection_pending è assente (tool/audit legacy o audit fallito) → None →
    STUCK NON scatta (status quo safe, niente CTA no-op). Gli snapshot privi di
    collection_pending vengono ignorati nel calcolo (storia pre-sdoppiamento):
    STUCK attende 48h di storia nuova prima di poter scattare — niente FP.
    """
    if collection_pending is None or collection_pending < watermark:
        return False
    try:
        now_t = datetime.strptime(now_ts, TS_FMT)
    except (ValueError, KeyError):
        return False
    cutoff = now_t - timedelta(hours=window_hours)
    anchor: tuple[datetime, int] | None = None   # più recente con t <= cutoff
    in_window_ok = True                           # nessun calo sotto watermark in finestra
    for s in history:
        cp = s.get("collection_pending")
        if cp is None:
            continue
        try:
            t = datetime.strptime(s["ts"], TS_FMT)
        except (ValueError, KeyError):
            continue
        if t <= cutoff:
            if anchor is None or t > anchor[0]:
                anchor = (t, cp)
        else:  # dentro la finestra
            if cp < watermark:
                in_window_ok = False
    if anchor is None or anchor[1] < watermark:
        return False
    return in_window_ok


def is_wal_aging(queue_total: int, history: list[dict], now_ts: str,
                 window_hours: int) -> bool:
    """WAL global aging = WAL totale alto E non-decrescente nella finestra.

    Profondità + crescita, NON età-oldest statica: un WAL alto ma in calo
    (l'indexer sta drenando) NON allerta. Richiede uno snapshot-ancora a >=window:
    senza storia sufficiente non si asserisce "non drena" → False.
    """
    if queue_total < WARNING_THRESHOLD:
        return False
    ref = _ref_before_cutoff(history, now_ts, window_hours)
    if ref is None:
        return False
    return queue_total >= int(ref.get("queue_total", 0) or 0)


def load_history() -> list[dict]:
    if not os.path.exists(SNAP_PATH):
        return []
    out = []
    with open(SNAP_PATH) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def append_snapshot(snap: dict) -> None:
    os.makedirs(os.path.dirname(SNAP_PATH), exist_ok=True)
    with open(SNAP_PATH, "a") as f:
        f.write(json.dumps(snap) + "\n")


def prune_history(keep_days: int) -> None:
    if not os.path.exists(SNAP_PATH):
        return
    cutoff = datetime.now() - timedelta(days=keep_days)
    kept_lines = []
    with open(SNAP_PATH) as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                s = json.loads(stripped)
                t = datetime.strptime(s["ts"], TS_FMT)
            except (json.JSONDecodeError, KeyError, ValueError):
                continue
            if t >= cutoff:
                kept_lines.append(stripped)
    tmp = SNAP_PATH + ".tmp"
    with open(tmp, "w") as f:
        if kept_lines:
            f.write("\n".join(kept_lines) + "\n")
    os.replace(tmp, SNAP_PATH)


def read_alert_state() -> dict:
    if not os.path.exists(STATE_PATH):
        return {}
    try:
        with open(STATE_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def write_alert_state(state: dict) -> None:
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_PATH)


def _oldest_age_hours(oldest: str | None, now_ts: str) -> float | None:
    if not oldest:
        return None
    try:
        o = datetime.strptime(oldest, TS_FMT)
        n = datetime.strptime(now_ts, TS_FMT)
    except ValueError:
        return None
    return (n - o).total_seconds() / 3600


def format_stuck_alert(snap: dict, watermark: int, window_hours: int) -> str:
    """STUCK azionabile — PAGE con CTA repair. Backlog per-collection sopra
    watermark che non drena da >=window: l'auto-flush non ha rimediato."""
    lines = [f"🚨 *TAILOR queue alert — STUCK azionabile*"]
    lines.append(f"Collection pending: `{snap.get('collection_pending')}` (watermark {watermark})")
    lines.append(f"Queue depth (globale): `{snap['queue_total']}`")
    age = _oldest_age_hours(snap.get("oldest"), snap["ts"])
    if snap.get("oldest"):
        if age is not None:
            lines.append(f"Oldest pending: `{snap['oldest']}` ({age:.1f}h fa)")
        else:
            lines.append(f"Oldest pending: `{snap['oldest']}`")
    lines.append(f"Stuck: collection_pending >= watermark e non drena da ≥{window_hours}h "
                 "(auto-flush non ha rimediato).")
    lines.append("Run: `python scripts/maintenance/repair_hnsw_index.py`")
    return "\n".join(lines)


def format_wal_aging_alert(snap: dict, level: str, window_hours: int) -> str:
    """WAL global aging — osservabilità, NESSUN CTA repair. WAL totale alto e
    non-decrescente, ma nessuna collection sopra watermark (non auto-remediabile
    via flush per-collection)."""
    emoji = "🚨" if level == "CRITICAL" else "⚠️"
    lines = [f"{emoji} *TAILOR WAL aging — {level}*"]
    lines.append(f"Queue depth (globale): `{snap['queue_total']}`")
    if level == "CRITICAL":
        lines.append(f"Critical threshold: {CRITICAL_THRESHOLD} (sync_threshold {SYNC_THRESHOLD})")
    else:
        lines.append(f"Warning threshold: {WARNING_THRESHOLD}")
    cp = snap.get("collection_pending")
    lines.append(f"Collection pending: `{cp}` (sotto watermark / n/d)")
    lines.append(f"WAL globale non drena (non-decrescente da ≥{window_hours}h) e nessuna "
                 "collection sopra watermark — indaga `sync_threshold` / burst di ingest.")
    lines.append("Nessuna azione repair: non è un backlog #6975 azionabile per collection.")
    return "\n".join(lines)


def send_telegram(text: str) -> bool:
    if not TG_TOKEN or not TG_CHAT:
        log("ERROR", "Telegram skipped: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID empty")
        return False
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    body = {"chat_id": TG_CHAT, "text": text, "parse_mode": "Markdown"}
    try:
        resp = requests.post(url, json=body, timeout=15)
    except Exception as e:
        log("ERROR", f"Telegram POST raised: {type(e).__name__}: {e}")
        return False
    if resp.status_code == 200:
        return True
    if resp.status_code == 400 and "can't parse" in resp.text.lower():
        body.pop("parse_mode", None)
        try:
            resp = requests.post(url, json=body, timeout=15)
        except Exception as e:
            log("ERROR", f"Telegram retry POST raised: {type(e).__name__}: {e}")
            return False
        if resp.status_code == 200:
            return True
    log("ERROR", f"Telegram non-200: status={resp.status_code} body={resp.text[:200]}")
    return False


def maintenance_lock_present() -> bool:
    return os.path.exists(MAINTENANCE_LOCK)


def _override_paths(args: argparse.Namespace) -> None:
    """Override module-level paths for tests."""
    global DB_PATH, SNAP_PATH, STATE_PATH, LOG_PATH, MAINTENANCE_LOCK
    global DRIFT_SNAP_PATH, DRIFT_STATE_PATH
    if args.db_path:
        DB_PATH = args.db_path
    if args.snap_path:
        SNAP_PATH = args.snap_path
    if args.state_path:
        STATE_PATH = args.state_path
    if args.log_path:
        LOG_PATH = args.log_path
    if args.maintenance_lock:
        MAINTENANCE_LOCK = args.maintenance_lock
    if args.drift_snap_path:
        DRIFT_SNAP_PATH = args.drift_snap_path
    if args.drift_state_path:
        DRIFT_STATE_PATH = args.drift_state_path


# ─────────────────────────── drift monitor (azione 3) ───────────────────────

def load_jsonl(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    out = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def append_jsonl(path: str, obj: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(obj) + "\n")


def prune_jsonl(path: str, keep_days: int) -> None:
    if not os.path.exists(path):
        return
    cutoff = datetime.now() - timedelta(days=keep_days)
    kept = []
    with open(path) as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                s = json.loads(stripped)
                t = datetime.strptime(s["ts"], TS_FMT)
            except (json.JSONDecodeError, KeyError, ValueError):
                continue
            if t >= cutoff:
                kept.append(stripped)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        if kept:
            f.write("\n".join(kept) + "\n")
    os.replace(tmp, path)


def _decode_seq(v) -> int | None:
    """max_seq_id.seq_id may be stored as little-endian bytes or as int."""
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray)):
        return int.from_bytes(v, "little")
    return int(v)


def read_drift_readings() -> list[dict] | None:
    """Read-only: per collection, drift = META.max_seq_id - VECTOR.max_seq_id.

    Collections lacking a complete VECTOR+METADATA seq pair are skipped.
    """
    try:
        con = sqlite3.connect(f"file:{DB_PATH}?mode=ro&immutable=1", uri=True, timeout=5)
        cur = con.cursor()
        ts = datetime.now().strftime(TS_FMT)
        readings = []
        for cid, cname in cur.execute("SELECT id, name FROM collections").fetchall():
            segs = cur.execute(
                "SELECT scope, id FROM segments WHERE collection=?", (cid,)
            ).fetchall()
            vec_seg = next((sid for scope, sid in segs if scope == "VECTOR"), None)
            meta_seg = next((sid for scope, sid in segs if scope == "METADATA"), None)
            vec_seq = meta_seq = None
            if vec_seg:
                row = cur.execute(
                    "SELECT seq_id FROM max_seq_id WHERE segment_id=?", (vec_seg,)
                ).fetchone()
                vec_seq = _decode_seq(row[0]) if row else None
            if meta_seg:
                row = cur.execute(
                    "SELECT seq_id FROM max_seq_id WHERE segment_id=?", (meta_seg,)
                ).fetchone()
                meta_seq = _decode_seq(row[0]) if row else None
            if vec_seq is None or meta_seq is None:
                continue
            readings.append({
                "ts": ts,
                "collection_name": cname,
                "vector_seq": vec_seq,
                "meta_seq": meta_seq,
                "drift": meta_seq - vec_seq,
            })
        con.close()
        return readings
    except Exception as e:
        log("ERROR", f"drift read failed: {type(e).__name__}: {e}")
        return None


def classify_drift(drift: int) -> str:
    if drift > DRIFT_CRITICAL:
        return "CRITICAL"
    if drift > DRIFT_WARNING:
        return "WARNING"
    return "NORMAL"


def _last_drift_for(collection_name: str, history: list[dict]) -> dict | None:
    for s in reversed(history):
        if s.get("collection_name") == collection_name:
            return s
    return None


def read_drift_state() -> dict:
    if not os.path.exists(DRIFT_STATE_PATH):
        return {}
    try:
        with open(DRIFT_STATE_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def write_drift_state(state: dict) -> None:
    os.makedirs(os.path.dirname(DRIFT_STATE_PATH), exist_ok=True)
    tmp = DRIFT_STATE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, DRIFT_STATE_PATH)


def within_snooze_key(state: dict, key: str, hours: int) -> bool:
    rec = state.get(key)
    if not isinstance(rec, dict):
        return False
    last_ts = rec.get("last_alert_ts")
    if not last_ts:
        return False
    try:
        t = datetime.strptime(last_ts, TS_FMT)
    except ValueError:
        return False
    return (datetime.now() - t) < timedelta(hours=hours)


def format_drift_alert(snap: dict, label: str, oldest_age: float | None,
                       oldest: str | None) -> str:
    emoji = {"WARNING": "⚠️", "CRITICAL": "🚨",
             "STUCK_VECTOR": "🧊", "STUCK_VECTOR_UNCONFIRMED": "👀"}.get(label, "⚠️")
    title = "STUCK_VECTOR" if label == "STUCK_VECTOR_UNCONFIRMED" else label
    suffix = " (non confermato)" if label == "STUCK_VECTOR_UNCONFIRMED" else ""
    lines = [f"{emoji} *TAILOR drift alert — {title}{suffix}*"]
    lines.append(f"Collection: `{snap['collection_name']}`")
    lines.append(f"Drift (META-VECTOR): `{snap['drift']}`")
    lines.append(f"vector_seq: `{snap['vector_seq']}`  meta_seq: `{snap['meta_seq']}`")
    vd = snap.get("vector_seq_delta_since_last")
    md = snap.get("meta_seq_delta_since_last")
    if vd is not None:
        lines.append(f"Δ dal check precedente — vector: `{vd}`  meta: `{md}`")
    # CTA repair SOLO quando il flush del gate ha già dimostrato di non risolvere
    # (STUCK_VECTOR confermato, condizione C). Negli altri casi è osservabilità.
    cta = False
    if label == "STUCK_VECTOR":
        lines.append("VECTOR fermo mentre META avanza E l'ultimo flush del gate NON "
                     "ha risolto (drift ≥ sync_threshold) → consumer HNSW bloccato (#6975).")
        cta = True
    elif label == "STUCK_VECTOR_UNCONFIRMED":
        lines.append(f"VECTOR fermo con drift ≥ sync_threshold ({SYNC_THRESHOLD}), ma esito "
                     "flush del gate non disponibile (cache assente/stale): osservabilità, "
                     "NON confermato bloccato. Attendere il prossimo run del gate.")
    elif label == "WARNING":
        # Informativo: zona lazy-compaction [DRIFT_WARNING, SYNC_THRESHOLD), l'auto-flush
        # risolve da sé → niente CTA repair (sarebbe un'azione manuale no-op). Il caso
        # "drift davvero non scende" è coperto da STUCK_VECTOR confermato (CTA propria).
        lines.append(f"Soglia WARNING: drift > {DRIFT_WARNING} "
                     f"(lazy compaction sotto sync_threshold {SYNC_THRESHOLD}, informativo)")
    elif label == "CRITICAL":
        lines.append(f"Soglia CRITICAL: drift > {DRIFT_CRITICAL} (sync_threshold {SYNC_THRESHOLD})")
        cta = True
    if oldest:
        if oldest_age is not None:
            lines.append(f"Queue oldest pending: `{oldest}` ({oldest_age:.1f}h fa)")
        else:
            lines.append(f"Queue oldest pending: `{oldest}`")
    if cta:
        lines.append("Run: `python scripts/maintenance/repair_hnsw_index.py`")
    return "\n".join(lines)


def _emit_queue_signal(args: argparse.Namespace, state: dict, key: str,
                       kind: str, msg: str, summary: str) -> tuple[bool, bool]:
    """Invia (o dry-run) un segnale queue con snooze per-chiave.

    Ritorna (attempted, send_ok): attempted=False se snoozato/dry-run.
    Aggiorna `state[key]` in place su invio riuscito.
    """
    snoozed = within_snooze_key(state, key, SNOOZE_HOURS) and not args.ignore_snooze
    if snoozed:
        log("SNOOZED", f"{kind} {summary}")
        print(f"SNOOZED {kind} {summary}")
        return False, False
    if args.dry_run:
        log("DRY_RUN", f"{kind} {summary}")
        print(f"DRY-RUN would alert ({kind}) {summary}:\n{msg}")
        return False, False
    ok = send_telegram(msg)
    if ok:
        state[key] = {"last_alert_ts": datetime.now().strftime(TS_FMT), "kind": kind}
    log("ALERT", f"{kind} {summary} sent={ok}")
    print(f"ALERT {kind} {summary} sent={ok}")
    return True, ok


def handle_queue(args: argparse.Namespace) -> tuple[int, dict | None]:
    snap = read_queue_snapshot()
    if snap is None:
        return 1, None
    history = load_history()
    watermark = args.flush_watermark if args.flush_watermark is not None else flush_watermark()
    collection_pending = read_collection_pending(args)

    level = classify_level(snap["queue_total"])
    # SEGNALE 1 — STUCK azionabile: collection-pending >= watermark sostenuto >=48h.
    stuck = is_actionable_stuck(collection_pending, watermark, history,
                                snap["ts"], STUCK_WINDOW_HOURS)
    # SEGNALE 2 — WAL aging: queue_total alto+crescente E nessuna collection sopra
    # watermark (mutuamente esclusivo con STUCK per costruzione).
    no_actionable = collection_pending is None or collection_pending < watermark
    aging = no_actionable and is_wal_aging(snap["queue_total"], history,
                                           snap["ts"], WAL_AGING_WINDOW_HOURS)

    snap["collection_pending"] = collection_pending
    snap["watermark"] = watermark
    snap["level"] = level
    snap["stuck"] = stuck
    snap["wal_aging"] = aging
    append_snapshot(snap)
    prune_history(KEEP_DAYS)

    summary = (f"queue={snap['queue_total']} collection_pending={collection_pending} "
               f"watermark={watermark} level={level} stuck={stuck} aging={aging}")
    state = read_alert_state()
    rc = 0

    if stuck:
        msg = format_stuck_alert(snap, watermark, STUCK_WINDOW_HOURS)
        attempted, ok = _emit_queue_signal(args, state, "stuck", "STUCK", msg, summary)
        if attempted and not ok:
            rc = 1
    elif aging:
        msg = format_wal_aging_alert(snap, level, WAL_AGING_WINDOW_HOURS)
        attempted, ok = _emit_queue_signal(args, state, f"wal_aging:{level}",
                                           "WAL_AGING", msg, summary)
        if attempted and not ok:
            rc = 1
    else:
        log("OK", summary)
        print(f"OK {summary}")
        return 0, snap

    if not args.dry_run:
        write_alert_state(state)
    return rc, snap


def handle_drift(args: argparse.Namespace, queue_snap: dict | None) -> int:
    readings = read_drift_readings()
    if readings is None:
        return 1
    history = load_jsonl(DRIFT_SNAP_PATH)
    state = read_drift_state()
    new_state = dict(state)
    oldest = queue_snap.get("oldest") if queue_snap else None
    now_ts = datetime.now().strftime(TS_FMT)
    oldest_age = _oldest_age_hours(oldest, now_ts) if oldest else None
    gate_cache = _read_gate_cache(args)  # condizione C di STUCK_VECTOR (esito flush del gate)
    rc = 0
    for r in readings:
        cname = r["collection_name"]
        last = _last_drift_for(cname, history)
        if last and last.get("vector_seq") is not None and last.get("meta_seq") is not None:
            vdelta = r["vector_seq"] - last["vector_seq"]
            mdelta = r["meta_seq"] - last["meta_seq"]
        else:
            vdelta = mdelta = None
        # Base: vector fermo + meta avanzato tra 2 letture del monitor.
        base_stall = (vdelta == 0 and mdelta is not None and mdelta > 0)
        # A (pavimento): vector fermo è sospetto solo da SYNC_THRESHOLD in su; sotto
        # [DRIFT_WARNING, SYNC_THRESHOLD) è lazy compaction normale (era il FP).
        a_floor = r["drift"] >= SYNC_THRESHOLD
        # C (allineamento al gate): l'ultimo flush ha risolto? True=conferma #6975,
        # False=benigno (sopprimi), None=non valutabile (ricade su A).
        c_verdict = stuck_vector_verdict(gate_cache, cname, r["vector_seq"]) if base_stall and a_floor else None
        stuck_vector = base_stall and a_floor and c_verdict is not False
        stuck_confirmed = stuck_vector and c_verdict is True
        level = classify_drift(r["drift"])
        if level == "CRITICAL":
            status = "CRITICAL"
        elif stuck_confirmed:
            status = "STUCK_VECTOR"
        elif stuck_vector:
            status = "STUCK_VECTOR_UNCONFIRMED"
        elif level == "WARNING":
            status = "WARNING"
        else:
            status = "NORMAL"
        snap = {
            "ts": r["ts"],
            "collection_name": cname,
            "vector_seq": r["vector_seq"],
            "meta_seq": r["meta_seq"],
            "drift": r["drift"],
            "vector_seq_delta_since_last": vdelta,
            "meta_seq_delta_since_last": mdelta,
            "status": status,
            "stuck_vector": stuck_vector,
            "stuck_vector_confirmed": stuck_confirmed,
        }
        append_jsonl(DRIFT_SNAP_PATH, snap)
        triggers = []
        if level in ("WARNING", "CRITICAL"):
            triggers.append(level)
        if stuck_confirmed:
            triggers.append("STUCK_VECTOR")
        elif stuck_vector:
            triggers.append("STUCK_VECTOR_UNCONFIRMED")
        for label in triggers:
            key = f"{cname}:{label}"
            snoozed = within_snooze_key(state, key, SNOOZE_HOURS) and not args.ignore_snooze
            if snoozed:
                log("DRIFT_SNOOZED", f"{key} drift={r['drift']} status={status}")
                print(f"DRIFT_SNOOZED {key} drift={r['drift']}")
                continue
            msg = format_drift_alert(snap, label, oldest_age, oldest)
            if args.dry_run:
                log("DRIFT_DRY_RUN", f"{key} drift={r['drift']} status={status}")
                print(f"DRIFT DRY-RUN would alert {key}:\n{msg}")
                continue
            ok = send_telegram(msg)
            if ok:
                new_state[key] = {
                    "last_alert_ts": r["ts"],
                    "drift": r["drift"],
                    "status": status,
                }
            log("DRIFT_ALERT", f"{key} drift={r['drift']} status={status} sent={ok}")
            print(f"DRIFT_ALERT {key} drift={r['drift']} sent={ok}")
            if not ok:
                rc = 1
        if not triggers:
            log("DRIFT_OK", f"{cname} drift={r['drift']} status={status}")
            print(f"DRIFT_OK {cname} drift={r['drift']} status={status}")
    prune_jsonl(DRIFT_SNAP_PATH, KEEP_DAYS)
    if not args.dry_run:
        write_drift_state(new_state)
    return rc


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force-alert", action="store_true",
                        help="Bypass evaluation; send a dummy Telegram alert for path validation.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Evaluate as usual but do not send Telegram nor write state.")
    parser.add_argument("--ignore-snooze", action="store_true",
                        help="Bypass snooze window (tests).")
    parser.add_argument("--db-path", help="Override DB path (tests).")
    parser.add_argument("--snap-path", help="Override snapshots.jsonl path (tests).")
    parser.add_argument("--state-path", help="Override alert_state.json path (tests).")
    parser.add_argument("--log-path", help="Override monitor.log path (tests).")
    parser.add_argument("--maintenance-lock", help="Override maintenance.lock path (tests).")
    parser.add_argument("--drift-snap-path", help="Override drift_snapshots.jsonl path (tests).")
    parser.add_argument("--drift-state-path", help="Override drift_alert_state.json path (tests).")
    parser.add_argument("--skip-queue", action="store_true", help="Skip queue evaluation (tests).")
    parser.add_argument("--skip-drift", action="store_true", help="Skip drift evaluation.")
    parser.add_argument("--collection-pending", type=int, default=None,
                        help="Forza collection_queue_pending_count, salta la cache (tests).")
    parser.add_argument("--skip-audit", action="store_true",
                        help="Non leggere la cache audit del gate (collection_pending=None) (tests).")
    parser.add_argument("--audit-cache-path",
                        help="Override del path cache audit del gate (default: auto_flush_queue.AUDIT_CACHE_PATH) (tests).")
    parser.add_argument("--flush-watermark", type=int, default=None,
                        help="Override del flush-watermark (default: auto_flush_queue.queue_min()) (tests).")
    args = parser.parse_args()
    _override_paths(args)

    if args.force_alert:
        msg = (
            "⚠️ TEST FORCED ALERT — ignore content\n\n"
            "🚨 *TAILOR queue alert — STUCK azionabile (TEST)*\n"
            "Collection pending: `9999` (watermark forced)\n"
            "Queue depth (globale): `9999` (forced)\n"
            "Oldest pending: `forced`\n"
            f"Stuck: forced (>={STUCK_WINDOW_HOURS}h simulato)\n"
            "Run: `python scripts/maintenance/repair_hnsw_index.py`"
        )
        ok = send_telegram(msg)
        log("FORCE_ALERT", f"sent={ok}")
        print(f"force-alert sent={ok}")
        return 0 if ok else 1

    if maintenance_lock_present():
        log("SKIP", "maintenance.lock present")
        print("SKIP maintenance.lock present")
        return 0

    queue_rc, queue_snap = 0, None
    if not args.skip_queue:
        queue_rc, queue_snap = handle_queue(args)

    drift_rc = 0
    if not args.skip_drift:
        drift_rc = handle_drift(args, queue_snap)

    return queue_rc or drift_rc


if __name__ == "__main__":
    sys.exit(main())
