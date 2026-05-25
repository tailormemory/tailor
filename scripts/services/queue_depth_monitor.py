#!/usr/bin/env python3
"""
TAILOR — Queue depth monitor for chromadb embeddings_queue.

Polls `embeddings_queue` depth on a 30-min schedule (LaunchDaemon
`com.tailor.queue-depth-monitor`). Read-only SQLite, no chromadb client
instantiated — zero risk of racing the live MCP process.

Alert ladder:
- WARNING  : queue_total >= 600   (above observed regime ceiling ~510)
- CRITICAL : queue_total >= 850   (15% margin under chromadb sync_threshold 1000)
- STUCK    : oldest_iso unchanged AND queue_total non-decreasing for >=12h
             (consumer freeze signature — observed 12/05–23/05 freeze @ 597)

Snapshots written append-only to `logs/queue_depth/snapshots.jsonl`,
pruned to last 8 days. Alert state in `alert_state.json`; 6h snooze
between consecutive alerts.

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
from env_loader import load_env  # noqa: E402

load_env()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "db", "chroma.sqlite3")
SNAP_DIR = os.path.join(BASE_DIR, "logs", "queue_depth")
SNAP_PATH = os.path.join(SNAP_DIR, "snapshots.jsonl")
LOG_PATH = os.path.join(SNAP_DIR, "monitor.log")
STATE_PATH = os.path.join(SNAP_DIR, "alert_state.json")
MAINTENANCE_LOCK = os.path.join(BASE_DIR, "maintenance.lock")

WARNING_THRESHOLD = 600
CRITICAL_THRESHOLD = 850
SYNC_THRESHOLD = 1000  # chromadb default
STUCK_WINDOW_HOURS = 12
SNOOZE_HOURS = 6
KEEP_DAYS = 8

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


def is_stuck(current: dict, history: list[dict], window_hours: int) -> bool:
    """Stuck = oldest unchanged AND queue_total non-decreasing for >=window_hours.

    Requires at least one historical snapshot whose own ts is <= now-window.
    A queue with no oldest (None) or count=0 is never stuck.
    """
    if not current.get("oldest") or current.get("queue_total", 0) == 0:
        return False
    if not history:
        return False
    try:
        now_t = datetime.strptime(current["ts"], TS_FMT)
    except (ValueError, KeyError):
        return False
    cutoff = now_t - timedelta(hours=window_hours)
    older_or_equal = []
    for s in history:
        try:
            t = datetime.strptime(s["ts"], TS_FMT)
        except (ValueError, KeyError):
            continue
        if t <= cutoff:
            older_or_equal.append((t, s))
    if not older_or_equal:
        return False
    older_or_equal.sort(key=lambda x: x[0])
    _, ref = older_or_equal[-1]  # most recent snapshot still <= cutoff
    if ref.get("oldest") != current["oldest"]:
        return False
    if current["queue_total"] < ref.get("queue_total", 0):
        return False
    return True


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


def within_snooze(last: dict, hours: int) -> bool:
    last_ts = last.get("last_alert_ts")
    if not last_ts:
        return False
    try:
        t = datetime.strptime(last_ts, TS_FMT)
    except ValueError:
        return False
    return (datetime.now() - t) < timedelta(hours=hours)


def _oldest_age_hours(oldest: str | None, now_ts: str) -> float | None:
    if not oldest:
        return None
    try:
        o = datetime.strptime(oldest, TS_FMT)
        n = datetime.strptime(now_ts, TS_FMT)
    except ValueError:
        return None
    return (n - o).total_seconds() / 3600


def format_alert(snap: dict, level: str, stuck: bool) -> str:
    if level != "NORMAL" and stuck:
        flag = f"{level} + STUCK"
    elif level != "NORMAL":
        flag = level
    elif stuck:
        flag = "STUCK"
    else:
        flag = "INFO"
    lines = [f"🚨 *TAILOR queue alert — {flag}*"]
    lines.append(f"Queue depth: `{snap['queue_total']}`")
    if level == "CRITICAL":
        lines.append(f"Critical threshold: {CRITICAL_THRESHOLD} (sync_threshold {SYNC_THRESHOLD})")
    elif level == "WARNING":
        lines.append(f"Warning threshold: {WARNING_THRESHOLD}")
    age = _oldest_age_hours(snap.get("oldest"), snap["ts"])
    if snap.get("oldest"):
        if age is not None:
            lines.append(f"Oldest pending: `{snap['oldest']}` ({age:.1f}h fa)")
        else:
            lines.append(f"Oldest pending: `{snap['oldest']}`")
    if stuck:
        lines.append(f"Stuck: oldest non avanza da ≥{STUCK_WINDOW_HOURS}h e depth non scende")
        lines.append("Run: `python scripts/maintenance/repair_hnsw_index.py`")
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
    args = parser.parse_args()
    _override_paths(args)

    if args.force_alert:
        msg = (
            "⚠️ TEST FORCED ALERT — ignore content\n\n"
            "🚨 *TAILOR queue alert — TEST*\n"
            "Queue depth: `9999` (forced)\n"
            f"Critical threshold: {CRITICAL_THRESHOLD} (sync_threshold {SYNC_THRESHOLD})\n"
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

    snap = read_queue_snapshot()
    if snap is None:
        return 1

    history = load_history()
    level = classify_level(snap["queue_total"])
    stuck = is_stuck(snap, history, STUCK_WINDOW_HOURS)
    snap["level"] = level
    snap["stuck"] = stuck

    append_snapshot(snap)
    prune_history(KEEP_DAYS)

    last_alert = read_alert_state()
    should_alert = (level != "NORMAL") or stuck
    snoozed = within_snooze(last_alert, SNOOZE_HOURS) and not args.ignore_snooze

    if should_alert and not snoozed:
        if args.dry_run:
            preview = format_alert(snap, level, stuck)
            log("DRY_RUN", f"level={level} stuck={stuck} queue={snap['queue_total']}")
            print(f"DRY-RUN would alert ({level}, stuck={stuck}):\n{preview}")
            return 0
        ok = send_telegram(format_alert(snap, level, stuck))
        if ok:
            write_alert_state({
                "last_alert_ts": snap["ts"],
                "last_level": level,
                "last_stuck": stuck,
                "last_queue_total": snap["queue_total"],
            })
        log("ALERT", f"level={level} stuck={stuck} queue={snap['queue_total']} sent={ok}")
        print(f"ALERT level={level} stuck={stuck} queue={snap['queue_total']} sent={ok}")
        return 0 if ok else 1

    if should_alert and snoozed:
        log("SNOOZED", f"level={level} stuck={stuck} queue={snap['queue_total']}")
        print(f"SNOOZED level={level} stuck={stuck} queue={snap['queue_total']}")
        return 0

    log("OK", f"queue={snap['queue_total']} level=NORMAL stuck=False")
    print(f"OK queue={snap['queue_total']} level=NORMAL")
    return 0


if __name__ == "__main__":
    sys.exit(main())
