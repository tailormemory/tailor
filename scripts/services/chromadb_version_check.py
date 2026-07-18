#!/usr/bin/env python3
"""
TAILOR — Biweekly chromadb upstream version check.

Reads the pinned chromadb version from requirements.txt (single source of
truth), queries PyPI, and sends a Telegram alert ONLY if a newer version is
available. Zero noise on no-update or network failure.

Scheduled via launchd (com.tailor.chromadb_version_check, StartInterval=14d).

Part of post-Fase-C decision (4 May 2026): stay on chromadb 1.5.8 with
active monitoring for 1.5.9+ upstream fix of HNSW persist threshold (#6975).
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime

import requests
from packaging.version import InvalidVersion, Version

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "lib"))
from env_loader import load_env  # noqa: E402
from telegram_notify import send_telegram  # noqa: E402

load_env()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
REQUIREMENTS = os.path.join(BASE_DIR, "requirements.txt")
LOG_PATH = os.path.join(BASE_DIR, "logs", "chromadb_version_check.log")
PYPI_URL = "https://pypi.org/pypi/chromadb/json"
PINNED_RE = re.compile(r"^chromadb==(?P<ver>\d+\.\d+\.\d+(?:[-.+\w]*)?)\s*$", re.MULTILINE)


def log(status: str, message: str) -> None:
    """Append a single log line. Status: UPDATE_AVAILABLE | NO_UPDATE | ERROR."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "a") as f:
        f.write(f"[{ts}] {status}: {message}\n")


def read_pinned_version() -> str | None:
    """Extract `X.Y.Z` from `chromadb==X.Y.Z` line in requirements.txt."""
    try:
        with open(REQUIREMENTS) as f:
            text = f.read()
    except OSError as e:
        log("ERROR", f"cannot read {REQUIREMENTS}: {e}")
        return None
    m = PINNED_RE.search(text)
    if not m:
        log("ERROR", f"no `chromadb==X.Y.Z` exact pin found in {REQUIREMENTS}")
        return None
    return m.group("ver")


def fetch_latest_pypi() -> tuple[str, str] | None:
    """Return (version, upload_time) of latest chromadb release on PyPI."""
    try:
        resp = requests.get(PYPI_URL, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log("ERROR", f"PyPI fetch failed: {type(e).__name__}: {e}")
        return None
    latest = data.get("info", {}).get("version")
    if not latest:
        log("ERROR", "PyPI response missing info.version")
        return None
    upload_time = ""
    releases = data.get("releases", {}).get(latest, [])
    if releases:
        ut = releases[0].get("upload_time", "")
        upload_time = ut.split("T")[0] if ut else ""
    return latest, upload_time


def build_alert(pinned: str, latest: str, upload_date: str) -> str:
    return (
        f"🔔 *chromadb update available*\n"
        f"Current pinned: `{pinned}`\n"
        f"Latest on PyPI: `{latest}`\n"
        f"Released: {upload_date or 'unknown'}\n"
        f"Release notes: https://github.com/chroma-core/chroma/releases/tag/{latest}\n"
        f"Reminder: review issue #6975 status before upgrading.\n"
        f"Update via: edit `requirements.txt` + `pip install --upgrade chromadb` in venv."
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force-alert",
        action="store_true",
        help="Bypass version check, send dummy Telegram alert (path validation only).",
    )
    args = parser.parse_args()

    if args.force_alert:
        msg = build_alert(pinned="X.Y.Z (forced)", latest="X.Y.W (forced)", upload_date="2026-05-04")
        msg = "⚠️ TEST FORCED ALERT — ignore content\n\n" + msg
        ok = send_telegram(msg, log=log)
        log("FORCE_ALERT", f"sent={ok}")
        print(f"force-alert sent={ok}")
        return 0 if ok else 1

    pinned = read_pinned_version()
    if not pinned:
        return 1

    fetched = fetch_latest_pypi()
    if not fetched:
        return 1
    latest, upload_date = fetched

    try:
        v_pinned = Version(pinned)
        v_latest = Version(latest)
    except InvalidVersion as e:
        log("ERROR", f"invalid version string: {e}")
        return 1

    if v_latest > v_pinned:
        msg = build_alert(pinned, latest, upload_date)
        ok = send_telegram(msg, log=log)
        log("UPDATE_AVAILABLE", f"pinned={pinned} latest={latest} released={upload_date} telegram_sent={ok}")
        print(f"UPDATE_AVAILABLE pinned={pinned} latest={latest} telegram_sent={ok}")
        return 0
    log("NO_UPDATE", f"pinned={pinned} latest={latest}")
    print(f"NO_UPDATE pinned={pinned} latest={latest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
