#!/bin/bash
# secret_scan.sh — Scan notturno del working tree TAILOR per pattern di secret in chiaro.
# Misura 1 post-incident 4 maggio 2026 (typo zsh che ha scritto TELEGRAM_BOT_TOKEN
# in 4 file untracked). Tool: gitleaks (brew). Schedule: 00:15 via LaunchAgent.

set -euo pipefail

# ── single-instance lock (pattern allineato a backup_db.sh) ──────────────
_LOCK_DIR="/tmp/tailor_secret_scan.lock"
_acquire() {
    if mkdir "$_LOCK_DIR" 2>/dev/null; then echo $$ > "$_LOCK_DIR/pid"; return 0; fi
    if [ -f "$_LOCK_DIR/pid" ]; then
        _old=$(cat "$_LOCK_DIR/pid" 2>/dev/null || true)
        if [ -n "$_old" ] && ! kill -0 "$_old" 2>/dev/null; then
            rm -rf "$_LOCK_DIR"
            mkdir "$_LOCK_DIR" 2>/dev/null && echo $$ > "$_LOCK_DIR/pid" && return 0
        fi
    fi
    return 1
}
if ! _acquire; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] secret_scan: another instance running, skipping" >&2
    exit 0
fi

TAILOR_DIR="${TAILOR_HOME:-$(cd "$(dirname "$0")/../.." && pwd)}"
LOG_FILE="$TAILOR_DIR/logs/secret_scan.log"
CONFIG="$TAILOR_DIR/scripts/maintenance/secret_scan.gitleaks.toml"
REPORT="/tmp/tailor_secret_scan_report.json"

trap 'rm -rf "$_LOCK_DIR"; rm -f "$REPORT"' EXIT INT TERM HUP
# ── end lock ──────────────────────────────────────────────────────────────

TELEGRAM_BOT_TOKEN=""
TELEGRAM_CHAT_ID=""
if [ -r /etc/tailor/env ]; then
    # shellcheck disable=SC1091
    source /etc/tailor/env
fi

log() {
    mkdir -p "$(dirname "$LOG_FILE")"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

send_telegram() {
    local text="$1"
    if [ -z "${TELEGRAM_BOT_TOKEN:-}" ] || [ -z "${TELEGRAM_CHAT_ID:-}" ]; then
        log "send_telegram: TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID non set, alert non inviato"
        return 0
    fi
    # Plain text — niente parse_mode per evitare escape di char speciali nei path.
    curl -s -m 10 -o /dev/null \
        -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
        --data-urlencode "chat_id=${TELEGRAM_CHAT_ID}" \
        --data-urlencode "text=${text}" \
        || log "send_telegram: curl POST fallito"
}

log "START secret_scan"

if ! command -v gitleaks >/dev/null 2>&1; then
    log "ERROR gitleaks non trovato in PATH ($PATH)"
    exit 0
fi

# gitleaks dir: scansiona filesystem (tracked + untracked), non git history.
# --exit-code 0 evita di trasformare findings in fail del run (alert è la responsabilità nostra).
gitleaks dir "$TAILOR_DIR" \
    --config "$CONFIG" \
    --redact \
    --exit-code 0 \
    --no-banner \
    --max-target-megabytes 5 \
    --log-level error \
    --report-format json \
    --report-path "$REPORT" \
    2>>"$LOG_FILE" || true

if [ ! -s "$REPORT" ]; then
    log "ERROR report non generato o vuoto: $REPORT"
    exit 0
fi

COUNT=$(python3 -c "import json,sys; print(len(json.load(open('$REPORT'))))" 2>/dev/null || echo "0")

if [ "$COUNT" = "0" ]; then
    log "clean (0 findings)"
    exit 0
fi

log "$COUNT FINDING — dettaglio sotto:"
python3 - <<PY >>"$LOG_FILE"
import json
data = json.load(open("$REPORT"))
for f in data:
    # Mai stampare Match/Secret nel log — solo metadati.
    print(f"  - {f.get('File')}:{f.get('StartLine')}  RuleID={f.get('RuleID')}  Desc={f.get('Description','')[:60]}")
PY

# Costruisci messaggio Telegram — top 5 file con count, no valori.
ALERT=$(python3 - <<PY
import json
from collections import Counter
data = json.load(open("$REPORT"))
files = Counter(f.get('File','?') for f in data)
rules = Counter(f.get('RuleID','?') for f in data)
lines = ["[ALERT] TAILOR secret_scan: " + str(len(data)) + " finding"]
lines.append("Top file:")
for path, n in files.most_common(5):
    lines.append("  " + str(n) + "x " + path)
lines.append("Rules: " + ", ".join(r + "(" + str(n) + ")" for r, n in rules.most_common(5)))
lines.append("Log: ~/tailor/logs/secret_scan.log")
print("\n".join(lines))
PY
)

send_telegram "$ALERT"
log "alert Telegram inviato"
log "END secret_scan"
exit 0
