#!/bin/bash
# sync_email.sh — Unified email sync: Gmail API or IMAP based on config
# Replaces sync_gmail.sh in cron. Falls back gracefully.

# ── single-instance lock (cron + launchd condividono questo) ──────────────
_LOCK_DIR="/tmp/tailor_sync_email.lock"
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
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] sync_email: another instance running, skipping" >&2
    exit 0
fi
trap 'rm -rf "$_LOCK_DIR"' EXIT INT TERM HUP
# ── end lock ──────────────────────────────────────────────────────────────

TAILOR_DIR="${TAILOR_HOME:-$(cd "$(dirname "$0")" && pwd)}"
LOG="$TAILOR_DIR/logs/sync_email.log"
PY="$TAILOR_DIR/.venv/bin/python3"

# Load API keys: env → /etc/tailor/env → plist (legacy fallback)
if [ -r /etc/tailor/env ]; then
    set -a
    . /etc/tailor/env
    set +a
fi
# Legacy plist fallback (deprecated — kept for environments not yet
# migrated to /etc/tailor/env)
if command -v plutil &>/dev/null && [ -f /Library/LaunchDaemons/com.tailor.mcp.plist ]; then
    export OPENAI_API_KEY="${OPENAI_API_KEY:-$(plutil -extract EnvironmentVariables.OPENAI_API_KEY raw /Library/LaunchDaemons/com.tailor.mcp.plist 2>/dev/null)}"
    export ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:-$(plutil -extract EnvironmentVariables.ANTHROPIC_API_KEY raw /Library/LaunchDaemons/com.tailor.mcp.plist 2>/dev/null)}"
    export GOOGLE_API_KEY="${GOOGLE_API_KEY:-$(plutil -extract EnvironmentVariables.GOOGLE_API_KEY raw /Library/LaunchDaemons/com.tailor.mcp.plist 2>/dev/null)}"
fi

SINCE=$(date -v-7d '+%Y/%m/%d')
echo "========================================" >> "$LOG"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] START sync_email" >> "$LOG"
cd "$TAILOR_DIR"

# Detect provider from YAML
PROVIDER=$("$PY" -c "
import sys; sys.path.insert(0,'scripts/lib')
from config import get, load_config; load_config()
print(get('email','provider') or 'gmail')
" 2>/dev/null)

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Provider: $PROVIDER" >> "$LOG"

# Step 1: Export
if [ "$PROVIDER" = "imap" ]; then
    export EMAIL_EXPORT_FILE="$TAILOR_DIR/data/gmail_export_imap.jsonl"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [1/5] IMAP export..." >> "$LOG"
    "$PY" scripts/gmail/export_imap.py --since "$SINCE" >> "$LOG" 2>&1
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [1/5] Gmail export..." >> "$LOG"
    "$PY" scripts/gmail/export_gmail.py --since "$SINCE" >> "$LOG" 2>&1
fi
[ $? -ne 0 ] && echo "[$(date '+%Y-%m-%d %H:%M:%S')] WARN: Export had errors" >> "$LOG"

# Steps 2-5 are the same regardless of provider
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [2/5] Triage..." >> "$LOG"
"$PY" scripts/gmail/triage_gmail.py run >> "$LOG" 2>&1

echo "[$(date '+%Y-%m-%d %H:%M:%S')] [3/5] Chunking..." >> "$LOG"
"$PY" scripts/gmail/chunk_gmail.py >> "$LOG" 2>&1

echo "[$(date '+%Y-%m-%d %H:%M:%S')] [4/5] Ingest..." >> "$LOG"
"$PY" scripts/gmail/ingest_gmail.py >> "$LOG" 2>&1

if [ -n "$ANTHROPIC_API_KEY" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [5/5] Entities..." >> "$LOG"
    "$PY" scripts/enrichment/extract_entities.py --backend anthropic --source email >> "$LOG" 2>&1
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] END sync_email" >> "$LOG"
