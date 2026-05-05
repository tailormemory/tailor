#!/bin/bash
# ingest_safe.sh — Manual safe ingest workaround for chromadb #6975
#
# Bypasses the "Failed to apply logs to the hnsw segment writer" segfault
# by running ingest_docs.py with MCP fully DOWN (sudo bootout) — no
# concurrent compactor → no HNSW segment writer SIGSEGV.
#
# Trade-off vs nightly sync_and_ingest.sh:
#   - Heavier: full backup + bootout/bootstrap (sudo) instead of SIGUSR1/2
#   - Slower restart: ~10s vs <1s for SIGUSR2
#   - Safer: zero MCP-process interference with chromadb during ingest
#
# Use until chromadb 1.5.8 #6975 is upstream-fixed or migration plan is
# decided (Qdrant / 1.4.x dump+restore / other).
#
# Flow:
#   1. Pre-flight (count baseline, queue baseline, files pending)
#   2. Backup db/ via rsync (skipped with --no-backup)
#   3. Stop MCP (sudo launchctl bootout)
#   4. Run ingest_docs.py [--full]
#   5. Audit post-ingest (count delta + repair_hnsw_index.py --audit)
#   6. Restart MCP (sudo launchctl bootstrap)
#   7. Smoke test (HTTP 200 on /api/dashboard/stats)
#   8. Telegram summary
#   9. Log to logs/ingest_safe_TIMESTAMP.log
#
# Usage:
#   ./ingest_safe.sh                  # incremental safe ingest (default)
#   ./ingest_safe.sh --dry-run        # pre-flight + status only, no changes
#   ./ingest_safe.sh --no-backup      # skip rsync (expert use)
#   ./ingest_safe.sh --full           # pass --full to ingest_docs.py

set -uo pipefail

# ── CONFIG ──
TAILOR_DIR="${TAILOR_HOME:-$(cd "$(dirname "$0")/../.." && pwd)}"
if [ ! -f "$TAILOR_DIR/mcp_server.py" ]; then
    echo "FATAL: cannot find TAILOR project root (tried $TAILOR_DIR)" >&2
    exit 1
fi
cd "$TAILOR_DIR"
export PYTHONPATH="$TAILOR_DIR:${PYTHONPATH:-}"

PYTHON="$TAILOR_DIR/.venv/bin/python3"
DB="$TAILOR_DIR/db/chroma.sqlite3"
COLLECTION_ID="9358d5d7-8b90-4ed2-882c-3309b6dacd1f"
MCP_PLIST="/Library/LaunchDaemons/com.tailor.mcp.plist"
MCP_SERVICE="system/com.tailor.mcp"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$TAILOR_DIR/logs/ingest_safe_${TIMESTAMP}.log"
BACKUP_DIR="$TAILOR_DIR/backups/db_pre_ingest_safe_${TIMESTAMP}"

# Load env (TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, API keys for ingest)
if [ -r /etc/tailor/env ]; then
    set -a
    . /etc/tailor/env
    set +a
fi
TG_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
TG_CHAT="${TELEGRAM_CHAT_ID:-}"

# CLI parsing
DRY_RUN=0
NO_BACKUP=0
FULL=0
for arg in "$@"; do
    case "$arg" in
        --dry-run)   DRY_RUN=1 ;;
        --no-backup) NO_BACKUP=1 ;;
        --full)      FULL=1 ;;
        -h|--help)
            sed -n '1,/^set -uo pipefail$/p' "$0" | sed -e '$d' -e 's/^# \?//'
            exit 0
            ;;
        *)
            echo "Unknown argument: $arg" >&2
            echo "Use --help for usage" >&2
            exit 2
            ;;
    esac
done

# ── HELPERS ──
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

bail() {
    log "BAIL OUT: $1"
    send_telegram "🚨 *ingest_safe BAIL OUT*: $1
Log: $LOG_FILE"
    exit 1
}

send_telegram() {
    if [ -z "$TG_TOKEN" ] || [ -z "$TG_CHAT" ]; then
        log "Telegram skipped: TG_TOKEN or TG_CHAT empty"
        return 0
    fi
    local text="$1"
    local url="https://api.telegram.org/bot${TG_TOKEN}/sendMessage"
    local payload http_code
    payload=$("$PYTHON" -c '
import json, sys
sys.stdout.write(json.dumps({"chat_id": sys.argv[1], "text": sys.argv[2], "parse_mode": "Markdown"}))
' "$TG_CHAT" "$text" 2>/dev/null) || return 1
    http_code=$(curl -s -o /dev/null -w "%{http_code}" \
        -X POST "$url" \
        -H "Content-Type: application/json" \
        --data-binary @- <<< "$payload" 2>/dev/null)
    if [ "$http_code" = "200" ]; then
        log "Telegram sent OK"
    else
        log "Telegram FAILED: http=$http_code"
    fi
}

mcp_pid() {
    pgrep -f "mcp_server\.py$" 2>/dev/null | head -1
}

mcp_count_running() {
    pgrep -fc "mcp_server\.py$" 2>/dev/null
}

sql_count() {
    sqlite3 "$DB" "$1" 2>/dev/null
}

# ── 0. INIT ──
mkdir -p "$TAILOR_DIR/logs"
log "================================================================"
log "START ingest_safe (dry_run=$DRY_RUN no_backup=$NO_BACKUP full=$FULL)"
log "Timestamp: $TIMESTAMP"
log "Log: $LOG_FILE"

START_TIME=$(date +%s)

# ── 1. PRE-FLIGHT ──
log "── STEP 1: pre-flight ──"

# MCP must be UP for accurate baseline counts via SQL
# (collection.count() inside MCP would also work, but SQL read-only is safer
#  and doesn't risk triggering #6975 on a stats endpoint)
COUNT_BASELINE=$(sql_count "SELECT COUNT(*) FROM embeddings;")
QUEUE_BASELINE=$(sql_count "SELECT COUNT(*) FROM embeddings_queue;")
log "  embeddings baseline: $COUNT_BASELINE"
log "  queue baseline: $QUEUE_BASELINE"

# Files pending — call ingest_docs.py --status
log "  scanning ingest queue (ingest_docs.py --status)..."
STATUS_OUT=$("$PYTHON" scripts/ingest/ingest_docs.py --status 2>&1)
STATUS_RC=$?
if [ $STATUS_RC -ne 0 ]; then
    log "WARNING: --status returned rc=$STATUS_RC, output: $(echo "$STATUS_OUT" | head -3)"
fi
# Extract counts from --status output (Italian labels: Nuovi/Modificati/Unchanged)
FILES_NEW=$(echo "$STATUS_OUT" | grep -E "^[[:space:]]*Nuovi:" | head -1 | grep -oE '[0-9]+' | head -1)
FILES_MODIFIED=$(echo "$STATUS_OUT" | grep -E "^[[:space:]]*Modificati:" | head -1 | grep -oE '[0-9]+' | head -1)
FILES_UNCHANGED=$(echo "$STATUS_OUT" | grep -E "^[[:space:]]*Unchanged:" | head -1 | grep -oE '[0-9]+' | head -1)
FILES_FOUND=$(echo "$STATUS_OUT" | grep -E "Trovati [0-9]+ file" | head -1 | grep -oE '[0-9]+' | head -1)
FILES_NEW=${FILES_NEW:-?}
FILES_MODIFIED=${FILES_MODIFIED:-?}
FILES_UNCHANGED=${FILES_UNCHANGED:-?}
FILES_FOUND=${FILES_FOUND:-?}
log "  files: found=$FILES_FOUND new=$FILES_NEW modified=$FILES_MODIFIED unchanged=$FILES_UNCHANGED"
# Save full --status block for the dry-run report
echo "--- ingest_docs.py --status (full output) ---" >> "$LOG_FILE"
echo "$STATUS_OUT" >> "$LOG_FILE"
echo "--- end --status ---" >> "$LOG_FILE"

# ── DRY-RUN: stop here ──
if [ "$DRY_RUN" = "1" ]; then
    log "── DRY-RUN: stopping after pre-flight ──"
    log "  baseline embeddings: $COUNT_BASELINE"
    log "  baseline queue: $QUEUE_BASELINE"
    log "  files found: $FILES_FOUND  new: $FILES_NEW  modified: $FILES_MODIFIED  unchanged: $FILES_UNCHANGED"
    log "DRY-RUN OK (no modifications)"
    echo
    echo "DRY-RUN summary:"
    echo "  embeddings: $COUNT_BASELINE"
    echo "  queue: $QUEUE_BASELINE"
    echo "  files: found=$FILES_FOUND new=$FILES_NEW modified=$FILES_MODIFIED unchanged=$FILES_UNCHANGED"
    echo "  log: $LOG_FILE"
    exit 0
fi

# ── 2. BACKUP ──
if [ "$NO_BACKUP" = "0" ]; then
    log "── STEP 2: backup db/ → $BACKUP_DIR ──"
    if ! mkdir -p "$BACKUP_DIR"; then
        bail "mkdir backup dir failed"
    fi
    BACKUP_START=$(date +%s)
    if ! rsync -a "$TAILOR_DIR/db/" "$BACKUP_DIR/" 2>>"$LOG_FILE"; then
        bail "rsync backup failed (rc=$?)"
    fi
    BACKUP_DURATION=$(( $(date +%s) - BACKUP_START ))
    BACKUP_SIZE=$(du -sh "$BACKUP_DIR" 2>/dev/null | cut -f1)
    log "  backup OK: $BACKUP_SIZE in ${BACKUP_DURATION}s"
else
    log "── STEP 2: backup SKIPPED (--no-backup) ──"
    BACKUP_DIR="(skipped)"
fi

# ── 3. STOP MCP ──
log "── STEP 3: stop MCP via sudo launchctl bootout ──"
if [ -n "$(mcp_pid)" ]; then
    OLD_PID=$(mcp_pid)
    log "  current MCP PID: $OLD_PID"
    if ! sudo launchctl bootout "$MCP_SERVICE" 2>>"$LOG_FILE"; then
        bail "sudo launchctl bootout failed"
    fi
    # Wait up to 30s for MCP to die
    for i in $(seq 1 30); do
        sleep 1
        if [ -z "$(mcp_pid)" ]; then
            log "  MCP DOWN after ${i}s"
            break
        fi
    done
    if [ -n "$(mcp_pid)" ]; then
        bail "MCP still UP after 30s — bootout failed silently"
    fi
else
    log "  MCP already DOWN (no action)"
fi

# ── 4. INGEST ──
log "── STEP 4: ingest_docs.py (full=$FULL) ──"
INGEST_ARGS=()
if [ "$FULL" = "1" ]; then
    INGEST_ARGS+=("--full")
fi
INGEST_START=$(date +%s)
INGEST_OUTPUT=$(PYTHONUNBUFFERED=1 "$PYTHON" scripts/ingest/ingest_docs.py "${INGEST_ARGS[@]}" 2>&1)
INGEST_RC=$?
INGEST_DURATION=$(( $(date +%s) - INGEST_START ))
echo "--- ingest_docs.py output ---" >> "$LOG_FILE"
echo "$INGEST_OUTPUT" >> "$LOG_FILE"
echo "--- end ingest_docs.py ---" >> "$LOG_FILE"
log "  ingest finished: rc=$INGEST_RC duration=${INGEST_DURATION}s"

if [ "$INGEST_RC" = "139" ]; then
    log "  CRITICAL: SIGSEGV (rc=139) — chromadb #6975 fired despite MCP DOWN"
    log "  This means the bug is NOT MCP-concurrency-induced. Re-evaluate strategy."
    # Continue to step 6 to restart MCP — leaving it down would compound the problem
    INGEST_FAILED=1
else
    INGEST_FAILED=0
fi

CHUNKS_ADDED=$(echo "$INGEST_OUTPUT" | grep "Chunk aggiunti:" | awk '{print $3}' | tr -d ',')
CHUNKS_ADDED=${CHUNKS_ADDED:-0}
FILES_PROCESSED=$(echo "$INGEST_OUTPUT" | grep "File processati:" | awk '{print $3}' | tr -d ',')
FILES_PROCESSED=${FILES_PROCESSED:-0}
log "  chunks added: $CHUNKS_ADDED, files processed: $FILES_PROCESSED"

# ── 5. AUDIT POST-INGEST ──
log "── STEP 5: audit drift HNSW vs SQL ──"
COUNT_POST=$(sql_count "SELECT COUNT(*) FROM embeddings;")
QUEUE_POST=$(sql_count "SELECT COUNT(*) FROM embeddings_queue;")
log "  embeddings: $COUNT_BASELINE → $COUNT_POST (delta: $((COUNT_POST - COUNT_BASELINE)))"
log "  queue: $QUEUE_BASELINE → $QUEUE_POST"

AUDIT_FILE="/tmp/ingest_safe_audit_${TIMESTAMP}.json"
"$PYTHON" scripts/maintenance/repair_hnsw_index.py --audit --json --no-history 2>/dev/null > "$AUDIT_FILE" || true
if [ -s "$AUDIT_FILE" ]; then
    AUDIT_SUMMARY=$("$PYTHON" -c "
import json
with open('$AUDIT_FILE') as f: d = json.load(f)
print(f\"sql={d.get('sql_total')} hnsw={d.get('hnsw_total')} queue={d.get('queue_total')} sql_only={len(d.get('sql_only_orphans', []))} ghosts={len(d.get('hnsw_only_ghosts', []))}\")
" 2>/dev/null)
    log "  $AUDIT_SUMMARY"
    SQL_ONLY=$("$PYTHON" -c "import json; print(len(json.load(open('$AUDIT_FILE')).get('sql_only_orphans', [])))" 2>/dev/null)
    SQL_ONLY=${SQL_ONLY:-?}
else
    log "  WARNING: audit JSON empty/failed"
    AUDIT_SUMMARY="(audit failed)"
    SQL_ONLY="?"
fi

# ── 6. RESTART MCP ──
log "── STEP 6: restart MCP via sudo launchctl bootstrap ──"
if ! sudo launchctl bootstrap system "$MCP_PLIST" 2>>"$LOG_FILE"; then
    log "CRITICAL: sudo launchctl bootstrap failed"
    send_telegram "🚨 *ingest_safe CRITICAL*: MCP restart failed after ingest. Manual intervention required.
Log: $LOG_FILE"
    exit 1
fi
sleep 8
if [ -z "$(mcp_pid)" ]; then
    log "CRITICAL: MCP not running after bootstrap"
    send_telegram "🚨 *ingest_safe CRITICAL*: MCP not running after bootstrap. Manual intervention required.
Log: $LOG_FILE"
    exit 1
fi
NEW_PID=$(mcp_pid)
log "  MCP UP, new PID: $NEW_PID"

# ── 7. SMOKE TEST ──
log "── STEP 7: smoke test (HTTP probe /api/dashboard/stats) ──"
SMOKE_OK=0
for i in $(seq 1 5); do
    HTTP_CODE=$(curl -s -o /tmp/ingest_safe_smoke.json -w "%{http_code}" \
        --max-time 30 \
        -H "Authorization: Bearer ${TAILOR_API_KEY:-}" \
        "http://127.0.0.1:8787/api/dashboard/stats" 2>/dev/null)
    if [ "$HTTP_CODE" = "200" ]; then
        SMOKE_OK=1
        log "  smoke OK on attempt $i (HTTP 200)"
        break
    fi
    log "  smoke attempt $i: HTTP=$HTTP_CODE — retry in 5s"
    sleep 5
done
if [ "$SMOKE_OK" = "0" ]; then
    log "WARNING: smoke test failed after 5 attempts (last HTTP=$HTTP_CODE)"
fi

# Final SQL coherence check
COUNT_FINAL=$(sql_count "SELECT COUNT(*) FROM embeddings;")
log "  final embeddings count: $COUNT_FINAL"

# ── 8. TELEGRAM SUMMARY ──
END_TIME=$(date +%s)
TOTAL_DURATION=$(( END_TIME - START_TIME ))

if [ "$INGEST_FAILED" = "1" ]; then
    ICON="🚨"
    STATUS="FAILED (SIGSEGV)"
elif [ "$SMOKE_OK" = "0" ]; then
    ICON="⚠️"
    STATUS="completed with smoke FAIL"
else
    ICON="✅"
    STATUS="completed"
fi

TG_MSG="${ICON} *ingest_safe ${STATUS}*

⏱ Duration: ${TOTAL_DURATION}s
📦 Backup: ${BACKUP_DIR}
📄 Files processed: ${FILES_PROCESSED}
📥 Chunks added: ${CHUNKS_ADDED}
📊 Embeddings: ${COUNT_BASELINE} → ${COUNT_FINAL} (Δ$((COUNT_FINAL - COUNT_BASELINE)))
🧷 Queue: ${QUEUE_BASELINE} → ${QUEUE_POST}
🔍 Audit: ${AUDIT_SUMMARY}
🖥 MCP: PID ${NEW_PID:-?} (smoke=$([ "$SMOKE_OK" = "1" ] && echo OK || echo FAIL))
📋 Log: ${LOG_FILE}"

send_telegram "$TG_MSG"

# ── 9. EXIT ──
log "END ingest_safe (${TOTAL_DURATION}s)"
log "================================================================"

if [ "$INGEST_FAILED" = "1" ]; then
    exit 1
elif [ "$SMOKE_OK" = "0" ]; then
    exit 2
fi
exit 0
