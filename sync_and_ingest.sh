#!/bin/bash
# sync_and_ingest.sh — Pipeline notturna completa TAILOR
# Executed by cron every night at 3:00
#
# Flusso:
#   1. rclone sync (provider e cartelle dal YAML)
#   2. Stop MCP server
#   3. Garbage collection
#   4. Ingest nuovi documenti
#   5. Generate missing doc_summary
#   5b. Entity extraction (incremental)
#   5c. Rebuild entity index
#   5d. Rigenera user profile
#   5e. Fact extraction (multi-backend: Gemini + OpenAI, max 20k/notte)
#   5f. Fact supersession (confronto fact-vs-fact)
#   5g. Fact derivation (inferenze da cluster di fatti)
#   6. Restart MCP server
#   7. Notifica Telegram
#
# v3 - 2026-04-02: garbage collection + notifica Telegram riepilogativa
# v4 - 2026-04-04: entity extraction + index rebuild post-ingest
# v5 - 2026-04-04: user profile auto-regen post-pipeline
# v6 - 2026-04-05: atomic fact extraction (multi-backend) + fact supersession
# v8 - 2026-04-07: trap MCP restart on exit + timeout per step (60/30/30 min)
# v9 - 2026-04-07: rclone sync da YAML (multi-provider), fix path facts.sqlite3

# Resolve project root — verify by checking for mcp_server.py
TAILOR_DIR="${TAILOR_HOME:-$(cd "$(dirname "$0")" 2>/dev/null && pwd)}"
if [ ! -f "$TAILOR_DIR/mcp_server.py" ]; then
    # dirname failed (common in cron) — try script's absolute path
    TAILOR_DIR=$(cd "$(dirname "$(readlink -f "$0" 2>/dev/null || echo "$0")" )" 2>/dev/null && pwd)
fi
if [ ! -f "$TAILOR_DIR/mcp_server.py" ]; then
    echo "FATAL: cannot find TAILOR project root (tried $TAILOR_DIR). Set TAILOR_HOME in crontab." >&2
    exit 1
fi
cd "$TAILOR_DIR"
LOG_FILE="$TAILOR_DIR/logs/sync_and_ingest.log"
RCLONE="${RCLONE_PATH:-$(command -v rclone 2>/dev/null || echo /opt/homebrew/bin/rclone)}"
PYTHON="$TAILOR_DIR/.venv/bin/python3"
# Detect OS for service management
OS_TYPE="$(uname -s)"
MCP_PLIST="/Library/LaunchDaemons/com.tailor.mcp.plist"
MCP_SERVICE="tailor-mcp"  # systemd service name for Linux

# Load API keys from LaunchDaemon (macOS) or keep from environment
if command -v plutil &>/dev/null && [ -f "$MCP_PLIST" ]; then
    export ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:-$(/usr/bin/plutil -extract EnvironmentVariables.ANTHROPIC_API_KEY raw "$MCP_PLIST" 2>/dev/null)}"
    export OPENAI_API_KEY="${OPENAI_API_KEY:-$(/usr/bin/plutil -extract EnvironmentVariables.OPENAI_API_KEY raw "$MCP_PLIST" 2>/dev/null)}"
    export GOOGLE_API_KEY="${GOOGLE_API_KEY:-$(/usr/bin/plutil -extract EnvironmentVariables.GOOGLE_API_KEY raw "$MCP_PLIST" 2>/dev/null)}"
    TG_TOKEN="${TELEGRAM_BOT_TOKEN:-$(/usr/bin/plutil -extract EnvironmentVariables.TELEGRAM_BOT_TOKEN raw "$MCP_PLIST" 2>/dev/null)}"
    TG_CHAT="${TELEGRAM_CHAT_ID:-$(/usr/bin/plutil -extract EnvironmentVariables.TELEGRAM_CHAT_ID raw "$MCP_PLIST" 2>/dev/null)}"
else
    TG_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
    TG_CHAT="${TELEGRAM_CHAT_ID:-}"
fi

# Contatori
SYNC_ERRORS=0
GC_REMOVED=0
CHUNKS_ADDED=0
SUMMARIES_CREATED=0
ENTITIES_EXTRACTED=0
FACTS_EXTRACTED=0
FACTS_SUPERSEDED=0
FACTS_DERIVED=0
DERIVES_ENTITIES=0
MCP_STATUS="OK"
ERRORS=""

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

# Translate text via i18n (for user-facing Telegram messages)
translate() {
    "$PYTHON" -c "
import sys; sys.path.insert(0, '$TAILOR_DIR')
from scripts.lib.i18n import t
print(t(sys.stdin.read().strip()))
" <<< "$1" 2>/dev/null || echo "$1"
}

send_telegram() {
    if [ -n "$TG_TOKEN" ] && [ -n "$TG_CHAT" ]; then
        curl -s -X POST "https://api.telegram.org/bot${TG_TOKEN}/sendMessage" \
            -H "Content-Type: application/json" \
            -d "{\"chat_id\": \"${TG_CHAT}\", \"text\": \"$1\", \"parse_mode\": \"Markdown\"}" > /dev/null 2>&1
    fi
}

# Safety net: exit maintenance mode on ANY exit (crash, kill, timeout, success)
IN_MAINTENANCE=0
ensure_mcp_exit_maintenance() {
    if [ "$IN_MAINTENANCE" = "1" ]; then
        log "TRAP: exiting MCP maintenance mode..."
        MCP_PID=$(ps aux | grep mcp_server.py | grep -v grep | awk '{print $2}')
        if [ -n "$MCP_PID" ]; then
            kill -USR2 "$MCP_PID" 2>/dev/null
            sleep 2
            if [ -f "$TAILOR_DIR/maintenance.lock" ]; then
                log "TRAP: WARNING — maintenance.lock still present, removing manually"
                rm -f "$TAILOR_DIR/maintenance.lock"
            fi
            log "TRAP: MCP server exited maintenance mode"
        else
            log "TRAP: WARNING — MCP process not found, removing lock file"
            rm -f "$TAILOR_DIR/maintenance.lock"
        fi
    fi
}
trap ensure_mcp_exit_maintenance EXIT

echo "========================================" >> "$LOG_FILE"
log "START sync_and_ingest v9"
START_TIME=$(date +%s)
# Read pipeline config from YAML (with defaults)
DEADLINE_HOUR=$("$PYTHON" -c "
from scripts.lib.config import get
print(get('pipeline', 'deadline_hour', 8))
" 2>/dev/null || echo 8)
EXTRACTION_MAX_SEC=$("$PYTHON" -c "
from scripts.lib.config import get
print(int(get('pipeline', 'extraction_max_min', 200)) * 60)
" 2>/dev/null || echo 12000)
SUPERSESSION_MAX_SEC=$("$PYTHON" -c "
from scripts.lib.config import get
print(int(get('pipeline', 'supersession_max_min', 90)) * 60)
" 2>/dev/null || echo 5400)
DERIVATION_MAX_SEC=$("$PYTHON" -c "
from scripts.lib.config import get
print(int(get('pipeline', 'derivation_max_min', 60)) * 60)
" 2>/dev/null || echo 3600)
log "Pipeline config: deadline=${DEADLINE_HOUR}:00 extraction=$((EXTRACTION_MAX_SEC/60))m supersession=$((SUPERSESSION_MAX_SEC/60))m derivation=$((DERIVATION_MAX_SEC/60))m"
DEADLINE=$(date -v+1d -j -f "%H" "$DEADLINE_HOUR" +%s 2>/dev/null || date -d "tomorrow ${DEADLINE_HOUR}:00" +%s 2>/dev/null || echo $((START_TIME + 18000)))
# If we started before midnight, deadline is today not tomorrow
CURRENT_HOUR=$(date +%H)
if [ "$CURRENT_HOUR" -lt "$DEADLINE_HOUR" ]; then
    DEADLINE=$(date -j -f "%H" "$DEADLINE_HOUR" +%s 2>/dev/null || date -d "today ${DEADLINE_HOUR}:00" +%s 2>/dev/null || echo $((START_TIME + 18000)))
fi
log "Hard deadline: $(date -r $DEADLINE +%H:%M 2>/dev/null || date -d @$DEADLINE +%H:%M 2>/dev/null)"

# ── 0. STOP BACKGROUND JOBS CHE USANO CHROMADB ──
for SCRIPT in create_conv_summaries create_missing_summaries extract_entities build_entity_index extract_facts extract_facts_nightly fact_supersession derive_facts; do
    PIDS=$(pgrep -f "$SCRIPT" 2>/dev/null)
    if [ -n "$PIDS" ]; then
        log "Killing background job: $SCRIPT (PIDs: $PIDS)"
        kill -15 $PIDS 2>/dev/null
        sleep 15
        if kill -0 $PIDS 2>/dev/null; then
            kill -9 $PIDS 2>/dev/null
        fi
        sleep 1
    fi
done

# ── 1. RCLONE SYNC (legge provider e cartelle dal YAML) ──
log "Starting cloud sync"
SYNC_OUTPUT=$("$PYTHON" -c "
import yaml, os
cfg_path = os.path.join('$TAILOR_DIR', 'config', 'tailor.yaml')
with open(cfg_path) as f:
    cfg = yaml.safe_load(f)
cs = cfg.get('cloud_sync', [])
if isinstance(cs, dict):
    cs = [cs]  # backward compat: single provider
for provider in cs:
    rn = provider.get('remote_name', '')
    rr = provider.get('remote_root', '')
    lr = provider.get('local_root', '')
    for folder in provider.get('folders', []):
        remote = f'{rn}:{rr}/{folder}' if rr else f'{rn}:{folder}'
        local = os.path.join(lr, folder)
        print(f'{remote}|{local}')
" 2>&1)

while IFS='|' read -r REMOTE LOCAL; do
    [ -z "$REMOTE" ] && continue
    FNAME=$(basename "$LOCAL")
    log "rclone sync: $FNAME"
    if ! "$RCLONE" sync "$REMOTE" "$LOCAL" --log-level ERROR 2>> "$LOG_FILE"; then
        SYNC_ERRORS=$((SYNC_ERRORS + 1))
    fi
done <<< "$SYNC_OUTPUT"
log "rclone sync completed (errors: $SYNC_ERRORS)"

# ── 2. ENTER MAINTENANCE MODE ──
# Send SIGUSR1 to MCP server — it closes ChromaDB without dying
# No sudo needed: process runs as current user. No service manager interaction.
# KeepAlive/systemd never triggers because the process stays alive.
log "Entering MCP maintenance mode..."
MCP_PID=$(ps aux | grep mcp_server.py | grep -v grep | awk '{print $2}')
if [ -n "$MCP_PID" ]; then
    kill -USR1 "$MCP_PID" 2>> "$LOG_FILE"
    # Wait for maintenance.lock file (confirms ChromaDB is released)
    for i in $(seq 1 10); do
        sleep 1
        if [ -f "$TAILOR_DIR/maintenance.lock" ]; then
            log "MCP entered maintenance mode after ${i}s (PID $MCP_PID still alive)"
            break
        fi
    done
    if [ ! -f "$TAILOR_DIR/maintenance.lock" ]; then
        log "WARNING: maintenance.lock not created — MCP may not have received signal"
        log "Falling back to SIGTERM..."
        kill -15 "$MCP_PID" 2>> "$LOG_FILE"
        sleep 5
    fi
else
    log "No MCP server process found"
fi
IN_MAINTENANCE=1

# ── 2a. CHROMADB INTEGRITY CHECK ──
log "ChromaDB integrity check..."
CHROMA_CHECK=$("$PYTHON" -c "
import chromadb, sys
try:
    client = chromadb.PersistentClient(path='$TAILOR_DIR/db/chroma')
    col = client.get_collection('$(python3 -c "import yaml; c=yaml.safe_load(open('$TAILOR_DIR/config/tailor.yaml')); print(c.get('kb',{}).get('collection','tailor_kb'))" 2>/dev/null || echo tailor_kb)')
    count = col.count()
    if count == 0:
        print(f'EMPTY:0')
        sys.exit(1)
    print(f'OK:{count}')
except Exception as e:
    print(f'ERROR:{e}')
    sys.exit(1)
" 2>&1)

CHROMA_STATUS=$(echo "$CHROMA_CHECK" | head -1 | cut -d: -f1)
CHROMA_COUNT=$(echo "$CHROMA_CHECK" | head -1 | cut -d: -f2-)

if [ "$CHROMA_STATUS" != "OK" ]; then
    log "CRITICAL: ChromaDB integrity check FAILED — $CHROMA_CHECK"
    log "Aborting pipeline to prevent data loss. Restoring from backup..."
    # Find latest backup
    LATEST_BACKUP=$(ls -t "$TAILOR_DIR/backups"/chroma_*.sqlite3.gz 2>/dev/null | head -1)
    if [ -n "$LATEST_BACKUP" ]; then
        log "Restoring from $LATEST_BACKUP"
        gunzip -k "$LATEST_BACKUP" -c > "$TAILOR_DIR/db/chroma/chroma.sqlite3"
        log "Backup restored"
    else
        log "ERROR: No backup found!"
    fi
    ERRORS="${ERRORS}ChromaDB integrity check failed — pipeline aborted. "
    send_telegram "$(translate "🚨 *CRITICAL*: ChromaDB integrity check failed. Pipeline aborted. Backup restored automatically.")"
    # Still restart MCP and exit
    exit 1
fi
log "ChromaDB OK: $CHROMA_COUNT chunks"

# ── 2b. LOG ROTATION ──
log "Log rotation..."
LOG_DIR="$TAILOR_DIR/logs"
ROTATED=0
for logfile in "$LOG_DIR"/*.log; do
    SIZE=$(stat -f%z "$logfile" 2>/dev/null || echo 0)
    if [ "$SIZE" -gt 5242880 ]; then
        BASENAME=$(basename "$logfile")
        tail -1000 "$logfile" > "$logfile.tmp" && mv "$logfile.tmp" "$logfile"
        ROTATED=$((ROTATED + 1))
        log "  Troncato $BASENAME (era $(( SIZE / 1048576 ))MB)"
    fi
done
log "Log rotation completed: $ROTATED files truncated"

# ── 3. GARBAGE COLLECTION ──
log "Starting garbage collection"
GC_OUTPUT=$("$PYTHON" scripts/maintenance/garbage_collect.py 2>&1)
echo "$GC_OUTPUT" >> "$LOG_FILE"
GC_REMOVED=$(echo "$GC_OUTPUT" | sed -n 's/.*\([0-9][0-9]*\) file rimossi.*/\1/p' || echo "0")
log "Garbage collection completed: $GC_REMOVED removed"

# ── 4. INGEST DOCUMENTI ──
log "Starting document ingest"
cd "$TAILOR_DIR"
INGEST_OUTPUT=$("$PYTHON" scripts/ingest/ingest_docs.py 2>&1)
echo "$INGEST_OUTPUT" >> "$LOG_FILE"
CHUNKS_ADDED=$(echo "$INGEST_OUTPUT" | grep "Chunk aggiunti:" | awk '{print $3}')
CHUNKS_ADDED=${CHUNKS_ADDED:-0}
FILES_PROCESSED=$(echo "$INGEST_OUTPUT" | grep "File processati:" | awk '{print $3}')
FILES_PROCESSED=${FILES_PROCESSED:-0}
log "Ingest completed: $CHUNKS_ADDED chunks from $FILES_PROCESSED files"

# ── 5. GENERATE MISSING DOC_SUMMARY ──
if [ -n "$ANTHROPIC_API_KEY" ]; then
    log "Starting missing doc_summary generation"
    SUMMARY_OUTPUT=$("$PYTHON" scripts/enrichment/create_missing_summaries.py --workers 2 2>&1)
    echo "$SUMMARY_OUTPUT" >> "$LOG_FILE"
    SUMMARIES_CREATED=$(echo "$SUMMARY_OUTPUT" | sed -n 's/.*\([0-9][0-9]*\) summary creati.*/\1/p' || echo "0")
    SUMMARY_ERRORS=$(echo "$SUMMARY_OUTPUT" | sed -n 's/.*\([0-9][0-9]*\) errori.*/\1/p' || echo "0")
    log "Summaries completed: $SUMMARIES_CREATED created, $SUMMARY_ERRORS errors"
else
    log "WARNING: ANTHROPIC_API_KEY not found, skipping summary"
    ERRORS="${ERRORS}API key mancante. "
fi

# ── 5b. ENTITY EXTRACTION (incremental) ──
if [ -n "$OPENAI_API_KEY" ]; then
    log "Starting entity extraction (incremental)"
    ENTITY_OUTPUT=$("$PYTHON" scripts/enrichment/extract_entities.py --workers 10 2>&1)
    echo "$ENTITY_OUTPUT" >> "$LOG_FILE"
    ENTITIES_EXTRACTED=$(echo "$ENTITY_OUTPUT" | grep "Chunks processed:" | awk '{print $3}' | tr -d ',')
    ENTITIES_EXTRACTED=${ENTITIES_EXTRACTED:-0}
    log "Entity extraction completed: $ENTITIES_EXTRACTED chunks"
else
    log "WARNING: OPENAI_API_KEY not found, skipping entity extraction"
fi

# ── 5c. REBUILD ENTITY INDEX ──
log "Rebuild entity index"
INDEX_OUTPUT=$("$PYTHON" scripts/enrichment/build_entity_index.py 2>&1)
echo "$INDEX_OUTPUT" >> "$LOG_FILE"
INDEX_ENTITIES=$(echo "$INDEX_OUTPUT" | grep "Entities indexed:" | awk '{print $3}' | tr -d ',')
INDEX_ENTITIES=${INDEX_ENTITIES:-0}
log "Entity index: $INDEX_ENTITIES entities indexed"

# ── 5d. RIGENERA USER PROFILE ──
if [ -n "$ANTHROPIC_API_KEY" ]; then
    log "Rigenerazione user profile"
    PROFILE_OUTPUT=$("$PYTHON" scripts/enrichment/generate_user_profile.py 2>&1)
    echo "$PROFILE_OUTPUT" >> "$LOG_FILE"
    if echo "$PROFILE_OUTPUT" | grep -q 'PROFILO GENERATO'; then
        log "User profile rigenerato"
    else
        log "WARNING: rigenerazione profilo fallita"
    fi
fi

# ── 5e. ATOMIC FACT EXTRACTION (multi-backend, max 20k/notte) ──
log "Starting fact extraction (multi-backend)"
# Timeout 60 min
"$PYTHON" scripts/enrichment/extract_facts_nightly.py --workers 0 >> "$LOG_FILE" 2>&1 &
FACT_PID=$!
FACT_ELAPSED=0
while kill -0 $FACT_PID 2>/dev/null; do
    sleep 10
    FACT_ELAPSED=$((FACT_ELAPSED + 10))
    REMAINING=$((DEADLINE - $(date +%s) - SUPERSESSION_MAX_SEC - DERIVATION_MAX_SEC))  # leave room for supersession+derivation
        if [ "$FACT_ELAPSED" -ge "$EXTRACTION_MAX_SEC" ] || [ "$REMAINING" -le 0 ]; then
        log "TIMEOUT: fact extraction after $((FACT_ELAPSED/60)) min (deadline in $((REMAINING/60)) min) — killing"
        kill -15 $FACT_PID 2>/dev/null
        sleep 15
        if kill -0 $FACT_PID 2>/dev/null; then
            kill -9 $FACT_PID 2>/dev/null
        fi
        wait $FACT_PID 2>/dev/null
        ERRORS="${ERRORS}Timeout fact extraction. "
        break
    fi
done
wait $FACT_PID 2>/dev/null
FACT_OUTPUT=$(tail -20 "$LOG_FILE")
FACTS_EXTRACTED=$(echo "$FACT_OUTPUT" | grep "Facts extracted:" | awk '{print $3}' | tr -d ',')
FACTS_EXTRACTED=${FACTS_EXTRACTED:-0}
FACTS_CHUNKS=$(echo "$FACT_OUTPUT" | grep "Chunks processed:" | awk '{print $3}' | tr -d ',')
FACTS_CHUNKS=${FACTS_CHUNKS:-0}
FACTS_BACKEND=$(echo "$FACT_OUTPUT" | grep "Backend:" | sed 's/.*Backend: //')
log "Fact extraction completed: $FACTS_EXTRACTED facts from $FACTS_CHUNKS chunks"

# ── 5f. FACT SUPERSESSION (confronto fact-vs-fact sui nuovi fatti) ──
log "Starting fact supersession"
# Timeout 30 min
"$PYTHON" scripts/enrichment/fact_supersession.py --workers 5 >> "$LOG_FILE" 2>&1 &
SUP_PID=$!
SUP_ELAPSED=0
while kill -0 $SUP_PID 2>/dev/null; do
    sleep 10
    SUP_ELAPSED=$((SUP_ELAPSED + 10))
    REMAINING=$((DEADLINE - $(date +%s) - DERIVATION_MAX_SEC))  # leave room for derivation
        if [ "$SUP_ELAPSED" -ge "$SUPERSESSION_MAX_SEC" ] || [ "$REMAINING" -le 0 ]; then
        log "TIMEOUT: fact supersession after $((SUP_ELAPSED/60)) min (deadline in $((REMAINING/60)) min) — killing"
        kill -15 $SUP_PID 2>/dev/null
        sleep 15
        if kill -0 $SUP_PID 2>/dev/null; then
            kill -9 $SUP_PID 2>/dev/null
        fi
        wait $SUP_PID 2>/dev/null
        ERRORS="${ERRORS}Timeout fact supersession. "
        break
    fi
done
wait $SUP_PID 2>/dev/null
SUPERSESSION_OUTPUT=$(tail -20 "$LOG_FILE")
FACTS_SUPERSEDED=$(echo "$SUPERSESSION_OUTPUT" | grep "SUPERSEDES:" | awk '{print $2}' | tr -d ',')
FACTS_SUPERSEDED=${FACTS_SUPERSEDED:-0}
FACTS_COMPARED=$(echo "$SUPERSESSION_OUTPUT" | grep "Coppie confrontate:" | awk '{print $3}' | tr -d ',')
FACTS_COMPARED=${FACTS_COMPARED:-0}
log "Fact supersession completed: $FACTS_SUPERSEDED superseded out of $FACTS_COMPARED comparisons"
# ── 5g. FACT DERIVATION ──
log "Starting fact derivation (nightly)"
# Timeout 30 min
"$PYTHON" scripts/enrichment/derive_facts.py --nightly >> "$LOG_FILE" 2>&1 &
DER_PID=$!
DER_ELAPSED=0
while kill -0 $DER_PID 2>/dev/null; do
    sleep 10
    DER_ELAPSED=$((DER_ELAPSED + 10))
    REMAINING=$((DEADLINE - $(date +%s) - 300))  # leave 5min for cleanup
        if [ "$DER_ELAPSED" -ge "$DERIVATION_MAX_SEC" ] || [ "$REMAINING" -le 0 ]; then
        log "TIMEOUT: fact derivation after $((DER_ELAPSED/60)) min (deadline in $((REMAINING/60)) min) — killing"
        kill -15 $DER_PID 2>/dev/null
        sleep 15
        if kill -0 $DER_PID 2>/dev/null; then
            kill -9 $DER_PID 2>/dev/null
        fi
        wait $DER_PID 2>/dev/null
        ERRORS="${ERRORS}Timeout fact derivation. "
        break
    fi
done
wait $DER_PID 2>/dev/null
DERIVES_OUTPUT=$(tail -5 "$LOG_FILE")
# Last line is JSON: {"entities_processed": N, "facts_derived": N, "errors": N}
DERIVES_JSON=$(echo "$DERIVES_OUTPUT" | tail -1)
FACTS_DERIVED=$(echo "$DERIVES_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('facts_derived',0))" 2>/dev/null || echo "0")
DERIVES_ENTITIES=$(echo "$DERIVES_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('entities_processed',0))" 2>/dev/null || echo "0")
log "Fact derivation completed: $FACTS_DERIVED facts derived from $DERIVES_ENTITIES entities"

# ── 6. EXIT MAINTENANCE MODE ──
# Send SIGUSR2 to MCP server — it reopens ChromaDB and resumes normal operation
# No sudo needed. No service manager interaction.
log "Exiting MCP maintenance mode..."
MCP_PID=$(ps aux | grep mcp_server.py | grep -v grep | awk '{print $2}')
if [ -n "$MCP_PID" ]; then
    kill -USR2 "$MCP_PID" 2>> "$LOG_FILE"
    sleep 2
    if [ -f "$TAILOR_DIR/maintenance.lock" ]; then
        log "WARNING: maintenance.lock still present, removing manually"
        rm -f "$TAILOR_DIR/maintenance.lock"
    fi
    # Verify MCP responds
    MCP_RESPONDS=0
    for i in $(seq 1 5); do
        HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8787/ 2>/dev/null)
        if echo "$HTTP_CODE" | grep -q "^[2-4]"; then
            MCP_RESPONDS=1
            log "MCP server resumed OK (HTTP $HTTP_CODE)"
            break
        fi
        sleep 2
    done
    if [ "$MCP_RESPONDS" = "1" ]; then
        MCP_STATUS="OK"
    else
        log "WARNING: MCP not responding after maintenance exit"
        MCP_STATUS="ERROR"
        ERRORS="${ERRORS}MCP non risponde dopo maintenance. "
    fi
else
    log "WARNING: MCP process not found — may have crashed during maintenance"
    MCP_STATUS="ERROR"
    ERRORS="${ERRORS}MCP non trovato. "
    rm -f "$TAILOR_DIR/maintenance.lock"
fi
IN_MAINTENANCE=0

END_TIME=$(date +%s)
DURATION=$(( END_TIME - START_TIME ))

log "END sync_and_ingest v9 (${DURATION}s)"

# ── 7. NOTIFICA TELEGRAM ──
if [ -n "$ERRORS" ]; then
    ICON="⚠️"
    STATUS="with errors"
else
    ICON="✅"
    STATUS="completed"
fi

# Calcola stats facts totali
FACTS_TOTAL=$("$PYTHON" -c "
import sqlite3, os
db = os.path.join('$TAILOR_DIR', 'db', 'facts.sqlite3')
if os.path.exists(db):
    c = sqlite3.connect(db)
    total = c.execute('SELECT COUNT(*) FROM facts').fetchone()[0]
    import chromadb
    cc = chromadb.PersistentClient(path=os.path.join('$TAILOR_DIR', 'db', 'chroma'))
    total_chunks = cc.get_collection('tailor_kb').count()
    sup = c.execute('SELECT COUNT(*) FROM facts WHERE superseded_by IS NOT NULL').fetchone()[0]
    chunks = c.execute('SELECT COUNT(DISTINCT chunk_id) FROM facts').fetchone()[0]
    remaining = total_chunks - chunks
    c.close()
    print(f'{total}|{sup}|{chunks}|{remaining}')
else:
    print('0|0|0|155000')
" 2>/dev/null)

FACTS_DB_TOTAL=$(echo "$FACTS_TOTAL" | cut -d'|' -f1)
FACTS_DB_SUPERSEDED=$(echo "$FACTS_TOTAL" | cut -d'|' -f2)
FACTS_DB_CHUNKS=$(echo "$FACTS_TOTAL" | cut -d'|' -f3)
FACTS_DB_REMAINING=$(echo "$FACTS_TOTAL" | cut -d'|' -f4)

# Compose message in English, then translate
MSG_EN="${ICON} *Nightly pipeline ${STATUS}* (v9)

⏱ Duration: ${DURATION}s
📥 Sync: ${SYNC_ERRORS} errors
🗑 GC: ${GC_REMOVED} orphans removed
📄 Ingest: ${CHUNKS_ADDED} chunks (${FILES_PROCESSED} files)
🏷 Entities: ${ENTITIES_EXTRACTED} chunks extracted
📝 Summary: ${SUMMARIES_CREATED} created
🧠 Facts: +${FACTS_EXTRACTED} facts (${FACTS_CHUNKS} chunks)
🔄 Supersession: ${FACTS_SUPERSEDED} facts superseded (${FACTS_COMPARED} comparisons)
🔗 Derives: ${FACTS_DERIVED} inferred from ${DERIVES_ENTITIES} entities
🖥 MCP: ${MCP_STATUS}

📊 *KB total*: ${FACTS_DB_TOTAL} facts | ${FACTS_DB_SUPERSEDED} superseded | ${FACTS_DB_CHUNKS} chunks covered | ${FACTS_DB_REMAINING} remaining"

# Translate via i18n (uses persistent cache — near-zero cost after first run)
MSG=$(translate "$MSG_EN")

if [ -n "$ERRORS" ]; then
    MSG="${MSG}

⚠️ ${ERRORS}"
fi

send_telegram "$MSG"