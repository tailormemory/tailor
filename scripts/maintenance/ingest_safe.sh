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
#   4b. Run extract_entities.py (incremental, skipped with --no-entities)
#   4c. Run create_missing_summaries.py (incremental, skipped with --no-summaries)
#   5. Audit post-ingest (count delta + repair_hnsw_index.py --audit)
#   6. Restart MCP (sudo launchctl bootstrap)
#   7. Smoke test (HTTP 200 on /api/dashboard/stats)
#   7b. Snapshot retention (keep last KEEP_SNAPSHOTS, only on success, skipped with --no-backup)
#   8. Telegram summary
#   9. Log to logs/ingest_safe_TIMESTAMP.log
#
# Usage:
#   ./ingest_safe.sh                  # full safe pipeline: ingest + entities + summaries (default)
#   ./ingest_safe.sh --dry-run        # pre-flight + status only, no changes
#   ./ingest_safe.sh --no-backup      # skip rsync (expert use)
#   ./ingest_safe.sh --no-entities    # skip step 4b extract_entities
#   ./ingest_safe.sh --no-summaries   # skip step 4c create_missing_summaries
#   ./ingest_safe.sh --ingest-only    # only step 4 ingest_docs (= --no-entities --no-summaries)
#   ./ingest_safe.sh --full           # pass --full to ingest_docs.py + extract_entities.py

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
COLLECTION_ID="d6e49a14-f0e2-4e86-996f-95788e0413bf"
MCP_PLIST="/Library/LaunchDaemons/com.tailor.mcp.plist"
MCP_SERVICE="system/com.tailor.mcp"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$TAILOR_DIR/logs/ingest_safe_${TIMESTAMP}.log"
BACKUP_ROOT="$TAILOR_DIR/backups"
BACKUP_DIR="$BACKUP_ROOT/db_pre_ingest_safe_${TIMESTAMP}"

# Snapshot da conservare dopo un run RIUSCITO. La retention (step 7b) prune
# i più vecchi tenendo gli ultimi KEEP_SNAPSHOTS. Girata SOLO su successo:
# se l'ingest è andato male gli snapshot vecchi sono il path di recovery,
# non spazzatura. Con --no-backup non crea né cancella nulla.
KEEP_SNAPSHOTS=2

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
NO_ENTITIES=0
NO_SUMMARIES=0
FULL=0
for arg in "$@"; do
    case "$arg" in
        --dry-run)      DRY_RUN=1 ;;
        --no-backup)    NO_BACKUP=1 ;;
        --no-entities)  NO_ENTITIES=1 ;;
        --no-summaries) NO_SUMMARIES=1 ;;
        --ingest-only)  NO_ENTITIES=1; NO_SUMMARIES=1 ;;
        --full)         FULL=1 ;;
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
    # Send via Telegram, retry without Markdown on 400 (avoids underscore-
    # in-path parse errors). Pattern matches sync_and_ingest.sh.
    if [ -z "$TG_TOKEN" ] || [ -z "$TG_CHAT" ]; then
        log "Telegram skipped: TG_TOKEN or TG_CHAT empty"
        return 0
    fi
    local text="$1"
    local url="https://api.telegram.org/bot${TG_TOKEN}/sendMessage"
    local payload http_code resp_file body
    resp_file=$(mktemp -t tailor_tg.XXXXXX)
    payload=$("$PYTHON" -c '
import json, sys
sys.stdout.write(json.dumps({"chat_id": sys.argv[1], "text": sys.argv[2], "parse_mode": "Markdown"}))
' "$TG_CHAT" "$text" 2>/dev/null) || { rm -f "$resp_file"; return 1; }
    http_code=$(curl -s -o "$resp_file" -w "%{http_code}" \
        -X POST "$url" \
        -H "Content-Type: application/json" \
        --data-binary @- <<< "$payload" 2>/dev/null)
    body=$(cat "$resp_file" 2>/dev/null)
    if [ "$http_code" = "200" ]; then
        log "Telegram sent OK"
        rm -f "$resp_file"
        return 0
    fi
    if [ "$http_code" = "400" ]; then
        log "Telegram retry without markdown (400 parse error)"
        payload=$("$PYTHON" -c '
import json, sys
sys.stdout.write(json.dumps({"chat_id": sys.argv[1], "text": sys.argv[2]}))
' "$TG_CHAT" "$text" 2>/dev/null) || { rm -f "$resp_file"; return 1; }
        http_code=$(curl -s -o "$resp_file" -w "%{http_code}" \
            -X POST "$url" \
            -H "Content-Type: application/json" \
            --data-binary @- <<< "$payload" 2>/dev/null)
        body=$(cat "$resp_file" 2>/dev/null)
        if [ "$http_code" = "200" ]; then
            log "Telegram sent OK (plain)"
            rm -f "$resp_file"
            return 0
        fi
    fi
    log "Telegram FAILED: http=$http_code body=$body"
    rm -f "$resp_file"
    return 1
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

prune_snapshots() {
    # Retention per le dir di backup db_pre_ingest_safe_*.
    # Args: $1 = backups root (path ASSOLUTO), $2 = quante tenere.
    #
    # Ordinamento per timestamp nel NOME (glob → ordine lessicografico, e con
    # naming a larghezza fissa YYYYmmdd_HHMMSS lessicografico = cronologico),
    # NON per mtime — l'mtime può essere riscritto da rsync/touch/restore e
    # mentire sull'ordine di creazione.
    #
    # Guardia: glob assoluto costruito da $root, mai cd + glob relativo; e
    # ogni candidato è ri-filtrato sul basename esatto db_pre_ingest_safe_*
    # così una root sbagliata non può far cancellare altro. nullglob → se non
    # matcha nulla il loop non gira (niente pattern literal).
    local root="$1" keep="$2"
    local snaps=() d
    shopt -s nullglob
    for d in "$root"/db_pre_ingest_safe_*/; do
        d="${d%/}"
        [ -d "$d" ] || continue
        case "$(basename "$d")" in
            db_pre_ingest_safe_*) snaps+=("$d") ;;
        esac
    done
    shopt -u nullglob

    local total=${#snaps[@]}
    if [ "$total" -le "$keep" ]; then
        log "  retention: $total snapshot ≤ keep=$keep, niente da rimuovere"
        return 0
    fi

    # snaps è già in ordine lessicografico (glob) = cronologico: i primi
    # (total-keep) sono i più vecchi da rimuovere.
    local remove=$(( total - keep ))
    log "  retention: $total snapshot, keep=$keep → rimuovo i $remove più vecchi"
    local i freed
    for (( i = 0; i < remove; i++ )); do
        d="${snaps[$i]}"
        freed=$(du -sh "$d" 2>/dev/null | cut -f1)
        if rm -rf "$d" 2>>"$LOG_FILE"; then
            log "  retention: rimosso $(basename "$d") (liberati ${freed:-?})"
        else
            log "  retention: WARNING rm fallito su $d"
        fi
    done
    return 0
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
INGEST_START=$(date +%s)
# NB: bash 3.2 (macOS default) errors on `"${arr[@]}"` when arr is empty
# under `set -u`. Branch on FULL flag instead of using an array.
if [ "$FULL" = "1" ]; then
    INGEST_OUTPUT=$(PYTHONUNBUFFERED=1 "$PYTHON" scripts/ingest/ingest_docs.py --full 2>&1)
else
    INGEST_OUTPUT=$(PYTHONUNBUFFERED=1 "$PYTHON" scripts/ingest/ingest_docs.py 2>&1)
fi
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

# ── 4b. EXTRACT ENTITIES (incremental) ──
# Run extract_entities on the chunks just added so they're searchable by
# entity filter (kb_search_by_entity etc.) immediately. Same MCP-down
# window — no extra coordination needed. Uses defensive try/except on
# paginated read (commit 9b2e2bc) so corrupted batches skip gracefully.
log "── STEP 4b: extract_entities.py (incremental) ──"
ENTITIES_PROCESSED=0
ENTITIES_FAILED=0
if [ "$INGEST_FAILED" = "1" ]; then
    log "  SKIPPED: ingest failed (rc=139), not running entities to avoid compounding"
elif [ "$NO_ENTITIES" = "1" ]; then
    log "  SKIPPED: --no-entities flag"
else
    ENTITIES_START=$(date +%s)
    if [ "$FULL" = "1" ]; then
        ENTITIES_OUTPUT=$(PYTHONUNBUFFERED=1 "$PYTHON" scripts/enrichment/extract_entities.py --full 2>&1)
    else
        ENTITIES_OUTPUT=$(PYTHONUNBUFFERED=1 "$PYTHON" scripts/enrichment/extract_entities.py 2>&1)
    fi
    ENTITIES_RC=$?
    ENTITIES_DURATION=$(( $(date +%s) - ENTITIES_START ))
    echo "--- extract_entities.py output ---" >> "$LOG_FILE"
    echo "$ENTITIES_OUTPUT" >> "$LOG_FILE"
    echo "--- end extract_entities.py ---" >> "$LOG_FILE"
    log "  entities finished: rc=$ENTITIES_RC duration=${ENTITIES_DURATION}s"
    # Extract count of processed chunks (script prints "Chunks processed: N")
    ENTITIES_PROCESSED=$(echo "$ENTITIES_OUTPUT" | grep -E "Chunks processed:" | tail -1 | awk '{print $3}' | tr -d ',')
    ENTITIES_PROCESSED=${ENTITIES_PROCESSED:-0}
    log "  entities: $ENTITIES_PROCESSED chunks tagged"
    if [ "$ENTITIES_RC" = "139" ]; then
        log "  CRITICAL: SIGSEGV in extract_entities — chromadb #6975 cascade"
        ENTITIES_FAILED=1
    elif [ "$ENTITIES_RC" -ne 0 ]; then
        log "  WARNING: extract_entities rc=$ENTITIES_RC (defensive try/except on read may have triggered)"
    fi
fi

# ── 4c. CREATE MISSING SUMMARIES (incremental) ──
# Generate doc_summary chunks for files in registry that don't have one yet.
# Same MCP-down window as ingest + entities. Anthropic Haiku via API.
# Each summary = 1 col.add() — chromadb write, hence kept in MCP-down window.
log "── STEP 4c: create_missing_summaries.py (incremental) ──"
SUMMARIES_CREATED=0
SUMMARIES_FAILED=0
if [ "$INGEST_FAILED" = "1" ] || [ "$ENTITIES_FAILED" = "1" ]; then
    log "  SKIPPED: prior step failed (rc=139), not running summaries to avoid compounding"
elif [ "$NO_SUMMARIES" = "1" ]; then
    log "  SKIPPED: --no-summaries flag"
elif [ -z "${ANTHROPIC_API_KEY:-}" ]; then
    log "  SKIPPED: ANTHROPIC_API_KEY not set in env"
else
    SUMMARIES_START=$(date +%s)
    SUMMARIES_OUTPUT=$(PYTHONUNBUFFERED=1 "$PYTHON" scripts/enrichment/create_missing_summaries.py --workers 2 2>&1)
    SUMMARIES_RC=$?
    SUMMARIES_DURATION=$(( $(date +%s) - SUMMARIES_START ))
    echo "--- create_missing_summaries.py output ---" >> "$LOG_FILE"
    echo "$SUMMARIES_OUTPUT" >> "$LOG_FILE"
    echo "--- end create_missing_summaries.py ---" >> "$LOG_FILE"
    log "  summaries finished: rc=$SUMMARIES_RC duration=${SUMMARIES_DURATION}s"
    # Extract count: script prints "DONE: N summary creati, M errori"
    SUMMARIES_CREATED=$(echo "$SUMMARIES_OUTPUT" | sed -n 's/.*DONE: \([0-9][0-9]*\) summary creati.*/\1/p' | tail -1)
    SUMMARIES_CREATED=${SUMMARIES_CREATED:-0}
    log "  summaries: $SUMMARIES_CREATED created"
    if [ "$SUMMARIES_RC" = "139" ]; then
        log "  CRITICAL: SIGSEGV in create_missing_summaries — chromadb #6975 cascade"
        SUMMARIES_FAILED=1
    elif [ "$SUMMARIES_RC" -ne 0 ]; then
        log "  WARNING: create_missing_summaries rc=$SUMMARIES_RC"
    fi
fi

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

# ── 7b. SNAPSHOT RETENTION ──
# Solo su run RIUSCITO: se l'ingest è fallito (SIGSEGV) gli snapshot vecchi
# sono il path di recovery. bail() e i critical-exit precedenti non arrivano
# fin qui, quindi non prunano mai. Uno smoke-fail (exit 2) non è un fallimento
# d'ingest: i dati sono integri, la retention è sicura.
if [ "$NO_BACKUP" = "1" ]; then
    log "── STEP 7b: retention SKIPPED (--no-backup: non ha creato nulla) ──"
elif [ "$INGEST_FAILED" = "1" ] || [ "$ENTITIES_FAILED" = "1" ] || [ "$SUMMARIES_FAILED" = "1" ]; then
    log "── STEP 7b: retention SKIPPED (run fallito, snapshot vecchi preservati) ──"
else
    log "── STEP 7b: snapshot retention (keep last $KEEP_SNAPSHOTS) ──"
    prune_snapshots "$BACKUP_ROOT" "$KEEP_SNAPSHOTS"
fi

# ── 8. TELEGRAM SUMMARY ──
END_TIME=$(date +%s)
TOTAL_DURATION=$(( END_TIME - START_TIME ))

if [ "$INGEST_FAILED" = "1" ] || [ "$ENTITIES_FAILED" = "1" ] || [ "$SUMMARIES_FAILED" = "1" ]; then
    ICON="🚨"
    STATUS="FAILED (SIGSEGV)"
elif [ "$SMOKE_OK" = "0" ]; then
    ICON="⚠️"
    STATUS="completed with smoke FAIL"
else
    ICON="✅"
    STATUS="completed"
fi

if [ "$NO_ENTITIES" = "1" ]; then
    ENTITIES_LINE="🏷 Entities: SKIPPED (--no-entities)"
elif [ "$INGEST_FAILED" = "1" ]; then
    ENTITIES_LINE="🏷 Entities: SKIPPED (ingest failed)"
else
    ENTITIES_LINE="🏷 Entities: ${ENTITIES_PROCESSED} chunks tagged"
fi

if [ "$NO_SUMMARIES" = "1" ]; then
    SUMMARIES_LINE="📝 Summaries: SKIPPED (--no-summaries)"
elif [ "$INGEST_FAILED" = "1" ] || [ "$ENTITIES_FAILED" = "1" ]; then
    SUMMARIES_LINE="📝 Summaries: SKIPPED (prior step failed)"
elif [ -z "${ANTHROPIC_API_KEY:-}" ]; then
    SUMMARIES_LINE="📝 Summaries: SKIPPED (ANTHROPIC_API_KEY not set)"
else
    SUMMARIES_LINE="📝 Summaries: ${SUMMARIES_CREATED} created"
fi

TG_MSG="${ICON} *ingest_safe ${STATUS}*

⏱ Duration: ${TOTAL_DURATION}s
📦 Backup: ${BACKUP_DIR}
📄 Files processed: ${FILES_PROCESSED}
📥 Chunks added: ${CHUNKS_ADDED}
${ENTITIES_LINE}
${SUMMARIES_LINE}
📊 Embeddings: ${COUNT_BASELINE} → ${COUNT_FINAL} (Δ$((COUNT_FINAL - COUNT_BASELINE)))
🧷 Queue: ${QUEUE_BASELINE} → ${QUEUE_POST}
🔍 Audit: ${AUDIT_SUMMARY}
🖥 MCP: PID ${NEW_PID:-?} (smoke=$([ "$SMOKE_OK" = "1" ] && echo OK || echo FAIL))
📋 Log: ${LOG_FILE}"

send_telegram "$TG_MSG"

# ── 9. EXIT ──
log "END ingest_safe (${TOTAL_DURATION}s)"
log "================================================================"

if [ "$INGEST_FAILED" = "1" ] || [ "$ENTITIES_FAILED" = "1" ] || [ "$SUMMARIES_FAILED" = "1" ]; then
    exit 1
elif [ "$SMOKE_OK" = "0" ]; then
    exit 2
fi
exit 0
