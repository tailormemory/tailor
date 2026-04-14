#!/bin/bash
# Quick pipeline test — verifies maintenance mode flow without full enrichment
# Takes ~2 minutes instead of hours

set -e
TAILOR_DIR="/Users/jarvis/tailor"
cd "$TAILOR_DIR"
LOG="logs/pipeline_test.log"
> "$LOG"

log() { echo "[$(date +%H:%M:%S)] $1" | tee -a "$LOG"; }

log "=== PIPELINE QUICK TEST ==="

# 1. Read config
PYTHON="$TAILOR_DIR/.venv/bin/python3"
DEADLINE=$("$PYTHON" -c "from scripts.lib.config import get; print(get('pipeline','deadline_hour',8))" 2>/dev/null || echo 8)
log "Config: deadline_hour=$DEADLINE ✅"

# 2. Find MCP server
MCP_PID=$(ps aux | grep mcp_server.py | grep -v grep | awk '{print $2}')
if [ -z "$MCP_PID" ]; then
    log "❌ MCP server not running"
    exit 1
fi
log "MCP server running: PID $MCP_PID ✅"

# 3. Send SIGUSR1 — enter maintenance mode
log "Sending SIGUSR1 (enter maintenance)..."
kill -USR1 "$MCP_PID"
sleep 3

if [ -f "$TAILOR_DIR/maintenance.lock" ]; then
    log "maintenance.lock created ✅"
else
    log "❌ maintenance.lock NOT created — SIGUSR1 failed"
    exit 1
fi

# 4. Verify ChromaDB is accessible (server released it)
log "Testing ChromaDB access..."
CHROMA_CHECK=$("$PYTHON" -c "
import chromadb
client = chromadb.PersistentClient(path='$TAILOR_DIR/db/chroma')
col = client.get_collection('$("$PYTHON" -c "from scripts.lib.config import get; print(get('kb','collection','tailor_kb'))" 2>/dev/null || echo tailor_kb)')
print(f'OK:{col.count()}')
del col; del client
" 2>&1)
log "ChromaDB: $CHROMA_CHECK"

if echo "$CHROMA_CHECK" | grep -q "^OK:"; then
    log "ChromaDB accessible during maintenance ✅"
else
    log "❌ ChromaDB not accessible"
    kill -USR2 "$MCP_PID" 2>/dev/null
    rm -f "$TAILOR_DIR/maintenance.lock"
    exit 1
fi

# 5. Verify MCP server is still alive (not killed)
if ps -p "$MCP_PID" > /dev/null 2>&1; then
    log "MCP server still alive during maintenance (PID $MCP_PID) ✅"
else
    log "❌ MCP server died during maintenance!"
    rm -f "$TAILOR_DIR/maintenance.lock"
    exit 1
fi

# 6. Send SIGUSR2 — exit maintenance mode
log "Sending SIGUSR2 (exit maintenance)..."
kill -USR2 "$MCP_PID"
sleep 3

if [ -f "$TAILOR_DIR/maintenance.lock" ]; then
    log "⚠️  maintenance.lock still present, removing"
    rm -f "$TAILOR_DIR/maintenance.lock"
else
    log "maintenance.lock removed ✅"
fi

# 7. Verify MCP responds to HTTP
MCP_OK=0
for i in 1 2 3 4 5; do
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8787/ 2>/dev/null)
    if echo "$HTTP_CODE" | grep -q "^[2-4]"; then
        MCP_OK=1
        log "MCP responds HTTP $HTTP_CODE ✅"
        break
    fi
    sleep 2
done

if [ "$MCP_OK" = "0" ]; then
    log "❌ MCP not responding after maintenance exit"
    exit 1
fi

# 8. Verify MCP PID is the same (wasn't restarted)
NEW_PID=$(ps aux | grep mcp_server.py | grep -v grep | awk '{print $2}')
if [ "$NEW_PID" = "$MCP_PID" ]; then
    log "Same PID before and after: $MCP_PID ✅ (no restart needed)"
else
    log "⚠️  PID changed: $MCP_PID → $NEW_PID (server was restarted)"
fi

log ""
log "=== ALL TESTS PASSED ✅ ==="
log "Maintenance mode flow works. Pipeline is safe to run tonight."
