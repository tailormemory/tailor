#!/bin/bash
# backup_db.sh — Backup notturno ChromaDB + file critici
# Cron: 02:30 (prima del sync_and_ingest delle 03:00)
# Tiene le ultime 3 copie compresse (~550MB ciascuna)

TAILOR_DIR="${TAILOR_HOME:-$(cd "$(dirname "$0")/../.." && pwd)}"
BACKUP_DIR="$TAILOR_DIR/backups"
LOG_FILE="$TAILOR_DIR/logs/backup.log"
TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
KEEP=3

mkdir -p "$BACKUP_DIR"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

log "START backup $TIMESTAMP"

# Backup ChromaDB SQLite (compresso)
if gzip -c "$TAILOR_DIR/db/chroma.sqlite3" > "$BACKUP_DIR/chroma_${TIMESTAMP}.sqlite3.gz" 2>> "$LOG_FILE"; then
    SIZE=$(du -h "$BACKUP_DIR/chroma_${TIMESTAMP}.sqlite3.gz" | awk '{print $1}')
    log "OK chroma.sqlite3 -> chroma_${TIMESTAMP}.sqlite3.gz ($SIZE)"
else
    log "ERROR backup chroma.sqlite3 fallito"
fi

# Backup doc_registry e reminders (piccoli, non compressi)
cp "$TAILOR_DIR/db/doc_registry.json" "$BACKUP_DIR/doc_registry_${TIMESTAMP}.json" 2>> "$LOG_FILE"
cp "$TAILOR_DIR/db/reminders.json" "$BACKUP_DIR/reminders_${TIMESTAMP}.json" 2>> "$LOG_FILE"
log "OK doc_registry + reminders"

# Cleanup: keep only the last N copies
ls -t "$BACKUP_DIR"/chroma_*.sqlite3.gz 2>/dev/null | tail -n +$((KEEP+1)) | xargs rm -f 2>/dev/null
ls -t "$BACKUP_DIR"/doc_registry_*.json 2>/dev/null | tail -n +$((KEEP+1)) | xargs rm -f 2>/dev/null
ls -t "$BACKUP_DIR"/reminders_*.json 2>/dev/null | tail -n +$((KEEP+1)) | xargs rm -f 2>/dev/null
log "Pulizia: mantenute ultime $KEEP copie"

log "END backup $TIMESTAMP"
