#!/bin/bash
# backup_db.sh — Backup notturno ChromaDB + file critici
# Cron: 02:30 (prima del sync_and_ingest delle 03:00)
# Tiene le ultime 3 copie compresse (~550MB ciascuna)

# ── single-instance lock (cron + launchd condividono questo) ──────────────
_LOCK_DIR="/tmp/tailor_backup_db.lock"
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
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] backup_db: another instance running, skipping" >&2
    exit 0
fi
trap 'rm -rf "$_LOCK_DIR"' EXIT INT TERM HUP
# ── end lock ──────────────────────────────────────────────────────────────

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

# Full DB snapshot (chroma + HNSW segments + facts + entities + lexical index
# + WAL sidecars + state json)
# Complements chroma_*.sqlite3.gz above: needed for disaster recovery where
# HNSW vector dirs and facts/entities/lexical SQLite would otherwise be lost.
# lexical_index.sqlite3 e' ricostruibile da Chroma via
# reconcile_lexical_index.py, ma il rebuild costa e senza snapshot un restore
# lascia la ricerca lessicale muta finche' non gira.
# sync_and_ingest.sh's auto-rollback still consumes chroma_*.sqlite3.gz —
# migrating that to use the tar.gz is a separate task.
HNSW_DIRS=$(cd "$TAILOR_DIR" && find db -maxdepth 1 -type d -regex 'db/[0-9a-f]\{8\}-[0-9a-f]\{4\}-[0-9a-f]\{4\}-[0-9a-f]\{4\}-[0-9a-f]\{12\}$' 2>/dev/null)

# Lista unica dei SQLite dello snapshot: i sidecar -wal/-shm qui sotto sono
# derivati da questa, cosi' non possono divergere dai file principali.
FULL_SQLITE="db/chroma.sqlite3 db/facts.sqlite3 db/entity_index.sqlite3 db/lexical_index.sqlite3 db/secrets.sqlite3"

# Sidecar WAL dei SQLite inclusi. facts.sqlite3 e secrets.sqlite3 girano in
# journal_mode=wal: li' le transazioni committate vivono nel -wal finche' un
# checkpoint non le travasa nel file principale. Un tar del solo principale
# quindi NON e' lo stato committato — e' lo stato all'ultimo checkpoint, e i
# commit successivi si perdono in silenzio al restore (per facts.sqlite3
# significa perdere i fatti estratti piu' recenti, o i marker di quarantena
# del purge, senza che nulla lo segnali).
# Il -shm e' ricostruibile e non servirebbe, ma e' minuscolo e tenerlo
# appaiato al -wal evita di ragionare su quale dei due basta.
# `ls ... 2>/dev/null` come per HNSW_DIRS: i -wal esistono solo se il db e'
# stato aperto in wal, e un file assente non deve far fallire tar.
# NB: e' comunque uno snapshot best-effort. La copia garantita-consistente di
# un SQLite vivo e' `VACUUM INTO` / `.backup`, non un tar; qui il backup gira
# a servizi fermi (o poco attivi) e il -wal serve a non buttare via l'ultimo
# tratto di lavoro.
# `|| true`: `ls` esce 1 se anche solo uno degli argomenti non esiste, ed e' il
# caso NORMALE qui (chroma/entity/lexical sono in journal_mode=delete e un
# -wal non ce l'hanno mai). Oggi lo script non ha `set -e` e l'exit status
# verrebbe ignorato, ma il giorno che qualcuno lo aggiunge questa riga
# abortirebbe il backup — e un backup che non parte si scopre tardi.
WAL_FILES=$(cd "$TAILOR_DIR" && ls $(for _f in $FULL_SQLITE; do echo "${_f}-wal" "${_f}-shm"; done) 2>/dev/null || true)

FULL_BACKUP="$BACKUP_DIR/tailor_db_full_${TIMESTAMP}.tar.gz"
if tar czf "$FULL_BACKUP" \
    -C "$TAILOR_DIR" \
    $FULL_SQLITE \
    $WAL_FILES \
    db/doc_registry.json \
    db/reminders.json \
    db/user_profile.json \
    db/user_profile_overrides.json \
    db/translations_it.json \
    db/supplement_plan.json \
    $HNSW_DIRS 2>> "$LOG_FILE"; then
    SIZE=$(du -h "$FULL_BACKUP" | awk '{print $1}')
    log "OK full snapshot -> tailor_db_full_${TIMESTAMP}.tar.gz ($SIZE)"
else
    log "ERROR full snapshot tar fallito"
fi

# Cleanup: keep only the last N copies
ls -t "$BACKUP_DIR"/chroma_*.sqlite3.gz 2>/dev/null | tail -n +$((KEEP+1)) | xargs rm -f 2>/dev/null
ls -t "$BACKUP_DIR"/doc_registry_*.json 2>/dev/null | tail -n +$((KEEP+1)) | xargs rm -f 2>/dev/null
ls -t "$BACKUP_DIR"/reminders_*.json 2>/dev/null | tail -n +$((KEEP+1)) | xargs rm -f 2>/dev/null
ls -t "$BACKUP_DIR"/tailor_db_full_*.tar.gz 2>/dev/null | tail -n +$((KEEP+1)) | xargs rm -f 2>/dev/null
log "Pulizia: mantenute ultime $KEEP copie"

log "END backup $TIMESTAMP"
