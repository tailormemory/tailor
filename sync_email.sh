#!/bin/bash
# sync_email.sh — Unified email sync: Gmail API or IMAP based on config
# Replaces sync_gmail.sh in cron. Falls back gracefully.
#
# FAIL-LOUD (2026-07-14) — lezione del freeze da 60 notti:
#   * Triage passato da `run` (path Batch API, stato TERMINALE per-batch,
#     nessuna nozione di msg_id già triagiato → radice del freeze) a
#     `run-sync --resume` (progress per-msg_id, incrementale, fail-loud:
#     commit c1cc972 / a2992dd / 4985e5d).
#   * OGNI step controlla l'exit code e ABORTA con exit non-zero se fallisce:
#     un triage/export rotto NON deve più far proseguire chunk/ingest su dati
#     vecchi riportando successo.
#   * Guardia anti-silenzio a TRE strati, post-ingest (legge la KB da sqlite
#     in mode=ro, nessun client ChromaDB; riusa le funzioni reali di
#     chunk_gmail.py — non le reimplementa). Insieme, Layer C + Layer A
#     chiudono per enumerazione la catena triage→chunks→KB a ogni run;
#     Layer B è la rete di sicurezza, non il controllo primario.
#       Layer A (exit 10) — invariante chunks→KB: dopo l'ingest OGNI chunk_id
#                 prodotto da chunk_gmail DEVE essere in KB. Se ne manca uno →
#                 FATAL (silent-drop ingest).
#       Layer C (exit 12) — invariante triage→chunks: OGNI email di
#                 gmail_triage.jsonl con useful=true che passa i filtri di
#                 chunk_gmail (should_keep: sender OR unico TO) e che dopo
#                 strip_quoted ha ancora testo DEVE avere >=1 chunk in
#                 chunks_gmail.jsonl. Se ne manca una → FATAL (perdita
#                 silenziosa nel chunking, che Layer A da solo non vedrebbe:
#                 gli basta che il chunk superstite sia in KB).
#       Layer B (exit 11) — heartbeat stateful: se il conteggio chunk email_*
#                 in KB non cresce per N notti consecutive MENTRE l'export
#                 porta id non ancora in KB → FATAL. Rete di sicurezza contro
#                 il freeze silenzioso. N = SYNC_STALE_NIGHTS_MAX (default 3:
#                 sulla mailbox reale gli zero-run normali sono <=2 giorni,
#                 ~6 email/giorno; l'unico zero-run lungo fu il guasto). Falso
#                 allarme = un rm; falso silenzio = 61 giorni. Bias: gridare a
#                 vuoto, mai restare ciechi.

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
DB="$TAILOR_DIR/db/chroma.sqlite3"
STATE="$TAILOR_DIR/data/.sync_email_heartbeat.json"
# Segnale WARN rilevabile per lo step Entities (non-bloccante ma non da
# seppellire): marker JSON self-clearing — presente = c'è un warning attivo.
WARN_FILE="$TAILOR_DIR/data/.sync_email_warn.json"
STALE_MAX="${SYNC_STALE_NIGHTS_MAX:-3}"

# ── fail-loud helper: logga su LOG + stderr (cron/launchd) e aborta ────────
# Il trap EXIT rimuove comunque il lock.
fatal() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] FATAL: $*"
    echo "$msg" >> "$LOG"
    echo "$msg" >&2
    exit 1
}

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
cd "$TAILOR_DIR" || fatal "cd $TAILOR_DIR fallito"

# Detect provider from YAML
PROVIDER=$("$PY" -c "
import sys; sys.path.insert(0,'scripts/lib')
from config import get, load_config; load_config()
print(get('email','provider') or 'gmail')
" 2>>"$LOG")
rc=$?
[ $rc -ne 0 ] && fatal "Provider detection fallita (exit $rc) — config non caricabile"
[ -z "$PROVIDER" ] && fatal "Provider vuoto dalla config"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Provider: $PROVIDER" >> "$LOG"

# Step 1: Export
if [ "$PROVIDER" = "imap" ]; then
    export EMAIL_EXPORT_FILE="$TAILOR_DIR/data/gmail_export_imap.jsonl"
    EXPORT_FILE="$EMAIL_EXPORT_FILE"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [1/5] IMAP export..." >> "$LOG"
    "$PY" scripts/gmail/export_imap.py --since "$SINCE" >> "$LOG" 2>&1
    rc=$?
else
    EXPORT_FILE="$TAILOR_DIR/data/gmail_export.jsonl"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [1/5] Gmail export..." >> "$LOG"
    "$PY" scripts/gmail/export_gmail.py --since "$SINCE" >> "$LOG" 2>&1
    rc=$?
fi
# FAIL-LOUD: un export rotto non deve far girare triage/chunk/ingest su dati
# vecchi (era la vecchia WARN silenziosa che lasciava proseguire la pipeline).
[ $rc -ne 0 ] && fatal "Export fallito (exit $rc) — STOP, niente triage/chunk/ingest a valle"

# Step 2: Triage — run-sync --resume: incrementale per-msg_id, fail-loud.
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [2/5] Triage (run-sync --resume)..." >> "$LOG"
"$PY" scripts/gmail/triage_gmail.py run-sync --resume >> "$LOG" 2>&1
rc=$?
[ $rc -ne 0 ] && fatal "Triage fallito (exit $rc) — STOP, niente chunk/ingest su dati vecchi"

# Step 3: Chunking
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [3/5] Chunking..." >> "$LOG"
"$PY" scripts/gmail/chunk_gmail.py >> "$LOG" 2>&1
rc=$?
[ $rc -ne 0 ] && fatal "Chunking fallito (exit $rc) — STOP, niente ingest"

# Step 4: Ingest
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [4/5] Ingest..." >> "$LOG"
"$PY" scripts/gmail/ingest_gmail.py >> "$LOG" 2>&1
rc=$?
[ $rc -ne 0 ] && fatal "Ingest fallito (exit $rc)"

# ── GUARDIA ANTI-SILENZIO (Layer A + Layer C + Layer B) ───────────────────
# Verifica per-run che RIUSA le funzioni reali di chunk_gmail.py (should_keep,
# strip_quoted, dedup_by_thread) via import — NON reimplementa la pipeline,
# così non può divergere e mentire. Apre sqlite una sola volta, post-ingest.
# Exit: 0 ok · 10 Layer A · 12 Layer C · 11 Layer B · altro = errore.
verify_out=$("$PY" - "$DB" "$EXPORT_FILE" "$STATE" "$STALE_MAX" "$TAILOR_DIR" <<'PYEOF' 2>>"$LOG"
import sys, os, json, sqlite3, datetime
db, export_f, state_f, stale_max, base = sys.argv[1:6]
stale_max = int(stale_max)

# Riusa le funzioni REALI del chunker (no duplicazione). L'import non lancia
# process() (solo sotto __main__): calcola INPUT_FILE/OUTPUT_FILE dai path reali.
sys.path.insert(0, os.path.join(base, "scripts", "gmail"))
import chunk_gmail as cg
triage_f = cg.INPUT_FILE
chunks_f = cg.OUTPUT_FILE

# --- KB: una sola apertura sqlite (mode=ro, no client Chroma) ---
con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
kb_ids = set(r[0] for r in con.execute(
    "SELECT embedding_id FROM embeddings "
    "WHERE embedding_id LIKE 'email\\_%' ESCAPE '\\'"))
post_kb = len(kb_ids)

# --- chunks_gmail.jsonl: chunk_id + email_id effettivamente prodotti ---
chunk_ids = set()
present_email_ids = set()
with open(chunks_f) as fh:
    for line in fh:
        line = line.strip()
        if not line:
            continue
        c = json.loads(line)
        cid = c.get("chunk_id")
        if cid:
            chunk_ids.add(cid)
        eid = (c.get("metadata") or {}).get("email_id")
        if eid:
            present_email_ids.add(eid)

# --- Layer A — ogni chunk_id prodotto da chunk_gmail deve essere in KB ---
missing_a = sorted(chunk_ids - kb_ids)

# --- Layer C — enumerazione triage→chunks con le funzioni reali del chunker:
# ogni email useful=true che passa should_keep e ha testo dopo strip_quoted
# DEVE avere >=1 chunk (email_id presente in chunks_gmail.jsonl). ---
emails = []
with open(triage_f) as fh:
    for line in fh:
        line = line.strip()
        if line:
            emails.append(json.loads(line))
kept = [e for e in emails if cg.should_keep(e)]
deduped = cg.dedup_by_thread(kept)
expected_email_ids = set()
for e in deduped:
    if cg.strip_quoted(e.get("body", "")).strip():
        eid = e.get("id")
        if eid:
            expected_email_ids.add(eid)
missing_c = sorted(expected_email_ids - present_email_ids)

# --- Layer B — export id senza rappresentazione in KB (rete di sicurezza).
# NB: noise/filtrate non diventano mai chunk → export_new quasi sempre >0;
# in pratica la guardia si riduce a "KB email piatta per N notti". Voluto. ---
kb_prefixes = set()
for k in kb_ids:
    body = k[len("email_"):]
    j = body.rfind("_chunk_")
    if j != -1:
        kb_prefixes.add(body[:j])
export_new = 0
try:
    with open(export_f) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            eid = json.loads(line).get("id")
            if eid and eid not in kb_prefixes:
                export_new += 1
except FileNotFoundError:
    pass

st = {"consecutive_no_progress": 0, "last_kb_count": None}
if os.path.exists(state_f):
    try:
        st = json.load(open(state_f))
    except Exception:
        pass
last = st.get("last_kb_count")
kb_grew = (last is None) or (post_kb > last)
if (not kb_grew) and export_new > 0:
    counter = int(st.get("consecutive_no_progress", 0)) + 1
else:
    counter = 0
new_state = {
    "consecutive_no_progress": counter,
    "last_kb_count": post_kb,
    "last_run": datetime.datetime.now().isoformat(timespec="seconds"),
    "export_new": export_new,
    "missing_a": len(missing_a),
    "missing_c": len(missing_c),
}
tmp = state_f + ".tmp"
with open(tmp, "w") as fh:
    json.dump(new_state, fh)
os.replace(tmp, state_f)

print(f"POST_KB={post_kb} CHUNKS={len(chunk_ids)}")
print(f"MISSING_A={len(missing_a)}")
print(f"EXPECTED_C={len(expected_email_ids)} PRESENT_C={len(present_email_ids)} MISSING_C={len(missing_c)}")
print(f"EXPORT_NEW={export_new} NO_PROGRESS_NIGHTS={counter}/{stale_max}")
if missing_a[:5]:
    print("MISSING_A_SAMPLE=" + ",".join(missing_a[:5]))
if missing_c[:5]:
    print("MISSING_C_SAMPLE=" + ",".join(missing_c[:5]))

if len(missing_a) > 0:
    sys.exit(10)
if len(missing_c) > 0:
    sys.exit(12)
if counter >= stale_max:
    sys.exit(11)
sys.exit(0)
PYEOF
)
rc=$?
echo "$verify_out" >> "$LOG"
_oneline=$(echo "$verify_out" | tr '\n' ' ')
case $rc in
    0)  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Guardia OK ($_oneline)" >> "$LOG" ;;
    10) fatal "Layer A: chunk prodotti da chunk_gmail NON in KB dopo l'ingest — ingest silenzioso. [$_oneline]" ;;
    12) fatal "Layer C: email triage useful=true che passano filtri+strip SENZA chunk in chunks_gmail.jsonl — perdita silenziosa nel chunking. [$_oneline]" ;;
    11) fatal "Layer B: KB email piatta da >=$STALE_MAX notti mentre l'export porta id nuovi — freeze silenzioso SOSPETTO. Controlla export/auth + triage. Se periodo legittimamente quieto, resetta il contatore: rm '$STATE'. [$_oneline]" ;;
    *)  fatal "Verifica post-ingest fallita (exit $rc) [$_oneline]" ;;
esac

# Step 5: Entities — enrichment post-ingest, NON critico per il dato in KB:
# fallimento loggato come WARN e REGISTRATO in un marker rilevabile
# ($WARN_FILE, self-clearing), non aborta (le email sono già ingestate).
if [ -n "$ANTHROPIC_API_KEY" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [5/5] Entities..." >> "$LOG"
    "$PY" scripts/enrichment/extract_entities.py --backend anthropic --source email >> "$LOG" 2>&1
    rc=$?
    if [ $rc -ne 0 ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] WARN: Entities extraction fallita (exit $rc) — non-bloccante, dato email già in KB (marker: $WARN_FILE)" >> "$LOG"
        # Marker visibile: incrementa un contatore di fallimenti consecutivi.
        "$PY" - "$WARN_FILE" "$rc" <<'PYEOF' 2>>"$LOG"
import sys, os, json, datetime
f, rc = sys.argv[1], int(sys.argv[2])
st = {"consecutive_failures": 0}
if os.path.exists(f):
    try:
        st = json.load(open(f))
    except Exception:
        pass
st = {
    "step": "entities",
    "consecutive_failures": int(st.get("consecutive_failures", 0)) + 1,
    "last_exit_code": rc,
    "last_failure": datetime.datetime.now().isoformat(timespec="seconds"),
}
tmp = f + ".tmp"
with open(tmp, "w") as fh:
    json.dump(st, fh)
os.replace(tmp, f)
print(f"entities WARN registrato: {st['consecutive_failures']} fallimenti consecutivi")
PYEOF
    else
        # Successo → azzera il marker (self-clearing).
        rm -f "$WARN_FILE"
    fi
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] END sync_email (OK)" >> "$LOG"
