# Dry-run runbook — nightly pipeline post-incident validation

**Date**: 2026-05-01 (post-incident recovery)
**Target validation**: `v1.2.4.2` (cron secrets sourcing) + `v1.2.4.3` (chromadb path fix)
**Mode**: detached tmux, **no real-time monitoring**, post-mortem analysis on operator return.

## Why this runbook

The 1 May 2026 ChromaDB incident exposed two latent bugs in the nightly pipeline:

1. **v1.2.4.2** (commit `1f8d06a`) — bash cron scripts sourced API keys from a now-empty MCP plist instead of `/etc/tailor/env` (where v1.2.5 hygiene moved them). Pipeline ran null-op.
2. **v1.2.4.3** (commit `6a1ee5a`) — `sync_and_ingest.sh:225` and `:247` pointed `chromadb.PersistentClient` at the vestigial `db/chroma/` instead of the real production `db/`, so the integrity check read a phantom DB.

Both fixes were applied and **smoke-tested inline** (`source /etc/tailor/env` exports 5 keys; the patched integrity check returns `OK:156307`), but **never validated end-to-end through the cron flow**. This dry-run is the first honest E2E test.

The pipeline traverses the same maintenance toggle (SIGUSR1 + SIGUSR2 to MCP) that broke during the manual run, and writes 75-90 min worth of state into chromadb. It is the natural stress test.

## Pre-flight (operator, before tmux launch)

```bash
# Cron sync_and_ingest must remain disabled (commented out) for the duration.
crontab -l | grep sync_and_ingest
# Expected: line present but with leading "#".

# Production daemon healthy.
curl -sf http://localhost:8787/api/dashboard/services > /dev/null && echo "MCP OK"

# Secrets file readable.
test -r /etc/tailor/env && echo "secrets OK"

# Ollama up (required for entity extraction, fact extraction, possibly summaries).
curl -sf http://localhost:11434/api/tags > /dev/null && echo "Ollama OK"
```

All three must print `OK`. If any fails, abort the dry-run and investigate.

## Launch (operator)

```bash
cd /Users/jarvis/tailor && \
  tmux new-session -d -s tailor-nightly \
  'bash sync_and_ingest.sh 2>&1; rc=$?; echo ""; echo "===DONE rc=$rc ($(date))===" ; sleep 7200'

tmux ls
# Expected: tailor-nightly: 1 windows (...)
```

Pipeline will run ~75-90 min. The trailing `sleep 7200` keeps the tmux pane alive for 2h post-completion so the operator can re-attach and read the trailing output without scrolling logs.

Re-attach (read-only):
```bash
tmux attach -t tailor-nightly
# Detach without killing: Ctrl-B  d
```

Emergency kill:
```bash
tmux kill-session -t tailor-nightly
```

## Post-mortem checkpoints (operator return)

These are **post-hoc reads of the log**, not real-time triggers. The pipeline runs to completion (or fails) without intervention.

Log path: `/Users/jarvis/tailor/logs/sync_and_ingest.log`

### Checkpoint 1 — early-stage markers

```bash
grep -E "(START sync_and_ingest|rclone sync completed|ChromaDB OK|Ingest completed|Entity index)" \
  /Users/jarvis/tailor/logs/sync_and_ingest.log | tail -10
```

| Expected line | Meaning if present | Meaning if absent |
|---|---|---|
| `START sync_and_ingest v9` | run started | pipeline never launched — investigate cron / secrets / path |
| `rclone sync completed (errors: 0)` | cloud sync OK | errors > 0 = rclone issue, non-blocking |
| `ChromaDB OK: <~156k> chunks` | **v1.2.4.3 path fix validated** | if instead `CRITICAL: ChromaDB integrity check FAILED` → fix is wrong, daemon DB unreachable |
| `Ingest completed: <X> chunks from <Y> files` | step done | acceptable if absent (still in ingest) |
| `Entity index: <N> entities indexed` | steps 5b+5c done | acceptable if absent |

**RED FLAG #1**: `CRITICAL: ChromaDB integrity check FAILED` in the log. Means the patched integrity check still reads the wrong path. Decision: do not re-enable cron tonight; investigate path resolution. The auto-restore logic (also patched) would have fired on this failure and overwritten `db/chroma.sqlite3` with the latest backup — verify mtime of `db/chroma.sqlite3` is unchanged compared to pre-launch.

### Checkpoint 2 — mid-stage markers

```bash
grep -E "(Starting fact extraction|Fact extraction completed|TIMEOUT|Starting fact supersession|Fact supersession completed|Starting fact derivation)" \
  /Users/jarvis/tailor/logs/sync_and_ingest.log | tail -10

sqlite3 /Users/jarvis/tailor/db/chroma.sqlite3 "SELECT COUNT(*) FROM embeddings_queue;"
```

| Marker | Meaning |
|---|---|
| `Starting fact extraction (multi-backend)` present, completion absent | step running |
| `Fact extraction completed: <F> facts from <C> chunks` | step done |
| `TIMEOUT: fact extraction after ...` | watchdog killed it (200 min hard cap), non-fatal |
| Queue size > 1500 | drift accumulating fast — risk of repeating today's incident on next daemon restart. Flag for review but do **not** intervene mid-run |

### Checkpoint 3 — final markers

```bash
grep -E "(END sync_and_ingest|Exiting MCP maintenance|MCP server resumed|MCP not responding|TRAP|maintenance.lock still present)" \
  /Users/jarvis/tailor/logs/sync_and_ingest.log | tail -15

tmux capture-pane -t tailor-nightly -p | tail -20

curl -sf -o /dev/null -w "stats=%{http_code}\n" --max-time 5 \
  http://localhost:8787/api/dashboard/stats

ls -la /Users/jarvis/tailor/maintenance.lock 2>&1

sqlite3 /Users/jarvis/tailor/db/chroma.sqlite3 "SELECT COUNT(*) FROM embeddings_queue;"
```

| Marker | Meaning |
|---|---|
| `END sync_and_ingest v9 (<seconds>s)` | pipeline completed (full or partial success) |
| `===DONE rc=0===` in tmux pane | exit code OK |
| `===DONE rc=1===` | non-fatal errors (see `ERRORS=` accumulator in script) |
| `Exiting MCP maintenance mode...` + `MCP server resumed OK` | trap clean, daemon online |
| `WARNING: MCP not responding after maintenance exit` | **incident-lookalike** — same failure mode as 1 May |
| `TRAP: WARNING — maintenance.lock still present` | trap had to clean up manually — not fatal but indicates SIGUSR2 race |
| `maintenance.lock` file present | **RED FLAG #2**: orphan lock, daemon may be in zombie state |
| `stats=200` | daemon healthy at end of pipeline |
| Queue < 200 | drained normally, healthy state |

**Telegram check**: end-of-run message should arrive on operator's phone within ~5 min of the `END` log line. Telegram credentials now flow through `/etc/tailor/env` (post-v1.2.4.2) and have not been validated through the cron flow either. Absent message = `send_telegram()` failed silently (most likely cause: env vars not exported by sourcing, or token actually missing).

## GC NameError noise

The garbage_collect.py step has a known `NameError: CLOUD_LOCAL_ROOT` that fails gracefully but emits to stderr. Captured by `$GC_OUTPUT`, parsed tolerantly via `sed ... || echo "0"` at [sync_and_ingest.sh:274](../sync_and_ingest.sh#L274). Result: `GC_REMOVED=0` in the Telegram report. Expected noise; not a failure signal.

## Decision matrix on operator return

| Observed state | Action |
|---|---|
| Checkpoint 1+2+3 all green, Telegram received | Push `v1.2.4.3` (`git push origin main && git push origin v1.2.4.3`); re-enable cron line. |
| Checkpoint 1 RED FLAG (`CRITICAL: ChromaDB integrity check FAILED`) | Do not push. Do not re-enable cron. Investigate why patched path still resolves wrong. Possibly the auto-restore overwrote production DB — check `db/chroma.sqlite3` mtime + size. |
| Checkpoint 3 RED FLAG (orphan lock + daemon down) | Run recovery sequence below. Do not re-enable cron until daemon stable. |
| Pipeline killed mid-run by external cause | Run recovery sequence below if daemon is in zombie state, otherwise just `kill tmux`. |

## Emergency recovery sequence (if daemon zombied post-run)

This is the same sequence executed during the 1 May incident.

```bash
# 1. Stop the daemon (sudo, TTY).
sudo launchctl unload /Library/LaunchDaemons/com.tailor.mcp.plist

# 2. Verify down.
ps aux | grep mcp_server.py | grep -v grep   # should be empty

# 3. Backup current queue (safety net).
sqlite3 /Users/jarvis/tailor/db/chroma.sqlite3 ".dump embeddings_queue" \
  > /tmp/queue_backup_recovery_$(date +%Y%m%d_%H%M%S).sql

# 4. Empty the queue. Required only if queue is large (>500) and daemon
#    segfaults on startup. The queue contents are duplicates of ops
#    already applied to the HNSW pickle (verified during 1 May incident).
sqlite3 /Users/jarvis/tailor/db/chroma.sqlite3 "DELETE FROM embeddings_queue;"

# 5. Remove orphan lock.
rm -f /Users/jarvis/tailor/maintenance.lock

# 6. Verify chromadb is openable from a fresh subprocess.
cd /Users/jarvis/tailor && .venv/bin/python3 -c "
import chromadb
c = chromadb.PersistentClient(path='/Users/jarvis/tailor/db')
col = c.get_collection('tailor_kb')
print('count:', col.count())
"
# Expected: count: <~156k>, exit 0. If exit 139 (SIGSEGV), DELETE queue
# was insufficient — escalate.

# 7. Restart daemon (sudo, TTY).
sudo launchctl load /Library/LaunchDaemons/com.tailor.mcp.plist

# 8. Smoke test (~10 sec).
sleep 5
curl -sf -o /dev/null -w "%{http_code}\n" http://localhost:8787/api/dashboard/stats
# Expected: 200.
```

If step 6 still segfaults after the DELETE, the bug is not the queue — escalate. Check the HNSW pickle integrity (size of `db/9ad2790b-.../data_level0.bin` should be ~500 MB, `index_metadata.pickle` ~14 MB).

## Backlog after this dry-run (post-validation)

Independent of dry-run outcome:
- Trap SIGTERM/SIGINT in nightly scripts that touch ChromaDB (root cause of the 1 May incident: brutal kill mid-write).
- `repair_hnsw_index.py` resilient to chromadb 1.5.x Rust upsert segfault (catch exit 139 batch-by-batch instead of dying).
- Implement `--prune-hnsw-ghosts` for the 50 superseded `doc_a288f7cb30f5` chunks.
- ~~Investigate the 44 SQL-only orphans skipped by `--apply` (`skipped_oversize`); add a `--best-effort-truncate` flag.~~ **Superseded (embedding-contract, 2026-07):** il contratto unico tronca a `MAX_EMBED_CHARS`, quindi i chunk lunghi non vengono piu' skippati ma ricostruiti (troncati) — il `--best-effort-truncate` proposto e' ora il comportamento di default. La chiave-risultato `skipped_oversize` e' stata rinominata `skipped_empty` e conta solo i chunk a documento vuoto (unico caso non recuperabile).
- `rebuild_db.py`: auto-detect `OLD_METADATA_SEGMENT` (currently hard-coded to a stale UUID) and `DB_DIR` path.
- `rm -rf /Users/jarvis/tailor/db/chroma/` vestigial directory (1.99 GB SQL phantom + corrupt HNSW dir, never read by production daemon).
- README section on "ChromaDB HNSW resilience" as known issue for downstream forks of TAILOR.
