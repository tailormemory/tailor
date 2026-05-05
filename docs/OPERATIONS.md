# OPERATIONS

Operator runbooks for TAILOR maintenance tasks that are NOT part of the
nightly automated pipeline.

---

## Manual Safe Ingest

**Script:** `scripts/maintenance/ingest_safe.sh`

### Use case

The nightly `sync_and_ingest.sh` pipeline is currently disabled because
chromadb 1.5.8 issue [#6975](https://github.com/chroma-core/chroma/issues/6975)
("Failed to apply logs to the hnsw segment writer") fires non-deterministically
during compaction. When it fires inside the nightly:

- `ingest_docs.py` exits with `rc=139` (SIGSEGV)
- subsequent steps (`create_missing_summaries`, `extract_entities`) cascade-fail
- queue accumulates write ops that crash MCP on next restart (#6975 replay)

`ingest_safe.sh` runs the same `ingest_docs.py` workload with MCP **fully
DOWN** (sudo `launchctl bootout`) instead of the SIGUSR1 maintenance-mode
trick. This eliminates any chance of MCP-induced concurrent chromadb access
during ingest, and surrounds the operation with a fresh full backup +
post-ingest drift audit + smoke test.

### Trade-offs vs nightly pipeline

| Aspect | nightly (`sync_and_ingest.sh`) | manual (`ingest_safe.sh`) |
|---|---|---|
| MCP availability during ingest | maintenance mode (SIGUSR1, ~1s) | DOWN (bootout, ~5-15 min) |
| Backup | none (relies on `backup_db.sh` cron) | full rsync of `db/` (~16GB, ~1m30s) |
| Sudo required | no | yes (bootout + bootstrap) |
| Telegram summary | yes | yes |
| Drift audit | post-pipeline | post-ingest |
| Concurrency safety | depends on SIGUSR1 race | guaranteed (MCP killed) |

### Usage

```bash
# Default: incremental ingest (only new/modified files)
./scripts/maintenance/ingest_safe.sh

# Pre-flight only: count baseline + files pending, no modifications
./scripts/maintenance/ingest_safe.sh --dry-run

# Skip backup (expert use; only when you have a recent backup already)
./scripts/maintenance/ingest_safe.sh --no-backup

# Re-ingest ALL documents (ignores doc_registry, slow)
./scripts/maintenance/ingest_safe.sh --full
```

### Flow

1. **Pre-flight** — read SQL baseline (`embeddings`, `embeddings_queue`),
   call `ingest_docs.py --status` for files pending count
2. **Backup** — `rsync -a db/ backups/db_pre_ingest_safe_TIMESTAMP/`
   (skipped with `--no-backup`)
3. **Stop MCP** — `sudo launchctl bootout system/com.tailor.mcp`
4. **Ingest** — `ingest_docs.py [--full]` (PYTHONUNBUFFERED=1 for live logs)
5. **Audit** — `repair_hnsw_index.py --audit --json` for drift verification
6. **Restart MCP** — `sudo launchctl bootstrap system /Library/LaunchDaemons/com.tailor.mcp.plist`
7. **Smoke test** — HTTP probe `/api/dashboard/stats`, retry 5× with 5s spacing
8. **Telegram** — summary message with deltas
9. **Log** — `logs/ingest_safe_TIMESTAMP.log`

### Bail-outs (non-resumable failures)

- backup `rsync` returns non-zero
- MCP still UP 30s after bootout
- bootstrap fails or MCP not running 8s after bootstrap
- `ingest_docs.py` rc=139 (SIGSEGV → #6975 reproduced even with MCP DOWN
  → re-evaluate strategy, do NOT continue scheduling)

On bail-out: Telegram `🚨` alert sent, exit 1, MCP state left as-is for
manual inspection. Backup directory preserved.

### Expected duration

| Workload | Estimated |
|---|---|
| backup rsync (16GB local SSD) | ~1m30s |
| MCP stop | ~5s |
| `ingest_docs.py` incremental (typical 10-200 files) | 2-15 min |
| `ingest_docs.py` full (3000+ docs) | 1-3 hours |
| audit | ~10s |
| MCP restart + smoke | ~15s |
| **Total typical (incremental)** | **~5-20 min** |

### Reading the Telegram summary

```
✅ ingest_safe completed

⏱ Duration: 423s
📦 Backup: /Users/jarvis/tailor/backups/db_pre_ingest_safe_20260505_120000
📄 Files processed: 47
📥 Chunks added: 312
📊 Embeddings: 154177 → 154489 (Δ312)
🧷 Queue: 0 → 0
🔍 Audit: sql=154489 hnsw=155322 queue=0 sql_only=0 ghosts=874
🖥 MCP: PID 56123 (smoke=OK)
📋 Log: /Users/jarvis/tailor/logs/ingest_safe_20260505_120000.log
```

`sql_only=0` is the critical health signal. Non-zero = chunks ingested
into SQL whose vector did not make it to HNSW (silent-write damage).

### Nightly automation status

The LaunchDaemon `com.tailor.sync_and_ingest` is **disabled** (`bootout`)
until a long-term decision is made on chromadb #6975 mitigation. The plist
file remains at `/Library/LaunchDaemons/com.tailor.sync_and_ingest.plist`
so the daemon can be re-enabled without re-installation:

```bash
# Re-enable nightly automation
sudo launchctl bootstrap system /Library/LaunchDaemons/com.tailor.sync_and_ingest.plist

# Disable (current state)
sudo launchctl bootout system/com.tailor.sync_and_ingest
```

### Long-term strategy candidates (NOT decided yet)

- **Qdrant migration** — replace chromadb entirely. Largest scope, cleanest
  outcome. Effort: weeks.
- **chromadb 1.4.x downgrade** — dump+restore via JSON export. Mitigates
  #6975 architecture difference (sync writes), introduces CVE #6926
  (pickle deserialization).
- **Upstream fix wait** — track [#6975](https://github.com/chroma-core/chroma/issues/6975).
  Zero work, indefinite timeline.
- **Stay on `ingest_safe.sh`** — manual operation forever. Acceptable if
  ingest cadence is low (manual is fine for weekly batches; daily is annoying).
