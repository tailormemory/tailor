# `repair_hnsw_index.py` — HNSW index drift audit & repair

A maintenance utility for ChromaDB SQL/HNSW segment drift in `tailor_kb`.

## What the tool does

Detects and (optionally) repairs **drift** between two halves of a ChromaDB collection:

- The **metadata segment** (rows in `embeddings` / `embedding_metadata` in `db/chroma.sqlite3`)
- The **HNSW vector segment** (`db/<segment-uuid>/index_metadata.pickle` + `data_level0.bin`)

These two segments are kept in sync by ChromaDB's queue consumers. When the on-disk HNSW pickle fails to persist after the SQL state has already advanced, you get drift in two flavours:

| Class | Symptom | Source of bug |
|---|---|---|
| **SQL-only orphans** | Chunks exist in metadata but not HNSW; `kb_search` returns "Internal error: Error finding id" | A successful `collection.upsert()` whose HNSW write was lost on shutdown |
| **HNSW-only ghosts** | Chunks live in the index without metadata; never queryable, but waste memory | A successful `collection.delete()` whose HNSW removal was lost on shutdown |

In both cases the API call returned without error, no logs were written, and the queue was purged once both segments' `max_seq_id` advanced past the op. The drift is silent until a search hits an affected id.

## When to run it

- **As a smoke test**: after any pipeline run that touches `collection.upsert` or `collection.delete` (document ingest, supersession, GC).
- **Around an MCP restart**: the queue replays during startup. If shutdown was unclean, the replay can produce drift. Audit before and after.
- **On user-visible symptoms**: `kb_search` errors mentioning `Error finding id`, or documents that should match a query producing no hits.
- **As a periodic check**: nightly or weekly. Audit-mode is cheap (sub-second) and produces a JSON history that lets you spot drift drift-rate trends.

## Usage

```
python scripts/maintenance/repair_hnsw_index.py            # audit, default
python scripts/maintenance/repair_hnsw_index.py --apply    # repair, requires preconditions
python scripts/maintenance/repair_hnsw_index.py --json     # machine-readable output
python scripts/maintenance/repair_hnsw_index.py --no-history  # skip writing audit JSON
```

### Three-mode pattern

| Mode | Reads | Writes | Use case |
|---|---|---|---|
| `--audit` (default) | SQLite read-only, HNSW pickle | `logs/hnsw_audits/audit_*.json`, one row in `maintenance_log` | Diagnosis |
| `--apply` | SQLite read-only + Chroma client | `collection.upsert()` for SQL-only orphans, one row in `maintenance_log` | Repair |
| `--prune-hnsw-ghosts` | — | — | **Deferred** (raises `NotImplementedError`) |

### Required preconditions for `--apply`

The tool refuses to mutate unless **both** of these are true:

1. **MCP is in maintenance mode** — `maintenance.lock` exists in the project root (set by `kill -USR1 <mcp_pid>`). Without this, the running MCP holds the same Chroma client and writes will race.
2. **Recent backup exists** — a file matching `backups/chroma_*.sqlite3.gz` or `db/chroma.sqlite3.backup.*` with mtime within the last 60 minutes. The tool does not take a backup automatically; the operator does.

If either precondition fails, the tool exits non-zero with instructions and does NOT touch the database.

### Refuses when audit is unsafe

`--apply` also refuses when the audit found **unknown** drift types — e.g. a SQL-only chunk whose `source` is not in the known set (`document`/`conversation`/`email`/`document` with `doc_summary_*` conv_id), or an HNSW-only chunk whose prefix is not under a recognised namespace. The script will not auto-fix what it cannot classify; investigate the `UNEXPECTED DRIFT TYPES` section first.

## Restart-aware workflow

The HNSW persist race is most likely to fire during a process shutdown that doesn't complete cleanly. The audit tool therefore supports a pre-/post-restart workflow:

1. **Pre-restart**: `python scripts/maintenance/repair_hnsw_index.py` — captures the baseline. Audit JSON written to `logs/hnsw_audits/audit_YYYYMMDD_HHMMSS.json`.
2. Operator restarts MCP (separately).
3. **Post-restart**: re-run the same command. If the previous audit JSON is <2h old, the report includes a **DELTA SINCE LAST AUDIT** section comparing orphan counts and listing newly-appeared / resolved orphan ids.

This catches the case where the restart itself introduced new drift via the same race.

If no recent audit exists, the delta section is silently omitted.

## `maintenance_log` table

Each run writes one row to the `maintenance_log` SQLite table for auditability:

| Column | Type | Value |
|---|---|---|
| `id` | INT PK | next monotonic id |
| `timestamp` | INT | unix epoch seconds |
| `operation` | TEXT | JSON blob with: `tool`, `mode` (audit/apply), `drift_summary` (counts), `action_taken`, `post_apply_sql_only_count` (apply only), `backup_used` (apply only) |

The `operation` column holds a single JSON object rather than separate columns — the existing table schema is minimal, and stuffing JSON keeps the schema unchanged.

Query history with:
```sql
SELECT id, datetime(timestamp,'unixepoch','localtime'), json_extract(operation,'$.mode'), json_extract(operation,'$.action_taken')
FROM maintenance_log ORDER BY id;
```

## Audit history

Every audit run writes its full state (drift counts, orphan ids, classifications, queue state) to a timestamped JSON under `logs/hnsw_audits/`. Files are kept indefinitely; the operator can prune manually. The `--no-history` flag suppresses this side-effect (useful in tests).

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Audit clean, or apply succeeded |
| 1 | Drift detected (audit mode only) |
| 2 | `--apply` refused: not in maintenance mode |
| 3 | `--apply` refused: no recent backup |
| 4 | `--apply` refused: unknown drift types found |
| 5 | `--apply` ran but post-audit shows residual drift |

Non-zero from `--audit` makes it suitable for cron with email-on-error; non-zero from `--apply` indicates a precondition failure or partial success that needs human review.

## Implementation notes

- SQLite is opened **read-only** (`mode=ro` URI) for the audit phase. Apply opens read-write only via `chromadb.PersistentClient` (we never poke the metadata segment directly).
- Re-embedding goes through `scripts/lib/embedding.py:get_embeddings` — the same path as the ingest pipeline, with the same provider config.
- For `source=document` chunks (not summaries), the original embedding text was `f"{prefix}\n\n{text}"` where prefix is built from `folder` / `doc_type` / `title` ([scripts/ingest/ingest_docs.py:644-665](../../scripts/ingest/ingest_docs.py)). The repair tool reconstructs this exactly — so the re-embedded vectors match what they would have been on original ingest, not "close enough".
- For `doc_summary_*` and conversation/email chunks, the original embedding was over the bare text with no prefix; repair uses `chroma:document` as-is.
- HNSW-only ghosts are **not** repaired in this version. Removing them requires either (a) writing into the HNSW pickle directly (fragile) or (b) calling `collection.delete(ids=...)` on chunks Chroma considers absent from metadata (untested behaviour). The 50 known ghosts in the current state have no functional impact — they cannot be returned in a result without a metadata row to resolve them — so this is safely deferred. See *Open question #3* in `/tmp/hnsw_forensic_report.md` for the design discussion.

## Why this tool exists

The drift this tool repairs is **not** a TAILOR bug. It's an upstream race in ChromaDB 1.x's HNSW persist-on-shutdown contract — the SQL `max_seq_id` advances when the queue consumer processes an op, but the on-disk HNSW pickle is only written during `System.stop()` / `atexit`. If the process exits before that hook completes (signal interruption, race with another shutdown step, silent fs error), the queue rows still get purged once both `max_seq_id`s pass them, and the in-memory HNSW state is lost forever.

Until the upstream race is fixed, running this tool periodically is part of normal operation for any project using ChromaDB 1.x with an active write workload.

There is a separate prevention layer planned: a **post-ingest verifier** in `scripts/ingest/ingest_docs.py` that does a vector-path read-back after each `collection.upsert()` and either retries or marks the chunks for repair on the next audit. That's tracked for v1.2.3 and is the complement to this audit-and-fix utility — write-time prevention versus read-time recovery.

A note on classification: the **Phase A** human forensic analysis grouped the 66 SQL-only orphans by source document and missed 5 of them (the `doc_summary_*` chunks from the same Apr-16 03:01 batch) because the manual inspection grouped by file rather than by chunk. This audit tool groups dynamically by `conv_id` and catches all chunks regardless of classification — which is one of the reasons a tool is more reliable than spot-checks for this class of bug.

## Known issue upstream

- **Affected versions**: ChromaDB 1.x — observed on `chromadb==1.5.5`.
- **Symptom**: `embeddings.max_seq_id` for the metadata segment advances after a successful `collection.upsert()` / `.delete()`, but the on-disk HNSW pickle is missing those entries. Once the vector segment's `max_seq_id` also advances, `embeddings_queue` rows are purged, and the drift becomes unrecoverable without re-embedding.
- **Trigger** (incomplete): some combination of MCP maintenance-mode handoff (SIGUSR1 drops the client reference) overlapping with the start of the ingest's own client. Out of ~155k chunks ingested across several months, only one window of 116 chunks (66 + 50) drifted. Reproduction conditions are not yet confirmed.
- **Expected resolution**: upstream fix in chromadb. GitHub issue link: `https://github.com/chroma-core/chroma/issues/TBD` (placeholder until filed).
- **Mitigation**: this tool, plus the planned post-ingest verifier in `scripts/ingest/ingest_docs.py` (tracked for v1.2.3).
