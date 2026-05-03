# Changelog

All notable changes to TAILOR are documented in this file.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning: [Semantic Versioning](https://semver.org/).

## [Unreleased]

<!--
Template for upcoming changes. Move entries under a new version heading on release.

### Added
### Changed
### Fixed
### Removed
### Security
### Docs
-->

---

## [1.2.6.1] — 2026-05-04 — Bug fix release: nightly pipeline silent failure

The nightly pipeline had been silently null-op since 2026-05-01 (`Ingest completed: 0 chunks from 0 files` every night). Two compounding defects masked the failure mode in logs:

1. `scripts/maintenance/garbage_collect.py` referenced an undefined `CLOUD_LOCAL_ROOT` and crashed on every run; the cron parser swallowed the traceback and reported "GC: 0 removed".
2. `scripts/lib/ingest_helpers.verified_upsert` called `collection.query()` immediately after `collection.upsert()` to detect HNSW drift. On `chromadb==1.5.5` this query SIGSEGVs at the C/Rust level (uncatchable in Python), killing the ingest process mid-iteration. Default-buffered stdout was lost, so cron logs showed only Tesseract OCR noise (unbuffered C stderr) between "Starting document ingest" and "Ingest completed: 0 chunks from 0 files". `doc_registry.json` was never reached, so each subsequent run retried the same files, accumulating duplicate chunks for whatever upserts did persist before the kill.

### Fixed

- **`scripts/maintenance/garbage_collect.py`**: define `CLOUD_LOCAL_ROOT` by reading `cloud_sync[0].local_root` from `tailor.yaml` (with fallback to `ingest.document_root`). Removes the longstanding `NameError: name 'CLOUD_LOCAL_ROOT' is not defined` traceback that hid behind the cron's "Garbage collection completed:  removed" log line.
- **`scripts/lib/ingest_helpers.py`**: short-circuit `verified_upsert` to return True immediately after `collection.upsert()`. The post-upsert `_vector_path_drift` query (the SIGSEGV trigger on chromadb 1.5.5) is bypassed. HNSW drift detection / repair is deferred to `scripts/maintenance/repair_hnsw_index.py`, which is the canonical drift authority per this module's own docstring. Re-enable verifier inline once chromadb is pinned past 1.5.5 (track [chroma-core/chroma#6975](https://github.com/chroma-core/chroma/issues/6975)).

### Changed

- **`sync_and_ingest.sh`**: each subprocess step (`garbage_collect`, `ingest_docs`, `create_missing_summaries`, `extract_entities`, `build_entity_index`, `generate_user_profile`) now captures `$?` and accumulates non-zero exits into `$ERRORS` with step name + rc — surfaced in the closing Telegram message. Previously these failures were silent: the pipeline only flagged its own bash-level errors (rclone, MCP toggle, fact-extraction timeouts) and ignored every Python step's exit code.
- **`sync_and_ingest.sh`**: ingest step now runs with `PYTHONUNBUFFERED=1`. Per-file progress (`[N/63] filename.pdf (N KB)`) reaches the log in real time instead of being lost in stdout buffers when the script is killed mid-run. Adds a soft-check: if ingest exits 0 but never printed the `File processati:` result block, that's flagged as `ingest_docs no result block` in `$ERRORS` (the segfault fingerprint).

### Validation

- Pre-fix reproduction: `verified_upsert` of 8 real Credem-extracted chunks → `exit code 139` (SIGSEGV) on the post-upsert query, after a clean `collection.upsert()` call.
- Post-fix smoke test: garbage_collect `--stats` runs to completion, reports `Registry: 3168 file | Orfani: 47 file (~865 chunk)` without traceback.
- `sync_and_ingest.sh` syntax-validated (`bash -n`).
- End-to-end nightly run pending operator launch (cron 03:00 or manual).

### Known issues / deferred

- **15 Credem RPV monthly statements** (Gen2025–Mar2026, 100–300 KB each) discovered un-ingested during diagnosis. Not fixed inline because chromadb writes silently no-op when the MCP daemon holds the database concurrently (separate from the SIGSEGV). The cron pipeline avoids this via SIGUSR1 maintenance toggle, so these files should ingest normally on the next nightly run with this release applied. A misleading registry update from a manual oneshot was rolled back to keep registry/KB consistent.
- chromadb 1.5.5 silent-write-while-MCP-holds-lock is unrelated to the verifier SIGSEGV but worth tracking. Not in scope for v1.2.6.1.

---

## [1.2.6] — 2026-05-03 — Cron→Launchd migration

Migrates all 6 TAILOR cron jobs to LaunchDaemons, the modern macOS scheduling subsystem. Resolves the Tahoe-26.1 block on `crontab(1)` editing (encountered during v1.2.5) by sidestepping cron entirely. The cron daemon is left in place but sent SIGSTOP, freezing it without removing it (SIP-protected service). Includes Telegram bot token rotation, now unblocked since cron-level env injection is gone.

### Added

- **6 new shell wrappers in `scripts/services/`** (`750 jarvis:staff`, same pattern as v1.2.5: source `/etc/tailor/env`, cd, exec): `run_backup_db.sh`, `run_sync_and_ingest.sh`, `run_sync_email.sh`, `run_heartbeat.sh`, `run_reminder_checker.sh`, `run_model_advisor.sh`.
- **6 new LaunchDaemon plists in `/Library/LaunchDaemons/`**: `com.tailor.{backup_db,sync_and_ingest,sync_email,heartbeat,reminder_checker,model_advisor}.plist`. All with `UserName=jarvis`, `GroupName=staff`, `RunAtLoad=false`, `KeepAlive=false`. Daily jobs use `StartCalendarInterval`; recurring jobs use `StartInterval` (heartbeat 300s, reminder_checker 60s).

### Changed

- **Cron daemon (PID 553) sent SIGSTOP** to freeze scheduling without uninstalling. SIP prevents `bootout` of `com.vix.cron`. Reversible with `kill -CONT 553` if rollback needed (not advisable: would cause double-execution with the new LaunchDaemons).
- **`ip-monitor.sh` cron job (out-of-TAILOR-scope, pointed to a non-existent `~/cerebro-bi/`)** is now also frozen as a side effect of SIGSTOP. The script was failing silently every 5 min for an undetermined period.

### Security

- **Telegram bot token rotated.** New token issued via @BotFather, installed via dashboard (`config/tailor.yaml`), Telegram daemon restarted manually. Old token in `/var/at/tabs/jarvis` is now invalidated — the file remains physically on disk but its contents are inert.

### Validation

24h soak validated end-to-end:
- All 6 new daemons triggered at their schedules with exit code 0
- sync_and_ingest 4h 28m runtime, fact derivation completed normally
- heartbeat / reminder_checker continued via launchd post-SIGSTOP, no lapse (state.json and reminders.json mtimes confirm regular ticks)
- Telegram bot smoke-tested with new token, natural-language reply OK

### Known issues / deferred

- **`/var/at/tabs/jarvis` cannot be removed from userland.** Tahoe 26.1 enforces Data Vault on `/System/Volumes/Data` (mount flag `protect`), blocking modify/delete syscalls on `/var/at/` to any non-Apple-signed process — even root + sudo. Diagnostic ruled out sandbox profiles (no cron-specific profile loaded, no deny entries in logs), BSD flags (file is clean), kqueue watchers (cron PID 553 has no FDs on `/var/at/tabs`). Workarounds tried: `rm`, `cp`, `install`, FDA grants, `launchctl bootout` (SIP-blocked) — all fail. The file is now inert (cron stopped) and contains an invalidated token. Removal requires Recovery Mode + rm against the mounted Data volume; deferred until next physical session at the Mac mini.
- **TAILOR_API_KEY URL exposure** unchanged from v1.2.5.1. Tracked as v1.3.0 sub-feature: OAuth provider in MCP server.
- **Dashboard `/api/dashboard/config/restart` only restarts MCP**, not other TAILOR daemons (Telegram, heartbeat, etc.). Discovered during this release while applying the Telegram token rotation. Tracked as v1.2.6.x bug fix: extend the endpoint to support per-service restart (Telegram, heartbeat, reminder_checker, model_advisor, sync_and_ingest, sync_email, backup_db). Currently restart of non-MCP daemons requires TTY: `sudo launchctl bootout/bootstrap`.

### Stats

- 6 new shell wrappers + 6 new LaunchDaemon plists
- 0 source files modified (pure scheduling-layer migration)
- 1 cron daemon stopped (PID 553, STAT=Ts)
- 1 Telegram bot token rotated
- 24h soak with all 6 jobs validated end-to-end
- 1 dashboard restart bug discovered (tracked for v1.2.6.x)

---

## [1.2.5.1] — 2026-05-02 — Token rotation hygiene

Patch release: scopes the API keys used by TAILOR to dedicated, project-specific keys, separating them from the general-purpose keys used by unrelated systems. Removes an obsolete plist backup that still contained pre-v1.2.5 plaintext secrets.

### Security

- **TAILOR now uses dedicated, project-scoped API keys** for Anthropic, OpenAI, and `TAILOR_API_KEY`. All three were rotated and updated in `/etc/tailor/env` and the encrypted secrets DB. Validated end-to-end via natural-language exchange with the Telegram bot (Brain call against the new Anthropic key succeeded).
  - **Anthropic**: previous key promoted to general-purpose for use in unrelated systems; new dedicated key (`tailor-prod`) provisioned and installed.
  - **OpenAI**: previous key revoked at provider console; new dedicated key installed.
  - **TAILOR_API_KEY**: regenerated; MCP daemon force-restarted via dashboard; claude.ai connector URL updated.
- **Removed `/Library/LaunchDaemons/com.tailor.mcp.plist.backup-20260430-181921`**, which still contained the pre-v1.2.5 plaintext secrets in `EnvironmentVariables`. Now that those secrets are dead, the file was deleted to reduce on-disk residual leak surface.

### Known issues / deferred (carried over from v1.2.5)

- **Telegram bot token NOT rotated.** Same blocker as v1.2.5: the legacy crontab still injects `TELEGRAM_BOT_TOKEN=<old>` as env var into `heartbeat.py`/`reminder_checker.py`, and `env_loader.load_env()` uses `setdefault` which won't override. Rotating now would silently break Telegram alerts. Deferred until v1.2.6 (cron→launchd migration).
- **TAILOR_API_KEY still leaks via URL query string.** claude.ai's custom MCP connector UI accepts only a URL field — no separate Bearer header config — so the token must be passed as `?token=<value>`, exposing it in browser history, server access logs, and conversation transcripts. The TAILOR MCP server already supports `Authorization: Bearer` headers, but claude.ai doesn't use them. Tracked as a v1.3.0 sub-feature: implement OAuth provider in MCP server (the same flow used by other claude.ai connectors).

### Stats

- 3 API keys rotated to dedicated TAILOR-scoped keys (Anthropic, OpenAI, TAILOR_API_KEY)
- 1 obsolete plist backup removed
- 0 source files modified

---

## [1.2.5] — 2026-05-02 — Daemon secrets hardening + cron classifier fix

Security-debt cleanup: removes plaintext API keys from `/Library/LaunchDaemons/` plists, unifies the daemon-secrets pattern across Telegram + MCP via shell wrappers that source `/etc/tailor/env` (the same file `sync_and_ingest.sh` and other cron wrappers already use since v1.2.4.2). Cron-driven Python scripts (`heartbeat.py`, `reminder_checker.py`) gain a small loader so they can self-source the same file once the inline `TELEGRAM_*=` cron-level env vars are retired. Includes a 1-line classifier perf fix unrelated to the hardening work but caught during validation.

### Security

- **Telegram daemon: secrets removed from `com.tailor.telegram.plist`.** Previously `EnvironmentVariables` held `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `ANTHROPIC_API_KEY`, `OPENAI_API_KEY` in plaintext (file mode `644 root:wheel`, world-readable). New plist runs `scripts/services/run_telegram_bot.sh` under `UserName=jarvis`; the wrapper sources `/etc/tailor/env` (mode `640 root:staff`, group-readable by `jarvis`) and `exec`s the bot. Daemon process now runs as uid 501 instead of root. Backup kept at `*.backup-20260502-123307`.
- **MCP daemon: same wrapper pattern.** `com.tailor.mcp.plist` previously used inline `/bin/sh -c "set -a; . /etc/tailor/env; exec ..."` (already non-leaking since v1.2.4.2 but inconsistent with the rest of the fleet). Now executes `scripts/services/run_mcp_server.sh`. `UserName=jarvis` was already in place; no behavioural change beyond pattern unification.
- Both plists now also declare `GroupName=staff` explicitly (was implicit on MCP, missing on Telegram).

### Added

- **`scripts/services/run_telegram_bot.sh`** + **`scripts/services/run_mcp_server.sh`** (28 lines each, `750 jarvis:staff`). Identical structure modulo the final `exec` target. Fail-loud if `/etc/tailor/env` is missing/unreadable — no silent fallback to a cron-level env.
- **`scripts/lib/env_loader.py`** (28 lines). Tiny `load_env(path="/etc/tailor/env")` helper using `os.environ.setdefault()` so a pre-set key (e.g. legacy crontab `TELEGRAM_BOT_TOKEN=` line) is preserved — backward-compatible during the migration window. Imported by `heartbeat.py` and `reminder_checker.py`.

### Changed

- **`scripts/services/heartbeat.py`** (+2 lines): `from env_loader import load_env; load_env()` after the existing `sys.path` insert. No semantic change when env vars are already present (the cron-level case today).
- **`scripts/services/reminder_checker.py`** (+4 lines): same pattern, plus the `sys.path` insert that this file lacked.

### Fixed

- **qwen3 classifier extended-thinking regression in `scripts/lib/llm_client.py`** (+3 lines, line 663). `OllamaClient.chat()` now appends an empty `<think>\n\n</think>\n\n` block right after the `<|im_start|>assistant\n` marker, suppressing qwen3.5's default chain-of-thought reasoning. Empirical impact: classifier latency drops from ~31s to ~0.5s per call, validated in vivo with intent classification on saluto/reminder messages. The pre-existing `"think": False` API parameter is silently ignored under `"raw": True` mode and is left in place as future-proofing if Ollama starts honouring it.
- **Heartbeat: missing `t` import in recovery branch.** `scripts/services/heartbeat.py` calls `t()` from `lib.i18n` for recovery alerts (line 131), but the import was never added. Latent until 2026-05-01 ~20:00, when MCP went down and back up: the next run hit the recovery path and crashed with `NameError`. Cron silently re-fired every 5 min for ~20 hours, each crash before writing log or state. Caught during v1.2.5 soak verification. +1 line: `from i18n import t`.
- **Telegram daemon now runs under `jarvis` uid**, restoring the principle of least privilege for a network-facing process. Previously ran as root with no operational reason.

### Known issues / deferred

- **`/var/at/tabs/jarvis` (the user's crontab) still contains plaintext `TELEGRAM_BOT_TOKEN=...` and `TELEGRAM_CHAT_ID=...`.** File mode is `600 root:wheel` so the leak is contained (root-only), but the lines are live cron-level env vars at every scheduled job. Editing the crontab via `crontab(1)` is currently blocked on macOS Tahoe 26.1 by what appears to be a kernel-level sandbox restriction on `/var/at/` (EINTR injected on `rename(2)` regardless of binary; even `sudo cp` and `install` fail; FDA grant on Terminal does not unblock — TCC is a separate denial layer). Investigated extensively during this release; no userland workaround found short of Recovery Mode + SIP-disable.
- **Tracked as v1.2.6: cron→launchd migration.** Once all cron entries become `LaunchDaemon` plists with `UserName=jarvis`, the inert `/var/at/tabs/jarvis` file no longer drives execution and its plaintext content becomes dead.
- **Telegram token rotation** intentionally deferred until **after** v1.2.6 lands. Reason: `env_loader.load_env()` uses `setdefault`, so as long as cron sets `TELEGRAM_BOT_TOKEN=<old>` in a child process's env, that value wins over `/etc/tailor/env`. Rotating the token while the cron lines remain would silently break heartbeat/reminder Telegram alerts.

### Stats

- 2 new shell wrappers (`run_telegram_bot.sh`, `run_mcp_server.sh`) + 1 new Python helper (`env_loader.py`)
- 3 files modified (`heartbeat.py`, `reminder_checker.py`, `llm_client.py`)
- 2 LaunchDaemon plists rewritten to wrapper pattern
- 2 bugs fixed: qwen3 classifier extended-thinking (~31s → ~0.5s), heartbeat `NameError` on recovery branch (silent crash loop, ~20 h)

---

## [1.2.4] — 2026-04-28

### Added

- **PaddleOCR backend for structured PDF extraction** (`PPStructureV3`). Optional second extractor pipeline for PDFs in configured `structured_doc_types` or with low native text. Produces table-aware HTML/markdown output alongside flat text. See `docs/v1.2.4_paddleocr_comparison.md` for full validation report and per-doctype verdicts.
- **Lazy module-level singleton** for the PaddleOCR pipeline (`_get_paddle_ocr()` in `scripts/ingest/ingest_docs.py`), avoiding re-init across multiple PDFs in the same process.
- **Content-based negative whitelist** for `bilancio gestionale centro-di-costo` templates (Sistemi/AGO ledger family). Detected via first-page text patterns; routed to legacy regardless of doctype config because PaddleOCR's table recognizer fails on this layout family (see report).
- **Fallback hierarchy**: per-page legacy fallback on PaddleOCR predict failures, with audit trail in `logs/paddle_fallback_<YYYYMMDD>.json`.
- **Section schema extension**: `sections[]` returned by `extract_pdf()` now include `markdown` field (HTML table structure when available) and `metadata.extractor` field (`"paddleocr"` / `"legacy_fallback"` / etc.) for downstream retrieval-quality analysis.
- **Validation report** at `docs/v1.2.4_paddleocr_comparison.md` documenting cross-doctype binding-ratio analysis on real Italian financial documents.

### Changed

- `_extract_pdf_legacy()` retains 1-indexed page numbering; `_extract_pdf_paddle()` matches this convention. New `_normalize_legacy_sections()` helper ensures schema consistency when whole-doc fallback occurs.
- `requirements.txt`: added `paddleocr>=3.0` and `paddlex[ocr]>=3.5`.

### Notes

- First PaddleOCR invocation triggers a one-time ~1 GB model download from Paddle's CDN (~52 minutes on residential bandwidth in our validation). Subsequent runs use the local cache in `~/.paddlex/official_models/`.
- Per-page predict latency on Apple Silicon CPU: ~19–32 s/page (mobile recognizer). Plan nightly batch budgets accordingly.
- v1.2.4 ships PaddleOCR as a backend, not as a structured extractor for all doctypes. AdE forms and similar binding-critical documents require post-processing not included in this release.

---

## [1.2.3] - 2026-04-27 — Post-ingest HNSW verifier + auto-persist after repair

Patch release that closes the chromadb 1.x persist-on-shutdown gap at write time, complementing the audit-and-fix utility shipped in v1.2.2. Two independent components.

### Added

#### Component A — post-ingest verifier

- **`scripts/lib/ingest_helpers.py:verified_upsert()`** — wraps `collection.upsert()` with a vector-path read-back: re-uses the just-upserted embeddings as `query_embeddings` with `n_results=1` and checks each chunk's top-1 self-match returns the chunk's own id (cosine ≈0). If HNSW is missing the write, the top-1 is some other chunk → drift. SQL-only readback (`collection.get(ids=...)`) was the obvious wrong primitive — it always succeeds in exactly the failure mode we're trying to catch — so this took some live inspection to design correctly. Single 500ms retry handles vector-consumer lag; persistent drift is logged for the next audit, not hard-failed. Cost on the live 155k-chunk collection: ~14ms per batch of 10.
- **Drift logging path**: `logs/hnsw_audits/pending_<run_id>.json` (full id list, append-only across batches in one run) + a row in the `maintenance_log` SQLite table with `mode: "verify"`, source attribution, capped `drift_ids` (first 50) and `drift_total` for full count. Mirrors the convention from `repair_hnsw_index.py:_ensure_maintenance_log_row`. Composes with the audit/repair tool: drift detected at write time gets repaired by the next `--apply` run.
- **`make_run_id(source)`** helper: `<source>_YYYYMMDD_HHMMSS_<pid>` to disambiguate same-second parallel runs (cron, manual back-to-back, tests).
- **All 4 ingest paths now call `verified_upsert()`**: `scripts/ingest/ingest_conversations.py:231`, `scripts/ingest/ingest_docs.py:677`, `scripts/ingest/ingest_upload.py:130`, `scripts/gmail/ingest_gmail.py:152`. Surrounding context (progress reporting, job-status updates, stdout) is preserved unchanged. Existing per-script error handling (log error, increment counter, continue loop) stays intact — `verified_upsert()` returns False on drift, never raises, and the caller bumps `errors` exactly as before.
- **End-of-run drift surfacing**: `sync_and_ingest.sh` queries `maintenance_log` for `mode='verify'` rows since `START_TIME` and appends a per-source drift line to the existing Telegram message. Reuses the existing `send_telegram()` function — no new alerting path.

#### Component B — auto-persist after repair

- **`scripts/lib/chroma_persist.py:force_chroma_persist()`** — issues `target_writes=1100` (sync_threshold 1000 + ~10% margin) idempotent re-upserts of a sample to bump chromadb's internal write counter past sync_threshold and trigger `_persist()`. Originally executed manually as "Track 3a" on 2026-04-25 against the 9ad2790b vector segment; now formalised. Returns success/iterations/total_writes plus optional mtime advancement check on the HNSW pickle. mtime check is **defensive observation, not enforcement** — a non-advancing mtime emits a stderr warning but the function still returns success, since the next audit catches any residual drift and failing here would just hide it.
- **`repair_hnsw_index.py --apply` now invokes `force_chroma_persist()` automatically** after re-embedding orphans, eliminating the "second restart dance" that previously had to be done by hand. The `repair()` function gained a `repaired_ids` field in its result dict, threaded through to the persist call. Empty repair set (nothing to fix) skips the persist step. The maintenance_log row for `--apply` gains an `auto_persist: {triggered, iterations, total_writes, mtime_advanced, success}` field for operator visibility from the dashboard.

### Changed

- `scripts/maintenance/repair_hnsw_index.py:repair()` now returns `repaired_ids: list[str]` in its result dict (previously only `repaired: int`, `batches: int`, `skipped_oversize: int`). Existing callers that used keys-by-name still work; positional access would break, but there were none.
- The `[1.2.2]` notes mentioned the verifier would land "in `scripts/ingest/ingest_docs.py`" — the actual scope expanded to all 4 ingest scripts via the shared helper. Defensible: the chromadb 1.x race can hit any upserter, not just the document path.

### Fixed

- Closes the chromadb 1.5.5 sync_threshold gap at write time without waiting for the upstream fix at chroma-core/chroma#6975. v1.2.2 recovers existing drift; v1.2.3 prevents new drift and forces persistence after recovery.

### Stats

- 2 new shared helpers (`scripts/lib/ingest_helpers.py`, `scripts/lib/chroma_persist.py`)
- 4 ingest scripts wired to the verifier (no behavioural change on the success path)
- 1 new auto-persist trigger in `repair_hnsw_index.py --apply`
- 358 tests passing (322 baseline + 12 from v1.2.2 + 24 new across `test_ingest_helpers.py` (10), `test_chroma_persist.py` (10), `test_repair_hnsw_index.py` (4 added))

### Notes

- The `auto_persist.mtime_advanced` flag in `maintenance_log` is observational — `False` does not mean the repair failed. It means we can't confirm the persist trigger fired this time. Re-running `--audit` after the next MCP restart is the authoritative check.
- The post-ingest verifier and the repair tool are now mutually reinforcing: verifier catches drift the moment it happens, audit catches anything the verifier missed (or anything that drifted from external causes like the original Apr 16 race).

---

## [1.2.2] - 2026-04-25 — HNSW index drift audit & repair utility

Patch release adding a maintenance utility for ChromaDB SQL/HNSW segment drift.

### Added

- **`scripts/maintenance/repair_hnsw_index.py`** — diagnostic and repair tool for ChromaDB 1.x persist-on-shutdown drift. Three-mode: `--audit` (default, read-only diagnosis), `--apply` (re-embeds SQL-only orphans via `collection.upsert()`), `--prune-hnsw-ghosts` (deferred — raises `NotImplementedError`). `--json` flag for machine-readable output. Read-only audit opens SQLite via `mode=ro` URI; apply uses `chromadb.PersistentClient` exclusively. Re-embedding goes through the existing `scripts/lib/embedding.py:get_embeddings` so the configured provider (Ollama / OpenAI / Google) is honoured.
- **Restart-aware delta tracking** — every audit run writes a timestamped JSON to `logs/hnsw_audits/`. When a previous run from <2h ago is on disk, the next audit emits a `DELTA SINCE LAST AUDIT` section listing newly-appeared and resolved orphan ids, plus queue/HNSW count changes. This makes race-induced drift around an MCP restart visible.
- **`maintenance_log` table** — the existing (previously empty) table now receives one row per `repair_hnsw_index.py` invocation. The `operation` TEXT column holds a JSON object with `tool`, `mode`, `drift_summary`, `action_taken`, and (for apply runs) `post_apply_sql_only_count` + `backup_used`. First write to this table since it was created.
- **Three preconditions for `--apply`**, all of which the tool refuses without operator action:
  1. MCP in maintenance mode — `maintenance.lock` present (`kill -USR1 <mcp_pid>`).
  2. Recent backup — file matching `backups/chroma_*.sqlite3.gz` or `db/chroma.sqlite3.backup.*` with mtime within 60 minutes.
  3. No "unknown" drift types — every SQL-only chunk and HNSW-only ghost must fit a known classification, or `--apply` blocks for human review.
- **Documentation**: `scripts/maintenance/repair_hnsw_index.md` covers the three modes, the restart-aware workflow, the `maintenance_log` schema convention, why the tool exists (upstream ChromaDB race, not a TAILOR bug), and the planned post-ingest verifier coming in v1.2.3 as the prevention complement.

### Fixed

- Recovers the `kb_search source_filter='document'` failure ("Internal error: Error finding id") caused by 66 chunks present in SQL metadata but missing from the HNSW pickle. All 66 trace back to a single ingest run on 2026-04-16 03:01:39. Repair re-embeds the affected chunks via `collection.upsert()`; running it requires v1.2.2 + the operator preconditions above.

### Stats

- 1 new utility (audit + apply, ~600 LoC including dataclasses)
- 1 new doc (`scripts/maintenance/repair_hnsw_index.md`)
- `maintenance_log` table now has its first writer
- 322 tests passing (310 baseline + 12 new in `tests/test_repair_hnsw_index.py`)

### Notes

- v1.2.3 will add the **post-ingest verifier** in `scripts/ingest/ingest_docs.py` — a vector-path read-back after each `collection.upsert()` that retries or marks chunks for repair if HNSW didn't catch them. This audit-and-fix utility recovers existing drift; the verifier prevents future drift. They are independent and ship separately.

---

## [1.2.1] - 2026-04-24 — Absolute `download_url` in KB payloads

Patch release fixing a hallucination vector in KB search responses served to smaller LLMs.

### Added

- **`server.public_base_url` config key** — new top-level `server:` section in `config/tailor.yaml.example` for setting the absolute base URL used when constructing KB download links. Defaults to `null` (auto-detect from `Host` + `X-Forwarded-Proto` request headers). Set explicitly when a reverse proxy or tunnel (Cloudflare, nginx, Caddy, Traefik) doesn't forward those headers the way auto-detect expects.
- **`_resolve_download_base_url(request)` helper** in `scripts/lib/kb_document_api.py` — single resolver consulted by `kb_search`, `kb_hybrid_search`, and `kb_find_document`. Returns `(base_url, source)` with `source ∈ {"config", "auto"}`, config taking precedence. Trailing-slash stripping is defensive (save-side validation already rejects them).
- **Dashboard Config → Server accordion** — single-field form for `server.public_base_url` with help text and a browser-origin placeholder as a hint at what auto-detect would produce.
- **`validate_server_section` config validator** in `scripts/lib/config_runtime.py` — rejects non-string, trailing-slash, and non-http(s) values on save with a clear 400. Prevents a typo from silently breaking every `download_url` emitted across the KB API.
- **Startup log** — one-line stderr log of `server.public_base_url` when configured. Silent in the default (auto-detect) case to avoid noise.

### Changed

- **`kb_search`, `kb_hybrid_search`, `kb_find_document`** now emit absolute `download_url` values for `source=document` results when either (a) `server.public_base_url` is configured, or (b) the incoming request carries a resolvable `Host`. Previous behaviour (relative `/api/kb/document?path=...`) is preserved for callers outside an HTTP context and when no `base_url` is threaded through. Rationale: smaller LLMs (GPT-4o mini, Haiku, DeepSeek) hallucinate absolute hostnames from relative tool-payload URLs (observed `https://api.kb/...`, invented vanity domains). Giving them the real hostname up front removes the guesswork.
- **`format_document_fileref(meta, base_url="")`** and **`find_documents(..., base_url="")`** gained a `base_url` keyword. Existing callers and tests that pass no `base_url` keep emitting relative URLs.

### Fixed

- Soft-reload correctly picks up `server.public_base_url` changes — the existing `config._config` cache-drop (phase 1 of `soft_reload()`) covers this field, so no MCP restart is needed when flipping the value from the dashboard.

### Docs

- Roadmap: removed "Absolute `download_url` in KB payloads" from **Next up** (shipped).

### Stats

- 1 new config key
- 1 new section in `tailor.yaml.example`
- 3 MCP tools updated (no breaking signature changes)
- 310 tests passing (baseline + new unit, helper, validator, and integration tests)

---

## [1.2.0] - 2026-04-22 — Chrome Web Store + Dynamic Model Picker

Minor release marking two step-changes: the browser extension is **live on the Chrome Web Store** (one-click install, no developer-mode gymnastics) and the dashboard's model selection moved from free-text to live provider catalogs.

### Added

#### Chrome Web Store availability

- The TAILOR browser extension is published on the Chrome Web Store: [install page](https://chromewebstore.google.com/detail/tailor-%E2%80%94-ai-memory-captur/phkhilpdfpmlbnfficopnbflhpmnfdfe).
- README install section rewritten to offer Chrome Web Store as the default path, with "developer mode from repo" kept as option B for contributors.
- New Chrome Web Store badge in the README header.

#### Dynamic model dropdown

- New endpoint `GET /api/chat/models?provider=<anthropic|openai|google|deepseek>` — fetches the live list of chat/reasoning models from each provider's `/v1/models` endpoint, filters out embedding/audio/image/tts models, and caches per provider for 1 hour in memory. Drop-in for any UI that needs to populate a model dropdown.
- Dashboard Config → **Chat interface** accordion: the per-row Model field is now a dropdown populated by the new endpoint (provider-aware, auto-refetched when the user flips the Provider select). Picks auto-fill Label with the model's display name when Label is empty or matches a prior auto-value. Fetch failures fall back to a free-text input so unlisted models stay usable.
- Dashboard Config → **Default LLM** accordion: same dynamic dropdown replacing the hardcoded two-option `<select>`. When `llm.model` doesn't appear in `chat_interface.available_providers` a neutral info tooltip explains the two sections serve different audiences (system-wide default vs. web-chat curated list) — no save-time block.
- Mobile Chat tab: the active model (or the pair selected for a new chat) is now tracked in the header badge via a new `mobileModelLabel` that falls through session pin → llm.* default → current dropdown pick.

### Fixed

- Chat model dropdown stuck on `Loading…` forever. The catalog prefetch used a `setModelCatalog` functional updater to mark "already fetching" before kicking off the HTTP request; React 18 batches the updater for the next render, so the guard flag stayed `false` every pass and `apiCall` was never reached. Replaced with a `fetchStateRef.current` map that's mutable synchronously across renders.

### Changed (performance)

- Dashboard static assets now ship with split cache policies:
  - `vendor/*` and `icons/*` (stable across releases): `public, max-age=604800, immutable` — browsers and CDNs skip revalidation entirely.
  - Everything else (index.jsx, chat.jsx, app-authored JS/CSS): `no-cache, must-revalidate` — revalidated on every load via conditional GET, 304 when unchanged.
  - `/dashboard` (index.html) previously had no `Cache-Control` header at all, now also `no-cache, must-revalidate`.
- Result: frontend changes show up immediately after deploy, without manual CDN purges or hard refreshes.

### Docs

- README roadmap: removed "Chrome Web Store — pending review" (shipped).
- README hero features: MCP tools count bumped from 21 to 22 (reflects `kb_find_document` added in v1.1.1).

### Stats

- 7 commits since v1.1.1
- 281 tests (baseline 273 + 8 new for `GET /api/chat/models`)
- 1 new HTTP endpoint
- 1 new significant frontend behaviour (dynamic dropdowns, 2 form integrations)

---

## [1.1.1] - 2026-04-21 — Runtime Provider Switching + Document Access

Patch release focused on two UX gaps surfaced while using v1.1.0 in production:
selecting a default LLM provider without editing YAML, and opening the source
document behind a KB answer.

### Added

#### Runtime Provider Switching

- `POST /api/chat/providers/default` — promote a `(provider, model)` pair from `chat_interface.available_providers` to the default `llm.*` configuration. Validates against the configured provider list and the supported-provider whitelist, rejects unknown pairs with 400. Writes `llm.provider` and `llm.model` to `tailor.yaml` (preserving `api_key`, `tool_use`, `max_tokens`, `temperature`), creates a timestamped backup, and soft-reloads `_brain`, `classifier`, `embedding`, and `i18n` singletons. No `.pending-save` marker — the narrow two-field change doesn't warrant the rollback pipeline.
- Dashboard **Chat** tab: "Set as default" button next to the provider dropdown, visible only when the selected pair differs from the current default. One click POSTs to the new endpoint, flashes `"✓ Default updated"` on success, or surfaces the error inline. Existing chat sessions keep whatever provider they locked to at creation.
- `" (default)"` suffix in the provider dropdown labels the currently-active pair.

#### Document Access

- `GET /api/kb/document?path=<relative>` — download the source file behind a KB chunk. Relative path is resolved against the new `ingest.document_root` config key with `realpath`-based path traversal guard. MIME inference covers `.docx`, `.xlsx`, `.pptx`, `.pdf`. Returns `Content-Disposition: inline` so browsers preview PDFs in place.
- `kb_find_document(query, n_results=10)` MCP tool — fuzzy finder over document metadata (filename, folder, path). Scoring prioritizes filename match, then folder, then path substring. Dedupes by `file_path`, filters fully-superseded documents, returns `title`, `file_path`, `folder`, `file_type`, `download_url`, `last_indexed_date`, `chunk_count`. Use when semantic content search isn't finding a document you know exists by name.
- Search-result enrichment: `kb_search` / `kb_hybrid_search` now include `file_path`, `file_type`, `folder`, and `download_url` in every result whose `source == "document"`. Non-document results are unchanged. Ranking, filtering, and supersession handling are untouched — this is pure output enrichment.
- New config key `ingest.document_root` in `config/tailor.yaml.example` (descriptive comment; must be an ancestor of every entry in `ingest.document_paths`).

### Changed

- Roadmap: removed "Runtime provider switching" (shipped in this release). Added "Voice I/O in chat" and "HUD chat skin" as separate future items (previously folded into the generic "Native Chat Interface" entry, now also shipped).

### Fixed

- Chat tab: `" (default)"` suffix no longer duplicates when the configured `chat_interface.available_providers` label already contains the word — case-insensitive guard in `labelFor`.
- `/api/kb/document` endpoint returned 401 Unauthorized on remote connections despite correct `ENDPOINT_PERMISSIONS` registration. The auth middleware's outer path whitelist was missing `/api/kb/`; requests fell through to the unauthenticated-fallback branch before endpoint-permission was checked. Whitelist updated.

### Data Migration (one-off)

- Removed spurious `superseded_by` + `superseded_at` metadata from **17 ChromaDB chunks across 5 documents** that had been erroneously marked as superseded by conversation summaries under the old chunk-level supersession pipeline (now archived in `archive/`). Legitimate `doc → doc_summary` supersessions (729 chunks) preserved. See `docs/supersession-requirements.md` for the semantic hierarchy rule that any future revival of chunk-level supersession must enforce.

### Docs

- New: `docs/supersession-requirements.md` — records the semantic hierarchy `document > doc_summary > conv_summary > {chatgpt, claude, email, telegram}` and the historical bug that prompted it.
- `config/tailor.yaml.example` — documents `ingest.document_root`.
- README: Changelog section already linking to `CHANGELOG.md` (added in v1.1.0); roadmap updated to reflect shipped items.

### Stats

- 10 commits since v1.1.0
- 273 tests (v1.1.0 baseline 249 + 24 new: 8 runtime provider + 7 document download + 3 search enrichment + 6 fuzzy finder)
- 2 new HTTP endpoints (`/api/chat/providers/default`, `/api/kb/document`)
- 1 new MCP tool (`kb_find_document`) — total now 22
- 17 chunks recovered from false supersession

---

## [1.1.0] - 2026-04-21 — Chat, Chrome, and Config

First major feature release after v1.0.0. Shifts TAILOR from "working memory engine" to "production-ready personal AI platform with its own ecosystem." Three headline additions — a native chat UI, a browser extension, and live runtime configuration — plus a full pass of security hardening.

No breaking changes from v1.0.0. See **Upgrade notes** below for optional migrations.

### Highlights

- **Native Chat Interface** — Dedicated Chat tab in the dashboard with SSE streaming, per-session provider selection (Anthropic / OpenAI / Google / DeepSeek / Ollama), SQLite session history, and a mobile-responsive shell. Provider is locked for the session's lifetime, so parallel chats on different models are possible.
- **Chrome Extension (Live Ingest)** — Browser extension captures conversations from Claude.ai, ChatGPT, and Gemini in real time and pushes them into the KB via `/api/ingest-live`. Full-conversation upsert with SHA-256 deduplication. Submitted to the Chrome Web Store.
- **Runtime Config Editor** — Edit `tailor.yaml` live from the dashboard (form UI + raw YAML editor). Every save is backed up, validated, and soft-reloaded without restarting the MCP server. Auto-rollback on boot failure.
- **API Key Management** — Encrypted storage (AES-256-GCM, per-entry random nonce) for provider API keys, backed by a master key at `~/.tailor/master.key`. Add, verify, rotate, delete from the dashboard; import from `.env` with diff preview.
- **Security hardening** — Rate limiting on failed auth, multi-token auth with per-tool permission tiers, hashed session tokens, path traversal guard.
- **Telegram Bot i18n** — All user-facing strings route through `t()` with English defaults.

### Added

#### Chat

- Native chat backend with SSE streaming and SQLite session store (`db/migrations/002_chat_sessions.sql`, `003_chat_sessions_provider.sql`).
- Unified `stream_chat_with_tools` execution path across providers.
- Dashboard **Chat** tab with streaming UI, session sidebar, and prompt suggestions.
- Per-session provider/model selection (dropdown on new chat).
- `GET /api/chat/providers` — list configured provider/model pairs.
- Mobile shell: off-canvas drawer, hamburger, iOS safe-area padding, 16px composer.
- `build_brain()` factory for ad-hoc per-session LLM clients.
- DeepSeek provider (OpenAI-compatible API).

#### Chrome Extension & Live Ingest

- Browser extension for Claude.ai, ChatGPT, and Gemini conversation capture.
- `POST /api/ingest-live` endpoint with full-conversation upsert semantics.
- CORS headers for Chrome extension origin.
- SHA-256/16-char dedup hash (upgraded from shorter hash).

#### Runtime Config Editor

- `scripts/lib/config_runtime.py` — backup + validation + soft-reload.
- Form view for common editable sections: `chat_interface`, `llm`, `classifier`, `embedding`, `persona`, `branding`, `user`.
- Advanced YAML view (CodeMirror 5 + js-yaml) for the rest.
- Client-side validation on Form (required + numeric); server-side `/validate` with 500 ms debounce on YAML, plus a **Validate now** button.
- Automatic backup to `config/.backups/tailor-YYYYMMDD-HHMMSS.yaml` before every save; rolling retention of the last 20 snapshots.
- Backups panel: list, download, restore. Restore is full-replace, including protected sections, with explicit confirmation modal.
- Force Restart button with confirmation, background polling of `/api/dashboard/setup`, and automatic dashboard reload on reconnect.
- In-process soft-reload of LLM brain, classifier, embedding module vars, and i18n language cache — most edits take effect immediately without restart.
- Auto-rollback on boot failure: every save writes a `.pending-save` marker. On boot, if the marker is younger than `SAFETY_WINDOW_SECONDS` (180 s), the previous backup is restored automatically — preventing a bad save from triggering a crashloop under KeepAlive. Outside the window the marker is stale and ignored, so unrelated crashes (OOM, disk full) do not falsely trigger rollback.
- 7 HTTP endpoints under `/api/dashboard/config/*` (current, validate, save, backups, backups/download, restore, restart).
- `docs/runtime-config-editor.md` (277 lines, hardware-agnostic).

#### API Key Management

- `scripts/lib/secrets_crypto.py` — master key lifecycle + AES-256-GCM helpers.
- `scripts/lib/secrets_store.py` — encrypted SQLite store (`db/secrets.sqlite3`) with FIFO backup rotation (max 20).
- `scripts/lib/secrets_verify.py` — per-provider connection test (Anthropic, OpenAI, Google, DeepSeek; 10 s timeout; zero key logging).
- `scripts/lib/secrets_env.py` — strict `.env` parser + diff computation.
- `scripts/lib/api_keys.py` — `resolve_api_key(provider, explicit)` with DB-first, env-var fallback.
- 10 HTTP endpoints under `/api/secrets/*` (list, set, delete, verify, backups, restore, master-key setup/export/import, env preview/import).
- Dashboard **API Keys** tab with add/verify/rotate/delete, `.env` import modal, master key card, backups panel.
- `db/migrations/004_secrets.sql`.

#### Auth & Security

- Rate limiting on failed auth attempts (5 tries → 30-minute IP ban).
- Multi-token auth with per-tool permission tiers (read / write / full).
- Hashed session tokens with reduced 7-day expiry.
- Status endpoints registered with read-level permission.

#### Telegram Bot

- `system_status` tool for operational queries (service health, uptime).
- Full i18n pass — all user-facing strings use `t()` with English defaults.
- Hardcoded Italian labels removed from bot prompt injection.

#### Other

- Ollama keep-alive by default (models stay hot).
- Dashboard screenshots in README + "Running in Production" section.
- Type hints added to public `mcp_server.py` functions and `scripts/lib/` modules.

### Changed

- `/api/dashboard/config/save` now creates a timestamped backup before writing, rejects blacklisted sections (`auth`, `paths`, `database`, `nightly`) with HTTP 403, validates the merged result is YAML-loadable, and soft-reloads config-derived singletons after a successful write. The Setup Wizard flow is unchanged.
- All config endpoints return HTTP status codes matching their response status (e.g. `/validate` returns 400/403 on failure instead of always 200 with a `valid: false` body).
- Live ingest redesigned to send full conversation with upsert semantics (instead of incremental append).
- Claude.ai DOM selectors updated; embedding size reduced for live capture.
- Fact extraction and entity extraction output labels aligned to English.
- Bot prompt injection uses English labels.
- Default persona fallback changed from `Jarvis` to `Tailor`.

### Fixed

- Form re-render dropping input focus on every keystroke (component hoisting fix).
- Runtime config restore UI not refreshing after successful restore.
- Spurious auto-rollback when explicit restart followed a save (`.pending-save` cleared before self-restart).
- CORS preflight dead code removed from `_handle_rest_api`.
- `_cors_headers` `NameError` in auth error responses.
- `_resolved_token` `UnboundLocalError` when auth failed before assignment.
- IMAP config reads aligned with nested `email.imap` YAML structure.
- Sent-message detection reads `email.addresses` (plural).
- Import ordering in `extract_facts.py`, `extract_entities.py`, `supersession.py`.
- `PYTHONPATH` exported in `sync_and_ingest.sh` for reliable imports.
- Facts DB queried directly for accurate timeout reporting.
- Files marked permanently failed after 3 ingest attempts.
- Bare `except:` clauses replaced with `except Exception` across codebase.
- Dashboard static file path traversal prevented via `realpath`.
- Embedding array flattened for `/api/ingest-live` endpoint.
- `/api/ingest-live` accessible from remote (Chrome extension origin).

### Removed

- `JARVIS_API_KEY` fallback from `mcp_server.py` (only `TAILOR_API_KEY` remains).
- 4 dead-code files moved to `archive/`.
- Unreachable CORS preflight branch.

### Docs

- New: `docs/runtime-config-editor.md`.
- README: roadmap updated, dashboard screenshots added, REST API table expanded, tool count corrected to 21, Telegram system-status queries documented, language config path corrected, Gemini parser description corrected, shipped features removed from roadmap.
- `config/tailor.yaml.example` — added `ollama`, `chat_interface`, `available_providers` (commented), optional embedding keys documented.
- `.claude/`, `docs/store/`, `.env*` added to `.gitignore`.

### Upgrade notes

No breaking changes. If you're coming from v1.0.0:

1. **Pull and restart.** Three new SQLite migrations (`002_chat_sessions.sql`, `003_chat_sessions_provider.sql`, `004_secrets.sql`) apply automatically on first boot.
2. **API keys: optional migration.** Existing env-var setups (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `GOOGLE_API_KEY`, `DEEPSEEK_API_KEY`) continue to work as a fallback. To migrate to encrypted storage: open the **API Keys** tab → generate a master key → click **Import from .env** → back up the master key.
3. **Chat interface: opt-in.** Set `chat_interface.enabled: true` in `tailor.yaml` and uncomment `available_providers` to enable the dropdown. See `config/tailor.yaml.example`.
4. **Multi-token auth: opt-in.** Existing single-token setups keep working. See the README for the multi-token config format.
5. **Chrome extension.** Pending Chrome Web Store approval. Manual install from `extension/` directory possible for advanced users.

### Stats

- 86 commits since v1.0.0
- 72 files changed, +12,734 / −619 lines
- 249 tests (all green)
- 3 new DB migrations
- 17 new HTTP endpoints (10 secrets + 7 config)
- 5 LLM providers supported (Anthropic, OpenAI, Google, DeepSeek, Ollama)

---

## [1.0.0] - 2026-04-13 — Initial Release

First public release. Self-hosted AI memory framework with persistent, searchable knowledge base — 100% on your hardware.

### Added

#### Knowledge Base

- ChromaDB vector store, 155k+ chunks capacity tested.
- Hybrid search: semantic + keyword + entity matching with ONNX cross-encoder reranking (~28ms/query).
- Atomic fact extraction with second-order derivation.
- Fact supersession: automatic detection and replacement of outdated information.
- Temporal grounding: dual-layer `document_date` + `event_date`.

#### Multi-Source Ingestion

- Documents: PDF, Excel, CSV, Word, PowerPoint (recursive folder scan).
- Email: Gmail (OAuth2) and generic IMAP.
- AI conversations: ChatGPT, Claude, Gemini export parsers.
- Cloud storage: OneDrive, Google Drive, Dropbox via rclone.
- Browser: drag-and-drop uploads from the dashboard.
- Telegram: real-time session capture.

#### LLM-Agnostic Backend

- 4 built-in providers: Anthropic, OpenAI, Google, Ollama.
- Config-driven enrichment with ordered multi-backend rotation on rate limits.
- Local intent classification via Ollama (Qwen 3.5).
- Proactive memory: LLMs save facts and session summaries autonomously.

#### MCP Native

- 21 tools via Model Context Protocol.
- Works with Claude, ChatGPT, and any MCP-compatible client.
- Autonomous tool use: the AI decides what to search and save.

#### Web Dashboard

- Real-time KB stats, service health, pipeline logs.
- 10-step Setup Wizard for first-time configuration.
- Live Config Editor for all settings.
- Conversation upload with background processing.
- Dark/light mode, responsive, zero external CDN dependencies.

#### Model Advisor

- Scans HuggingFace + Anthropic/Google/OpenAI APIs for new models.
- Filters by hardware constraints (RAM → max model size).
- One-click Ollama model install from the dashboard.
- Configurable schedule and tracked model families.

#### Telegram Bot

- Claude/GPT as conversation brain with full KB access.
- Natural language reminders with recurring schedules.
- Auto session capture on conversation timeout.
- Multi-language support via AI-powered i18n.

#### Nightly Pipeline

- Automated: cloud sync → ingest → entities → facts → supersession → derivation → profile.
- Maintenance mode via SIGUSR1/SIGUSR2 (zero downtime, no sudo).
- Config-driven timeouts and deadline.
- Telegram notifications on completion or error.

### Requirements

- macOS or Linux (Windows not supported).
- Python 3.11+.
- Ollama (for local embeddings).
- At least one LLM API key, or Ollama for fully local operation.

---

[Unreleased]: https://github.com/tailormemory/tailor/compare/v1.2.0...HEAD
[1.2.0]: https://github.com/tailormemory/tailor/compare/v1.1.1...v1.2.0
[1.1.1]: https://github.com/tailormemory/tailor/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/tailormemory/tailor/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/tailormemory/tailor/releases/tag/v1.0.0
