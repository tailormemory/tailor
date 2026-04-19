# Runtime Config Editor

The dashboard's **Configuration** tab lets you edit `config/tailor.yaml`
live — without SSHing into your TAILOR host or restarting the MCP server.
Every change is validated, backed up, and applied via an in-process
soft-reload of the affected components (LLM client, embedding module,
classifier, i18n cache).

This document covers:

1. [What the editor does](#what-the-editor-does) — UI-level behaviour.
2. [Operator / troubleshooting](#operator--troubleshooting) — backups,
   auto-rollback, manual recovery.
3. [What you cannot edit via UI](#what-you-cannot-edit-via-ui) — the
   protected sections and why.

---

## What the editor does

Open the **Configuration** tab and you'll see two nested panels:

- **Runtime Configuration** — the live editor.
- **Configuration Backups** — rolling snapshot history (collapsed by
  default; click to expand).

### Two views: Form and YAML

The runtime editor has a toggle between **Form** (default) and
**Advanced (YAML)**:

| View | Covers | Best for |
|---|---|---|
| **Form** | `chat_interface`, `llm`, `classifier`, `embedding`, `persona`, `branding`, `user` | Day-to-day tweaks: swap provider, change temperature, update suggested prompts |
| **YAML** | Everything in `tailor.yaml` except the four blacklisted sections (see below) | Structural changes, new keys, anything the form doesn't expose |

Both views share the same draft state, so edits made in one view are
visible in the other after switching. Switching from Form to YAML
reformats the buffer from the parsed draft — comments are preserved
while you stay in YAML view but are dropped if you round-trip through
Form. The toggle carries a reminder to this effect.

### Validation

| Layer | When | What it checks |
|---|---|---|
| **Form — client-side** | On click of **Save** | Required fields (`llm.provider`, `llm.model`, etc.) and numeric-type coercion. The first invalid field scrolls into view. |
| **YAML — client-side parse** | On every keystroke | YAML syntax. Parse errors appear inline with the line number. Save is disabled while the parse is broken. |
| **YAML — server-side** | 500 ms after typing stops | Full backend validation (blacklist, round-trip load). Indicator reads **… validating**, then **✓ Validates** or **⚠ {message}**. A **Validate now** button forces an immediate check. |
| **Save (both views)** | On click of **Save** | Same pipeline as `/validate`, plus the actual write and soft-reload. |

Semantic validity (real provider name, real model ID, temperature in a
sensible range) is **not** checked here. That's the provider's job at
call time — a bogus value surfaces the provider's own error message,
which is always more specific than what a pre-flight check could say.

### Every save creates a backup

Before writing the new config, the previous file is copied to
`<TAILOR_ROOT>/config/.backups/tailor-YYYYMMDD-HHMMSS.yaml`. The backup
dir holds at most **20** snapshots; rotation is FIFO (oldest removed
when the 21st is created).

The save response surfaces the backup filename — same thing you'll see
listed in the Backups panel.

### Backups panel

Expand **Configuration Backups** to see the rolling history. Per row:

- Human-readable timestamp (parsed from the filename).
- Size in KB.
- **Download** — fetches the raw YAML to your browser.
- **Restore** — replaces the current config with the backup (see
  [Restore](#restore) below).

No pagination — 20 rows fit a single list.

### Restore

Clicking **Restore** opens a confirmation modal that spells out:

1. This is a **full replace**, not a merge. Every key in the current
   config is discarded; the backup's content becomes the new live state.
2. **Protected sections are included.** A backup that was taken when
   `auth` / `paths` / `database` / `nightly` held a different value
   will put those old values back. The Form editor's blacklist does
   not apply to restore — a snapshot of a prior state is a snapshot of
   that prior state.
3. Your current config is backed up first. If the restore turns out to
   be the wrong one, the snapshot taken seconds ago is at the top of
   the Backups list and one more click reverses the operation.

### Soft reload, not restart

A normal save re-initialises the relevant in-process singletons — the
LLM brain, classifier, embedding module, and i18n language cache —
without restarting the MCP server. In-flight chat streams continue
on the pre-reload brain; new chats pick up the new config on the
next request.

### Force Restart MCP

For the cases soft-reload can't cover — changing a value your running
chat sessions are holding onto, picking up an environment variable
that changed on disk, or just "I want a clean slate" — the runtime
editor has a **Force Restart** button (styled danger, requires modal
confirmation).

On click:

1. The MCP process sends `SIGTERM` to itself.
2. Your process manager (LaunchDaemon, systemd, Docker `restart:
   always`, or whatever supervises your TAILOR host) relaunches it
   within a few seconds.
3. The dashboard polls `/api/dashboard/setup` in the background; when
   it responds, the page auto-reloads.

Expect ~5–10 s of unavailability. In-flight chat streams and any
other open requests will drop. Unsaved draft content in the runtime
editor is kept in memory across the reload (the page remounts but
the browser tab survives the server restart).

If polling runs past 30 s without a response, the modal shows a hint
to check your process-manager logs and offers a manual **Reload
dashboard anyway** button.

---

## Operator / troubleshooting

### Backup location

```
<TAILOR_ROOT>/config/.backups/tailor-YYYYMMDD-HHMMSS.yaml
```

- Timestamp reflects when the backup was **taken**, not when the
  backed-up file was last edited.
- Rotation is FIFO, keeping the last 20. Manually-dropped files in
  `.backups/` that don't match the `tailor-YYYYMMDD-HHMMSS.yaml`
  pattern are ignored by the rotator and the UI.
- The directory is `.gitignore`d by default.

### Auto-rollback on boot failure

If a save produces a config that breaks the MCP at boot (e.g. a
structural change that the loader can't handle, a lazy-init failure
on first request), the process manager will relaunch a crashing
server indefinitely. To prevent this crashloop, every save writes
a marker file:

```
<TAILOR_ROOT>/config/.pending-save
```

The marker records the save timestamp and the filename of the
pre-save backup. On every boot, the MCP checks for this file
**before** loading any module that reads `tailor.yaml`. The logic:

| Marker age | Meaning | Action |
|---|---|---|
| < 180 s (SAFETY_WINDOW) | Previous process likely died from this save | Copy the pre-save backup back over `tailor.yaml`, log, clear marker |
| ≥ 180 s | Save survived the window; death was something else | Clear marker, leave config untouched |
| Corrupt / unparseable | Something wrote garbage into it | Clear marker, no rollback |
| Backup listed but not found | Got rotated out | Clear marker, log, no rollback |

The 180 s window is a ceiling on how long a config-induced crash can
delay itself (import time + first request + process-manager relaunch).
Anything longer is almost certainly unrelated — OOM, disk full, an
upstream runtime bug — so we do not roll back.

### Debugging auto-rollback

Every rollback-related event logs to stderr with a `[config-rollback]`
prefix. Grep your process manager's log stream for it:

```bash
# systemd
journalctl -u <your-tailor-service> | grep config-rollback

# launchd (macOS)
grep config-rollback /var/log/system.log
grep config-rollback <TAILOR_ROOT>/logs/mcp.stderr.log

# Docker
docker logs <container> 2>&1 | grep config-rollback

# or wherever your deploy writes stderr
```

The messages tell you which branch fired (`rolled_back`, `stale`,
`backup_missing`, `cleared_corrupt`, `no_backup`) and the age of the
marker at boot. A typical rollback line:

```
[config-rollback] pending save aged 7s (<180s window), restored backup from .../config/.backups/tailor-20250419-213500.yaml
```

### Manual rollback

If auto-rollback didn't fire (or rolled back to a backup you don't
want), restore by hand:

```bash
# SSH into your TAILOR host
cd <TAILOR_ROOT>

# Inspect available backups
ls -lt config/.backups/

# Copy the one you want back over the live config
cp config/.backups/tailor-YYYYMMDD-HHMMSS.yaml config/tailor.yaml

# Restart the MCP server
# The exact command depends on your deploy's process manager:
#   systemd:   sudo systemctl restart <your-tailor-service>
#   launchd:   launchctl kickstart -k <your-launchd-label>
#   Docker:    docker restart <container>
#   manual:    kill the MCP process; your manager's KeepAlive relaunches it
```

The MCP server will pick up the restored config on its next start.
(Same result as clicking **Force Restart** in the dashboard once the
server is back online — handy when the dashboard itself is
unreachable.)

### When soft-reload is not enough

Most runtime edits take effect immediately. A few require a full
restart:

- **`embedding.provider` / `embedding.model` / `embedding.dimensions`**
  — changing these doesn't break the running server, but new embeddings
  will be produced by the new model while the existing Knowledge Base
  still holds the old vectors. You need a full re-embed for the change
  to matter. This is a larger operation than a simple restart.
- **`auth.*`**, **`paths.*`**, **`database.*`**, **`nightly.*`** —
  these are blacklisted from the UI entirely. Edit by hand (see below)
  and restart.

---

## What you cannot edit via UI

Four top-level sections are blocked from UI writes. If the UI sends a
payload containing any of these, the backend returns **403** with a
message naming the offending section. The YAML view will show the
current values of these sections — so you can see what's there — but
trying to save changes to them fails with the same 403.

| Section | Rationale |
|---|---|
| **`auth`** | API tokens and session credentials. A typo here means you lock yourself out of your own dashboard. |
| **`paths`** | On-disk layout (data directories, cache paths, model locations). A runtime change makes files unreachable or silently writes to the wrong place. |
| **`database`** | SQLite and ChromaDB connection config. An edit mid-flight can corrupt or lock the databases. |
| **`nightly`** | Ingest + enrichment pipeline schedule and limits. Changing these while a run is in progress leaves the pipeline in an inconsistent state. |

### Escape hatch

If you need to modify any of these sections, SSH into your TAILOR
host, edit `<TAILOR_ROOT>/config/tailor.yaml` directly, and restart
the MCP server via your process manager.

The standard backup dir is not populated by manual edits (backups are
only created by the UI save path). If you want a pre-edit snapshot,
copy `tailor.yaml` somewhere safe first:

```bash
cp <TAILOR_ROOT>/config/tailor.yaml \
   <TAILOR_ROOT>/config/.backups/tailor-manual-$(date +%Y%m%d-%H%M%S).yaml
```

Files matching the `tailor-YYYYMMDD-HHMMSS.yaml` pattern in that
directory show up in the Backups panel and can be restored from there.
Files that don't match the pattern (like `tailor-manual-*`) are
ignored by the UI — use your shell to restore those.
