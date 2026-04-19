# Changelog

All notable changes to TAILOR are documented here.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning: semantic, starting at the first tagged release.

## [Unreleased]

### Added

- **Runtime Configuration Editor** in the dashboard's **Configuration** tab.
  Live-edit `config/tailor.yaml` without SSHing into the host or restarting
  the MCP server. See [`docs/runtime-config-editor.md`](docs/runtime-config-editor.md).
  - Form view for the common editable sections: `chat_interface`, `llm`,
    `classifier`, `embedding`, `persona`, `branding`, `user`.
  - Advanced YAML view (CodeMirror 5 + js-yaml) for the rest.
  - Client-side validation on Form (required + numeric); server-side
    `/validate` with 500 ms debounce on YAML, plus a **Validate now** button.
  - Automatic backup to `config/.backups/tailor-YYYYMMDD-HHMMSS.yaml` before
    every save; rolling retention of the last 20 snapshots.
  - Backups panel: list, download, and restore any snapshot. Restore is
    full-replace, including protected sections, with an explicit
    confirmation modal.
  - Force Restart button with confirmation, background polling of
    `/api/dashboard/setup`, and automatic dashboard reload on reconnect.
  - In-process soft-reload of LLM brain, classifier, embedding module vars,
    and i18n language cache — most edits take effect immediately, without
    restarting the MCP server.
- **Auto-rollback on boot failure**: every save writes a `.pending-save`
  marker. On boot, if the marker is younger than `SAFETY_WINDOW_SECONDS`
  (180 s), the previous backup is restored automatically — preventing a
  bad save from triggering a crashloop under process-manager KeepAlive.
  Outside the window the marker is stale and ignored, so unrelated crashes
  (OOM, disk full, etc.) do not falsely trigger a rollback.
- **Config API endpoints** on the dashboard namespace, all following a
  normalized `{ok, ...}` / `{ok: false, error, status}` response contract:
  - `GET  /api/dashboard/config/current`
  - `POST /api/dashboard/config/validate` (dry-run)
  - `POST /api/dashboard/config/save`
  - `GET  /api/dashboard/config/backups`
  - `GET  /api/dashboard/config/backups/download?filename=…`
  - `POST /api/dashboard/config/restore`
  - `POST /api/dashboard/config/restart`

### Changed

- `/api/dashboard/config/save` now creates a timestamped backup before
  writing, rejects blacklisted sections (`auth`, `paths`, `database`,
  `nightly`) with HTTP 403, validates the merged result is YAML-loadable,
  and soft-reloads config-derived singletons after a successful write.
  The Setup Wizard flow is unchanged: it only writes whitelisted sections
  and its payload round-trips through the same validation.
- All config endpoints return HTTP status codes matching their response
  status (e.g. `/validate` now returns 400/403 on failure instead of
  always 200 with a `valid: false` body).
