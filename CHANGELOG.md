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
