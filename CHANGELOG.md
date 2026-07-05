# Changelog

All notable changes to TAILOR are documented in this file.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning: [Semantic Versioning](https://semver.org/).

## [Unreleased]

<!--
Template for upcoming changes. Move entries under a new version heading on release.

### Added

- **Layer-0 esteso con regole zero-FP per identificatori alfanumerici.** Aggiunte
  3 regole a `classify_noise` che escludono dalla derivation codici/ID che oggi
  passavano: P.IVA/VAT (`^[a-z]{2}\d{4,}$`), codice fiscale persona IT (16 char),
  UUID canonico. Zero-FP per costruzione: nessuna entità reale ha quelle strutture
  (verificato che `plus500`, `wd40`, `o2`, `3m`, `bp`, `sha256` passano). Esclude
  +77 entità (54 vat_id, 16 uuid, 7 tax_code). NON inclusi i prefissi-fattura
  (allowlist da curare) né i pattern alfanumerici larghi (catturano entità vere
  tipo plus500/covid-19). Non tocca il rumore semantico mono-parola.
  File: [`scripts/enrichment/derive_facts.py`](scripts/enrichment/derive_facts.py).

- **Filtro pre-derivation "zero-FP extraction-junk" (layer 0).** Gate in ingresso
  alla fact derivation che esclude le entità chiaramente non-reali da estrazione:
  chiavi puramente numeriche, token-junk (`null`/vuote), frammenti UI noti,
  periodi temporali (`mese anno`). Solo categorie a zero falsi positivi -- esclude
  ~4.500 entità (13%) su ~35k. NON filtra i concetti generici mono-parola (es.
  "prezzi", "court"): stessa forma di aziende mono-nome reali (Google, CONAI),
  richiedono entity-quality scoring non-distruttivo (filone separato). Le entità
  filtrate restano in KB e NON ricevono watermark -> rientrano in derivation se la
  blocklist si restringe (reversibile). Bypass in `--entity`. Log di osservabilità
  con conteggi/categorie/campione. File:
  [`scripts/enrichment/derive_facts.py`](scripts/enrichment/derive_facts.py).
### Changed

- **WARNING drift: rimossa la CTA `repair_hnsw_index.py`.** Il WARNING (drift > 800)
  resta come segnale informativo della zona di lazy compaction [800,1000), ma non
  suggerisce più l'azione manuale di repair: era un consiglio no-op su uno stato che
  l'auto-flush risolve da solo. Il caso "drift davvero non scende" è già coperto da
  STUCK_VECTOR confermato (CTA condizionata) e da CRITICAL (drift > 1500, fuori dalla
  zona benigna, mantiene la CTA). Completa l'allineamento al gate dei tre rami di alert
  (queue STUCK, STUCK_VECTOR, WARNING drift).
  File: [`scripts/services/queue_depth_monitor.py`](scripts/services/queue_depth_monitor.py).

- **Queue alert sdoppiato in due segnali distinti** (`queue_depth_monitor.py`).
  Il monitor decideva STUCK su `queue_total` globale + età-oldest, mentre il gate
  auto-flush decide su `collection_queue_pending_count` (dal 12/06, commit
  `6be02c4`): disallineamento che generava STUCK falsi positivi con CTA
  `repair_hnsw_index.py` no-op. Ora due fenomeni separati:
  - **STUCK azionabile** (PAGE, mantiene CTA repair): scatta solo se
    `collection_queue_pending_count >= flush-watermark` (la STESSA soglia del
    gate, `auto_flush_queue.queue_min()`) in modo continuo da ≥48h. Il monitor
    legge la STESSA metrica del gate dalla cache che il gate persiste a ogni run
    (`auto_flush_queue.write_audit_cache` → `logs/gate_audit_cache.json`: ts +
    collection_pending + queue_total), non più `COUNT(*)` sul WAL globale e SENZA
    rieffettuare l'audit ogni 30 min. Staleness accettata `CACHE_MAX_AGE_HOURS=13`
    (copre il gap notturno 20→08 del gate); oltre soglia / cache assente / malformata
    → STUCK non valutabile (degrado safe, niente CTA no-op). Se l'auto-flush skippa
    legittimamente (collection sotto soglia), STUCK NON parte.
  - **WAL global aging** (osservabilità, severità WARNING/CRITICAL, NESSUN CTA
    repair): `queue_total >= WARNING_THRESHOLD` E non-decrescente nella finestra
    (`WAL_AGING_WINDOW_HOURS=6`), E nessuna collection sopra watermark. Triggerato
    su profondità+crescita, non sull'età statica dell'oldest. Messaggio "indaga
    sync_threshold/burst" senza bottone repair: tiene osservabile il rischio
    chromadb-backlog (#6975) senza spacciarlo per stuck azionabile.

  Le soglie 1200/1800 governano ora il solo WAL-aging (severità); STUCK è
  disaccoppiato (collection-pending + tempo). Snooze 6h indipendente per segnale
  (chiavi `stuck` / `wal_aging:<level>`). Il monitor è un job a sé
  (`com.tailor.queue-depth-monitor`): nessun restart MCP. Test:
  [`tests/test_queue_alert_split.py`](tests/test_queue_alert_split.py).
  File: [`scripts/services/queue_depth_monitor.py`](scripts/services/queue_depth_monitor.py).

- **STUCK_VECTOR del drift monitor allineato al gate (A+C)** — stesso tipo di FP del
  queue STUCK, su un ramo diverso. Il trigger scattava su `vector_seq` fermo + `meta_seq`
  avanzato tra 2 letture con `drift > DRIFT_WARNING` (800), cioè dentro la zona [800,1000)
  che è lazy compaction benigna (incidente 22/06: alert "consumer bloccato" alle 06:17,
  flush del gate alle 08:00 drift `836→0` senza problemi). Ora:
  - **A — pavimento a `SYNC_THRESHOLD`** (1000): sotto soglia `vector_seq` fermo è lazy
    compaction per definizione, non stuck. Elimina alla radice il FP della zona benigna.
  - **C — subordinato all'esito del flush del gate**: STUCK_VECTOR è *confermato* (CTA
    repair) solo se la cache audit dice che l'ultimo flush NON ha risolto
    (`flush_resolved=False`, dal contratto `success` del tool) e il `vector_seq` live non
    è avanzato oltre quello cachato → consumer davvero bloccato (#6975). Se il flush ha
    risolto → soppresso (transitorio pre-flush). Se C non valutabile (cache assente/stale)
    → `STUCK_VECTOR_UNCONFIRMED`: osservabilità, **nessuna CTA** e niente wording "consumer
    bloccato". La cache del gate (`write_audit_cache`) è estesa con `collection_name`,
    `vector_seq`, `drift`, `flush_ran`, `flush_resolved` (aggiornata post-flush); l'audit
    JSON del tool espone ora `collection_name`. Snooze 6h indipendente per label. Test:
    [`tests/test_drift_monitor.py`](tests/test_drift_monitor.py),
    [`tests/test_auto_flush_queue.py`](tests/test_auto_flush_queue.py). File:
    [`scripts/services/queue_depth_monitor.py`](scripts/services/queue_depth_monitor.py),
    [`scripts/services/auto_flush_queue.py`](scripts/services/auto_flush_queue.py),
    [`scripts/maintenance/repair_hnsw_index.py`](scripts/maintenance/repair_hnsw_index.py).

### Fixed

- **Fix silent-drop delle scritture post-maintenance.** Durante la finestra di
  maintenance dell'auto-flush (SIGUSR1→SIGUSR2, ChromaDB rilasciato), le scritture
  (`kb_add`, `kb_update_session`, `/api/ingest-live`) risolvevano l'handle collection
  UNA volta e lo passavano congelato al retry loop: se cadeva nella finestra, restava
  `None` per tutti i tentativi -> `None.upsert()` inghiottito e mis-etichettato come
  falso "chromadb silent-drop", contenuto perso in silenzio. Fix (C): `verified_upsert`
  ora ri-risolve la collection a OGNI retry (accetta un getter oltre a un handle),
  così un `None` catturato in maintenance viene ri-fetchato al tentativo successivo e
  persiste. Fix (B): distingue `None per maintenance` (errore onesto "in maintenance,
  ritenta" / HTTP 503) dal vero silent-drop chromadb, eliminando i falsi positivi
  "frozen-collection" dalla telemetria a ogni ciclo flush. Un getter che solleva è
  classificato `error` (contratto "retry>0 never raises" preservato). Batch-caller
  (retry=0) invariati. File: [`mcp_server.py`](mcp_server.py),
  [`scripts/lib/ingest_helpers.py`](scripts/lib/ingest_helpers.py).

- **Fact derivation: ripresa incrementale con priorità ai nuovi.** La derivation
  notturna ripartiva da capo a ogni run (`last_run` non avanzava sui run killati
  da SIGTERM, resume-list azzerata dopo 1h) -> ri-processava sempre le stesse
  entità di testa e la coda non veniva mai raggiunta (buco permanente). Ora:
  (a) cursore durevole per-entità (`derived_watermarks`, calcolato solo sui fatti
  sorgente escludendo `relation_type='derived'` per evitare l'auto-riapertura);
  (b) `last_run` avanza solo a coda completamente drenata, mai su SIGTERM/run
  parziale; (c) priorità ai nuovi (fatti `created_at > last_run` per primi, poi
  smaltimento backlog col tempo residuo); (d) run manuali isolate dallo stato
  nightly. Handler SIGTERM salva il progresso prima del kill. `derivation_max_min`
  portato a 180 (sblocco one-shot del backlog accumulato). Corretto anche il
  report "0 entities" post-timeout (letto dal checkpoint, non dal tail del log).
  File: [`scripts/enrichment/derive_facts.py`](scripts/enrichment/derive_facts.py),
  [`sync_and_ingest.sh`](sync_and_ingest.sh).
### Removed
### Security
### Docs
-->

### Added

- **Supporto file Markdown (`.md`/`.markdown`) nell'ingest.** Aggiunte le
  estensioni a `ingest.supported_extensions`; nuovo path di chunking dedicato
  (`split_markdown_at_boundaries()`) che preserva la struttura: code fence
  atomici (mai spezzati internamente, oversize-ma-bilanciati se superano il
  target), tabelle spezzate solo a confine di riga, prosa a confine di frase.
  Overlap disattivato per i `.md` (chunk header-anchored, già autoconsistenti) —
  era una seconda fonte di fence sbilanciati. `split_text_at_boundaries`
  condiviso con PDF/docx/pptx invariato. `tailor.yaml.example` allineato.
  File: [`scripts/ingest/ingest_docs.py`](scripts/ingest/ingest_docs.py).

### Fixed

- **Auto-flush queue HNSW: burst allineato al `sync_threshold`.** Il burst di
  re-upsert (`repair_hnsw_index.py --flush-queue-backlog`) era dimensionato a
  `max(sync_threshold+margin, drift+margin)` fisso, lasciando un residuo
  `(drift + burst) mod sync_threshold` non persistito quando il backlog era
  vicino alla soglia → l'auto-flush falliva (rc=5) gonfiando la queue invece di
  drenarla (queue 501 -> 601). Ora il burst è calcolato perché `(drift + burst)`
  sia il più piccolo multiplo di `sync_threshold` >= `drift + margin` (residuo
  0), con loop di top-up capped e success-criterion che richiede l'allineamento
  (`drift % sync_threshold == 0`). Validato sul campo: queue 607 -> 0 al primo
  ciclo auto-flush. File: [`scripts/maintenance/repair_hnsw_index.py`](scripts/maintenance/repair_hnsw_index.py).


---

## [1.3.0] — 2026-06-12 — ChromaDB #6975 gate hardening: drift-aware, collection-scoped, rc=4 escalations

### Added

- **Auto-flush del backlog queue HNSW (chromadb #6975).** Nuovo job schedulato
  `com.tailor.auto-flush-queue` (LaunchDaemon `UserName=jarvis`, orari fissi
  08:00/14:00/20:00, fuori dal nightly delle 02:00; mitigazione B-lean della
  race residua tra manutenzioni non coordinate) che esegue non-presidiato la procedura 5-step del runbook quando il
  backlog è azionabile e #6975-puro: gate `queue ≥ 300 AND orphans=0 AND
  ghosts=0 AND unknown=0`. Elimina il flush manuale ripetuto (5 incidenti in 9
  giorni). Niente sudo interattivo: maintenance ON/OFF via `kill -USR1/-USR2`
  (MCP gira come `jarvis`), restart MCP graceful via il grant NOPASSWD esistente
  `/etc/sudoers.d/jarvis-mcp` (`launchctl unload`+`load`, SIGTERM = shutdown
  pulito). Telegram START/SUCCESS/FAIL/ESCALATION, lock `fcntl`, log dedicato
  `logs/auto_flush.log`, `maintenance OFF` garantito in `finally`. Con
  orphans/ghosts/unknown > 0 NON auto-remedia: manda escalation e si ferma.
  Nuovi: [`scripts/services/auto_flush_queue.py`](scripts/services/auto_flush_queue.py),
  `run_auto_flush_queue.sh`, `launchd_proposed/com.tailor.auto-flush-queue.plist`,
  `tests/test_auto_flush_queue.py`.

### Changed

- **Daemon gate drift-aware.** `evaluate_gate` agisce ora anche su `drift ≥
  soglia`, non più solo sul backlog queue: chiude il gap latente drift-blind per
  cui un drift senza backlog azionabile passava inosservato. Il valore di drift è
  esposto nel JSON di audit. Commit
  [`088f2bf`](https://github.com/tailormemory/tailor/commit/088f2bf).
- **Gate flush su `collection_queue_pending_count`.** Il gate valuta il backlog
  azionabile della collection invece del `queue_total` globale; `_queue_extents`
  resta come metrica globale. Commit
  [`6be02c4`](https://github.com/tailormemory/tailor/commit/6be02c4).
- **`rc=4` del tool classificato come escalation gestita.** I refuse con `rc=4`
  non sono più trattati come FAIL ma come escalation gestite (non auto-remediabili,
  notificate e ferme). Commit
  [`7025058`](https://github.com/tailormemory/tailor/commit/7025058).
- **Pin `chromadb==1.5.9` in `requirements.txt`.** Allineato al venv installato:
  elimina il falso alert di update e rende riproducibile il drift #6975. Commit
  [`cc090f2`](https://github.com/tailormemory/tailor/commit/cc090f2).

### Fixed

- **Ghost benigni distinti dagli anomali nell'auto-flush gate.** Il gate
  riconosce ora i ghost benigni — vettori HNSW residui di una `DELETE` pendente
  topic-scoped, attesi e recuperabili — e non li conta come anomalia: niente più
  escalation a ogni GC notturno. Soglie drift-monitor allineate (100/500 →
  800/1500) tra test, docstring e runtime. Commit
  [`49d6130`](https://github.com/tailormemory/tailor/commit/49d6130), test fixture
  (tabella `collections` in `_make_chroma_sqlite`)
  [`49c85e8`](https://github.com/tailormemory/tailor/commit/49c85e8), soglie
  [`d563def`](https://github.com/tailormemory/tailor/commit/d563def).
- **Health probe auto-flush corretto.** La sonda di liveness MCP usa ora
  `/api/auth/check` (200, nessun hit ChromaDB) invece di `/` (404). Consolida il
  filone auto-flush di cui il timeout cold-start 30s→90s
  ([`b6ebe08`](https://github.com/tailormemory/tailor/commit/b6ebe08)) e il pattern
  `verify_backup()` su `tailor_db_full_*.tar.gz`
  ([`c9dd2a6`](https://github.com/tailormemory/tailor/commit/c9dd2a6)) erano fix
  intermedi. Commit
  [`877ddb1`](https://github.com/tailormemory/tailor/commit/877ddb1).
- **Orphan path topic-scoped + risoluzione topic tri-state.** Il calcolo degli
  orphan filtra ora per `final_ops` topic-scoped, chiudendo il difetto
  cross-collection (orphan contati su collection non pertinenti), simmetrico al
  path già applicato ai ghost. Commit
  [`eeb0fec`](https://github.com/tailormemory/tailor/commit/eeb0fec).
- **Log-isolation del test auto-flush.** `LOG_PATH` isolato a `tmp_path` nei test,
  stop all'inquinamento del log di produzione. Commit
  [`47382f7`](https://github.com/tailormemory/tailor/commit/47382f7).

### Docs

- **Drain mechanics HNSW corrette nel runbook (#6975).** Nessun restart drena né
  fa replay: il segmento ricarica il pickle alla `max_seq_id` precedente e
  attende; l'unico drain è `_persist()` a `sync_threshold` o un burst flush. Il
  session log 02/06 è annotato come superato da §0. Commit
  [`2b02a84`](https://github.com/tailormemory/tailor/commit/2b02a84), annotazione
  [`231bc43`](https://github.com/tailormemory/tailor/commit/231bc43). Rimosso il
  pointer morto `--prune-hnsw-ghosts` dall'output di audit. Commit
  [`5d6ef38`](https://github.com/tailormemory/tailor/commit/5d6ef38).

<!-- Errata entry moved to [1.2.10] on release 2026-05-17 -->

---

## [1.2.10] — 2026-05-17 — Silent-drop closure on conversational write paths

### Fixed

- **Silent-drop closure on the three conversational write paths.** The
  `verified_upsert()` guard introduced in v1.2.6.3 to detect chromadb 1.5.8's
  silent-drop (an `upsert()` that returns success but does not persist) was
  wired into the four batch ingest scripts but not the three mcp_server-side
  write paths. `kb_add`, `kb_update_session` and `/api/ingest-live` now route
  through `verified_upsert` with bounded retry; on persistent verification
  failure the caller receives an explicit error instead of a silent loss.
  `_update_entity_index` was moved inside the success branch so a failed
  persist can no longer leave a dangling entity-index entry. Three real ghost
  vectors observed in production on 2026-05-16 confirmed the prior gap; deployed
  and verified across both daemons (MCP + Telegram, in-process import) the same
  day. Commits: [`bf3335d`](https://github.com/tailormemory/tailor/commit/bf3335d)
  test baseline, [`633e089`](https://github.com/tailormemory/tailor/commit/633e089)
  retry kwarg, [`599acc2`](https://github.com/tailormemory/tailor/commit/599acc2)
  shared mock, [`7954e49`](https://github.com/tailormemory/tailor/commit/7954e49)
  callsite wraps, [`f4f9e8c`](https://github.com/tailormemory/tailor/commit/f4f9e8c)
  repair classifier.
- **`repair_hnsw_index.py --apply` re-armed.** A table-driven ghost classifier
  (`_KNOWN_GHOST_PATTERNS`) recognises the `kb_add` / `kb_update_session` /
  live-capture ghost id shapes so `--apply` is no longer blocked by
  `unknown_count > 0`. The existing `doc_` / `_chunk_` path is preserved
  bit-identically (purely additive). Verified in production post-deploy:
  `unknown_count: 0`.

### Docs

- **Errata to the 2026-05-13 chromadb post-mortem** — appended as §11,
  documenting that v1.2.6.3's silent-drop fix covered four of seven chromadb
  write callsites, the evidence (three production ghosts plus a test suite red
  since v1.2.6.3), and two generalised lessons (verify by callsite enumeration
  not by sample; a fix shipped with a red suite is an unverified fix).
  Commit [`e913e00`](https://github.com/tailormemory/tailor/commit/e913e00).

---

## [1.2.9] — 2026-05-14 — Scheduler resilience & nightly hardening

Reliability-focused release: four fixes on the ChromaDB integrity check and nightly pipeline, a scheduler refactor that lets cron and launchd coexist on the same host, and two new nightly safety nets (gitleaks secret scan + full-DB tar.gz backup). One UI focus-loss regression closed, plus a new sync path for small Gmail triage jobs that don't fit the OpenAI Batch API SLA.

### Added

- **`run-sync` subcommand for Gmail triage** (`scripts/enrichment/gmail_*`). Parallel to the existing Batch API path, this calls `/v1/chat/completions` directly with a 5-worker `ThreadPoolExecutor`, exponential backoff with jitter on 429/5xx (3 attempts), and append-only progress in `data/gmail_triage_sync_progress.jsonl` (thread-safe; `--resume` skips already-processed `custom_id`s). Output schema identical to the batch path — `chunk_gmail.py` parses both unchanged. Intended for jobs ≲ 1000 emails; the batch path remains the right choice for large historical runs. Validated on 221 emails: 62.1 s wall, 3.6 req/s, ~$0.022 vs ~$0.011 batch (50% batch discount lost — negligible at this scale). Triggered by 2026-05-13/14 incident where a 221-email batch stalled at 0/221 for 50+ minutes with status `in_progress`.
- **Full-DB tar.gz disaster-recovery snapshot**. New `tailor_db_full_<TS>.tar.gz` on the 07:30 schedule, capturing everything needed for full restore: `chroma.sqlite3` + HNSW UUID segment dirs, `facts.sqlite3`, `entity_index.sqlite3`, `doc_registry.json`, `reminders.json`, user profile + overrides + translations + supplement plan, plus the encrypted `secrets.sqlite3` blob. Explicit whitelist (not blacklist); excludes WAL/SHM, maintenance lock, session DBs, `.bak` files. `KEEP=3`. The existing `chroma_*.sqlite3.gz` artifact is preserved — `sync_and_ingest.sh:321` still consumes it for fast rollback; migrating that callsite is a separate task. Smoke test: 2 m 18 s wall, 1.85 GB tar.gz, `gzip -t` OK, all five HNSW segment files present (`data_level0.bin` 507 MB, `link_lists.bin`, `header.bin`, `length.bin`, `index_metadata.pickle`).
- **Nightly secret scanner via gitleaks**. Countermeasure to the 2026-05-04 incident (a `zsh` redirect typo wrote `TELEGRAM_BOT_TOKEN` plaintext into 4 untracked working-tree files). Three components: (i) `scripts/maintenance/secret_scan.sh` — bash wrapper with atomic `/tmp/tailor_secret_scan.lock`, sources `/etc/tailor/env` for the Telegram token, invokes `gitleaks dir`, writes redacted findings to `logs/secret_scan.log` (path/line/RuleID only — never values), sends a Telegram alert when findings > 0, always exits 0 (alert, not crash); (ii) `scripts/maintenance/secret_scan.gitleaks.toml` — extends the default 200+-rule set, allowlists 17 paths (`.venv`, `db`, `data`, `backups`, `logs`, `tests`, `docs`, …) to suppress generic-api-key FPs from log session strings; (iii) `launchd_proposed/com.tailor.secret-scan.plist` — user LaunchAgent at `StartCalendarInterval` 00:15, before sync-and-ingest 01:30. Tool: `gitleaks` 8.30.1 via brew. Smoke test: 0.5 s clean run over a 54 GB working tree with `--max-target-megabytes 5`; planted fake `ghp_…` detected, log redacted, Telegram POST sent.

### Changed

- **Scheduler: single-instance lock per cron+launchd coexistence**. The 6 jobs migrated to user LaunchAgent now acquire an internal lock before running real work, so cron — which can't be cleanly disabled on this Mac (TCC blocks `crontab` edit; SIP blocks `bootout` of `com.vix.cron`) — coexists without double-runs. 3 bash jobs (`backup_db`, `sync_and_ingest`, `sync_email`): `mkdir`-atomic lock in `/tmp/tailor_<job>.lock` with stale-PID reclaim and `EXIT` trap cleanup. 3 Python jobs (`heartbeat`, `reminder_checker`, `model_advisor`): `fcntl.flock` advisory (`LOCK_EX|NB`), auto-released by the kernel on FD close. `model_advisor.py` aligned with siblings by adding `load_env()`. Cron silenced operationally via `launchctl disable system/com.vix.cron` + `kill -STOP $(pgrep -x cron)`.
- **LaunchAgent source-of-truth templates committed**. The 4 plists (`heartbeat`, `reminder-checker`, `model-advisor`, `sync-email`) were byte-identical with what is deployed under `~/Library/LaunchAgents/` but had never been committed into `launchd_proposed/`. Now tracked so future edits start from a consistent baseline.
- **Nightly step-budget caps tuned**. Derivation cap 60 → 55 min, extraction cap 200 → 205 min. Empirical p95 of natural derivation completions = 47.7 min (n=16, Apr 25 → May 10); 55 min leaves 7 min margin above p95. The 5 min reclaimed is reallocated to extraction.
- **`tailor.yaml` example: `email.addresses` bumped to 3 entries**. The template carried a single placeholder, but `chunk_gmail.py`'s identity filter (`is_user_sender OR is_user_sole_to`) already iterates the list. Three representative placeholders (personal, work, legacy-work) make the multi-mailbox case obvious during initial setup. Incident 2026-05-14: a 221-email triage was correctly classified (148 useful) but yielded zero chunks because the live `tailor.yaml` listed only two addresses, neither matching the export mailbox — the chunk filter silently dropped every useful email.

### Fixed

- **ChromaDB integrity check: signal-aware retry**. Incident 2026-05-13 01:31: a `SIGSEGV` in `chromadb_rust_bindings` (chromadb 1.5.8) emitted empty stdout+stderr; the check never read `$?`, so signal-killed Python was indistinguishable from a deterministic `EMPTY:0` / `ERROR:<msg>` — both fed the same `!= OK` branch and triggered auto-rollback. Hardening: 3 attempts total (1 + 2 retries, `sleep 5` between), retry **only** when exit > 128 (SIGSEGV=139, SIGABRT=134, SIGKILL=137); `EMPTY:0` / `ERROR:<msg>` still trigger immediate rollback (genuine-corruption safety net). Worst-case added latency on the 08:00-deadline pipeline: ~10 s. Five segfaults observed in the preceding 48 h (2026-05-12 04:01, 06:35; 2026-05-13 01:31, 04:01 ×2), all `KERN_INVALID_ADDRESS @ 0x0`.
- **ChromaDB integrity check: COW snapshot to dodge race**. Follow-up to the retry fix above. P0bis confirmed the segfault is deterministic, not flaky: any second `PersistentClient` opening the live DB path crashes when MCP holds FDs on it — so the 3-attempt retry alone would have failed all attempts. Root mitigation: APFS clone-copy `chroma.sqlite3` into `/tmp/chroma_check_$$` (~1.5 s for 2.5 GB) and run the check against the isolated directory. No second client ever touches the live path; the race is structurally eliminated. Cleanup via `ensure_mcp_exit_maintenance` `EXIT` trap (idempotent) plus an explicit `rm -rf` post-success. `cp` failure (disk full, perms) triggers a distinct Telegram alert and exits 1 **without** rollback — the live DB is fine, only the check infrastructure broke. The 3-attempt loop is kept as defense-in-depth.
- **`TAILOR_NIGHTLY_MODE=facts_only` + post-sync backup schedule restored**. The system-LaunchDaemon → user-LaunchAgent migration in `refactor(scheduler)` lost two settings: `TAILOR_NIGHTLY_MODE=facts_only` (without it, `sync_and_ingest.sh` defaults to `full` and re-runs `ingest_docs` / `create_missing_summaries` / `extract_entities` nightly — the ChromaDB-write steps `facts_only` intentionally skips); and the `backup-db` start hour (01:25, **before** sync-and-ingest 01:30 — a pre-sync snapshot). Restored `facts_only` in the new LaunchAgent and moved backup to 07:30 (after sync-email at 04:00) so the snapshot captures a quiet, coherent post-sync state. Manual cleanup post-migration: the 6 duplicate system LaunchDaemons removed; `/Library/LaunchDaemons/` now holds only the 4 genuinely system-level daemons (`chromadb_version_check`, `mcp`, `ollama`, `telegram`).
- **`ConfigPage` form re-render dropped input focus**. Same class of bug as the prior `RuntimeConfigEditor` fix (commit `e400ac4`): `Row`, `EditBtn`, `SaveBar`, and `Section` were defined inside `ConfigPage`'s render body, so every parent render produced fresh function references that React treated as new component types — the subtree unmounted and remounted, killing focus after every keystroke. Affected every text field across Intent Classifier / LLM Backend / Telegram / Identity / Enrichment / Pipeline Schedule. Fix hoists all four helpers to module scope and introduces `ConfigEditorCtx` carrying the previously closed-over locals (`themeName`, `editing`, `startEdit`, `saving`, `saveMsg`). The same "do NOT inline these back" comment block added above `ConfigPage` as for `RuntimeConfigEditor`.
- **Log wording: `deadline in X min` → `step budget remaining`**. The original message measured the budget remaining for the current step after reserving quota for subsequent steps — **not** the time until the absolute deadline. 3 occurrences renamed (extraction, supersession, derivation). No behavior change.

### Internal

- **gitleaks allowlist extended with `credentials/`**. The first nightly run (2026-05-14 00:15) flagged `credentials/gmail_credentials.json` and `credentials/gmail_token.json` (`generic-api-key` rule) — legitimate OAuth artifacts produced by the Gmail re-auth flow (`client_id`, `client_secret`, `refresh_token`), already `git check-ignore`'d, no push risk. Added to the allowlist alongside `data/`, `db/`, `backups/`.

### Docs

- **`CLAUDE.md` — conventions for AI agents**. 8-section convention file consolidating implicit conventions used across recent Roadmap #0 work: secrets handling, working-tree hygiene, authority boundaries (no `sudo` / no push / no root-owned edits), backup pre-edit pattern, validation post-edit, edit flow per round, restart-daemons mapping, commit & changelog conventions. Triggered by the 2026-05-04 token-echo incident. Also extends `.gitignore` with `.backups/` (covers root-level and nested `scripts/lib/.backups/` via a recursive pattern) to keep pre-edit code snapshots out of git.
- **CHANGELOG back-fill: v1.2.6.2 → v1.2.8**. Entries for v1.2.6.2, v1.2.6.3, v1.2.7, and v1.2.8 added retroactively, integrated with the GitHub Release bodies. Footer extended with the full compare-link chain from `[Unreleased]` through v1.0.0 (12 missing links added; v1.2.4.1 / v1.2.3.1 hotfixes intentionally out of CHANGELOG scope per existing convention). `[Unreleased]` section reset to the keep-a-changelog template comment.

### Stats

- 15 commits since v1.2.8
- 2 new nightly safety nets: gitleaks scan (00:15) + full-DB tar.gz backup (07:30)
- 1 new code path: `run-sync` Gmail triage, parallel to the Batch API path
- 4 LaunchAgent templates committed to `launchd_proposed/` (previously deployed-only)
- 5 chromadb 1.5.8 `SIGSEGV` incidents in the 48 h preceding the retry + COW-snapshot fixes
- 1 UI focus-loss regression closed (`ConfigPage`, 6 legacy sections affected)

---

## [1.2.8] — 2026-05-08 — Roadmap #0: chat dashboard as control plane

Five commits land the next phase of the dashboard-as-control-plane roadmap: the chat-side LLM can now manage reminders and read whitelisted personal documents end-to-end, plus a tightening of the REST API auth model. Internal cleanup: `validate_doc_path` extracted as a shared helper and a `SELF_PAGINATED_TOOLS` opt-out added to `tool_executor` for tools that handle their own pagination.

### Added

- **Reminder tools in the chat dashboard**: `create_reminder`, `create_recurring_reminder`, `list_reminders`, `delete_reminder` exposed via `scripts/lib/tool_executor.py` and `scripts/lib/llm_client.py`. Wrappers delegate directly to `mcp_server.*` (no HTTP hop, since the chat dashboard imports the MCP server as a Python module). The LLM can now create one-shot or weekly-recurring reminders, list them, and delete them by id without leaving the conversation.
- **`read_personal_doc` MCP tool**: read text content from whitelisted personal documents. Extensions: `.txt .md .csv .pdf .docx .xlsx`. Plain text via `open(encoding="utf-8", errors="replace")`; binary formats text-extracted by reusing `extract_text()` from `scripts/ingest/ingest_docs.py` (direct import, no refactor). Pagination via `offset` / `length` (default `length=50000`). Whitelist via `ingest.document_paths` in `tailor.yaml`; refuse-all if empty/missing. Wired through to the chat dashboard alongside the reminder tools.
- **`SELF_PAGINATED_TOOLS` set** in `tool_executor.py`. Tools that paginate via their own `offset`/`length` arguments (currently only `read_personal_doc`) bypass the default 6000-char output cap, avoiding silent corruption of paginated payloads. The default cap stays in place for all other tools.
- **`validate_in_whitelist()` helper** in `scripts/lib/kb_document_api.py`. Validates a resolved abs_path is under at least one whitelisted folder; raises `DocPathError(reason="whitelist_empty"|"not_whitelisted")`. Used by `read_personal_doc` after `validate_doc_path()`.

### Changed

- **`DocPathError` reasons extended** to cover the new whitelist checks. Set is now `{"empty", "absolute", "is_root", "outside_root", "whitelist_empty", "not_whitelisted"}`, all on a single exception class with a `reason` discriminator.
- **`dashboard/index.html`**: 8 `fetch()` and `<a href>` call sites no longer append `?token=${API_TOKEN}` to URLs. They now rely solely on the `tailor_session` cookie (`SameSite=Lax`, `HttpOnly`, `Path=/`).
- **`BearerAuthMiddleware` docstring** rewritten to spell out the asymmetric auth model per path.

### Fixed

- **Reminder ID collision** (`mcp_server.create_reminder`, `telegram_bot.add_reminder`/`add_recurring_reminder`). The previous `int(datetime.now().timestamp() * 1000)` produced colliding ids when two reminders were created within the same millisecond — common with rapid LLM tool calls. `delete_reminder` (id-equality filter) was non-idempotent on collisions. Switched to `time.time_ns()` (nanosecond resolution, chronological ordering preserved, zero collisions in practice). In `mcp_server.create_reminder` the public parameter `time` shadowed the module, so used a local `from time import time_ns as _time_ns` to avoid renaming the API.

### Security

- **BREAKING (REST API only)**: the `?token=` query parameter is no longer accepted on `/api/*` endpoints. Remote callers must use the `Authorization: Bearer <token>` header (programmatic clients) or the `tailor_session` cookie set by `POST /api/auth/login` (browser dashboard). Mitigates token leak via Cloudflare access logs, browser history, and HTTP `Referer` headers.
- The `/mcp` transport still accepts `?token=` because the Claude.ai cloud MCP connector UI provides no field to configure an `Authorization` header. Asymmetric on purpose — to be removed when the OAuth provider lands (tracked as v1.3.0). Token redaction in uvicorn access logs continues to cover the log-leak vector for `/mcp`.

### Internal

- **`validate_doc_path()` helper extracted** from `handle_kb_document_request` into `scripts/lib/kb_document_api.py` as a module-level function. Pure path-resolution: rejects empty / absolute paths, applies realpath containment under document root. Behavior diff = zero on the HTTP handler; the existing `tests/test_kb_document_api.py` (7 tests) pass invariant. Sets up the reuse by `read_personal_doc`.

### Docs

- Module header in `mcp_server.py` documents the asymmetric auth split: `/api/*` → header/cookie only; `/mcp` → header or `?token=`.
- `README.md` adds an "Auth model — asymmetric" callout under the remote-access setup section, explaining the `?token=` carve-out for `/mcp`.

### Stats

- 5 commits since v1.2.7
- 1 new MCP tool (`read_personal_doc`) + 4 chat-side wrappers (reminder family)
- 1 security regression closed (`?token=` URL exposure on REST API)
- 0 behavior changes on existing HTTP endpoint paths (the `validate_doc_path` refactor is zero-diff)

---

## [1.2.7] — 2026-05-06 — Operational hardening for chromadb 1.5.8 frozen-state recovery

Operational hardening release following the May 5 incident, where the chromadb 1.5.8 collection re-entered a "frozen state" 8 hours after the May 4 dump+restore (Fase C in v1.2.6.3). Adds tooling for safe manual ingest, a defensive try/except in entity extraction, NOPASSWD sudoers documentation for autonomous operation, and a `TAILOR_NIGHTLY_MODE=facts_only` env var to keep the nightly facts pipeline running while skipping chromadb-write steps. Rolls up 8 commits since v1.2.6.3.

The combination of upstream issues #6975 (HNSW persist threshold) and #6852 (Rust bindings SIGSEGV on macOS ARM64) makes write-path failures more frequent on macOS ARM64 self-hosted deployments with long-lived chromadb 1.5.8 collections. The tooling here is platform-agnostic and works correctly when the underlying issues don't manifest. The included `chromadb_version_check` daemon will notify when 1.5.9+ ships upstream with a potential fix.

### Added

- **`scripts/maintenance/ingest_safe.sh`** — manual ingest workaround that bypasses the chromadb 1.5.8 compactor concurrency window. Pattern: backup `db/` via rsync → stop MCP via `launchctl bootout` → run `ingest_docs.py` with no concurrent writers → audit drift → restart MCP → Telegram summary. Bail-outs on rsync fail, MCP not down within 30s, ingest rc=139 SIGSEGV, restart failure. Extended in subsequent commits with optional entity extraction (`extract_entities.py`) and doc summary generation (`create_missing_summaries.py`) to consolidate everything in a single MCP-down window. Flags: `--dry-run`, `--no-backup`, `--no-entities`, `--no-summaries`, `--ingest-only`, `--full`.
- **`TAILOR_NIGHTLY_MODE` env var** in `sync_and_ingest.sh`. Values: `full` (default, retrocompatible) — runs the full pipeline as before; `facts_only` — skips the three chromadb-write steps (`ingest_docs.py`, `create_missing_summaries.py`, `extract_entities.py`) while running the rest (rclone sync, GC, build_entity_index, generate_user_profile, extract_facts_nightly, fact_supersession, derive_facts). Activation via plist `EnvironmentVariables`. End-to-end validated: 5h57m run, +43,558 facts total, MCP stable throughout via SIGUSR1/2 maintenance mode (no hard down).
- **Defensive try/except on paginated read** in `scripts/enrichment/extract_entities.py:402`. The recurring "Error finding id" that affected 13 nightlies (Apr 14 – May 4) was caused by 98 corrupted chunk IDs, removed by Fase F. This patch ensures any future similar corruption events do not take down the entire entity extraction step. The script logs a warning, increments offset, and continues.
- **`docs/OPERATIONS.md`** — operational documentation for `ingest_safe.sh` usage, NOPASSWD sudoers setup (`/etc/sudoers.d/tailor` granting `launchctl bootout/bootstrap` on `com.tailor.mcp` only), removal procedure, and trade-off discussion.

### Changed

- **`ingest_safe.sh` Telegram delivery** — bash 3.2 array expansion bug fixed; retry-without-parse_mode pattern ported from v1.2.6.2 to handle markdown parse errors on paths with underscores.
- **`scripts/services/chromadb_version_check.py` + LaunchDaemon** — biweekly check (`StartInterval=1209600`) against PyPI for chromadb 1.5.9+ availability. Reads pinned version from `requirements.txt` (single source of truth), sends Telegram alert only on update available (zero noise on no-update or network error).

### Operational architecture (post-release)

| Component | Trigger | Mode |
|---|---|---|
| Nightly facts pipeline | LaunchDaemon `com.tailor.sync_and_ingest` at 03:00 | `TAILOR_NIGHTLY_MODE=facts_only` (skip chromadb writes) |
| Manual ingest + entities + summaries | `~/tailor/scripts/maintenance/ingest_safe.sh` on demand | MCP bootouted via NOPASSWD sudoers |
| Chromadb upstream monitor | LaunchDaemon `com.tailor.chromadb_version_check` biweekly | Telegram alert on 1.5.9+ |

Manual ingest is the workaround for the chromadb 1.5.8 #6975 risk; cadence is operator-driven (typically weekly when OneDrive backlog accumulates). When a stable upstream fix or a backend migration lands, the manual workaround can be deprecated by re-enabling step 4 in nightly mode.

### Validation

- Defensive try/except in `extract_entities.py`: `--stats` run completes in 19s, 152k entities aggregated, zero new WARNING entries (collection clean post-Fase-F).
- `TAILOR_NIGHTLY_MODE=facts_only` end-to-end: 21,408s wall-clock, +43,558 facts, +3,470 derived facts. Chromadb embeddings count delta +11 (live chat ingest from extension during the run, NOT pipeline writes — pipeline correctly skipped writes).
- `ingest_safe.sh` extended run: 204 files re-ingested, +2,199 chunks persisted, 0 SILENT DROP detections by the v1.2.6.3 sentinel. MCP HTTP 200 latency back to ~0.6s post-run (was ~10.3s with 41 sql_only_orphans pre-Fase-F).
- NOPASSWD sudoers: `visudo -c` validation OK, `sudo -n /bin/launchctl bootout system/com.tailor.mcp` returns expected error without password prompt.

### Known issues / deferred

- **chromadb 1.5.8 still pinned** (no 1.5.9 on PyPI as of release date). The biweekly monitor will surface upstream releases automatically. Issues #6975, #6926 (pickle deserialization CVE), #6852 (Rust SIGSEGV on macOS ARM64) all remain open upstream.
- **Manual ingest is operator-driven**, not automated. Auto-scheduling `ingest_safe.sh` would require additional hardening (lock file, silent-drop ratio guard, smoke-fail recovery with `KeepAlive=false`). Tracked for a future release if the manual cadence proves insufficient.
- **Chromadb migration strategy** (Qdrant self-hosted, pgvector, or stay on chromadb with current workarounds) is the next architectural decision and remains in backlog.
- **HNSW pickle persistence at 1000-write threshold** (#6975 root cause). The repair utility from v1.2.2 still works as fallback; the operational architecture in this release reduces but does not eliminate exposure.

### Stats

- 8 commits since v1.2.6.3
- 2 chromadb frozen-state events surfaced and recovered (May 5 morning + afternoon)
- 1 dump+restore execution on production (Fase F, ~10 min wall-clock, 154,079 chunks)
- 204 registry desync entries identified and cleaned
- 2,199 chunks re-ingested via `ingest_safe.sh`, 0 silent drops
- 43,558 facts added in the first `TAILOR_NIGHTLY_MODE=facts_only` validation run
- 0 permanent data loss beyond 27 ephemeral live-chat chunks

---

## [1.2.6.3] — 2026-05-04 — ChromaDB 1.5.8 silent drop: surface, resolve, recover

Critical hardening release closing the post-incident loop opened on 1 May. Surfaces and resolves a ChromaDB 1.5.8 collection-state bug (silent drop) that caused the nightly pipeline to log success while writing nothing to the vector DB. Includes the post-upsert sentinel that finally caught the underlying issue, plus the operational dump+restore run that recovered the production collection.

### The bug

The 1 May incident left collection `tailor_kb` in a state where ChromaDB 1.5.8 Rust bindings would return success on `upsert()` but **silently no-op the write**. Symptoms:

- Nightly logs reported `Ingest completed: N chunks from M files` ✅
- `count(*) FROM embeddings` did not move
- Files appeared in `doc_registry.json` with no chunks behind them
- A new collection on the same SQLite DB worked perfectly — the issue was scoped to the specific collection's HNSW state

The first diagnosis (4 May morning) blamed the MCP `_enter_maintenance` signal handler for not releasing the SQLite lock. That hypothesis led to v1.2.6.2 (real correctness bugs fixed, but did not stop the silent drop). A second forensic audit, triggered by an attempted manual ingest with MCP fully booted out, isolated the bug to the collection itself.

### Added

- **`verified_upsert()` post-upsert sentinel** in `scripts/lib/ingest_helpers.py`. After every batch upsert, samples a chunk ID with `col.get(ids=[sample])` and verifies persistence. On silent drop: logs `SILENT DROP detected: chunk_id=X file=Y` to stderr, returns `False`, propagates failure into `$ERRORS` for Telegram surfacing. Tested on a frozen collection clone (returns False ✅) and on a healthy clone (returns True ✅).
- **Maintenance utility `scripts/maintenance/dump_restore_collection.py`**: reads all chunks from a frozen collection via paginated `get()`, drops the collection, recreates it empty, restores all chunks via `add()` in batches. Validated via PoC on a 1,000-chunk clone before being run in production on the 156,307-chunk collection (25 min total: 45s dump + 18s drop + 177s restore + verification).

### Recovery procedure (production, 4 May 17:30–18:00)

1. **Backup**: `chroma_pre_fase_C_20260504_173928.sqlite3.gz` (862 MB) + `hnsw_pre_fase_C_20260504_174009.tar.gz` (416 MB)
2. **Stop MCP**: `sudo launchctl bootout system/com.tailor.mcp` (releases SQLite write lock)
3. **Dump**: 155,000 of 156,307 chunks recovered in 45s via paginated `get()`. The 1,307 unrecoverable rows were ghost entries from the original 1 May incident — `get()` returned `Error finding id` for them, confirming they were already dead pre-Fase-C.
4. **Drop + recreate** collection `tailor_kb`: 18s
5. **Restore**: 155,000 chunks in 177s via batched `add()`, 0 errors
6. **Test write**: pre 155,000 → post 155,001 → cleanup → 155,000 ✅
7. **Restart MCP**: clean.

Post-recovery: a stray `ingest_docs.py` run (later isolated and cleaned up via `col.delete(where={"file_path": ...})` for 164 orphan files) wrote 1,903 chunks across 164 files, all of which **persisted correctly** — empirical confirmation that the silent-drop bug is resolved at the collection level.

### Validation

- Sentinel tested on frozen-collection clone: returns `False` + stderr log ✅
- Sentinel tested on healthy clone: returns `True` (no false positive) ✅
- Production write delta after dump+restore: +1 (test write) ✅
- 1,903-chunk batch ingest in cleanup phase: full persistence ✅
- Registry vs ChromaDB coherence post-cleanup: 3,009 file_paths in both, 0 orphans, 81 zombies (pre-1-May, separately tracked).

### Known issues / deferred

- **ChromaDB 1.5.8 still pinned.** The frozen-collection bug appears to be triggered by abnormal shutdown during HNSW write (the 1 May incident). A clean collection on 1.5.8 has not reproduced the bug post-recovery. **Decision pending**: pin to 1.4.x (last known good) vs. wait for upstream fix in 1.5.9+. Tracked for v1.2.7.
- **`extract_entities rc=1`** observed during the 4 May audit. Orthogonal to silent drop; investigation deferred to v1.2.7.
- **81 zombie entries** in `doc_registry.json` (registry has them, ChromaDB doesn't) — pre-1-May residue, isolated and tracked but not cleaned in this release.

### Stats

- 1 critical bug surfaced + resolved (ChromaDB 1.5.8 silent drop)
- 156,307 → 155,000 chunks during dump+restore (1,307 ghost rows isolated)
- 1,903 chunks validated as written + persistent in post-recovery cleanup
- 11h continuous incident-response session (4 May 07:00–18:00)
- 0 data loss beyond the 1,307 pre-existing ghosts

---

## [1.2.6.2] — 2026-05-04 — Telegram delivery + strict mode + facts path

Pipeline-correctness patch in the post-1-May diagnostic chain. Did not stop the silent drop (which v1.2.6.3 nailed) but fixed three real correctness bugs surfaced during the audit: garbled Telegram messages on dollar-sign content, wrong sqlite path for the FACTS_TOTAL summary, and a `set -uo pipefail` upgrade for the orchestrator script.

### Added

- **`send_telegram()` rewrite** in `sync_and_ingest.sh`. Replaces shell-quoted JSON with `python3 json.dumps()` to handle dollar signs, quotes, and newlines correctly. Falls back to plain text on Markdown parse error 400. Logs delivery outcome explicitly (no more silent failures on token / chat_id mismatch).
- **`set -uo pipefail`** in `sync_and_ingest.sh`. Strict mode minus `-e` (too many steps tolerate failures). Adds `${PYTHONPATH:-}` guard and similar defensive expansions to coexist with `set -u`.

### Changed

- **`FACTS_TOTAL` query path corrected** from `db/chroma/facts.sqlite3` (vestigial pre-1-May path) to `db/facts.sqlite3` (active). The vestigial path silently returned 0, which made the Telegram summary always report `facts: 0` regardless of actual fact count.

### Known issues / deferred

- **Silent drop still active.** The audit assumed the SQLite-lock + maintenance-handler hypothesis was the cause; this release ships those fixes but does not address the underlying ChromaDB 1.5.8 collection-state bug. v1.2.6.3 closes that loop with the post-upsert sentinel and the production dump+restore.

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

[Unreleased]: https://github.com/tailormemory/tailor/compare/v1.3.0...HEAD
[1.3.0]: https://github.com/tailormemory/tailor/compare/v1.2.10...v1.3.0
[1.2.10]: https://github.com/tailormemory/tailor/compare/v1.2.9...v1.2.10
[1.2.9]: https://github.com/tailormemory/tailor/compare/v1.2.8...v1.2.9
[1.2.8]: https://github.com/tailormemory/tailor/compare/v1.2.7...v1.2.8
[1.2.7]: https://github.com/tailormemory/tailor/compare/v1.2.6.3...v1.2.7
[1.2.6.3]: https://github.com/tailormemory/tailor/compare/v1.2.6.2...v1.2.6.3
[1.2.6.2]: https://github.com/tailormemory/tailor/compare/v1.2.6.1...v1.2.6.2
[1.2.6.1]: https://github.com/tailormemory/tailor/compare/v1.2.6...v1.2.6.1
[1.2.6]: https://github.com/tailormemory/tailor/compare/v1.2.5.1...v1.2.6
[1.2.5.1]: https://github.com/tailormemory/tailor/compare/v1.2.5...v1.2.5.1
[1.2.5]: https://github.com/tailormemory/tailor/compare/v1.2.4...v1.2.5
[1.2.4]: https://github.com/tailormemory/tailor/compare/v1.2.3...v1.2.4
[1.2.3]: https://github.com/tailormemory/tailor/compare/v1.2.2...v1.2.3
[1.2.2]: https://github.com/tailormemory/tailor/compare/v1.2.1...v1.2.2
[1.2.1]: https://github.com/tailormemory/tailor/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/tailormemory/tailor/compare/v1.1.1...v1.2.0
[1.1.1]: https://github.com/tailormemory/tailor/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/tailormemory/tailor/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/tailormemory/tailor/releases/tag/v1.0.0
