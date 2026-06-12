# RUNBOOK — Alert drift / queue HNSW

Procedura operativa per gestire un alert TAILOR di **drift** o **queue backlog** sull'indice HNSW della collection `tailor_kb_v2`.

Tutti i comandi `.venv/bin/python3` (non il `python3` di sistema — manca `chromadb`). Le operazioni `sudo` / restart daemon / messaging sono **operatore-side** (Emiliano): l'agente AI riporta il comando, non lo esegue.

---

## 0. Contesto — chromadb #6975

`PersistentLocalHnswSegment` (chromadb 1.5.x/1.5.9) **persiste l'indice su disco solo** via `_persist()`, che ha **un solo** trigger automatico:

1. il contatore di scritture in-memory **cumulative** raggiunge `hnsw:sync_threshold` (default **1000** su `tailor_kb_v2`): `_persist()` applica gli UPSERT+DELETE pendenti, poi `purge_log()` ripulisce la WAL `embeddings_queue`.

In assenza di quel trigger, l'unico modo di forzare il drain è un **burst manuale** di re-upsert ≥ `sync_threshold` via `repair_hnsw_index.py --flush-queue-backlog` — che è un **trigger di `_persist()`**, non un consumer della queue (vedi step 3).

> ⚠️ **Non esiste un drain da shutdown.** L'atexit / `System.stop()` di chromadb 1.5.9 **non** persiste la WAL (verificato 10/06/2026). Il `PersistentClient` vive nei **globals del modulo**, slegato dal lifespan uvicorn: lo shutdown ASGI **non** chiama `System.stop()`. Quindi **nessun restart drena** — né graceful (bootout/bootstrap) né `kickstart -k`.

Conseguenze pratiche:

- Le scritture restano nel WAL `embeddings_queue` finché non scatta `_persist()` (sync_threshold cumulative **oppure** burst flush). Query e letture notturne **non** forzano il flush.
- Un **restart non drena il backlog** — in **qualunque** forma. Al boot il segmento ricarica il pickle alla `max_seq_id` vecchia e aspetta; il restart serve **solo a rinfrescare il client**, mai a drenare. `kickstart -k` (**SIGKILL**) è solo il caso più ovvio, ma anche il graceful lascia la queue **intatta**.
- **Vincolo operativo:** mai riavviare l'MCP con upsert in-memory non ancora persistiti — un crash/restart pre-`_persist()` li perde e vanno ri-consumati. Restart sicuro **solo dopo** drain confermato (audit `queue=0` o flush riuscito).
- Il **seq-drift** (gap `metadata_seq − vector_seq`) **non è perdita dati**: è lag del WAL. La perdita dati la misurano gli **orphans/ghosts** dell'audit, non il contatore seq.

Ref upstream: chroma-core/chroma#6975. Tooling interno: [scripts/maintenance/repair_hnsw_index.py](../../scripts/maintenance/repair_hnsw_index.py).

---

## 1. STEP 1 — Audit (read-only, sempre primo)

```sh
cd ~/tailor
.venv/bin/python3 scripts/maintenance/repair_hnsw_index.py --audit
```

Side-effect-free (scrive solo `logs/hnsw_audits/*.json` + una riga `maintenance_log`). Leggere queste tre righe:

```
SQL-only orphans:  <N>   (in SQL, mancanti da HNSW)
HNSW-only ghosts:  <M>   (in HNSW, mancanti da SQL)
Queue backlog:     <Q>   pending ops in embeddings_queue
```

---

## 2. Tabella verdetti

| orphans | ghosts | queue | Diagnosi | Azione |
|---|---|---|---|---|
| 0 | 0 | 0 | Indice in sync, nessun backlog | **Nessuna.** Falso allarme o già drenato. |
| 0 | 0 | > 0 | #6975 puro: backlog benigno nel WAL, nessuna perdita dati | **Flush** se `queue ≥ 300` (vedi step 3). Sotto 300: opzionale, può attendere accumulo naturale. |
| > 0 | 0 | qualsiasi | SQL-only orphans: chunk in SQL mai entrati in HNSW | `--apply` (re-embed orphans). **NON** `--flush-queue-backlog`. |
| 0 | > 0 | qualsiasi | HNSW-only ghosts: vettori orfani in HNSW | **Investigare** prima di agire. `--prune-hnsw-ghosts` non implementato. Escalation. |
| > 0 | > 0 | qualsiasi | Drift misto / corruzione | **STOP.** Non auto-remediare. Escalation + backup forense. |
| — | — | unknown types | Drift di tipo non riconosciuto | `--apply` e `--flush` **rifiutano** finché esistono. Investigare. |

> Il tool `--flush-queue-backlog` **rifiuta** da solo se rileva orphans / ghosts / unknown: gestisce **solo** il caso #6975 pulito (riga "REFUSING --flush-queue-backlog: this mode only handles clean #6975 queue backlog").

---

## 3. STEP 3 — Procedura flush completa (5 step)

Da eseguire **solo** sul verdetto `orphans=0, ghosts=0, collection_pending≥300` (o `drift≥800`). La gate di azionabilità è: `drift ≥ DRIFT_WARNING(800)` **OR** `collection_queue_pending_count ≥ FLUSH_QUEUE_BACKLOG_MIN(300)` — vedi commit `ccef05d` (gate dual) e lo switch a `collection_queue_pending_count` (#6975). Override soglia: `--queue-backlog-min N`.

> **Metrica di gating = `collection_queue_pending_count`, non `queue_total`.** La gate (tool e auto-flush) si basa sul backlog **azionabile per la collection** sopra il watermark (`collection_queue_pending_count`), non sul `queue_total` **globale** dell'`embeddings_queue`. Motivo: con WAL multi-topic un backlog di *altri* topic gonfia `queue_total` senza rendere questa collection flushabile (e viceversa). `queue_total` resta esposto come **metrica globale di osservabilità** in audit/log/Telegram, ma NON decide il gate. Missing-field in auto-flush: campo `collection_queue_pending_count` assente (tool vecchio) → fallback a `queue_total` (status-quo, WARN); presente ma malformato/negativo → `schema_invalid` → escalate. Refuse del tool quando il drift è ≥ soglia ma `collection_pending=0`: error `collection_queue_empty` (erede di `queue_empty`, entrambi allowlistati nel rolling deploy).

> **Automazione (default).** Questa intera procedura 5-step è ora eseguita non-presidiata dal job `com.tailor.auto-flush-queue` ([scripts/services/auto_flush_queue.py](../../scripts/services/auto_flush_queue.py), alle 08:00/14:00/20:00, fuori dal nightly delle 02:00). Gli orari non sovrapposti sono la mitigazione B-lean della race residua tra manutenzioni non coordinate. Il job agisce sul caso #6975-puro (`queue ≥ 300, orphans=0, ghosts=0, unknown=0`). Maintenance via segnali (no sudo), restart via grant NOPASSWD `unload`+`load`. Telegram START/SUCCESS/FAIL e log in `logs/auto_flush.log`. La procedura manuale qui sotto resta il **fallback** quando il job è disabilitato, fallisce (Telegram FAIL → investigare, non ritentare alla cieca), o quando il verdetto NON è #6975-puro (orphans/ghosts/unknown > 0 → il job manda ESCALATION e non agisce).

Il principio di sicurezza: **eliminare il secondo writer**. In maintenance mode il MCP è quiescente, quindi il `PersistentClient` del tool di flush è l'**unico** scrittore su Chroma → niente race subprocess-vs-MCP (SIGSEGV #6975 / 1.5.8). **Mai** lanciare flush/burst con MCP attivo e scrivente.

### 3.1 — Maintenance ON (operatore-side)

```sh
launchctl list | grep com.tailor.mcp        # → PID del daemon (col. 1)
sudo kill -USR1 <mcp_pid>                    # entra in maintenance mode → crea ~/tailor/maintenance.lock
ls -la ~/tailor/maintenance.lock             # conferma lock presente
```

### 3.2 — Backup (precondizione del tool: <60 min)

```sh
bash ~/tailor/scripts/maintenance/backup_db.sh
```

Il tool **rifiuta** il flush senza un backup recente (`backups/chroma_*.sqlite3.gz` o `db/chroma.sqlite3.backup.*`, età < 60 min).

### 3.3 — Flush

```sh
cd ~/tailor
.venv/bin/python3 scripts/maintenance/repair_hnsw_index.py --flush-queue-backlog
# soglia custom: ... --flush-queue-backlog --queue-backlog-min 200
```

Esegue un burst idempotente di `max(sync_threshold+100, drift+100)` re-upsert → supera la soglia → `_persist()` → `vector_seq` avanza, queue trimmata. L'output riporta `Trigger: drift|queue_backlog` e l'esito.

**Successo** (exit 0) richiede tutte e tre:
- `vector_seq_delta ≥ sync_threshold` (il burst ha superato la soglia → persist avvenuto)
- `post_drift < DRIFT_WARNING`
- `post_collection_queue_pending_count < queue_backlog_min` (il backlog azionabile della collection è davvero sceso; `post_queue_total` resta esposto come metrica globale)

Se `WARN — flush did not meet success criteria`: **non ritentare alla cieca**, ri-audit e investigare.

### 3.4 — Maintenance OFF (operatore-side)

```sh
sudo kill -USR2 <mcp_pid>                    # esce da maintenance mode → rimuove maintenance.lock
ls ~/tailor/maintenance.lock 2>/dev/null && echo "ANCORA PRESENTE — verifica" || echo "lock rimosso OK"
```

### 3.5 — Restart MCP (operatore-side)

Il flush ha scritto via client separato e ha **già persistito su disco** (`_persist()`): l'HNSW in-memory del MCP in esecuzione è ora **stale** rispetto al disco. Restart **solo per rinfrescare il client** e ricaricare il pickle fresco — il restart **non** drena nulla (§0):

```sh
sudo launchctl kickstart system/com.tailor.mcp      # rinfresca il client (vedi nota)
```

> **Il restart non drena** — né graceful né `-k` (§0). Qui è sicuro perché il drain è **già** avvenuto via flush: il disco è persistito, quindi qualunque forma di restart è data-safe (vale il **vincolo §0**: mai riavviare con upsert in-memory non persistiti). Il graceful (senza `-k`) resta preferibile come buona prassi di shutdown pulito, **non** per un drain che non avviene.

### Verifica finale (post-flush)

```sh
.venv/bin/python3 scripts/maintenance/repair_hnsw_index.py --audit
```

Atteso: `orphans=0, ghosts=0, queue=0`, "no drift detected — index is in sync".

---

## Discriminanti rapidi

- **`drift` alto ma `orphans=0`** → **non** è perdita dati, è lag WAL. Non serve `--apply`. Eventualmente flush se queue grande.
- **`orphans>0`** → perdita reale lato HNSW → `--apply` (re-embed), **non** flush.
- **`ghosts>0`** o **unknown** → **non** auto-remediare → investigare/escalation.
- **queue immobile dopo un restart** → atteso con **qualsiasi** restart (graceful o `-k`): il restart non drena (#6975, §0). Serve un `_persist()` reale → accumulo naturale a `sync_threshold` **oppure** burst `--flush-queue-backlog`.
- **flush con MCP attivo e scrivente** → **mai**: race SIGSEGV. Sempre maintenance mode prima.
- **`success=False` su run queue-triggered** → controllare `post_collection_queue_pending_count` (metrica di gating): se non è sceso sotto `queue_backlog_min`, il persist non ha drenato il backlog azionabile della collection → ri-audit. `post_queue_total` (globale) può restare alto per backlog di altri topic senza implicare un fallimento.

---

## Riferimenti

- **Commit `ccef05d`** — `fix(maintenance): gate --flush-queue-backlog su queue_total oltre a drift`: introduce la gate dual `drift OR queue_total`, `--queue-backlog-min`, success-check stretto su `post_queue_total`, e correzione della stringa audit fuorviante ("Will be consumed on next MCP restart" → drena solo via `_persist()` a `sync_threshold` o burst flush, **mai** da restart/shutdown).
- **Sessione 02/06/2026** — diagnosi del caso `drift 362 / queue 363 / orphans 0`: il restart `kickstart -k` non drenò la queue; identificata la causa (SIGKILL salta `System.stop()`) e formalizzata questa procedura. **[Nota: causa superata dal §0 (10/06) — il drain non dipende dal tipo di restart; NESSUN restart drena, né graceful né `-k`.]**
- **Sessione 30/05/2026** — incidente live `drift 821 → 21` via burst idempotente sopra soglia (primo uso documentato del pattern flush).
- Codice: [scripts/maintenance/repair_hnsw_index.py](../../scripts/maintenance/repair_hnsw_index.py) · [scripts/lib/chroma_persist.py](../../scripts/lib/chroma_persist.py)
- Note convenzioni: [docs/conventions.md](../conventions.md) · [CLAUDE.md](../../CLAUDE.md) §3 (authority boundaries), §7 (restart daemons)
