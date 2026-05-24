# Diagnostica freeze HNSW intermittente — 2026-05-23

**Periodo analizzato**: 23/05/2026 17:00 → 23:30 Europe/Rome
**Modalità**: read-only sul filesystem (MCP UP, PID 23190, uptime 6h43m da bootstrap 16:58:54)
**Versione**: chromadb 1.5.8, MCP server senza restart fra 16:58 e ora di stesura

> **Avvertenza al lettore**: questo report **revisiona sostanzialmente** le assunzioni del continuation prompt `TAILOR_continuation_2026-05-23_hnsw-freeze.md`. Tre delle sue affermazioni-cardine si rivelano errate alla verifica empirica. Vedi §D.

---

## TL;DR

1. Il freeze NON è permanente "dal 19/05 11:13". È **intermittente**, con finestre di operatività di **diverse ore** post-restart MCP.
2. Dopo il restart MCP delle 16:58 (post-ingest_safe), il sistema **ha scritto regolarmente** per **~3h53m** (16:58 → 20:50 UTC). 12+ POST `/api/ingest-live` accettati e committati senza errore. Centinaia di POST 200 OK in totale.
3. Il freeze si è ri-attivato alle **20:50:59 UTC** (22:50:59 Europe/Rome). Da allora, ogni `verified_upsert` fallisce e ogni `collection.count()` (chiamato da `/api/dashboard/stats`) ritorna 500.
4. **Trigger del freeze 20:50: non identificabile esternamente.** Nessun evento `sleep`/`wake` macOS, nessun cron schedulato, nessuna anomalia OneDrive, nessuno spike di traffico. Probabile accumulo in-memory interno chromadb.
5. **Restore atomico dei 3 backup full-DB (b21/b22/b23) è inutile per questo problema**: hash SHA-256 di `header.bin` e `length.bin` HNSW **identici** tra i 3 backup. Lo stato problematico è in-memory, non on-disk.

---

## A. Timeline fattuale (17:00-23:30 Europe/Rome del 23/05)

Eventi unificati da `mcp_stderr.log`, `mcp_stdout.log`, `heartbeat.log`, `telegram_v5_stderr.log`, `sqlite3 embeddings_queue`, launchd log macOS.

| Ora locale | Evento | Fonte |
|---|---|---|
| 16:48:42 | MCP shutdown (Step 3 ingest_safe) | mcp_stderr |
| 16:48 | HNSW segment `0eba77c6-…` ultimo write su disco (mtime header.bin) | filesystem |
| 16:48-16:58 | Step 4 ingest_safe: 29 chunk OK + 177 errori HNSW + 20 summary errori | ingest_safe_*.log |
| 16:53:16 | Heartbeat ALERT: MCP Server DOWN (atteso, ingest_safe) | heartbeat |
| 16:58:54 | MCP bootstrap, PID 23190 (uptime corrente) | mcp_stderr, launchd |
| 16:58:54 | Queue post-ingest: 218 (`doc_chunk` 198 op=2 + `doc_summary` 20 op=0) | ingest_safe log |
| 17:03:20 | Heartbeat RECOVERED: MCP Server | heartbeat |
| 17:52:35 | Prima transport session post-restart | mcp_stderr |
| 18:06-18:09 | 4× CallToolRequest (origine non determinata, presumibilmente Claude Desktop) | mcp_stderr |
| 20:45:02 | Telegram v5 errore `ConnectionResetError` (ricorrente, non correlato) | telegram_v5_stderr |
| 20:50:59 UTC | **Primo `live_chrome_ext` upsert in queue chromadb** (= 22:50:59 locale) | sqlite3 embeddings_queue |
| 22:50:59 | **Primo `[ingest_helpers] verified_upsert FAILED` su run_id ingest_live_20260523_225059_23190** | mcp_stderr |
| 22:56:20 | Secondo `verified_upsert FAILED` | mcp_stderr |
| 22:58:45 | Nuova transport session (probabilmente dashboard refresh utente) | mcp_stderr |
| 23:00:34 | Terzo `verified_upsert FAILED` | mcp_stderr |
| 23:01-23:04 | 6× CallToolRequest (utente che tenta operazioni) | mcp_stderr |
| 23:04+ | Cascade `GET /api/dashboard/stats` 500, `POST /api/ingest-live` 500 sui client extension (109.54.72.81, 109.53.83.42) | mcp_stdout |
| 23:38:50 UTC | Ultimo `live_chrome_ext` tentativo in queue (extension continua a ritentare) | sqlite3 |
| Ora corrente | Queue: **230** (12 live_chrome_ext + 198 doc_chunk + 20 doc_summary) | sqlite3 |

**Sleep/wake macOS**: nessun evento `Sleep transition`/`FullWake`/`DarkWake` rilevato tra 17:00 e 23:30. Solo `DesktopMode` toggle 16:52 (pre-finestra) e 23:34 (post-freeze, ininfluente).

**OneDrive sync**: nessun evento `process == "OneDrive"` nel periodo.

**Cron schedulati nella finestra**: nessuno. I cron tailor sono notturni (sync-and-ingest 01:30, sync-email 04:00, backup-db 07:30, secret-scan 00:15) e nessuno è scoccato fra 16:58 e 22:50.

**Telegram bot**: nessun crash/restart 17:00→23:30; solo l'errore ricorrente `ConnectionResetError` alle 20:45 (pattern che si ripete più volte in giorni precedenti, non correlato al freeze).

---

## B. Root cause aggiornata

**Quello che è successo davvero alle 22:50:59**

Tra le 16:58 (restart MCP) e le ~22:50 il sistema ha funzionato correttamente. Conteggi globali dei log uvicorn (intera vita di `mcp_stdout.log`):

- `POST /api/ingest-live`: 1496 totali → **1418 success (200)**, 48 fail (500), 17 auth (401) → success rate **94.8%**
- `GET /api/dashboard/stats`: 1921 totali → 1893 success (200), 28 fail (500) → success rate **98.6%**

Tra 16:58 e 22:50, **centinaia di scritture conversazionali** sono state accettate e committate. Esempi nel log da IP extension (151.13.15.250, 93.56.37.6, 109.52.197.5, 109.54.72.81 — tutti client extension dell'utente da differenti reti mobile/ufficio): linee 50675-55191 di stdout, **decine di POST 200 consecutivi**, intervallo regolare di pochi secondi.

Alle ~20:50:59 UTC = 22:50:59 locale, **il primo `verified_upsert` ha cominciato a fallire** con `chromadb.errors.InternalError: Error in compaction: Failed to apply logs to the hnsw segment writer`. Stesso pattern dei freeze precedenti (incidenti 13/05 segfault e silent-drop di maggio).

**Cosa è successo tra l'ultimo success e il primo fail (20:50)**:

- Nessun cron, nessuno sleep, nessuno wake, nessun restart MCP, nessuna sessione utente massiva.
- Solo carico continuo della Chrome extension che pollava + scriveva nuove conversazioni.
- **`mcp_stderr.log` è completamente silenzioso fra 20:54 (l'ultimo ListResourcesRequest innocuo) e 22:50:59 (primo fail)** — circa 2 ore di silenzio assoluto.

**Conclusione**: il freeze è scattato senza un evento esterno identificabile. La causa più probabile è **accumulo di stato in-memory** all'interno del processo chromadb (segment writer log buffer, HNSW dirty pages, o threshold interno di compaction). Il fatto che restart MCP ripristini l'operatività (come è accaduto alle 16:58 dopo che la mattina ingest_safe trovava il sistema già in freeze) suggerisce che lo stato problematico vive **nel processo Python**, non nei file su disco.

---

## C. Pattern del freeze

**Intermittente, NON permanente.** Sequenza confermata oggi:

1. Mattino del 23/05 (verosimilmente da qualche ora prima): freeze attivo. Sync_email alle 04:00 OK (109 chunk gmail), poi durante il giorno la queue cresceva (baseline pre-ingest_safe = 597).
2. 16:48-16:58: ingest_safe + restart MCP (Step 6 sudo bootstrap). Reset stato in-memory.
3. 16:58 → 22:50: **~6h di operatività piena**. Centinaia di POST OK, dashboard polling OK, ingest-live OK.
4. 22:50: freeze rientra. Da allora ogni upsert fallisce, `collection.count()` fallisce.
5. Queue ricomincia a crescere come prima del restart.

**Condizioni di re-attivazione** (ipotesi, non confermata da dato deterministico):
- Probabilmente uptime + volume di scritture: 12 nuovi `live_chrome_ext` upsert in 48 min hanno innescato il rientro? Threshold molto basso se così.
- Più verosimilmente: accumulo di pagine dirty / fragmentation HNSW raggiunto dopo N upsert dal restart. La N esatta non è misurabile dai log disponibili.

**Predizione (incerta)**: ogni restart MCP "compra" un periodo di funzionamento dell'ordine di alcune ore (3-6h misurato oggi), poi il freeze ricompare. La tregua è probabilmente correlata al volume di upsert post-restart, non al wall-clock.

---

## D. Validazione/smontaggio ipotesi del continuation prompt

### Ipotesi 3.1: «freeze attivo dal 19/05 11:13, persistente»

**FALSA.** Tre evidenze contraddittorie:

1. **HNSW segment ATTIVO**: il segment HNSW della collection `tailor_kb` (id `62156a36-…`) è `~/tailor/db/0eba77c6-5fca-4e57-9483-fa793b2fb291/`, con mtime **23/05 16:48** (data_level0.bin = 511 MB, header.bin appena prima del restart MCP). La directory `~/tailor/db/chroma/9ad2790b-…/` che il discovery precedente aveva ispezionato è un segment **stale/legacy non più attivo** (mtime 23/05 16:19, dimensione 321 KB, nessuna entry nella tabella `segments` di chroma.sqlite3 — solo `0eba77c6` è registrato).

   **Quindi il "ultimo persist HNSW 19/05 11:13" del continuation prompt era basato sulla dir sbagliata.** Il segment attivo è stato scritto OGGI alle 16:48.

2. **sync_email 23/05 04:00**: 109 chunk gmail ingestati con successo, count finale 158,382. Se il freeze fosse stato attivo continuativamente, sync_email avrebbe fallito.

3. **Centinaia di POST `/api/ingest-live` 200 OK** distribuiti nei giorni precedenti nei log uvicorn — 94.8% success rate totale.

Il pattern reale è **freeze ciclico**: si attiva, restart libera, ri-attiva dopo qualche ora.

### Ipotesi 3.2: «queue accumulation triggera freeze»

**Parzialmente falsa.** Correlazione queue ↔ freeze non lineare:

- Prima del freeze attuale: queue = 218 stuck (dal ingest_safe pomeridiano, mai drenati) + 12 nuovi live = 230. Soglia raggiunta: 230.
- Ieri (22/05): queue = 471 (backup b22). Sistema scriveva? Sì — backup mostra sistema in stato simile ma il problema potrebbe essersi ri-attivato/disattivato più volte.
- Avantieri (21/05): queue = 281 (backup b21).

Quindi quote >200 in queue da almeno 48h, e il freeze NON è strettamente correlato. La queue cresce perché il freeze non drena, ma la queue **non causa** il freeze direttamente.

Più probabile: la queue è un **sintomo**, non un trigger. Il vero trigger è interno al compactor/segment writer chromadb.

### Ipotesi 3.3: «21/05 backup è candidato red line per restore atomico»

**FALSA per due motivi indipendenti**:

a) Tutti e 3 i backup (b21/b22/b23) hanno **header.bin e length.bin HNSW con hash SHA-256 IDENTICI** tra loro:
   ```
   header.bin = a7efc39fdac218eb28dbcc3cddd36d361dcd71a8f602f90dcd5a2a9d76c84b55  (b21, b22, b23)
   length.bin = 9d8575dd4cb11f8101c12e72540f56454f798678950446dba17e663eebef0788  (b21, b22, b23)
   ```
   Il segment HNSW **non è stato modificato su disco** fra 21/05 e 23/05 07:30. I 3 backup catturano lo stesso identico stato HNSW. La scelta del backup non sposta nulla del corpus vettoriale.

b) L'header attuale (live) ha hash differente (`6465e44…`), perché ingest_safe alle 16:48 ha riscritto i file segment. Restore di QUALSIASI backup riporterebbe a uno stato HNSW più vecchio del 23/05 16:48 ma **NON eliminerebbe il problema**: il bug è in-memory nel processo chromadb, non nei file. Dopo il restore il restart MCP creerebbe un nuovo processo che, sotto carico, ri-andrebbe in freeze come oggi alle 22:50.

**Restore = non risolve. Restart = risolve temporaneamente.**

---

## E. Opzioni concrete

Tutte le opzioni richiedono `sudo launchctl kickstart -k system/com.tailor.mcp` (operatore-side, sezione 3 CLAUDE.md).

### Option 1 — Restart MCP nudo

**Comando**:
```sh
sudo launchctl kickstart -k system/com.tailor.mcp
```

**Effort**: 5 secondi.
**Effetto**: reset stato in-memory chromadb. Sistema torna operativo immediatamente (heartbeat RECOVERED in <5 min).
**Downside**: i 12 live_chrome_ext in queue (ingest_live tentativi falliti) sono **persi** (chromadb non rifa la coda dopo restart per items in failed state — sono stati emessi 401/500 al chiamante, quindi non c'è retry side server). L'utente perde le conversazioni delle ultime 2h che la extension ha tentato di scrivere ma sono finite in 500.
**Durata tregua attesa**: 3-6h sulla base del run odierno post-16:58. Non garantita.
**Probabilità di reggere 24h**: bassa (<30% basato su pattern odierno).

### Option 2 — Restart + drop queue manuale

Stesso comando Option 1 + cancellazione manuale `embeddings_queue` mentre MCP è DOWN.

**Effort**: 10 minuti.
**Effetto**: identico a Option 1 lato operatività. La drop della queue rimuove i 198 doc_chunk + 20 doc_summary + 12 live che sono comunque dead state.
**Downside**: i 12 live_chrome_ext vengono cancellati definitivamente (uguale a Option 1). I 198+20 ingest_safe vengono comunque persi (file PDF restano sul disco, andranno rifatti in next ingest_safe).
**Probabilità di reggere 24h**: come Option 1. Drop queue non risolve il bug interno chromadb.

### Option 3 — Restart + reset COW snapshot di db/

Pattern P0G del post-mortem 13/05: snapshot APFS clone di `db/chroma.sqlite3` + segment dir, swap atomico, riavvio.

**Effort**: 30 minuti, comandi documentati in [docs/postmortems/2026-05-13-chromadb-1.5.8-segfault.md](../postmortems/2026-05-13-chromadb-1.5.8-segfault.md) §6 G.
**Effetto**: cancella in-memory state E qualsiasi corruzione su disco. Più aggressivo.
**Downside**: ricostruisce HNSW (cosa che chromadb fa al primo `count()` post-restore), può richiedere alcuni minuti di startup lento.
**Probabilità di reggere 24h**: marginale miglioramento rispetto a Option 1 — il bug non sembra essere on-disk corruption.

### Option 4 — Restore atomico da backup full

**Sconsigliato per questo problema** (vedi §D.3.3). Backup hanno stesso HNSW state degli ultimi 3 giorni. Restore non sposta nulla.

### Option 5 — Upgrade/downgrade chromadb

Cambia versione (1.5.7 downgrade o 1.5.9/1.6.x upgrade) per scappare dal bug 1.5.8.

**Effort**: 2-4h (test compat, verifica metadata schema invariato, dry run su backup).
**Comando indicativo**:
```sh
~/tailor/.venv/bin/pip install 'chromadb==1.6.2'   # esempio
# Test: aprire PersistentClient su copia /tmp di chroma.sqlite3 + segment
```
**Downside**: rischio incompatibilità schema/API. Cambio versione è scope di una mini-release, non hot-fix.
**Probabilità di reggere 24h**: **alta** se la nuova versione fixa il bug (incertezza: bug non confermato fixato upstream — vedi post-mortem §9 "Has the upstream race been reported to chromadb? Not established").

### Matrice riassuntiva

| Option | Effort | Tregua attesa | Risolve struttura | Perde live extension queue |
|---|---|---|---|---|
| 1. Restart nudo | 5s | 3-6h | NO | SÌ (12 live) |
| 2. Restart + drop queue | 10min | 3-6h | NO | SÌ (12 live + 218 ingest_safe) |
| 3. Restart + COW snapshot | 30min | 3-6h | forse | SÌ |
| 4. Restore backup | 30min-2h | 3-6h | NO | SÌ + corpus rewind |
| 5. Upgrade chromadb | 2-4h | possibilmente permanente | SÌ se bug fixato | SÌ (12 live) |

**Raccomandazione operativa immediata**: Option 1 (restart) per ripristinare operatività ora. **Option 5 da pianificare** in finestra non-critica (es. domenica mattina) come fix strutturale.

---

## F. Domande aperte

1. **Quale soglia interna chromadb scatena il freeze?** Non identificabile dai log. Possibili candidati: numero di pagine HNSW dirty, numero di write log entries non ancora compactate, allocazione memoria del segment writer. Servirebbe profiling chromadb running con `py-spy` o `tracemalloc` durante uptime > 2h.

2. **Il freeze 22:50 è correlato a un trigger specifico o è puramente "tempo dal restart"?** Tra 16:58 e 22:50 ci sono state ~3h53m, ma il volume di POST è stato continuo. Non distinguo tra "trigger temporale" e "trigger volumetrico".

3. **Quale versione chromadb non ha questo bug?** Post-mortem 13/05 §9 lascia aperta. Servirebbe test su `chromadb 1.5.7`, `1.5.9`, `1.6.x` su copia isolata `/tmp` con replay del carico tipico.

4. **Esiste un knob env var per cambiare il `sync_threshold` chromadb?** Non trovato in `mcp_server.py` né in `tailor.yaml`. Potrebbe esistere come default chromadb interno (es. `CHROMA_HNSW_SYNC_THRESHOLD`). Da verificare in sorgenti chromadb.

5. **I 1418 POST 200 OK su `/api/ingest-live` nella vita del log sono TUTTI realmente persistiti?** `verified_upsert` con sample-get sentinel dovrebbe garantirlo, ma non ho fatto un audit drift end-to-end per confermare che ogni `live_*_<hash>` con response 200 sia presente nella collection. Servirebbe `ghost_audit.py` su id pattern `live_*`.

6. **La Chrome extension fa retry locale lato client su 500?** Dal codice `background.js` letto: NO. La fetch è one-shot, il messaggio scartato in caso di errore. Quindi le conversazioni perse nelle 2h post-freeze (22:50→ora) sono **definitivamente perse** (a meno che siano ancora recuperabili dalla session ChatGPT/Claude originaria).

---

## Riferimenti

- Continuation prompt rivisto: KB `TAILOR_continuation_2026-05-23_hnsw-freeze.md`
- Post-mortem precedente bug class: [docs/postmortems/2026-05-13-chromadb-1.5.8-segfault.md](../postmortems/2026-05-13-chromadb-1.5.8-segfault.md)
- Log fonti: `~/tailor/logs/mcp_stderr.log`, `~/tailor/logs/mcp_stdout.log`, `~/tailor/logs/heartbeat.log`, `~/tailor/logs/ingest_safe_20260523_164807.log`
- DB live: `~/tailor/db/chroma.sqlite3` (sqlite3 read-only), segment HNSW `~/tailor/db/0eba77c6-5fca-4e57-9483-fa793b2fb291/`
- Backup ispezionati: `tailor_db_full_20260521_073002.tar.gz`, `…22_073000`, `…23_073000`

— Diagnostica condotta in modalità read-only sabato 23/05/2026 ~23:30 Europe/Rome.
