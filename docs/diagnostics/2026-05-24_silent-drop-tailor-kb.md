# Silent drop su collection `tailor_kb` post-upgrade chromadb 1.5.9

**Data**: 2026-05-24 ~00:50 Europe/Rome
**Stato**: MCP UP (PID 33321, chromadb 1.5.9 loaded), KB read OK, KB write rotta
**Modalità**: read-only su `~/tailor/db/` + script Python standalone in /tmp/

## TL;DR

1. `tailor_kb` ha **drift strutturale** tra i suoi due segment: METADATA (`ef844a53`) `max_seq_id=334557`, HNSW (`0eba77c6`) `max_seq_id=334328`. Gap = **229 seq_id**. Sono i 230 chunk droppati dalla queue alle 00:04.
2. **221 chunk fantasma SQL-only** già presenti nella tabella `embeddings` con `seq_id ∈ [334329..334557]`, mai applicati all'indice HNSW. Generati dall'ingest_safe pomeridiano (Step 4, 23/05 16:48-16:58) + dai live_chrome_ext stuck (23/05 22:50-23:38).
3. chromadb 1.5.9 fa integrity check pre-write tra HNSW e META segment max_seq. **Trova mismatch e fail-silent** (vs fail-loud di 1.5.8 = "Error in compaction"). Coerente col fix [BUG] assertion in hnswlib integrity check delle release notes 1.5.9.
4. Conseguenza: **ogni upsert/update su `tailor_kb` viene scartato senza errore** — né insert, né update, né incremento count, né nuovo seq_id, né voce in queue.
5. Le READ funzionano (count, get, query) sui 158.577 metadata già presenti — ma i 221 SQL-only non sono queryable vettorialmente.
6. **Collection FRESH funzionano normalmente** (testato con `isolated_test_*`, upsert+get OK, max_seq_id incrementati). Quindi: bug specifico a `tailor_kb`, non al DB intero.

## A. Stato fattuale collection `tailor_kb`

### A.1 Versione confermata
```
chromadb 1.5.9
```

### A.2 Schema (estratto rilevante)
Tabelle interne chromadb (no trigger, no view):
```
collections, segments, segment_metadata, collection_metadata
embeddings, embedding_metadata, embedding_metadata_array
embeddings_queue, embeddings_queue_config
max_seq_id, acquire_write, maintenance_log
embedding_fulltext_search* (FTS5)
migrations, tenants, databases
```
Migrations applicate: `sysdb` 1-10, `metadb` 1-6, `embeddings_queue` 1-2. Tutto allineato.

### A.3 Collection + segments
```sql
collections:
  62156a36-101a-48a8-b3df-164af0231ece  tailor_kb  dim=768

segments:
  0eba77c6-…  urn:chroma:segment/vector/hnsw-local-persisted  VECTOR    → tailor_kb
  ef844a53-…  urn:chroma:segment/metadata/sqlite              METADATA  → tailor_kb

embeddings per segment_id:
  ef844a53-… : 158577   ← include i 221 ghost SQL-only
```

Config HNSW: `ef_construction=100, max_neighbors=16, ef_search=100, num_threads=10, batch_size=100, sync_threshold=1000, resize_factor=1.2`.

### A.4 HNSW segment on-disk
```
~/tailor/db/0eba77c6-5fca-4e57-9483-fa793b2fb291/
  data_level0.bin       511 MB  mtime 2026-05-23 16:48
  header.bin            100 B   mtime 2026-05-23 16:48
  index_metadata.pickle  14 MB  mtime 2026-05-23 16:48
  length.bin            637 KB  mtime 2026-05-23 16:48
  link_lists.bin        1.3 MB  mtime 2026-05-23 16:48
```
**Tutti i file con mtime 16:48** (ora del bootout MCP per ingest_safe). Da quel momento, HNSW segment NON è più stato toccato. Nessun file `.lock`/`.wal` orfano.

Directory `~/tailor/db/` contiene anche:
- `chroma/` — segment 9ad2790b legacy/stale (non in sysdb)
- `95dd1079-…/` — dir orfana da test isolato pre-CC (non in sysdb)
- `checkpoints/`, `migrations/`, `.audit_backups/`, `.secrets_backups/` — utility

### A.5 max_seq_id (smoking gun)
```sql
ef844a53 (META):  334557
0eba77c6 (HNSW):  334328

GAP = 229
```

`embeddings_queue` corrente: `0` (dropped manualmente alle 00:04).
`embeddings_queue_config`: `{"automatically_purge":true}`.

### A.6 Ghost SQL-only (chunk in META ma non in HNSW)
```sql
SELECT COUNT(*) FROM embeddings 
WHERE segment_id='ef844a53-…' AND CAST(seq_id AS INTEGER) > 334328;
→ 221
```

Esempi (primi 5 per seq_id ASC):
```
doc_83ae7d1b843f_chunk_0000   seq=334329   2026-05-23 14:48:49
doc_83ae7d1b843f_chunk_0001   seq=334330   2026-05-23 14:48:49
doc_1e3ef072b7ae_chunk_0000   seq=334331   2026-05-23 14:48:50
doc_9cc5b286c668_chunk_0000   seq=334332   2026-05-23 14:48:50
doc_a86eb1a76089_chunk_0000   seq=334333   2026-05-23 14:48:50
```
Ultimi (per seq_id DESC):
```
live_gemini_40deb6d50d361451   seq=334557   2026-05-23 21:38:50 UTC
live_claude_e902440249aff417   seq=334554   2026-05-23 21:00:34 UTC
live_claude_5f793347dd14f78e   seq=334551   2026-05-23 20:56:20 UTC
live_claude_cd477d460f4da7dc   seq=334548   2026-05-23 20:50:59 UTC
```
221 chunk = ~217 `doc_*_chunk_*` + `doc_summary_*` (ingest_safe pomeriggio) + 4 `live_*` (Chrome extension pre-restart). Sono i chunks che ingest_safe ha scritto in METADATA segment mentre HNSW writer era freeze: quei seq_id sono atterrati in `embeddings_queue`, poi droppati col `DELETE FROM embeddings_queue`, MA le righe in `embeddings` sono rimaste.

### A.7 chromadb-ops
Non installato (`pip show chromadb-ops` → not found). Esiste come tool maintain ma non è preinstallato. Non installato in questa diagnosi.

---

## B. Riproduzione del silent drop

### B.1 Test su `tailor_kb` (script `/tmp/repro_silent_drop.py`, eseguito 00:47)

```
[max_seq_id PRE]
  META(ef844a53): 334557
  HNSW(0eba77c6): 334328
  queue=0  count=158577

>>> upsert id=silent_drop_repro_1779576426_9400
  upsert: NO EXCEPTION
>>> get(ids=[silent_drop_repro_1779576426_9400])
  got: EMPTY (silent drop!)

[max_seq_id POST]
  META(ef844a53): 334557   ← INVARIATO
  HNSW(0eba77c6): 334328   ← INVARIATO
  queue=0  count=158577    ← INVARIATO

>>> sanity: get id già esistente 'live_gemini_40deb6d50d361451'
  sanity.ids: ['live_gemini_40deb6d50d361451']   ← OK lettura
```

**Esito**: l'upsert non lascia NESSUNA traccia (né embeddings row, né queue, né max_seq_id, né exception). È un drop letteralmente silenzioso a livello completo dello stack.

### B.2 Test confronto `tailor_kb` vs collection FRESH

Script `/tmp/repro2_isolated_collection.py`:

```
>>> TEST 1: create + upsert su collection FRESH (isolated_test_1779576479)
  upsert+get fresh: ids=['fresh_test_1779576479'] → OK
  max_seq_id: nuovo segment metadata = 1
  queue: 0→1
  embeddings: 158577→158578

>>> TEST 2: upsert UPDATE su id esistente 'live_gemini_40deb6d50d361451' in tailor_kb
  upsert: NO EXCEPTION
  POST: documento NON aggiornato (doc[:60] identico al PRE)
  Update effective: False
  max_seq_id tailor_kb segments: INVARIATI
```

**Esito**:
- Collection FRESH funziona perfettamente, incluso il flusso embeddings_queue → HNSW compactor → segment writer.
- `tailor_kb` rigetta sia INSERT che UPDATE. L'update di un id esistente non aggiorna il documento — la collection è in modalità read-only de facto.

### B.3 / B.4
La sample-get sentinel del `verified_upsert()` (rif. post-mortem 13/05 §11 errata, fix v1.2.6.3) rileva correttamente il silent drop sull'ingest live. Il problema non è il sentinel — sta facendo il suo lavoro. Il problema è che la condizione che innesca il sentinel è ora **strutturale e persistente**, non più transient.

---

## C. Confronto col post-mortem 2026-05-13

Quel post-mortem documenta:
- chromadb 1.5.8 SIGSEGV su race FD subprocess-vs-MCP (risolto strutturalmente con G COW snapshot).
- §11 errata: "invisible class" — drops che lasciano né SQL né HNSW trace. Doveva essere catchata dal `verified_upsert` sample-get sentinel.

§9 "Open questions": *"Should chromadb be pinned to a non-1.5.8 version?"* — domanda aperta. La sessione precedente (23/05 sera) ha risposto: upgrade 1.5.9 sì, e ha effettivamente fixato il freeze 1.5.8 (HNSW errors spariti dal log post-bootstrap 00:13:19).

Ma il fix 1.5.9 ora **manifesta in modo diverso** la corruzione preesistente:
- Sotto 1.5.8: la corruzione causava "Error in compaction: Failed to apply logs to the hnsw segment writer" → throw exception, verified_upsert FAIL loud, dashboard 500.
- Sotto 1.5.9: stessa corruzione causa silent drop a livello write. La sample-get sentinel rileva ed esce, dashboard 500.
- **Lato user, sintomo identico** (extension rossa, write rotti).
- **Lato system, il DB non corrompe più ulteriormente** (non si avanza max_seq_id su HNSW orfano). È un'evoluzione "safer" del bug.

Heartbeat log + maintenance_log entries da `repair_hnsw_index.py` mostrano che la drift HNSW vs SQL è osservata da settimane (entries 449-453 nella maintenance_log) — il problema è cronico.

---

## D. Opzioni di fix — trade-off

| # | Opzione | Effort | Probabilità successo | Rischio | Rollback |
|---|---|---|---|---|---|
| **A** | Resync `max_seq_id` HNSW = META (UPDATE SQL diretto) | 5 min | **alta** per sbloccare i NUOVI write | 221 ghost restano persi (vettore) | restore backup pre_upgrade |
| **B** | Migration via dump+restore: export 158.577, drop tailor_kb, recreate, re-upsert | 2-4 h | **alta** | downtime KB, rischio interruzione script | restore pre_upgrade |
| **C** | Migration a `tailor_kb_v2` parallela: copia, switch config, drop v1 | 2-4 h + edit config codice | alta | scope cambio config + test | switch nome in config |
| **D** | Force rebuild HNSW da SQL embeddings via chromadb-ops (esterno) | 1-3 h + rischio compat | ignota | dipende da tool non testato | restore pre_upgrade |
| **E** | Restore backup pre-corruzione | 0 | **0** (tutti backup post-corruzione) | — | — |

### A. Resync `max_seq_id` HNSW = META
```sql
UPDATE max_seq_id 
SET seq_id = 334557 
WHERE segment_id = '0eba77c6-5fca-4e57-9483-fa793b2fb291';
```
Effetto: HNSW writer dichiara di aver consumato tutto fino a 334557, il prossimo upsert riceve seq 334558, integrity check passa, write atterrano in HNSW.

Side effect: i 221 chunk con seq_id 334329-334557 **non saranno mai indicizzati in HNSW** (i loro vettori non sono nel file `data_level0.bin`, perché la compaction era freeze). Restano nella tabella `embeddings` SQL — visibili in `count()` e `get(ids=[...])` MA non queryable via `query(query_embeddings=...)`. Devono essere ri-ingestati (per documenti su disco è banale; per conversazioni live perse).

**Rischio fail-silent secondario**: se chromadb 1.5.9 fa anche checksum interno sui file segment.bin, la sync del solo `max_seq_id` potrebbe non bastare. Va testato.

### B. Dump+restore della stessa collection
```python
# Pseudocode
all_data = []
for batch in iter_collection(tailor_kb, batch=500):
    all_data.append(batch)  # ~317 batch totali
client.delete_collection("tailor_kb")
new = client.create_collection("tailor_kb", ...)  # stesso nome, stessa config
for batch in all_data:
    new.upsert(...)  # ~317 upsert
```
Pro: HNSW segment rebuilt from scratch, 158.577 chunk tutti indicizzati correttamente. Zero residui di corruzione.
Contro: 
- Lungo (~30-60 min wall per 158k chunk con embedding già presenti — nessun re-embed, ma serializzazione SQL massiva).
- Serve **MCP DOWN** per evitare race + cambi durante la migration.
- Se lo script si interrompe a metà → collection vuota, restore da backup.
- I 221 chunk SQL-only sarebbero comunque inclusi (sono in `embeddings`), e tornerebbero ad essere queryable.

### C. Migration a `tailor_kb_v2`
Crea collection nuova con nome diverso, copia, switch config TAILOR per puntare a v2. Lascia v1 in DB come fallback.
Pro: rollback istantaneo (cambio nome). Validation completa prima del switch.
Contro: serve grep + edit di tutte le `get_collection("tailor_kb")` nel codice MCP (mcp_server.py, scripts/lib/*, ecc.). Possibile dimentichi callsite (rif. errata 13/05: scope-creep nei callsite).

### D. chromadb-ops
Non valutata: pacchetto non installato, non documentato il behavior su 1.5.9. Da indagare in altra sessione se A/B/C non bastano.

### E. Restore backup
Scartata: i 3 backup full (b21/22/23) hanno tutti drift HNSW analogo (verificato nelle sessioni precedenti — header.bin SHA-256 identici tra i 3 = HNSW non si è mosso). Restore = identical state to current minus ultime modifiche = bug persiste.

---

## E. Raccomandazione operativa

**Strategia: A immediato → B notturno (con MCP down).**

### Fase 1 (ADESSO, 5 min): unblock con resync max_seq_id

Operazione SQL diretta sul DB live. **MCP è UP**, ma non scrive in `max_seq_id` (la legge solo al boot/compaction). Resync mentre il daemon è up è teoricamente safe ma il timing safer è MCP DOWN per evitare race.

**Step 1.1 — backup safety addizionale** (CC):
```sh
sqlite3 ~/tailor/db/chroma.sqlite3 ".backup ~/tailor/backups/manual_20260524_000010/chroma.sqlite3.pre_resync"
```

**Step 1.2 — bootout MCP** (OPERATORE, sezione 3 CLAUDE.md):
```sh
sudo launchctl bootout system/com.tailor.mcp
sleep 5
pgrep -f mcp_server.py    # deve essere vuoto
```

**Step 1.3 — resync max_seq_id** (CC):
```sh
sqlite3 ~/tailor/db/chroma.sqlite3 <<'EOF'
UPDATE max_seq_id 
SET seq_id = (
  SELECT seq_id FROM max_seq_id 
  WHERE segment_id = 'ef844a53-4442-49fa-8773-b0429ed164d9'
)
WHERE segment_id = '0eba77c6-5fca-4e57-9483-fa793b2fb291';

SELECT segment_id, seq_id FROM max_seq_id;
EOF
```
Atteso: entrambi `334557`.

**Step 1.4 — bootstrap MCP** (OPERATORE):
```sh
sudo launchctl bootstrap system /Library/LaunchDaemons/com.tailor.mcp.plist
sleep 8
pgrep -fl mcp_server
tail -20 ~/tailor/logs/mcp_stderr.log
```

**Step 1.5 — smoke test** (CC, script Python /tmp/):
- upsert+get di 1 chunk `unblock_test_<ts>` su tailor_kb
- atteso: ids=[chunk_id], count POST=158578, max_seq_id META=334558, HNSW=334558

**Step 1.6 — verifica end-to-end** (OPERATORE):
- Chrome extension click "Test Connection" → atteso verde
- Telegram bot messaggio test → atteso bot risponde + chunk salvato

Se Step 1.5 fallisce ancora con silent drop → l'ipotesi A è insufficiente, chromadb 1.5.9 valida anche file binari del segment. Passare direttamente a Fase 2.

### Fase 2 (notte / domenica mattina, 2-4 h): pulizia con dump+restore (Opzione B)

A operatività ripristinata, schedulare migration completa per eliminare definitivamente i 221 ghost SQL-only e ricostruire HNSW pulito.

Script-driven con checkpoint. Da preparare in sessione dedicata: non lo scrivo qui per evitare scope-creep e per permettere review prima dell'esecuzione massiva.

### Rollback path (qualsiasi step)
1. `sudo launchctl bootout system/com.tailor.mcp`
2. `cp ~/tailor/backups/manual_20260524_000010/chroma.sqlite3.pre_upgrade ~/tailor/db/chroma.sqlite3`
3. `~/tailor/.venv/bin/pip install chromadb==1.5.8 --force-reinstall`
4. `sudo launchctl bootstrap system /Library/LaunchDaemons/com.tailor.mcp.plist`
5. Stato torna a pre-Step 1 della sessione upgrade (queue 230, freeze 1.5.8).

---

## F. Domande aperte / rischi residui

1. **Il fix A è davvero sufficiente?** Ipotesi: chromadb 1.5.9 check solo `max_seq_id`. Se invece confronta anche `data_level0.bin` size vs expected vector count, dovremo passare a B/C. Test in Step 1.5 risponde.

2. **I 221 ghost sono recuperabili?** I `doc_*_chunk_*` sì: i PDF sono on disk in OneDrive, ingest_safe può rifare quei 21 documenti (`PROCURA…`, `Nexi_…`, etc.). I `live_*` chunk no: 4 conversazioni live Chrome extension del 23/05 sera (gemini + 3 claude) sono perse, recuperabili solo se le sessioni LLM originali sono ancora aperte.

3. **Recurrence**: anche dopo il fix, il freeze rientrerà? La causa originale del freeze 1.5.8 era un bug di compactor che 1.5.9 fixa secondo le release notes. Se il fix è solido, post-Fase 1 il sistema dovrebbe restare stabile. Da monitorare ≥48h.

4. **Dir orfana `95dd1079-…/`** in `~/tailor/db/`: residuo di test isolato pre-CC (mtime 00:40, 321 KB). Non referenziato in sysdb segments. Safe da `rm -rf`, ma decisione operatore. Stessa cosa per `db/chroma/9ad2790b-…/` (stale segment legacy del vecchio Apr/May).

5. **`acquire_write` table cresce continuamente**: 922 righe stasera vs 920 di un'ora fa. Non sembra impattare ma è un canary di "write attempts attempted". Da monitorare.

6. **Test suite TAILOR rispetto al silent drop attuale**: dopo Fase 2, vorrebbe aggiungere un test integration che simula drift HNSW/META e verifica che `verified_upsert` lo rilevi. Out of scope qui.

---

## Riferimenti

- Backup pre-upgrade: `~/tailor/backups/manual_20260524_000010/chroma.sqlite3.pre_upgrade` (sqlite3 .backup atomico, 2.5 GB, integrity ok)
- Backup pre-resync (da fare in Fase 1): `~/tailor/backups/manual_20260524_000010/chroma.sqlite3.pre_resync`
- Post-mortem precedente: [docs/postmortems/2026-05-13-chromadb-1.5.8-segfault.md](../postmortems/2026-05-13-chromadb-1.5.8-segfault.md)
- Diagnostica freeze intermittente: [docs/diagnostics/2026-05-23_hnsw-freeze-intermittent-analysis.md](2026-05-23_hnsw-freeze-intermittent-analysis.md)
- Script repro usati (cleanati): `/tmp/repro_silent_drop.py`, `/tmp/repro2_isolated_collection.py`

— Diagnosi conclusa 2026-05-24 ~00:50 Europe/Rome, read-only.
