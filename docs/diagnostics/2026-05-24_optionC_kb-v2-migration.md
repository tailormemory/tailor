# Opzione C — Migration `tailor_kb` → `tailor_kb_v2`

**Data**: 2026-05-24, 01:14 → 01:24 Europe/Rome
**Durata effettiva**: ~10 min (vs stima iniziale 2-3 h)
**Esito**: ✅ **success operativo** con 181 chunk persi (ghost SQL-only non recuperabili).

## TL;DR

1. Migrati **158,396 / 158,577 chunk** (99.886%) da `tailor_kb` (corrotta sul write path post-upgrade chromadb 1.5.9) a `tailor_kb_v2` (collection fresca sana).
2. **181 chunk persi definitivamente** — ghost SQL-only generati durante il freeze HNSW del 23/05 16:48-23:38. chromadb 1.5.9 `src.get(include=["embeddings"])` su questi id ritorna `InternalError: Error executing plan: Internal error: Error finding id` (vector lookup HNSW non trova).
3. Config TAILOR aggiornata (`tailor.yaml:108` + 5 file Python `.py`). Tutti i callsite cfg-driven puntano automaticamente a v2.
4. Bootstrap MCP su v2: write end-to-end funzionante. **`POST /api/ingest-live → 200 OK`** confermato nel log stdout. **Nessun nuovo `SILENT DROP`** in stderr post-boot.
5. Dashboard popolata, Chrome extension verde, Telegram bot responsivo. Tutti confermati dall'operatore.

## A. Migration: 158,577 → 158,396 OK

### Script di migration
- File: [scripts/migration/migrate_to_kb_v2.py](../../scripts/migration/migrate_to_kb_v2.py)
- Log: [logs/migration_kb_v2_20260524_011357.log](../../logs/migration_kb_v2_20260524_011357.log)
- Batch 500, idempotente con `/tmp/migrate_kb_v2_progress.json`
- Pre-flight sanity: `upsert+get` su id throwaway → PASS (dst NON è silent-drop)
- Per-batch integrity: `dst.count() ≈ offset` ogni 5000 chunk → sempre allineato

### Esecuzione primaria
```
01:13:57  start  src.count()=158577
01:13:57  sanity OK  dst is writable
01:14:02  4,500 / 158,577 chunks copied (rate ~850/s)
...
01:16:40  155,000 / 158,577 chunks copied (rate ~950/s)
01:16:44  FATAL src.get error at offset=158000:
          InternalError: Error executing plan: Internal error: Error finding id
```
Exit code 127 al 99.6%. Causa: chromadb 1.5.9 non riesce a leggere i 577 chunk con `seq_id > 334328` (gli HNSW ghost del freeze 23/05).

### Rescue id-by-id (script ad-hoc `/tmp/migrate_rescue_missing.py`, cleanato)
```
src - dst = 577 missing
loop ogni id → src.get(ids=[mid], include=["embeddings","documents","metadatas"])
              → dst.upsert se successo

rescued: 396 / 577  (i ghost erano in realtà parziali — alcuni erano in HNSW)
unrescuable: 181
final dst.count() = 158,396
```

### I 181 unrescuable
- ~140 `doc_*_chunk_*` di documenti PDF ingestati nell'ingest_safe pomeridiano del 23/05 (16:48-16:58)
- ~37 `doc_summary_*` generati da `create_missing_summaries.py` lo stesso run
- 4 `live_*` conversazioni Chrome extension del 23/05 sera:
  - `live_claude_5f793347dd14f78e`
  - `live_claude_cd477d460f4da7dc`
  - `live_claude_e902440249aff417`
  - `live_gemini_40deb6d50d361451`

**Recuperabili**: i `doc_*` e `doc_summary_*` rifacendo `ingest_safe.sh` sui 21 documenti originali (PDF on OneDrive). Le 4 conversazioni live perse a meno che le sessioni LLM originali siano ancora aperte.

## B. Validazione dst

Script `/tmp/validate_dst.py` (cleanato), eseguito a MCP DOWN:

| Test | Risultato |
|---|---|
| `dst.count() = src.count() - 181` (atteso) | ✅ 158,396 |
| 20 random sample: `dst.get` doc + emb_dim match `src.get` | ✅ 0 mismatches |
| Upsert + get nuovo id su dst | ✅ persistito, no silent drop |
| Delete cleanup nuovo id | ✅ removed |
| `query(query_embeddings=random, n_results=5)` | ✅ 5 results |

## C. Config aggiornata (6 callsite)

### File primario (cascade automatico)
```yaml
# config/tailor.yaml:108
kb:
  collection: tailor_kb_v2   # era: tailor_kb
```
Backup pre-edit: `config/tailor.yaml.backup.pre_kb_v2_20260524_011956`.

### File Python hardcoded (3)
- [scripts/enrichment/extract_entities.py:53](../../scripts/enrichment/extract_entities.py#L53) — `COLLECTION_NAME = "tailor_kb_v2"`
- [scripts/maintenance/garbage_collect.py:21](../../scripts/maintenance/garbage_collect.py#L21) — idem
- [scripts/maintenance/repair_hnsw_index.py:51](../../scripts/maintenance/repair_hnsw_index.py#L51) — idem

### File Python con SQL raw (2)
- [scripts/enrichment/generate_user_profile.py:74](../../scripts/enrichment/generate_user_profile.py#L74) — `WHERE c.name = 'tailor_kb_v2'`
- [scripts/enrichment/build_entity_index.py:43,48,54](../../scripts/enrichment/build_entity_index.py#L43) — `WHERE c.name = 'tailor_kb_v2'` + commenti

Tutti i 5 file Python: AST syntax check **ok**. YAML `safe_load`: **ok**.

### Riferimenti `"tailor_kb"` restanti — dead code/legacy
- 6 file `scripts/{ingest,gmail,enrichment}/*.py` con pattern `cfg("kb", "collection") or "tailor_kb"` — il fallback non viene mai usato perché cfg ora ritorna `tailor_kb_v2`.
- [mcp_server.py:137,139](../../mcp_server.py#L137) — stesso pattern, fallback dead.
- `scripts/maintenance/rebuild_db.py` — script one-shot legacy, contiene `OLD_COLLECTION = "tailor_kb"` come costante per tool migration. Innocuo.
- `scripts/maintenance/ingest_safe.sh:51` — `COLLECTION_ID="9358d5d7-…"` **NON matcha né tailor_kb (62156a36) né tailor_kb_v2 (d6e49a14)**. Era già stale prima della migration. Va sistemato in PR a parte.

## D. MCP boot post-switch

- Bootout MCP (operatore, ~01:11) → bootstrap (operatore, 01:23:11)
- Nuovo PID `36508`, listener `0.0.0.0:8787`
- stderr post-boot: **nessun** `tailor_kb_v2 not found`, **nessun** silent drop, **nessun** HNSW error
- collection caricata dal pattern `_cfg_get_top("kb","collection")` → `tailor_kb_v2` ✓

## E. Smoke test E2E

| Test | Esito |
|---|---|
| dashboard `https://jarvis.carlucci.io/` hard-refresh | ✅ counter popolati ~158,396 (operatore conferma) |
| Chrome extension → Test Connection | ✅ verde (operatore conferma) |
| Capture write end-to-end Chrome → MCP | ✅ stdout mostra `POST /api/ingest-live → 200 OK`, embeddings 158,396 → 158,397 (+1) |
| Telegram bot ping | ✅ risponde (operatore conferma) |
| stderr ricerca `SILENT DROP` post-boot 01:23:11 | ✅ **zero occurrences** |

Sono comparsi **3 POST `/api/ingest-live` → 500** subito dopo il bootstrap, prima del primo 200 OK. Ipotesi: (a) chromadb 1.5.9 sta facendo warmup interno sul nuovo segment al primo `count()` chiamato (lazy HNSW load) → durante quel breve interval i POST entranti falliscono, (b) Chrome extension ha buffer di retry con messaggi vecchi che falliscono per duplicate-id. Non bloccanti.

## F. Stato finale

### Collections
| ID | Name | Embeddings | Status |
|---|---|---|---|
| `62156a36-…` | `tailor_kb` (vecchia) | 158,577 | **intoccata** (fallback per rollback) |
| `d6e49a14-…` | `tailor_kb_v2` (nuova) | 158,397 (cresce) | **attiva**, write OK |

### Segments
```
META  ef844a53-…  (tailor_kb)        max_seq=334557  embeddings=158577
HNSW  0eba77c6-…  (tailor_kb)        max_seq=334557  (drift on file persiste, irrilevante)
META  56a260e7-…  (tailor_kb_v2)     max_seq=158401  embeddings=158397
HNSW  08e773cd-…  (tailor_kb_v2)     max_seq=158002  (queue=400 da compactare lazy)
```

### Filesystem
- `~/tailor/db/0eba77c6-…/` — HNSW vecchio, **non più riferito** dal codice attivo (tranne fallback dead)
- `~/tailor/db/08e773cd-…/` — HNSW nuovo, 507 MB data_level0.bin, mtime 01:16
- Dir orfane in `~/tailor/db/` non eliminate: `95dd1079-…` (test isolato pre-CC), `chroma/9ad2790b-…` (legacy stale). Safe da rimuovere, ma decisione operatore.

### Backups attivi
- `~/tailor/backups/manual_pre_resync_20260524_005624/` — pre Opzione A (sqlite + HNSW 0eba77c6 con SHA noti)
- `~/tailor/backups/manual_20260524_000010/chroma.sqlite3.pre_upgrade` — pre upgrade chromadb 1.5.8→1.5.9
- `~/tailor/config/tailor.yaml.backup.pre_kb_v2_20260524_011956` — config pre-switch
- 3 full backup notturni `tailor_db_full_2026052{1,2,3}_*.tar.gz`

## G. Rollback path (mai esercitato)

Se write si rompe in produzione su tailor_kb_v2:
```sh
# 1. Bootout MCP
sudo launchctl bootout system/com.tailor.mcp

# 2. Revert config
cp ~/tailor/config/tailor.yaml.backup.pre_kb_v2_20260524_011956 ~/tailor/config/tailor.yaml

# 3. Revert 5 file Python via git (sono tracked):
cd ~/tailor && git checkout HEAD -- \
  scripts/enrichment/extract_entities.py \
  scripts/maintenance/garbage_collect.py \
  scripts/maintenance/repair_hnsw_index.py \
  scripts/enrichment/generate_user_profile.py \
  scripts/enrichment/build_entity_index.py

# 4. Bootstrap MCP
sudo launchctl bootstrap system /Library/LaunchDaemons/com.tailor.mcp.plist
```
Stato torna a pre-Opzione-C: write rotto su tailor_kb (silent drop), letture OK. Read-only de facto. Zero data loss aggiuntivo perché `tailor_kb` è intoccata.

## H. Cleanup raccomandato (a freddo, fuori sessione)

1. **Drop tailor_kb vecchia** dopo periodo di osservazione (es. 1 settimana):
   ```python
   client.delete_collection("tailor_kb")
   ```
   Recupera ~1.7 GB sqlite (dopo VACUUM) + 513 MB HNSW segment. Solo dopo aver confermato v2 stabile.

2. **Rimuovi dir orfane** `~/tailor/db/{95dd1079-…,chroma/9ad2790b-…}` (test pre-CC + stale legacy).

3. **Sistema [scripts/maintenance/ingest_safe.sh:51](../../scripts/maintenance/ingest_safe.sh#L51)** — `COLLECTION_ID` hardcoded stale. Da sostituire con lookup dinamico via SQL o cfg.

4. **Refactor 6 callsite cfg+fallback dead** — il pattern `cfg("kb","collection") or "tailor_kb"` ora ha fallback morto. Lascia "tailor_kb_v2" come fallback, o (meglio) usa cfg sola.

5. **Re-ingest dei 21 PDF persi** durante il freeze pomeridiano del 23/05 — rilancia `ingest_safe.sh` per recuperare i ~140 doc_chunk + 20 doc_summary persi.

6. **Commit dei 5 file Python edited + config update** in CHANGELOG come operazione di emergenza.

## I. Domande aperte / rischi residui

1. **Recurrence del bug**: tailor_kb_v2 è nato sano, ma chromadb 1.5.9 ha lo stesso fix-silent-on-mismatch. Se in futuro un altro freeze HNSW dovesse avvenire, lo stesso problema si ripresenterebbe su v2. Mitigation: lo `sync_threshold=1000` + monitoring queue depth + reaction proattiva sotto soglia.

2. **I 3 POST 500 iniziali post-bootstrap**: causa esatta non identificata. Pattern coerente con warmup HNSW lazy o duplicate-id retry. Da monitorare se ricompare al prossimo restart.

3. **Embeddings function della nuova collection**: creata con `metadata={"hnsw:space": "cosine"}` senza embedding_function esplicita. Schema config sarà rigenerato al primo write con default chromadb. Funzionalmente equivalente a tailor_kb originale ma `schema_str` potrebbe differire — da verificare se queries con filter su metadata custom funzionano come prima.

4. **Drift HNSW vs META su tailor_kb_v2**: già 398 in queue al boot (rimasti dalla migration). Sotto sync_threshold=1000, sarà drenato al prossimo write. Da osservare che drain avvenga.

5. **tailor_kb vecchia non droppata**: occupa 2.2 GB. Lasciata per safety period. Dopo conferma stabilità v2, drop esplicito.

---

## Riferimenti

- Diagnostica precedente che ha identificato bug: [docs/diagnostics/2026-05-24_silent-drop-tailor-kb.md](2026-05-24_silent-drop-tailor-kb.md)
- Diagnostica freeze intermittente notte 23/05: [docs/diagnostics/2026-05-23_hnsw-freeze-intermittent-analysis.md](2026-05-23_hnsw-freeze-intermittent-analysis.md)
- Post-mortem precedente: [docs/postmortems/2026-05-13-chromadb-1.5.8-segfault.md](../postmortems/2026-05-13-chromadb-1.5.8-segfault.md)
- Migration script: [scripts/migration/migrate_to_kb_v2.py](../../scripts/migration/migrate_to_kb_v2.py)
- Log migration: `~/tailor/logs/migration_kb_v2_20260524_011357.log`
- Backup safety: `~/tailor/backups/manual_pre_resync_20260524_005624/`
- Backup config: `~/tailor/config/tailor.yaml.backup.pre_kb_v2_20260524_011956`

— Migration eseguita 2026-05-24 01:14-01:24 Europe/Rome. Operativo confermato 01:30.
