#!/usr/bin/env python3
"""Riallinea in KB i documenti spostati o rinominati su disco.

================================================================================
IL PROBLEMA
================================================================================
L'ingest identifica un documento per HASH del contenuto ma lo registra per
PATH. Le due chiavi divergono appena un file si muove:

  * il vecchio path resta in registry e nei metadati dei chunk;
  * il nuovo path non entra, perche' la dedup per hash lo vede gia' dentro.

Il contenuto resta quindi recuperabile, la collocazione no: `file_path`,
`folder` e `title` continuano a indicare un posto che sul disco non esiste
piu'. Quattro casi d'uso, un solo meccanismo — in tutti e quattro l'hash non
cambia:

  spostamento | cancella-e-ricarica altrove | rinomina sul posto |
  rinomina + spostamento

STATO ALLA SCRITTURA (2026-08-21): zero casi. Registry 3.563 entry, nessun
path inesistente su disco. Questo strumento serve PRIMA di una
riorganizzazione delle cartelle, non per riparare un danno gia' fatto.

================================================================================
PERCHE' E' UN UPDATE E NON UN RE-INGEST
================================================================================
`file_path` e' metadato puro: il testo del chunk non contiene il path.
Aggiornarlo non richiede re-embedding e non tocca l'HNSW — nessun vettore
cambia, cambiano solo le colonne di embedding_metadata. Un re-ingest del
documento spostato costerebbe l'estrazione, l'embedding e un delete+upsert di
tutti i chunk per ottenere esattamente lo stesso risultato.

================================================================================
L'AGGANCIO E' file_path, NON extends
================================================================================
Un documento in KB e' due cose: i chunk (`conv_id = doc_<hash12>`) e i suoi
doc_summary (`category = doc_summary`, `conv_id = doc_summary_<data-batch>`).
I due gruppi NON condividono il conv_id.

Tutti e 3.849 i doc_summary hanno pero' `file_path`, identico a quello dei
chunk del documento: e' quella la chiave che li tiene insieme. `extends` no —
copre 1.316 summary su 3.849, e riallineare per extends lascerebbe indietro i
due terzi. Esempio misurato: Atto_Marche_425.pdf -> 31 righe con quel
file_path = 29 chunk + 2 summary.

Da qui la regola: **si aggiorna per file_path, tutte le righe insieme.** Il
conv_id serve solo come rete di recupero quando il file_path ricalcolato non
trova nulla (vedi resolve_old_file_path).

================================================================================
COSA NON TOCCA
================================================================================
  * facts.sqlite3 / entity_index.sqlite3 — indicizzano per chunk_id, che non
    cambia. Non contengono path: non c'e' niente da riallineare.
  * lexical_index.sqlite3 — contiene file_path, title, folder e doc_type, ma
    si riconcilia DOPO e in blocco con reconcile_lexical_index.py. Un
    micro-update riga per riga qui duplicherebbe la logica del reconciler e
    divergerebbe alla prima modifica.
  * gli ORFANI — path sparito e hash introvabile. Segnalati e basta: la
    rimozione dalla KB e' materia di purge_lowvalue_docs.py, fuori perimetro.
  * il `category` delle righe doc_summary — vedi sotto.

================================================================================
IL PERIMETRO DEI CAMPI — TRE REGIMI, NON UNO
================================================================================
Su OGNI riga del documento (chunk e doc_summary insieme):

    file_path, folder, title, file_type

Su ogni riga, ma solo se il nuovo nome CLASSIFICA e cambia il verdetto:

    doc_type — sta su tutte e 74.250 le righe con file_path, i 3.849 summary
    inclusi, e deriva dal nome file: una rinomina lo sposta. Riscriverlo alla
    cieca sarebbe pero' distruttivo. Misurato sulla KB di oggi: 41.932 righe
    su 74.250 (56%) portano un doc_type che `infer_doc_type(title)` NON
    produce — `business_plan`, `report_analytics`, `comunicazione_fiscale`,
    `forecast`, `altro` non compaiono nemmeno in DOC_TYPE_PATTERNS. Vengono
    da un arricchimento storico che nessun code path in repo oggi rigenera,
    quindi sostituirli con `documento`/`spreadsheet` e' una perdita a senso
    unico. Servono due condizioni: verdetti diversi E un nuovo nome che fa
    scattare un pattern. La sola divergenza e' asimmetrica nel verso
    sbagliato — `Contratto locazione.pdf -> scan.pdf` scriverebbe il ripiego
    sopra la classificazione. Vedi doc_type_update().

SOLO sulle righe chunk:

    category — porta DUE semantiche sotto lo stesso nome. Sui doc_summary e'
    la costante "doc_summary" (3.849 su 3.849, verificato): e' il marcatore
    di tipo con cui purge_lowvalue_docs, garbage_collect e
    create_conv_summaries riconoscono un summary. Sui chunk e' invece una
    cartella del path — la prima sottocartella sotto la watch folder — e in
    quanto path-derivata va riallineata. Ricalcolarla su una riga summary la
    trasformerebbe in un nome di cartella e renderebbe quel summary
    invisibile a chiunque lo cerchi per marcatore.

Che una riga sia summary lo dice il `category` che PORTA GIA', letto prima di
sovrascrivere, e l'update lo riverifica dopo (vedi _apply_one).

LIMITE NOTO E ACCETTATO del discriminante: una watch folder che contenesse una
sottocartella chiamata letteralmente "doc_summary" darebbe ai suoi chunk
`category == "doc_summary"`, e questo codice li scambierebbe per summary
lasciandogli il `category` invariato. Sui dati di oggi non succede — 0 chunk
con quel valore, 3.849 summary che ce l'hanno tutti — e non esiste un
discriminante migliore: e' l'unico campo che i due gruppi condividono con
valori disgiunti. Se un giorno nascesse quella cartella, il sintomo sarebbe
chunk con `category` rimasto al vecchio valore dopo un riallineamento.

================================================================================
RESUME — la finestra di crash e' idempotente
================================================================================
Il registry si salva DOPO la KB, di proposito: l'ordine inverso lascerebbe la
KB al vecchio file_path senza piu' un'entry di registry che ci porti. La
finestra di crash che ne risulta — KB gia' al nuovo path, registry ancora al
vecchio — e' quindi la modalita' di fallimento NORMALE, e va gestita, non
subita.

Al run successivo: il vecchio path e' ancora chiave di registry e sul disco non
esiste, l'hash porta al file nuovo, ma il file_path ricalcolato dal vecchio
path non trova piu' righe. Scatta il recupero via conv_id, che restituisce il
file_path NUOVO: `old_fp` e `new_fp` coincidono. _apply_one riconosce il caso
(already_aligned), riapplica i metadati — che e' idempotente — e salta la
verifica sui residui, che a path coincidenti misurerebbe il successo e lo
chiamerebbe fallimento. Poi il registry viene sistemato e il documento e'
chiuso.

Senza quel riconoscimento la verifica falliva con "righe ancora al vecchio
file_path <path NUOVO>": un messaggio che si contraddice da solo, su quella
che e' la traiettoria di ripresa attesa.

================================================================================
LE TRE CATEGORIE
================================================================================
RIALLINEABILE  path non esiste su disco, hash presente altrove
               -> proposta di aggiornamento (l'unica categoria che agisce)
ORFANO         path non esiste, hash non trovato da nessuna parte
               -> sola segnalazione
MODIFICATO     path esiste ma l'hash e' cambiato
               -> sola segnalazione, e va letta come DUE fatti insieme:
                  in KB c'e' un documento vecchio ormai obsoleto, e su disco
                  c'e' contenuto nuovo mai indicizzato. Non e' una
                  cancellazione: e' un re-ingest che deve girare. Caso noto:
                  DettaglioOperazioni.xls.

AMBIGUO non e' una quarta categoria ma un riallineabile che si RIFIUTA di
agire: se lo stesso hash sta su disco in piu' punti (i 595 duplicati noti),
quale sia il path canonico non lo decide uno script. Segnalato e saltato.
Stessa sorte per un candidato gia' presente in registry sotto il suo path
(sarebbe una collisione di chiave, non uno spostamento) e per due entry
diverse che puntano allo stesso nuovo path.

================================================================================
VINCOLI OPERATIVI
================================================================================
  * --dry-run e' il DEFAULT. --execute va scritto per esteso.
  * il dry-run non apre MAI un PersistentClient: legge Chroma via sqlite
    mode=ro. Due PersistentClient sullo stesso path = SIGSEGV, e un censimento
    non vale quel rischio.
  * --execute scrive SOLO via API Chroma (collection.update a batch). Mai
    UPDATE SQL diretto sul file sqlite: bypasserebbe il log della collection e
    la coda degli embedding, e lascerebbe l'indice a raccontare un'altra cosa.
  * --execute pretende maintenance mode PID-validata (il MCP deve aver
    RILASCIATO il client Chroma), il lock ingest e la guardia entity_index.
    Stesso gate di purge_lowvalue_docs.py, importato e non riscritto. La
    guardia entity_index qui non serve per i DATI (entity_index indicizza per
    chunk_id, che il riallineamento non tocca) ma per il PROCESSO:
    build_entity_index.py apre un PersistentClient sullo stesso path, e due
    client concorrenti su chromadb sono un SIGSEGV.
  * un fallimento DOPO che la scrittura e' partita ferma il run (exit 4). Non
    e' un documento "saltato": e' un documento le cui righe possono essere
    divise fra due file_path, e proseguire ne accumulerebbe altri mentre il
    log rassicura. Solo il fallimento PRIMA di ogni scrittura
    (RealignPreflight) fa proseguire.
  * il piano si calcola PRIMA di aprire il client, sulla connessione mode=ro,
    e poi la connessione si chiude. Dry-run ed execute pianificano con lo
    stesso codice: quello che il dry-run stampa e' quello che l'execute fa.

================================================================================
SEQUENZA OPERATIVA (di Emiliano — lo script non tocca daemon/launchctl)
================================================================================
    1. maintenance ON:  kill -USR1 <mcp_pid>
    2. backup COMPLETO: bash scripts/maintenance/backup_db.sh
    3. dry-run:  ./.venv/bin/python scripts/maintenance/realign_moved_docs.py
       -> conferma UMANA del piano a video
    4. execute:  ./.venv/bin/python scripts/maintenance/realign_moved_docs.py --execute
    5. reconciler: ./.venv/bin/python scripts/maintenance/reconcile_lexical_index.py
    6. maintenance OFF: kill -USR2 <mcp_pid>
"""
from __future__ import annotations

import argparse
import os
import re as _re
import sqlite3
import sys
from datetime import datetime

# --- path setup ---
_HERE = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(_HERE))            # repo root
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "ingest"))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "lib"))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "maintenance"))

import ingest_docs as ing                                          # noqa: E402

# Gate maintenance PID-validato: unica fonte, mai duplicata.
from backfill_email_youniversal import (                           # noqa: E402
    MaintenanceLost,
    maintenance_state,
)
# Guardia entity_index: stessa ragione di purge_lowvalue_docs.py, gia' scritta
# e testata. Qui NON serve per i dati — entity_index indicizza per chunk_id,
# che il riallineamento non tocca — ma per il processo: build_entity_index.py
# apre un PersistentClient sullo stesso path, e due client concorrenti su
# chromadb sono un SIGSEGV, non un conflitto di lock.
from reingest_xlsx import (                                        # noqa: E402
    EntityIndexBusy,
    _assert_entity_index_stable,
    _build_entity_index_running,
    _entity_index_fingerprint,
)

LOGS_DIR = os.path.join(BASE_DIR, "logs")
CHROMA_SQLITE = os.path.join(ing.DB_DIR, "chroma.sqlite3")

# Campi riallineati su OGNI riga del documento, chunk e doc_summary insieme.
UPDATED_FIELDS = ("file_path", "folder", "title", "file_type")

# `doc_type` sta su tutte e 74.250 le righe con file_path (3.849 summary
# inclusi) e deriva dal nome file: una rinomina lo sposta, e va riallineato
# ovunque come gli altri. Non e' pero' scrivibile alla cieca — vedi
# doc_type_update().
DOC_TYPE_FIELD = "doc_type"

# `category` ha DUE semantiche sotto lo stesso nome:
#   * sui doc_summary e' la costante "doc_summary" — 3.849 righe su 3.849,
#     verificato. E' il marcatore di tipo con cui purge_lowvalue_docs,
#     garbage_collect e create_conv_summaries riconoscono un summary.
#   * sui chunk e' una cartella del path (la prima sottocartella sotto la
#     watch folder), quindi path-derivata e da riallineare.
# Ricalcolarla su una riga summary la trasformerebbe in un nome di cartella e
# renderebbe quel summary invisibile a chiunque lo cerchi per marcatore. Si
# aggiorna quindi SOLO sulle righe chunk.
CHUNK_ONLY_FIELDS = ("category",)

# Il valore che marca una riga come doc_summary nel campo `category`.
SUMMARY_CATEGORY = "doc_summary"

# Righe per chiamata a collection.update(). Un documento Excel puo' avere
# migliaia di chunk e passarli in un colpo solo significa tenere in memoria
# altrettanti dict di metadati e affidare a una sola transazione tutto il
# lavoro: a batch il fallimento e' parziale e diagnosticabile.
UPDATE_BATCH = 500

# Categorie
RIALLINEABILE = "riallineabile"
ORFANO = "orfano"
MODIFICATO = "modificato"


# ============================================================================
# ERRORI — la differenza che conta e' "ho gia' scritto?"
# ============================================================================
class RealignPreflight(RuntimeError):
    """Fallito PRIMA di qualunque scrittura: la KB e' intatta.

    Solo questo caso e' saltabile: il documento resta esattamente com'era e il
    run puo' passare al successivo senza aver lasciato niente a meta'.
    """


class RealignInconsistent(RuntimeError):
    """Fallito DOPO aver scritto: il documento e' in uno stato incoerente.

    Non e' un documento "saltato": le sue righe possono essere divise fra due
    file_path, o aver perso il marcatore doc_summary. Proseguire vorrebbe dire
    accumulare documenti spezzati mentre il log dice che va tutto bene, quindi
    questo errore FERMA il run.
    """


# ============================================================================
# LETTURA CHROMA — sola lettura, nessun PersistentClient
# ============================================================================
def _ro(db_path: str) -> sqlite3.Connection:
    """Sola lettura coordinata coi writer: mode=ro (mai immutable=1)."""
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    con.execute("PRAGMA busy_timeout=30000")
    return con


def rel_path_for(abs_path: str) -> str:
    """Il `file_path` che l'ingest avrebbe scritto per questo path assoluto.

    Stessa espressione di scan_folders(): relativo al commonpath delle watch
    folder. E' pura manipolazione di stringhe, non richiede che il file
    esista — che e' esattamente il caso qui (il vecchio path non c'e' piu').
    """
    watch = list(ing.WATCH_FOLDERS)
    if not watch:
        return abs_path
    return os.path.relpath(abs_path,
                           os.path.commonpath(watch + [abs_path]))


def count_rows_ro(con: sqlite3.Connection, file_path: str) -> tuple[int, int]:
    """(chunk, summary) vivi con quel file_path.

    Il JOIN su `embeddings` non e' decorativo: filtra le righe di metadati che
    non hanno piu' un embedding vivo dietro. Contarle sarebbe promettere
    aggiornamenti su righe che la collection non conosce piu'.
    """
    row = con.execute(
        """SELECT
             SUM(CASE WHEN c.string_value = 'doc_summary' THEN 0 ELSE 1 END),
             SUM(CASE WHEN c.string_value = 'doc_summary' THEN 1 ELSE 0 END)
           FROM embedding_metadata fp
           JOIN embeddings e ON e.id = fp.id
           LEFT JOIN embedding_metadata c
                  ON c.id = fp.id AND c.key = 'category'
           WHERE fp.key = 'file_path' AND fp.string_value = ?""",
        (file_path,),
    ).fetchone()
    return int(row[0] or 0), int(row[1] or 0)


def file_path_by_conv_id_ro(con: sqlite3.Connection, h12: str) -> str | None:
    """Il file_path scritto sui chunk di doc_<h12>, letto dalla KB.

    Rete di recupero per quando il file_path ricalcolato non trova righe:
    significa che le watch folder sono cambiate dall'ingest, e il valore in KB
    e' l'unica fonte che non stia indovinando. Se i chunk disaccordano fra
    loro non si sceglie: si restituisce None e il caso va guardato a mano.
    """
    vals = {r[0] for r in con.execute(
        """SELECT DISTINCT fp.string_value
           FROM embedding_metadata cid
           JOIN embeddings e ON e.id = cid.id
           JOIN embedding_metadata fp ON fp.id = cid.id AND fp.key = 'file_path'
           WHERE cid.key = 'conv_id' AND cid.string_value = ?""",
        (f"doc_{h12}",),
    )}
    return vals.pop() if len(vals) == 1 else None


def resolve_old_file_path(con: sqlite3.Connection, abs_path: str,
                          doc_hash: str) -> tuple[str | None, int, int, str]:
    """(file_path, chunk, summary, provenienza) del documento in KB.

    provenienza: "path" (ricalcolato dal path di registry, caso normale),
    "conv_id" (recuperato dai chunk) o "" se in KB non c'e' nulla.
    """
    fp = rel_path_for(abs_path)
    chunks, summaries = count_rows_ro(con, fp)
    if chunks or summaries:
        return fp, chunks, summaries, "path"

    alt = file_path_by_conv_id_ro(con, (doc_hash or "")[:12])
    if alt:
        chunks, summaries = count_rows_ro(con, alt)
        if chunks or summaries:
            return alt, chunks, summaries, "conv_id"
    return None, 0, 0, ""


# ============================================================================
# SCANSIONE DISCO
# ============================================================================
def scan_disk(log=print) -> tuple[list[dict], dict[str, list[dict]]]:
    """(file scansionati, hash -> [file]). Hashing di tutto: ~3s su 3,8 GB."""
    files = ing.scan_folders()
    by_hash: dict[str, list[dict]] = {}
    for f in files:
        f["hash"] = ing.file_hash(f["filepath"])
        by_hash.setdefault(f["hash"], []).append(f)
    log(f"[disco]   {len(files):,} file scansionati, "
        f"{len(by_hash):,} hash distinti")
    return files, by_hash


def _hash_on_disk(path: str, scanned: dict[str, dict]) -> str | None:
    """Hash del file, dallo scan se c'e' altrimenti calcolato.

    Un path puo' essere in registry ed esistere su disco senza comparire nello
    scan: denylist, estensione non piu' supportata, cartella entrata in
    IGNORE_FOLDERS. Classificarlo "modificato" solo perche' lo scan non lo
    vede sarebbe una segnalazione falsa, quindi l'hash si calcola comunque.
    """
    got = scanned.get(path)
    if got:
        return got["hash"]
    try:
        return ing.file_hash(path)
    except OSError:
        return None


# ============================================================================
# doc_type — ricalcolo dal nome, ma solo quando il nome ha cambiato idea
# ============================================================================
def name_classifies(filename: str) -> bool:
    """Il nome fa scattare un pattern di DOC_TYPE_PATTERNS?

    Predicato, non una seconda inferenza: il VALORE resta quello che ritorna
    `infer_doc_type`. Serve a distinguere le due cose che quella funzione
    ritorna sotto lo stesso tipo — una classificazione vera ("il nome dice
    contratto") e un ripiego ("il nome non dice niente, ecco l'estensione o
    `documento`"). Legge DOC_TYPE_PATTERNS, la stessa costante dell'ingest, e
    quindi segue automaticamente ogni pattern che venga aggiunto li'.
    """
    return any(_re.search(pat, filename, _re.IGNORECASE)
               for pat, _dt in ing.DOC_TYPE_PATTERNS)


def doc_type_update(old_filename: str, new_filename: str) -> tuple[str, str, str | None]:
    """(verdetto vecchio nome, verdetto nuovo nome, valore da scrivere o None).

    `infer_doc_type` guarda il nome file e, se nessun pattern matcha, ripiega
    sul testo — che qui non abbiamo — e infine sull'estensione. Ricalcolarlo
    alla cieca sul solo nome sarebbe distruttivo: sulla KB di oggi 41.932
    righe su 74.250 (56%) portano un doc_type che `infer_doc_type(title)` NON
    produce — `business_plan`, `report_analytics`, `comunicazione_fiscale`,
    `forecast`, `altro` non sono nemmeno in DOC_TYPE_PATTERNS. Sono di un
    arricchimento storico che nessun code path in repo oggi rigenera:
    sovrascriverli con `documento` o `spreadsheet` non e' un ricalcolo, e'
    una perdita a senso unico.

    Servono quindi DUE condizioni, non una:

      1. i due nomi devono dare verdetti diversi — altrimenti il ricalcolo non
         porta niente che la KB non abbia gia' (ed e' il caso di ogni
         spostamento puro, dove il nome non cambia affatto);
      2. il NUOVO nome deve classificare davvero, cioe' far scattare un
         pattern. Il solo (1) e' asimmetrico nel verso sbagliato:
         `Contratto locazione.pdf -> scan.pdf` da' ('contratto',
         'documento') e scriverebbe il ripiego sopra la classificazione, che
         e' esattamente il degrado che la regola doveva impedire. Non riguarda
         solo `documento`: `Contratto locazione.xlsx -> scan.xlsx` da'
         ('contratto', 'spreadsheet') e sarebbe la stessa perdita travestita
         da tipo specifico. Da qui il predicato name_classifies(), che guarda
         se un pattern ha deciso invece di guardare il valore prodotto.

    Il verso opposto era gia' coperto da (1): `scan.pdf -> Contratto
    Henkel.pdf` non scrive perche' nemmeno il nuovo nome fa scattare un
    pattern (quel pattern vuole locazione/affitto/compravendita/fornitura
    dopo "contratto") e i due verdetti restano entrambi 'documento'.
    """
    old_dt = ing.infer_doc_type(old_filename)
    new_dt = ing.infer_doc_type(new_filename)
    write = new_dt if (new_dt != old_dt and name_classifies(new_filename)) else None
    return old_dt, new_dt, write


# ============================================================================
# PIANO
# ============================================================================
def build_plan(registry: dict, by_hash: dict[str, list[dict]],
               scanned: dict[str, dict], con: sqlite3.Connection) -> dict:
    """Classifica ogni entry del registry. Nessuna scrittura, nessun client.

    Ritorna {"realign": [...], "skipped": [...], "orphans": [...],
             "modified": [...], "aligned": int}.
    """
    realign: list[dict] = []
    skipped: list[dict] = []
    orphans: list[dict] = []
    modified: list[dict] = []
    aligned = 0

    for old_path in sorted(registry):
        entry = registry[old_path]
        doc_hash = entry.get("hash") or ""

        # --- il path esiste: allineato oppure MODIFICATO -------------------
        if os.path.exists(old_path):
            cur = _hash_on_disk(old_path, scanned)
            if cur is None:
                skipped.append({"old_path": old_path, "hash": doc_hash,
                                "reason": "file illeggibile (hash non calcolabile)"})
                continue
            if cur == doc_hash:
                aligned += 1
                continue
            fp, chunks, summaries, _ = resolve_old_file_path(
                con, old_path, doc_hash)
            # Il contenuto NUOVO e' gia' in KB sotto un altro path? Cambia
            # cosa serve fare: se si', l'unica cosa obsoleta e' il documento
            # vecchio; se no, il contenuto nuovo non e' mai stato indicizzato.
            new_indexed = any(
                f["filepath"] != old_path
                and registry.get(f["filepath"], {}).get("hash") == cur
                for f in by_hash.get(cur, []))
            modified.append({
                "path": old_path, "old_hash": doc_hash, "new_hash": cur,
                "kb_file_path": fp, "chunks": chunks, "summaries": summaries,
                "new_content_indexed": new_indexed,
                "status": entry.get("status", "ok"),
                "fail_count": entry.get("fail_count", 0),
                "last_error": entry.get("last_error", ""),
            })
            continue

        # --- il path non esiste: RIALLINEABILE oppure ORFANO ---------------
        cands = [f for f in by_hash.get(doc_hash, [])
                 if f["filepath"] != old_path] if doc_hash else []
        fp, chunks, summaries, prov = resolve_old_file_path(
            con, old_path, doc_hash)

        if not cands:
            orphans.append({"path": old_path, "hash": doc_hash,
                            "kb_file_path": fp,
                            "chunks": chunks, "summaries": summaries})
            continue

        if len(cands) > 1:
            skipped.append({
                "old_path": old_path, "hash": doc_hash,
                "reason": f"{len(cands)} candidati su disco con lo stesso hash",
                "candidates": [c["filepath"] for c in cands],
            })
            continue

        new = cands[0]
        if new["filepath"] in registry:
            skipped.append({
                "old_path": old_path, "hash": doc_hash,
                "reason": "il candidato e' gia' in registry sotto il suo path "
                          "(duplicato indicizzato, non uno spostamento)",
                "candidates": [new["filepath"]],
            })
            continue

        if fp is None:
            skipped.append({
                "old_path": old_path, "hash": doc_hash,
                "reason": "nessuna riga in KB per questo documento "
                          "(niente da riallineare)",
                "candidates": [new["filepath"]],
            })
            continue

        old_name = entry.get("filename") or os.path.basename(old_path)
        dt_old, dt_new, dt_write = doc_type_update(old_name, new["filename"])

        updates = {
            "file_path": new["rel_path"],
            "folder": ing.infer_folder(new["filepath"]),
            "title": new["filename"],
            "file_type": new["extension"],
        }
        if dt_write is not None:
            updates[DOC_TYPE_FIELD] = dt_write

        realign.append({
            "old_path": old_path,
            "new_path": new["filepath"],
            "hash": doc_hash,
            "old_file_path": fp,
            "old_fp_source": prov,
            "chunks": chunks,
            "summaries": summaries,
            # applicato a OGNI riga: chunk e doc_summary insieme
            "updates": updates,
            # applicato SOLO alle righe chunk: sui summary `category` e' il
            # marcatore di tipo, non una cartella (vedi CHUNK_ONLY_FIELDS).
            # `category` esce dallo scan, non e' reimplementata qui: e' la
            # prima sottocartella sotto la watch folder che contiene il file.
            "chunk_updates": {"category": new["category"]},
            "doc_type_old": dt_old,
            "doc_type_new": dt_new,
            "filename": new["filename"],
        })

    # --- collisioni fra proposte: due entry verso lo stesso nuovo path -----
    # Applicarle entrambe farebbe vincere l'ultima e perderebbe l'altra in
    # silenzio, sia in registry (stessa chiave) sia in KB (stesso file_path).
    seen: dict[str, list[dict]] = {}
    for r in realign:
        seen.setdefault(r["new_path"], []).append(r)
    colliding = {p for p, rs in seen.items() if len(rs) > 1}
    if colliding:
        kept = []
        for r in realign:
            if r["new_path"] in colliding:
                skipped.append({
                    "old_path": r["old_path"], "hash": r["hash"],
                    "reason": "piu' entry di registry propongono lo stesso "
                              "nuovo path",
                    "candidates": [r["new_path"]],
                })
            else:
                kept.append(r)
        realign = kept

    return {"realign": realign, "skipped": skipped, "orphans": orphans,
            "modified": modified, "aligned": aligned}


# ============================================================================
# STAMPA DEL PIANO
# ============================================================================
def _fmt_rows(r: dict) -> str:
    tot = r["chunks"] + r["summaries"]
    return f"{tot:,} righe ({r['chunks']:,} chunk + {r['summaries']:,} summary)"


def print_plan(plan: dict, registry_size: int, show: int, log=print) -> None:
    realign, skipped = plan["realign"], plan["skipped"]
    orphans, modified = plan["orphans"], plan["modified"]

    log("")
    log("-" * 78)
    log("PIANO")
    log("-" * 78)

    if realign:
        log(f"\nRIALLINEABILI — {len(realign)} documenti:")
        for r in realign:
            log(f"\n  {r['old_path']}")
            log(f"    -> {r['new_path']}")
            log(f"    KB: {_fmt_rows(r)} con file_path "
                f"{r['old_file_path']!r}"
                + ("  [file_path recuperato via conv_id]"
                   if r["old_fp_source"] == "conv_id" else ""))
            log(f"    su TUTTE le {r['chunks'] + r['summaries']:,} righe:")
            for k in UPDATED_FIELDS:
                log(f"       {k:<10} -> {r['updates'][k]!r}")
            if DOC_TYPE_FIELD in r["updates"]:
                log(f"       {DOC_TYPE_FIELD:<10} -> "
                    f"{r['updates'][DOC_TYPE_FIELD]!r}   "
                    f"(il nome cambia verdetto: {r['doc_type_old']!r} -> "
                    f"{r['doc_type_new']!r})")
            else:
                log(f"       {DOC_TYPE_FIELD:<10} -- invariato: il nome dice "
                    f"ancora {r['doc_type_old']!r}, si conserva il valore in "
                    f"KB (puo' venire da un arricchimento)")
            log(f"    solo sulle {r['chunks']:,} righe chunk "
                f"(sui {r['summaries']:,} summary `category` e' il marcatore "
                f"di tipo e resta {SUMMARY_CATEGORY!r}):")
            for k in CHUNK_ONLY_FIELDS:
                log(f"       {k:<10} -> {r['chunk_updates'][k]!r}")
    else:
        log("\nRIALLINEABILI — nessuno.")

    if skipped:
        log(f"\nSALTATI — {len(skipped)} documenti (riallineamento NON "
            f"automatizzabile):")
        for s in skipped:
            log(f"\n  {s['old_path']}")
            log(f"    motivo: {s['reason']}")
            for c in (s.get("candidates") or [])[:show]:
                log(f"      candidato: {c}")
            extra = len(s.get("candidates") or []) - show
            if extra > 0:
                log(f"      ... e altri {extra} candidati non elencati "
                    f"(--show {show})")

    if orphans:
        log(f"\nORFANI — {len(orphans)} documenti (path sparito, hash "
            f"introvabile su disco). Nessuna azione qui: la rimozione e' "
            f"materia di purge_lowvalue_docs.py.")
        for o in orphans[:show]:
            log(f"  {o['path']}")
            log(f"    KB: {_fmt_rows(o)}")
        if len(orphans) > show:
            log(f"  ... e altri {len(orphans) - show} orfani non elencati "
                f"(--show {show})")

    if modified:
        log(f"\nMODIFICATI — {len(modified)} documenti (path presente, "
            f"contenuto cambiato). Due fatti insieme: il documento in KB e' "
            f"obsoleto E il contenuto nuovo va indicizzato. Non e' una "
            f"cancellazione — serve un re-ingest, non questo strumento.")
        for m in modified[:show]:
            log(f"\n  {m['path']}")
            log(f"    hash registry: {m['old_hash'][:12]}  "
                f"-> in KB {_fmt_rows(m)} ormai obsolete")
            log(f"    hash disco   : {m['new_hash'][:12]}  "
                f"-> contenuto nuovo "
                + ("gia' indicizzato sotto un altro path"
                   if m["new_content_indexed"] else "MAI indicizzato"))
            # Un'entry `failed` NON ha mai messo in KB l'hash che porta: senza
            # dirlo, "in KB 0 righe" sembra una perdita di dati invece che un
            # documento che l'estrazione non e' mai riuscita a leggere.
            if m["status"] == "failed":
                log(f"    registry     : status=failed "
                    f"fail_count={m['fail_count']} "
                    f"last_error={m['last_error']!r}"
                    + ("  -> le 0 righe in KB sono coerenti con questo, non "
                       "una perdita" if not (m["chunks"] or m["summaries"])
                       else ""))
        if len(modified) > show:
            log(f"  ... e altri {len(modified) - show} modificati non "
                f"elencati (--show {show})")

    rows = sum(r["chunks"] + r["summaries"] for r in realign)
    log("")
    log("-" * 78)
    log("CONTEGGI")
    log("-" * 78)
    log(f"  registry              : {registry_size:,} entry")
    log(f"  allineati             : {plan['aligned']:,}")
    log(f"  RIALLINEABILI         : {len(realign):,}  "
        f"({rows:,} righe Chroma da aggiornare)")
    log(f"  ORFANI                : {len(orphans):,}")
    log(f"  MODIFICATI            : {len(modified):,}")
    log(f"  saltati (ambigui)     : {len(skipped):,}")


# ============================================================================
# DRY-RUN
# ============================================================================
def dry_run(show: int) -> int:
    print("=" * 78)
    print("REALIGN MOVED DOCS — DRY-RUN (READ-ONLY, zero scritture)")
    print("=" * 78)

    registry = ing.load_registry()
    print(f"[registry] {len(registry):,} entry ({ing.REGISTRY_FILE})")
    _, by_hash = scan_disk()
    scanned = {f["filepath"]: f for fs in by_hash.values() for f in fs}

    con = _ro(CHROMA_SQLITE)
    try:
        plan = build_plan(registry, by_hash, scanned, con)
    finally:
        con.close()

    print_plan(plan, len(registry), show)

    if plan["realign"]:
        print("\n[next] applicare:  ./.venv/bin/python "
              "scripts/maintenance/realign_moved_docs.py --execute")
        print("       (richiede maintenance mode: kill -USR1 <mcp_pid>, e un "
              "backup completo prima)")
    else:
        print("\nNiente da riallineare: --execute non avrebbe nulla da fare.")
    return 0


# ============================================================================
# EXECUTE
# ============================================================================
def _apply_one(collection, r: dict, log=print) -> int:
    """Aggiorna tutte le righe del documento. Ritorna le righe toccate.

    Due famiglie di errore, ed e' la distinzione che conta per chi chiama:

      * RealignPreflight — niente e' stato scritto, il documento e' intatto.
      * RealignInconsistent — la scrittura e' partita e lo stato risultante
        non e' quello atteso. Il documento resta spezzato: fermarsi.

    Riconosce anche il caso RESUME. Il registry si salva DOPO la KB, quindi la
    finestra di crash naturale lascia la KB gia' al nuovo file_path e il
    registry ancora al vecchio path. Al giro dopo resolve_old_file_path
    recupera il file_path via conv_id e trova quello NUOVO: `old_fp` e
    `new_fp` coincidono. Prima questo caso finiva nella verifica "righe ancora
    al vecchio path" e falliva con un messaggio assurdo (il path citato era
    quello nuovo). Adesso e' riconosciuto: si riapplicano i metadati — che e'
    idempotente — e si lascia che il chiamante sistemi il registry.
    """
    old_fp = r["old_file_path"]
    new_fp = r["updates"]["file_path"]
    # old_fp == new_fp puo' venire SOLO dal recupero via conv_id su una KB
    # gia' allineata: quando il file_path si ricalcola dal path di registry i
    # due sono diversi per costruzione (i path di partenza sono diversi).
    already_aligned = (old_fp == new_fp)

    got = collection.get(where={"file_path": old_fp}, include=["metadatas"])
    ids = got.get("ids") or []
    metas = got.get("metadatas") or []
    if not ids:
        raise RealignPreflight(
            f"nessuna riga con file_path {old_fp!r} al momento dell'update "
            f"(KB mossa dopo il piano?) — nessuna scrittura tentata")
    if len(metas) != len(ids):
        raise RealignPreflight(
            f"{len(ids)} id ma {len(metas)} metadati per {old_fp!r} — "
            f"nessuna scrittura tentata")

    # Si riscrive il dict COMPLETO, non solo i campi cambiati: cosi' il
    # risultato e' lo stesso che update() faccia merge o replace del
    # metadato, e non dipende da un dettaglio di implementazione di chromadb.
    #
    # La riga summary si riconosce dal `category` che PORTA GIA', non dal
    # nuovo: e' il valore in KB a dire se quella riga e' un marcatore o una
    # cartella, e va letto prima di sovrascriverlo.
    chunk_only = r.get("chunk_updates") or {}
    new_metas = []
    n_chunk = n_summary = 0
    for m in metas:
        md = dict(m or {})
        is_summary = md.get("category") == SUMMARY_CATEGORY
        md.update(r["updates"])
        if is_summary:
            n_summary += 1
        else:
            md.update(chunk_only)
            n_chunk += 1
        new_metas.append(md)

    if already_aligned:
        log(f"  [resume] la KB e' gia' al file_path {new_fp!r}: "
            f"{len(ids):,} righe, riapplico i metadati (idempotente)")

    # Il primo batch e' lo spartiacque: se salta quello non e' stato scritto
    # niente, se salta un batch successivo il documento e' gia' diviso.
    for i in range(0, len(ids), UPDATE_BATCH):
        try:
            collection.update(ids=ids[i:i + UPDATE_BATCH],
                              metadatas=new_metas[i:i + UPDATE_BATCH])
        except Exception as e:                              # noqa: BLE001
            if i == 0:
                raise RealignPreflight(
                    f"update del primo batch fallito ({e}) — nessuna "
                    f"scrittura andata a segno") from e
            raise RealignInconsistent(
                f"update fallito al batch che parte dalla riga {i} di "
                f"{len(ids)} ({e}): le righe precedenti sono gia' a "
                f"{new_fp!r}, le altre no") from e

    # VERIFICA: che l'update sia avvenuto lo dice una rilettura, non l'assenza
    # di eccezioni. Da qui in giu' abbiamo scritto, quindi ogni fallimento e'
    # RealignInconsistent.
    #
    # Il controllo sui residui non ha senso a resume: old_fp E' new_fp, e
    # trovarci righe e' il risultato voluto, non un residuo.
    if not already_aligned:
        left = collection.get(where={"file_path": old_fp}, include=[])
        n_left = len(left.get("ids") or [])
        if n_left:
            raise RealignInconsistent(
                f"{n_left} righe ancora al vecchio file_path {old_fp!r} dopo "
                f"l'update: il documento e' diviso fra {old_fp!r} e {new_fp!r}")

    now = collection.get(where={"file_path": new_fp}, include=[])
    n_now = len(now.get("ids") or [])
    if n_now < len(ids):
        raise RealignInconsistent(
            f"attese almeno {len(ids)} righe al nuovo file_path {new_fp!r}, "
            f"trovate {n_now}: mancano {len(ids) - n_now} righe")

    # VERIFICA del marcatore: nessun summary deve aver perso il suo
    # `category`. E' l'invariante che rende il campo a doppia semantica sicuro
    # da toccare, quindi si misura invece di darla per buona.
    if n_summary:
        back = collection.get(where={"file_path": new_fp},
                              include=["metadatas"])
        still = sum(1 for m in (back.get("metadatas") or [])
                    if (m or {}).get("category") == SUMMARY_CATEGORY)
        if still != n_summary:
            raise RealignInconsistent(
                f"marcatore doc_summary perso: {n_summary} righe summary "
                f"prima dell'update, {still} dopo")

    residui = "n/d (resume: vecchio e nuovo path coincidono)" \
        if already_aligned else "0"
    log(f"  {len(ids):,} righe aggiornate ({n_chunk:,} chunk + "
        f"{n_summary:,} summary; residui al vecchio path: {residui}, "
        f"al nuovo: {n_now:,})")
    return len(ids)


def execute(show: int) -> int:
    ok, why = maintenance_state()
    if not ok:
        sys.stderr.write(
            f"\nRIFIUTO --execute: MCP NON in maintenance mode ({why}).\n"
            f"  Entra in maintenance: kill -USR1 <mcp_pid>\n"
            f"  (il lock da solo non basta: serve che il MCP abbia RILASCIATO\n"
            f"   il client ChromaDB, cosa che fa solo nel handler SIGUSR1)\n"
            f"  Poi backup COMPLETO: bash scripts/maintenance/backup_db.sh\n")
        return 2
    print(f"[gate]  {why}")

    busy, bwhy = _build_entity_index_running()
    if busy:
        sys.stderr.write(f"\nRIFIUTO --execute: {bwhy}.\n")
        return 2
    entity_baseline = _entity_index_fingerprint()

    lock_fd = ing.acquire_single_instance_lock()
    if lock_fd is None:
        sys.stderr.write(
            f"\nRIFIUTO --execute: un altro ingest e' in esecuzione "
            f"({ing.LOCK_FILE}).\n")
        return 2

    os.makedirs(LOGS_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(LOGS_DIR, f"realign_moved_docs_{ts}.log")
    logf = open(log_path, "a")

    def log(msg: str = "") -> None:
        print(msg, flush=True)
        logf.write(msg + "\n")
        logf.flush()

    log(f"[execute] start {ts}  log={log_path}")
    log(f"[lock]    {ing.LOCK_FILE} acquisito (pid {os.getpid()})")

    try:
        registry = ing.load_registry()
        log(f"[registry] {len(registry):,} entry")
        _, by_hash = scan_disk(log)
        scanned = {f["filepath"]: f for fs in by_hash.values() for f in fs}

        # Piano sulla connessione mode=ro, CHIUSA prima di aprire il client:
        # un lettore sqlite sullo stesso file su cui il PersistentClient
        # scrive incrocia lock condivisi con le sue transazioni, per numeri
        # che qui servono una volta sola.
        con = _ro(CHROMA_SQLITE)
        try:
            plan = build_plan(registry, by_hash, scanned, con)
        finally:
            con.close()

        print_plan(plan, len(registry), show, log)

        if not plan["realign"]:
            log("\nNiente da riallineare: nessuna scrittura.")
            return 0

        ok, why = maintenance_state()
        if not ok:
            raise MaintenanceLost(f"prima di PersistentClient: {why}")
        _assert_entity_index_stable(entity_baseline, "prima di PersistentClient")
        import chromadb
        client = chromadb.PersistentClient(path=ing.DB_DIR)
        ok, why = maintenance_state()
        if not ok:
            raise MaintenanceLost(f"prima di get_collection: {why}")
        collection = client.get_collection(name=ing.COLLECTION_NAME)

        docs_ok = rows_ok = 0
        skipped_docs = 0
        for idx, r in enumerate(plan["realign"], 1):
            log(f"\n[{idx}/{len(plan['realign'])}] {os.path.basename(r['old_path'])}"
                f"  doc_{r['hash'][:12]}")
            log(f"  {r['old_file_path']}  ->  {r['updates']['file_path']}")
            ok, why = maintenance_state()
            if not ok:
                raise MaintenanceLost(f"prima dell'update di doc_"
                                      f"{r['hash'][:12]}: {why}")
            _assert_entity_index_stable(
                entity_baseline, f"prima dell'update di doc_{r['hash'][:12]}")
            try:
                rows_ok += _apply_one(collection, r, log)
            except RealignPreflight as e:
                # Niente e' stato scritto: il documento e' intatto e saltarlo
                # e' onesto. E' l'UNICO caso in cui il run prosegue.
                skipped_docs += 1
                log(f"  SALTATO (nessuna scrittura): {e}")
                log(f"  KB e registry intatti per questo documento.")
                continue

            # Registry DOPO la KB. Se il processo muore in questa finestra, la
            # KB e' al nuovo file_path e il registry ancora al vecchio path: al
            # giro dopo resolve_old_file_path recupera il file_path via conv_id
            # e trova gia' quello nuovo, _apply_one riconosce il caso
            # (already_aligned), riapplica i metadati e si arriva qui a
            # sistemare il registry. L'ordine inverso lascerebbe invece la KB
            # al vecchio path senza piu' un'entry che ci porti.
            entry = dict(registry.pop(r["old_path"]))
            entry["filename"] = r["filename"]
            registry[r["new_path"]] = entry
            ing.save_registry(registry)
            docs_ok += 1

        log("")
        log("-" * 78)
        log(f"RIALLINEATI: {docs_ok}/{len(plan['realign'])} documenti, "
            f"{rows_ok:,} righe Chroma")
        if skipped_docs:
            log(f"SALTATI: {skipped_docs} documenti, nessuna scrittura "
                f"tentata su di loro — vedi sopra")

        log("\n[next] reconciler:  ./.venv/bin/python "
            "scripts/maintenance/reconcile_lexical_index.py")
        log("       OBBLIGATORIO: lexical_index.sqlite3 contiene file_path, "
            "title e folder e adesso e' disallineato dalla KB.")
        log("[next] maintenance OFF: kill -USR2 <mcp_pid>")
        return 1 if skipped_docs else 0

    except RealignInconsistent as e:
        # NON e' un documento saltato: e' un documento SPEZZATO. Proseguire
        # significherebbe accumularne altri mentre il log rassicura.
        msg = (
            f"\nABORT: documento in stato INCOERENTE — il run si ferma qui.\n"
            f"  {e}\n"
            f"  Documento : {r['old_path']}\n"
            f"              doc_{r['hash'][:12]}\n"
            f"  file_path : {r['old_file_path']!r} -> "
            f"{r['updates']['file_path']!r}\n"
            f"  Il registry NON e' stato aggiornato per questo documento: la\n"
            f"  sua chiave e' ancora il vecchio path, quindi il piano lo\n"
            f"  ripropone al prossimo run. Ma la KB e' gia' stata scritta in\n"
            f"  parte: guardare com'e' messo PRIMA di rilanciare.\n"
            f"  Ispezione: le righe del documento stanno sotto uno dei due\n"
            f"  file_path qui sopra (o divise fra i due).\n")
        sys.stderr.write(msg)
        log(f"[ABORT] incoerente su {r['old_path']}: {e}")
        return 4

    except (MaintenanceLost, EntityIndexBusy) as e:
        sys.stderr.write(f"\nABORT: {e}\n")
        log(f"[ABORT] {e}")
        return 3
    finally:
        logf.close()
        lock_fd.close()


# ============================================================================
def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", default=True,
                   help="censimento READ-ONLY (default)")
    g.add_argument("--execute", action="store_true",
                   help="applica il riallineamento (richiede maintenance mode)")
    ap.add_argument("--show", type=int, default=10, metavar="N",
                    help="quante voci elencare per lista lunga (default 10). "
                         "I riallineabili sono sempre elencati tutti: sono il "
                         "piano")
    args = ap.parse_args()

    if args.execute:
        return execute(args.show)
    return dry_run(args.show)


if __name__ == "__main__":
    sys.exit(main())
