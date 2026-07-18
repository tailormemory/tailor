#!/usr/bin/env python3
"""Ri-embedding dei chunk il cui vettore fu calcolato su testo PREFISSATO.

================================================================================
CONTESTO E CRITERIO (P1)
================================================================================
Prima dell'allineamento al contratto `scripts/lib/embedding_contract.py::
embedding_text` (commit bd0914c / 9934a35 / d393f3a, 2026-07-14) alcuni writer
d'ingest embeddavano `prefisso\\n\\ntesto` ma salvavano in `chroma:document` il
solo testo NUDO troncato. Vettore ≠ documento → asimmetria con la query (sempre
nuda) → drift silenzioso. Il prefisso fu RIMOSSO dai writer:
  - source=document : "Cartella: {folder} | Tipo: {doc_type} | File: {title}"
                      -> scripts/ingest/ingest_docs.py (vecchio ingest_chunks,
                         rimosso in 9934a35)
  - source=email    : "Email | From: {from[:50]} | To: {to[:50]} | Subject:
                         {subject[:100]}"
                      -> scripts/gmail/chunk_gmail.py:344 (embed_prefix) +
                         scripts/gmail/ingest_gmail.py (rimosso in 9934a35)
  (le famiglie conversation — claude/chatgpt/gemini — prefissavano anch'esse
   "Conversazione: {title} ({date})", ma il censimento 16/07 vi trovò ZERO
   violatori; sono scandite come coorte di CONTROLLO, mai come target.)

CRITERIO DI IDENTIFICAZIONE — EMPIRICO VETTORIALE (replica censimento 16/07).
Il criterio NON è ricostruibile da metadata soli: un chunk ri-ingerito dopo il
contratto ha gli stessi metadata ma vettore nudo. La lista P1 NON esiste su
disco e va RI-DERIVATA a runtime così:

  1. PRE-FILTRO metadata (solo per limitare i candidati, MAI come verdetto):
     source ∈ {document, email}. Può solo sovra-includere, mai escludere.
  2. VERDETTO per candidato, sul VETTORE MEMORIZZATO:
       v_stored  = vettore in HNSW (letto offline dal segmento)
       v_nudo    = embed( embedding_text(...) )            # testo da contratto
       v_pref    = embed( prefisso_storico + "\\n\\n" + doc )  # testo di allora
       cos(v_stored, v_nudo) ≈ 1  -> SANO
       cos(v_stored, v_pref) ≈ 1  -> VIOLATORE (target)
       nessuno dei due            -> IRRIPRODUCIBILE (riportato a parte, NON
                                     toccato; il censimento ne trovò zero)
     Ricostruzione ESATTA senza il testo originale: `chroma:document` = testo
     nudo troncato a 4000, e sia il ramo nudo sia quello prefissato passano per
     la stessa troncatura [:4000] di embedding._embed_ollama, quindi
     `(prefisso + "\\n\\n" + document)[:4000]` riproduce byte-per-byte la stringa
     embeddata di allora (verificato: cos=1.000000 sui violatori noti).

================================================================================
ORCHESTRAZIONE (di Emiliano — questo script NON tocca daemon/launchctl/backup)
================================================================================
Lo script fa SOLO il censimento (dry-run) o il re-embedding (execute). La
sequenza operativa attorno a --execute è dell'operatore:

    1. maintenance ON:   kill -USR1 <mcp_pid>           (crea maintenance.lock)
    2. backup:           bash scripts/maintenance/backup_db.sh
    3. re-embedding:     ./.venv/bin/python scripts/maintenance/reembed_prefixed.py \\
                             --execute --targets logs/reembed_prefixed_targets_<ts>.txt
    4. maintenance OFF:  rm maintenance.lock  (o meccanismo equivalente)
    5. persist durevole: sudo launchctl kickstart -k system/com.tailor.mcp

Vincoli tecnici:
  * --dry-run  : READ-ONLY. sqlite via mode=ro; vettori letti da uno SNAPSHOT
                 `cp -c` (APFS clone) del segmento in dir scratch. MAI
                 PersistentClient. Ollama e lo snapshot sono ammessi (sono il
                 costo del verdetto sul dato). ZERO scritture su db/.
  * --execute  : RIFIUTA di partire se il MCP non è in maintenance mode (stesso
                 meccanismo di repair_hnsw_index.py: file maintenance.lock).
                 Due PersistentClient simultanei = SIGSEGV Rust non catchabile:
                 per questo il gate è obbligatorio.
"""
from __future__ import annotations

import argparse
import os
import pickle
import shutil
import struct
import subprocess
import sqlite3
import sys
import time
from datetime import datetime, timezone

import numpy as np

# --- path setup (scripts/lib per embedding.py; scripts per il branch lib.*) ---
_HERE = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(_HERE))            # repo root
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "lib"))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts"))

from embedding import get_embeddings, info as embedding_info   # noqa: E402
from embedding_contract import embedding_text, MAX_EMBED_CHARS  # noqa: E402

# maintenance-mode gate: unica fonte, condivisa con repair_hnsw_index.py
from repair_hnsw_index import is_in_maintenance_mode, MAINTENANCE_LOCK  # noqa: E402

# ----------------------------------------------------------------------------
DB_DIR = os.path.join(BASE_DIR, "db")
SQLITE_PATH = os.path.join(DB_DIR, "chroma.sqlite3")
COLLECTION_NAME = "tailor_kb_v2"
LOGS_DIR = os.path.join(BASE_DIR, "logs")

# Pre-filtro: le uniche famiglie con prefissati (censimento 16/07).
CANDIDATE_SOURCES = ("document", "email")
# Coorte di controllo: prefissavano anch'esse, ma zero violatori attesi.
CONTROL_SOURCES = ("claude", "chatgpt", "gemini")

# Verdetto: match = coseno essenzialmente unitario (i sani danno 1.000000
# esatto in float32; i near-miss stanno ≤ 0.995 — gap enorme).
MATCH_COS = 0.99990

BLOCK = 256          # chunk per blocco (embedding batch + progress)
PROGRESS_EVERY = 20  # blocchi tra un progress e l'altro


# ============================================================================
# LETTURA HNSW OFFLINE (dal segmento vettoriale, senza PersistentClient)
# ============================================================================
class HnswReader:
    """Legge (embedding_id -> vettore) da uno snapshot del segmento HNSW.

    Formato chroma-hnswlib (fork): header.bin + data_level0.bin +
    index_metadata.pickle. I 4 campi utili dell'header stanno a offset fissi,
    validati contro la dimensione del file (fail-loud se il formato cambia).
    """

    def __init__(self, seg_dir: str):
        h = open(os.path.join(seg_dir, "header.bin"), "rb").read()
        self.cur_count = struct.unpack_from("<Q", h, 20)[0]
        self.size_per_el = struct.unpack_from("<Q", h, 28)[0]
        self.label_offset = struct.unpack_from("<Q", h, 36)[0]
        self.offset_data = struct.unpack_from("<Q", h, 44)[0]
        self.dim = (self.label_offset - self.offset_data) // 4

        d0 = os.path.join(seg_dir, "data_level0.bin")
        d0size = os.path.getsize(d0)
        if self.size_per_el * self.cur_count != d0size:
            raise RuntimeError(
                f"header.bin non coerente con data_level0.bin "
                f"({self.size_per_el}*{self.cur_count} != {d0size}); "
                f"formato chroma-hnswlib cambiato — verifica gli offset."
            )
        self.mm = np.memmap(d0, dtype=np.uint8, mode="r",
                            shape=(self.cur_count, self.size_per_el))
        labels = self.mm[:, self.label_offset:self.label_offset + 8]
        labels = labels.copy().view(np.uint64).ravel()
        self._label2int = {int(l): i for i, l in enumerate(labels)}

        m = pickle.load(open(os.path.join(seg_dir, "index_metadata.pickle"), "rb"))
        self._id_to_label = m["id_to_label"]
        self.live_ids = set(self._id_to_label.keys())

    def vector(self, embedding_id: str):
        lab = self._id_to_label.get(embedding_id)
        if lab is None:
            return None
        i = self._label2int.get(int(lab))
        if i is None:
            return None
        raw = self.mm[i, self.offset_data:self.offset_data + self.dim * 4].tobytes()
        return np.frombuffer(raw, dtype=np.float32)


# ============================================================================
# RICOSTRUZIONE DEL TESTO EMBEDDATO DI ALLORA (prefisso storico)
# ============================================================================
def reconstruct_prefixed(source: str, meta: dict, document: str) -> str | None:
    """Stringa embeddata dal writer PRE-contratto, ricostruita da metadata +
    documento nudo. Ritorna None se la famiglia non prefissava.

    Il troncamento [:MAX_EMBED_CHARS] replica embedding._embed_ollama.
    """
    if source == "document":
        # ingest_docs.py (vecchio ingest_chunks, rimosso in 9934a35)
        parts = []
        folder = meta.get("folder") or ""
        doc_type = meta.get("doc_type") or ""
        if folder:
            parts.append(f"Cartella: {folder}")
        if doc_type:
            parts.append(f"Tipo: {doc_type}")
        parts.append(f"File: {meta.get('title', '')}")
        prefix = " | ".join(parts)
    elif source == "email":
        # chunk_gmail.py:344 embed_prefix + ingest_gmail.py (rimosso in 9934a35)
        fa = meta.get("email_from") or ""
        ta = meta.get("email_to") or ""
        fs = (fa.split("<")[0].strip().strip('"') if "<" in fa else fa)[:50]
        ts = (ta.split("<")[0].strip().strip('"') if "<" in ta else ta)[:50]
        subj = meta.get("title") or ""
        if subj == "(nessun oggetto)":   # placeholder quando il subject era vuoto
            subj = ""
        prefix = f"Email | From: {fs} | To: {ts} | Subject: {subj[:100]}"
    elif source in CONTROL_SOURCES:
        # ingest_conversations.py / ingest_upload.py (rimosso in 9934a35)
        title = meta.get("title", "")
        date = meta.get("date") or ""
        prefix = f"Conversazione: {title}"
        if date:
            prefix += f" ({date})"
    else:
        return None
    return f"{prefix}\n\n{document}"[:MAX_EMBED_CHARS]


def contract_text(source: str, meta: dict, document: str) -> str:
    """Testo NUDO da contratto (quello che il vettore corretto deve embeddare)."""
    return embedding_text(source, meta, document)


def cos(a: np.ndarray, b: np.ndarray) -> float:
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-12))


# ============================================================================
# ACCESSO SQLITE READ-ONLY
# ============================================================================
def ro_connect() -> sqlite3.Connection:
    return sqlite3.connect(f"file:{SQLITE_PATH}?mode=ro&immutable=1", uri=True)


def vector_segment_dir(con: sqlite3.Connection) -> str:
    row = con.execute(
        "SELECT s.id FROM segments s JOIN collections c ON c.id = s.collection "
        "WHERE c.name = ? AND s.scope = 'VECTOR'", (COLLECTION_NAME,)).fetchone()
    if not row:
        raise RuntimeError(f"segmento VECTOR non trovato per '{COLLECTION_NAME}'")
    return os.path.join(DB_DIR, row[0])


# chiavi metadata necessarie alla ricostruzione + report
_META_KEYS = ("chroma:document", "source", "folder", "doc_type", "title",
              "email_from", "email_to", "date")


def load_candidates(con: sqlite3.Connection, sources) -> list[dict]:
    """Ritorna [{eid, rowid, created_at, meta{...}}] per i source dati."""
    ph = ",".join("?" * len(sources))
    rows = con.execute(
        f"SELECT e.embedding_id, e.id, e.created_at "
        f"FROM embeddings e JOIN embedding_metadata mm "
        f"  ON mm.id = e.id AND mm.key = 'source' "
        f"WHERE mm.string_value IN ({ph})", tuple(sources)).fetchall()
    by_rowid = {rid: {"eid": eid, "rowid": rid, "created_at": ca, "meta": {}}
                for eid, rid, ca in rows}
    if not by_rowid:
        return []
    # bulk-load delle chiavi necessarie
    rowids = list(by_rowid.keys())
    kph = ",".join("?" * len(_META_KEYS))
    for chunk_start in range(0, len(rowids), 900):
        block = rowids[chunk_start:chunk_start + 900]
        iph = ",".join("?" * len(block))
        q = (f"SELECT id, key, string_value FROM embedding_metadata "
             f"WHERE key IN ({kph}) AND id IN ({iph})")
        for rid, key, sv in con.execute(q, (*_META_KEYS, *block)):
            by_rowid[rid]["meta"][key] = sv
    return list(by_rowid.values())


# ============================================================================
# CLASSIFICAZIONE (verdetto vettoriale)
# ============================================================================
def classify_block(cands: list[dict], reader: HnswReader) -> list[dict]:
    """Ritorna, per ciascun candidato, un dict con verdict e cosine.

    verdict ∈ {healthy, violator, irreproducible, no_hnsw}.
    Ottimizzazione: embedda prima il NUDO (maggioranza sana -> 1 embed); solo i
    non-match ricevono anche l'embed PREFISSATO.
    """
    docs = [c["meta"].get("chroma:document", "") or "" for c in cands]
    nude_texts = [contract_text(c["meta"].get("source", ""), c["meta"], d)
                  for c, d in zip(cands, docs)]
    e_nude = get_embeddings(nude_texts)   # fail-loud: eccezione propagata

    results = []
    pending = []   # indici che serve ri-testare col prefisso
    for idx, c in enumerate(cands):
        sv = reader.vector(c["eid"])
        if sv is None:
            results.append({**c, "verdict": "no_hnsw", "cos_nude": None,
                            "cos_pref": None})
            continue
        cn = cos(sv, np.asarray(e_nude[idx], dtype=np.float32))
        r = {**c, "verdict": None, "cos_nude": cn, "cos_pref": None, "_sv": sv}
        results.append(r)
        if cn >= MATCH_COS:
            r["verdict"] = "healthy"
        else:
            pending.append(idx)

    if pending:
        pref_texts = []
        valid = []
        for idx in pending:
            c = results[idx]
            pref = reconstruct_prefixed(c["meta"].get("source", ""), c["meta"],
                                        c["meta"].get("chroma:document", "") or "")
            if pref is None:
                c["verdict"] = "irreproducible"
                continue
            pref_texts.append(pref)
            valid.append(idx)
        if pref_texts:
            e_pref = get_embeddings(pref_texts)
            for k, idx in enumerate(valid):
                c = results[idx]
                cp = cos(c["_sv"], np.asarray(e_pref[k], dtype=np.float32))
                c["cos_pref"] = cp
                if cp >= MATCH_COS and cp > c["cos_nude"]:
                    c["verdict"] = "violator"
                else:
                    c["verdict"] = "irreproducible"
    for r in results:
        r.pop("_sv", None)
    return results


# ============================================================================
# DRY-RUN
# ============================================================================
def snapshot_segment(seg_dir: str, work_root: str) -> str:
    """APFS clone (`cp -c`) del segmento in dir scratch; fallback copytree."""
    dst = os.path.join(work_root, "seg_snap")
    if os.path.exists(dst):
        shutil.rmtree(dst)
    os.makedirs(dst, exist_ok=True)
    try:
        for name in os.listdir(seg_dir):
            subprocess.run(["cp", "-c", os.path.join(seg_dir, name), dst],
                           check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        shutil.rmtree(dst)
        shutil.copytree(seg_dir, dst)
    return dst


def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def dry_run(work_root: str, include_control: bool) -> int:
    print("=" * 78)
    print("REEMBED PREFIXED — DRY-RUN (READ-ONLY, zero scritture su db/)")
    print("=" * 78)
    print(f"[embed] {embedding_info()}")
    con = ro_connect()
    seg_dir = vector_segment_dir(con)
    print(f"[seg]   segmento vettoriale: {os.path.basename(seg_dir)}")

    snap = snapshot_segment(seg_dir, work_root)
    print(f"[snap]  clone in {snap}")
    reader = HnswReader(snap)
    print(f"[hnsw]  count={reader.cur_count} dim={reader.dim} "
          f"live_ids={len(reader.live_ids)}")

    sources = list(CANDIDATE_SOURCES) + (list(CONTROL_SOURCES) if include_control else [])
    cands = load_candidates(con, sources)
    con.close()
    # ordina per created_at per un progress leggibile
    cands.sort(key=lambda c: (c["meta"].get("source", ""), c["created_at"] or ""))
    total = len(cands)
    print(f"[cand]  {total} candidati (pre-filtro source ∈ {sources})")

    t0 = time.time()
    all_results = []
    for bi, start in enumerate(range(0, total, BLOCK)):
        block = cands[start:start + BLOCK]
        all_results.extend(classify_block(block, reader))
        done = min(start + BLOCK, total)
        if bi % PROGRESS_EVERY == 0 or done == total:
            el = time.time() - t0
            rate = done / el if el else 0
            eta = (total - done) / rate if rate else 0
            v = sum(1 for r in all_results if r["verdict"] == "violator")
            print(f"  ...{done}/{total} ({100*done/total:.1f}%) "
                  f"viol={v} {rate:.0f} chunk/s ETA {eta/60:.1f}m", flush=True)

    # --- aggregazione ---
    viol = [r for r in all_results if r["verdict"] == "violator"]
    healthy = [r for r in all_results if r["verdict"] == "healthy"]
    irrip = [r for r in all_results if r["verdict"] == "irreproducible"]
    nohnsw = [r for r in all_results if r["verdict"] == "no_hnsw"]

    # target file
    os.makedirs(LOGS_DIR, exist_ok=True)
    ts = _now_ts()
    targets_path = os.path.join(LOGS_DIR, f"reembed_prefixed_targets_{ts}.txt")
    cps = sorted(r["cos_pref"] for r in viol if r["cos_pref"] is not None)
    cos_line = (f"cos_pref target: min={cps[0]:.6f} max={cps[-1]:.6f} "
                f"(n={len(cps)})" if cps else "cos_pref target: n/d (0 target)")
    with open(targets_path, "w") as f:
        f.write(f"# reembed_prefixed targets — {ts}\n")
        f.write(f"# criterio: empirico vettoriale (cos_pref>={MATCH_COS})\n")
        f.write(f"# collection: {COLLECTION_NAME}\n")
        f.write(f"# {cos_line}\n")
        for r in sorted(viol, key=lambda r: r["eid"]):
            f.write(f"{r['eid']}\n")

    # --- report ---
    print("\n" + "=" * 78)
    print("REPORT")
    print("=" * 78)
    print(f"VIOLATORI (target)   : {len(viol)}")
    print(f"SANI                 : {len(healthy)}")
    print(f"IRRIPRODUCIBILI      : {len(irrip)}   (attesi ~0; se >0 è un dato)")
    print(f"NO-HNSW (SQL-only)   : {len(nohnsw)}")
    print(f"{cos_line}")
    print(f"target file          : {targets_path}")

    def brk(rows):
        d = {}
        for r in rows:
            d[r["meta"].get("source", "?")] = d.get(r["meta"].get("source", "?"), 0) + 1
        return d
    print(f"\nbreakdown violatori per source : {brk(viol)}")
    print(f"breakdown irriprod. per source : {brk(irrip)}")
    if include_control:
        cviol = [r for r in viol if r["meta"].get("source") in CONTROL_SOURCES]
        print(f"coorte CONTROLLO {CONTROL_SOURCES}: violatori={len(cviol)} "
              f"(atteso 0)")

    # finestra created_at dei violatori (cross-check finestra prefissi)
    if viol:
        cas = sorted(r["created_at"] for r in viol if r["created_at"])
        print(f"\ncreated_at violatori: min={cas[0]}  max={cas[-1]}")

    # sanity vs censimento 16/07
    print(f"\nSANITY: atteso ~2983 meno i cancellati (reconciler oggi ~281 delete)"
          f" -> ~2702 in giù. Trovati: {len(viol)}")

    # 5 esempi: testo-attuale-embeddato (prefissato) vs testo-da-contratto (nudo)
    print("\n" + "-" * 78)
    print("5 ESEMPI — testo ATTUALMENTE embeddato (prefissato) vs da CONTRATTO (nudo)")
    print("-" * 78)
    for r in viol[:5]:
        src = r["meta"].get("source", "")
        doc = r["meta"].get("chroma:document", "") or ""
        pref = reconstruct_prefixed(src, r["meta"], doc)
        nude = contract_text(src, r["meta"], doc)
        print(f"\n• {r['eid']}  [{src}]  created={r['created_at']}")
        print(f"  cos_nude={r['cos_nude']:.6f}  cos_pref={r['cos_pref']:.6f}")
        print(f"  ATTUALE (embeddato, prefissato):\n    {pref[:220]!r}")
        print(f"  CONTRATTO (nudo, corretto)     :\n    {nude[:220]!r}")

    print("\n[dry-run] nessuna scrittura su db/. Fine.")
    return 0


# ============================================================================
# EXECUTE
# ============================================================================
class MaintenanceLost(RuntimeError):
    """maintenance.lock sparito a metà run -> abort immediato.

    Il lock è verificato non solo all'avvio ma PRIMA di ogni lettura e PRIMA di
    ogni scrittura: se il MCP riparte (o qualcuno rimuove il lock) a metà run,
    continuare significherebbe due PersistentClient concorrenti = SIGSEGV Rust.
    Il checkpoint .done rende il resume sicuro dopo l'abort.
    """


def _assert_maintenance(stage: str) -> None:
    if not is_in_maintenance_mode():
        raise MaintenanceLost(stage)


def _targets_cos_header(targets_path: str) -> str | None:
    """Rilegge la riga '# cos_pref ...' scritta dal dry-run, se presente."""
    with open(targets_path) as f:
        for line in f:
            if line.startswith("# cos_pref"):
                return line[1:].strip()
            if not line.startswith("#"):
                break
    return None


def execute(targets_path: str, work_root: str) -> int:
    # 1. GATE maintenance mode — obbligatorio (due PersistentClient = SIGSEGV)
    if not is_in_maintenance_mode():
        sys.stderr.write(
            f"\nRIFIUTO --execute: MCP NON in maintenance mode "
            f"(nessun lock in {MAINTENANCE_LOCK}).\n"
            f"  Entra in maintenance: kill -USR1 <mcp_pid>\n"
            f"  Poi backup: bash scripts/maintenance/backup_db.sh\n")
        return 2

    if not targets_path or not os.path.exists(targets_path):
        sys.stderr.write(f"\nRIFIUTO: --targets mancante o inesistente: "
                         f"{targets_path}\n")
        return 2

    targets = []
    with open(targets_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                targets.append(line)
    print(f"[execute] {len(targets)} target da {targets_path}")
    _cos_hdr = _targets_cos_header(targets_path)
    print(f"[execute] {_cos_hdr}" if _cos_hdr
          else "[execute] cos_pref target: n/d (targets file pre-fix)")

    # checkpoint/resume
    ckpt_path = targets_path + ".done"
    done = set()
    if os.path.exists(ckpt_path):
        with open(ckpt_path) as f:
            done = {l.strip() for l in f if l.strip()}
        print(f"[resume] {len(done)} già fatti, salto")
    todo = [t for t in targets if t not in done]
    print(f"[execute] {len(todo)} da processare")

    # PersistentClient SOLO qui, SOLO sotto maintenance mode
    import chromadb
    client = chromadb.PersistentClient(path=DB_DIR)
    coll = client.get_collection(COLLECTION_NAME)

    ok = skipped = 0
    ckpt = open(ckpt_path, "a")
    t0 = time.time()
    try:
        for start in range(0, len(todo), BLOCK):
            block = todo[start:start + BLOCK]
            _assert_maintenance(f"prima di coll.get (offset {start})")
            got = coll.get(ids=block, include=["documents", "metadatas"])
            gid = {i: (d, m) for i, d, m in
                   zip(got["ids"], got["documents"], got["metadatas"])}
            ids, embeds, documents, metadatas = [], [], [], []
            for eid in block:
                if eid not in gid:
                    skipped += 1
                    continue
                doc, meta = gid[eid]
                # embed sul testo da CONTRATTO; ma documento e metadata
                # riscritti VERBATIM come riletti da questo stesso coll.get:
                # l'upsert cambia SOLO gli embeddings, nessun campo omesso.
                nude = contract_text((meta or {}).get("source", ""), meta or {},
                                     doc or "")
                ids.append(eid)
                documents.append(doc)
                metadatas.append(meta)
                embeds.append(nude)
            if not ids:
                continue
            vecs = get_embeddings(embeds)            # fail-loud
            _assert_maintenance(f"prima di coll.upsert (offset {start})")
            coll.upsert(ids=ids, embeddings=vecs, documents=documents,
                        metadatas=metadatas)         # fail-loud
            for eid in ids:
                ckpt.write(eid + "\n")
            ckpt.flush()
            os.fsync(ckpt.fileno())
            ok += len(ids)
            el = time.time() - t0
            print(f"  ...ok={ok} skipped={skipped} {ok/el:.0f}/s", flush=True)
    except MaintenanceLost as e:
        sys.stderr.write(
            f"\nABORT: maintenance.lock sparito ({e}).\n"
            f"  lock atteso in {MAINTENANCE_LOCK}\n"
            f"  processati fin qui: ok={ok} skipped={skipped} "
            f"(checkpoint {ckpt_path})\n"
            f"  rientra in maintenance (kill -USR1 <mcp_pid>) e rilancia lo "
            f"stesso comando: il resume riparte da qui.\n")
        return 3
    finally:
        ckpt.close()

    print(f"\n[execute] SUMMARY ok={ok} skipped={skipped}")
    print("[execute] persist durevole: kickstart -k del MCP (operatore).")
    return 0


# ============================================================================
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", default=True,
                   help="censimento READ-ONLY (default)")
    g.add_argument("--execute", action="store_true",
                   help="ri-embedding (richiede maintenance mode)")
    ap.add_argument("--targets", help="file target per --execute")
    ap.add_argument("--include-control", action="store_true",
                    help="dry-run: scandisci anche claude/chatgpt/gemini "
                         "come coorte di controllo (atteso 0 violatori)")
    ap.add_argument("--work-dir",
                    default=os.environ.get("REEMBED_WORK_DIR", "/tmp/reembed_prefixed"),
                    help="dir scratch per lo snapshot del segmento")
    args = ap.parse_args()

    os.makedirs(args.work_dir, exist_ok=True)
    if args.execute:
        return execute(args.targets, args.work_dir)
    return dry_run(args.work_dir, include_control=args.include_control)


if __name__ == "__main__":
    sys.exit(main())
