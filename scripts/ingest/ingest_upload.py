#!/usr/bin/env python3
"""TAILOR — Upload Ingest Worker. Background process for uploaded conversation files."""
import json, os, sys, time, zipfile, shutil, argparse, importlib, chromadb
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "lib"))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "ingest"))
from config import get as cfg, load_config
from embedding import get_embeddings

DB_DIR = os.path.join(BASE_DIR, "db")
COLLECTION_NAME = cfg("kb", "collection") or "tailor_kb"
BATCH_SIZE = 10
MAX_TEXT_CHARS = 4000
TARGET_CHUNK_CHARS = 1200
OVERLAP_CHARS = 200

def update_job(job_id, **kw):
    jf = os.path.join(BASE_DIR, "data", "uploads", "jobs", f"{job_id}.json")
    try:
        with open(jf) as f: d = json.load(f)
        d.update(kw)
        with open(jf, "w") as f: json.dump(d, f)
    except Exception as e: print(f"Job update warn: {e}")

def split_text(text, mx):
    if len(text) <= mx: return [text]
    segs = []; rem = text
    while len(rem) > mx:
        ch = rem[:mx]; cut = -1
        for sep, off in [("\n\n",2),("\n",1),(". ",2),(" ",1)]:
            p = ch.rfind(sep)
            if p > mx*0.3: cut = p+off; break
        if cut == -1: cut = mx
        segs.append(rem[:cut].rstrip()); rem = rem[cut:].lstrip()
    if rem.strip(): segs.append(rem.strip())
    return segs

def chunk_record(rec):
    rid=rec.get("id",""); title=rec.get("title","Untitled")
    text=rec.get("text",""); ct=rec.get("create_time",0)
    ds=rec.get("date",""); src=rec.get("source","unknown")
    if not text.strip(): return []
    parts=split_text(text, TARGET_CHUNK_CHARS); chunks=[]; prev=""
    for i,part in enumerate(parts):
        if prev and OVERLAP_CHARS>0:
            ov=prev[-OVERLAP_CHARS:].strip(); sp=ov.find(" ")
            if sp>0: ov=ov[sp+1:]
            ft=f"[...contesto precedente:] {ov}\n\n{part}"
        else: ft=part
        cid=f"{rid}_chunk_{i:04d}"
        if not ds and ct:
            try: ds=datetime.fromtimestamp(ct).strftime("%Y-%m-%d")
            except Exception: ds=""
        chunks.append({"chunk_id":cid,"conv_id":rid,"title":title,"date":ds,
            "create_time":ct,"chunk_index":i,"text":ft,"char_count":len(ft),"source":src})
        prev=part
    return chunks

def prepare_upload(stype, fpath):
    import glob as _g
    is_zip = fpath.lower().endswith(".zip")
    if not is_zip:
        try: is_zip = zipfile.is_zipfile(fpath)
        except Exception: is_zip = False
    if is_zip:
        exdir = fpath + "_extracted"
        os.makedirs(exdir, exist_ok=True)
        print(f"  Extracting ZIP...")
        with zipfile.ZipFile(fpath, "r") as zf: zf.extractall(exdir)
        contents = [c for c in os.listdir(exdir) if not c.startswith(".") and c != "__MACOSX"]
        adir = os.path.join(exdir, contents[0]) if len(contents)==1 and os.path.isdir(os.path.join(exdir, contents[0])) else exdir
        if stype == "chatgpt":
            jf = _g.glob(os.path.join(adir, "conversations*.json"))
            if not jf: jf = _g.glob(os.path.join(adir, "*", "conversations*.json"))
            if not jf: raise FileNotFoundError("No conversations*.json in ZIP")
            return {"name":stype,"parser":stype,"source_files":[os.path.basename(f) for f in jf],"enabled":True}, os.path.dirname(jf[0]), exdir
        else:
            return {"name":stype,"parser":stype,"source_dir":adir,"enabled":True}, adir, exdir
    else:
        if stype == "chatgpt":
            return {"name":stype,"parser":stype,"source_files":[os.path.basename(fpath)],"enabled":True}, os.path.dirname(fpath), None
        else:
            td = fpath + "_dir"; os.makedirs(td, exist_ok=True)
            dst = os.path.join(td, os.path.basename(fpath))
            if not os.path.exists(dst): shutil.copy2(fpath, dst)
            return {"name":stype,"parser":stype,"source_dir":td,"enabled":True}, td, td

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--file", required=True)
    ap.add_argument("--job", required=True)
    args = ap.parse_args()
    jid=args.job; stype=args.source; fpath=args.file; cdir=None
    print(f"[{datetime.now().isoformat()}] Upload ingest: {stype} | {fpath} | {jid}")
    try:
        update_job(jid, status="running", message="Preparing...")
        scfg, ddir, cdir = prepare_upload(stype, fpath)
        update_job(jid, message="Parsing...")
        load_config()
        try: pmod = importlib.import_module(f"parsers.{stype}")
        except ImportError as e:
            update_job(jid, status="error", message=f"Parser error: {e}"); return
        recs = pmod.parse(scfg, ddir)
        print(f"  Parsed: {len(recs)} records")
        update_job(jid, records=len(recs), message=f"{len(recs)} records. Chunking...")
        if not recs: update_job(jid, status="done", message="No records found", chunks=0); return
        achunks = []
        for r in recs: achunks.extend(chunk_record(r))
        print(f"  Chunked: {len(achunks)} chunks")
        update_job(jid, chunks=len(achunks), message=f"{len(achunks)} chunks. Embedding...")
        if not achunks: update_job(jid, status="done", message="No chunks"); return
        cl = chromadb.PersistentClient(path=DB_DIR)
        col = cl.get_collection(name=COLLECTION_NAME)
        total=len(achunks); proc=0; errs=0; t0=time.time()
        for bs in range(0, total, BATCH_SIZE):
            batch = achunks[bs:bs+BATCH_SIZE]
            txts = [f"Conversazione: {c['title']}" + (f" ({c['date']})" if c.get('date') else "") + f"\n\n{c['text']}" for c in batch]
            txts = [t[:MAX_TEXT_CHARS] for t in txts]
            try: embs = get_embeddings(txts)
            except Exception as e: print(f"  Emb err: {e}"); errs+=len(batch); continue
            ids=[c["chunk_id"] for c in batch]
            docs=[c["text"][:MAX_TEXT_CHARS] for c in batch]
            metas=[{"conv_id":c.get("conv_id",""),"title":c.get("title",""),"date":c.get("date",""),
                "create_time":c.get("create_time",0),"default_model":c.get("source",""),
                "chunk_index":c.get("chunk_index",0),"char_count":c.get("char_count",0),
                "turn_count":0,"source":c.get("source","unknown")} for c in batch]
            try: col.upsert(ids=ids, embeddings=embs, documents=docs, metadatas=metas)
            except Exception as e: print(f"  DB err: {e}"); errs+=len(batch); continue
            proc += len(batch)
            if proc % 50 < BATCH_SIZE:
                el=time.time()-t0; rate=proc/el if el>0 else 0
                eta=int((total-proc)/rate) if rate>0 else 0; pct=int(proc*100/total)
                update_job(jid, chunks_processed=proc, progress_pct=pct, rate=round(rate,1),
                    eta_seconds=eta, errors=errs, message=f"Ingesting... {proc}/{total} ({pct}%)")
        el=time.time()-t0; fc=col.count()
        update_job(jid, status="done", chunks_processed=proc, progress_pct=100, errors=errs,
            elapsed_seconds=round(el,1), kb_total=fc,
            message=f"Done! {proc} chunks in {el:.0f}s. KB: {fc:,}")
        print(f"  DONE: {proc} chunks, {errs} errors, KB: {fc:,}")
    except Exception as e:
        import traceback; print(f"  FATAL: {e}\n{traceback.format_exc()}")
        update_job(jid, status="error", message=f"Error: {str(e)[:500]}")
    finally:
        if cdir and os.path.exists(cdir):
            try: shutil.rmtree(cdir); print(f"  Cleaned {cdir}")
            except Exception: pass

if __name__ == "__main__":
    main()
