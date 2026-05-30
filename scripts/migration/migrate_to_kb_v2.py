"""Migrate tailor_kb → tailor_kb_v2 (Opzione C, 2026-05-24).

src = tailor_kb (corrupt write path, read OK)
dst = tailor_kb_v2 (fresh collection)

Idempotente: salva progress su /tmp/migrate_kb_v2_progress.json, ri-eseguibile.
Pre-flight sanity: upsert+get+delete su dst per confermare non sia silent-drop.
Per-batch sanity: dst.count() vs offset ogni 5000 chunks.

Esecuzione: ~/tailor/.venv/bin/python ~/tailor/scripts/migration/migrate_to_kb_v2.py
"""
import chromadb, json, time, os, sys

TAILOR_HOME = os.environ.get("TAILOR_HOME", os.path.expanduser("~/tailor"))
DB = f"{TAILOR_HOME}/db"
SRC_NAME = "tailor_kb"
DST_NAME = "tailor_kb_v2"
BATCH = 500
PROGRESS_FILE = "/tmp/migrate_kb_v2_progress.json"
LOG_FILE = f"{TAILOR_HOME}/logs/migration_kb_v2_{time.strftime('%Y%m%d_%H%M%S')}.log"

def log(msg):
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            return json.load(open(PROGRESS_FILE))
        except Exception:
            pass
    return {"offset": 0, "copied": 0, "errors": 0}

def save_progress(state):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(state, f)

log("=== migrate_to_kb_v2.py START ===")
log(f"chromadb {chromadb.__version__} | DB={DB}")
log(f"src={SRC_NAME}  dst={DST_NAME}  batch={BATCH}")

client = chromadb.PersistentClient(path=DB)
src = client.get_collection(SRC_NAME)
total = src.count()
log(f"src.count() = {total}")

existing = [c.name for c in client.list_collections()]
log(f"existing collections: {existing}")
dst = client.get_or_create_collection(name=DST_NAME, metadata={"hnsw:space": "cosine"})
log(f"dst.count() pre-migration = {dst.count()}")

# === Pre-flight sanity check ===
sanity_id = f"_migration_sanity_{int(time.time())}"
sanity_emb = [0.01] * 768
log(f"sanity check on dst: upsert+get id={sanity_id}")
dst.upsert(ids=[sanity_id], embeddings=[sanity_emb],
           documents=["sanity"], metadatas=[{"_sanity": "True"}])
got = dst.get(ids=[sanity_id])
if not got.get("ids"):
    log("FATAL: dst collection SILENT-DROP — bug è chromadb-wide, non solo tailor_kb. Aborting.")
    sys.exit(2)
log(f"sanity OK: dst.get returned {got['ids']}")
try:
    dst.delete(ids=[sanity_id])
    log("sanity cleaned up")
except Exception as e:
    log(f"warn: sanity cleanup failed (non-fatal): {e}")

# === Resume logic ===
state = load_progress()
log(f"resuming from offset={state['offset']} (copied={state['copied']} errors={state['errors']})")

# === Migration loop ===
start_t = time.time()
last_progress_t = start_t
log(f"start migration loop, target={total}")

while state["offset"] < total:
    batch_t = time.time()
    try:
        batch = src.get(
            limit=BATCH,
            offset=state["offset"],
            include=["embeddings", "documents", "metadatas"]
        )
    except Exception as e:
        log(f"FATAL src.get error at offset={state['offset']}: {type(e).__name__}: {e}")
        save_progress(state)
        sys.exit(3)

    ids = batch.get("ids") or []
    if not ids:
        log(f"src returned 0 ids at offset={state['offset']}, breaking loop")
        break

    embs = batch.get("embeddings")
    docs = batch.get("documents")
    metas = batch.get("metadatas")

    try:
        dst.upsert(ids=ids, embeddings=embs, documents=docs, metadatas=metas)
    except Exception as e:
        log(f"FATAL dst.upsert error at offset={state['offset']}: {type(e).__name__}: {e}")
        save_progress(state)
        sys.exit(4)

    state["offset"] += len(ids)
    state["copied"] += len(ids)
    save_progress(state)

    elapsed = time.time() - batch_t
    total_elapsed = time.time() - start_t

    # progress log every batch
    if time.time() - last_progress_t > 5 or state["offset"] >= total:
        rate = state["copied"] / max(total_elapsed, 0.1)
        eta = (total - state["offset"]) / max(rate, 0.1)
        log(f"  offset={state['offset']}/{total} ({100*state['offset']/total:.1f}%) "
            f"batch_t={elapsed:.1f}s rate={rate:.0f}/s ETA={eta:.0f}s")
        last_progress_t = time.time()

    # integrity check every 5000
    if (state["offset"] // BATCH) % 10 == 0:
        dc = dst.count()
        if dc < state["offset"] - 50:
            log(f"  WARN: dst.count()={dc} far below offset={state['offset']} (drift {state['offset']-dc})")
        else:
            log(f"  integrity: dst.count()={dc} ~= offset={state['offset']}")

# === Final report ===
log("migration loop done")
final_dst = dst.count()
log(f"FINAL: dst.count()={final_dst}  src.count()={total}  copied={state['copied']}  errors={state['errors']}")
log(f"total time: {time.time()-start_t:.0f}s")

if state["copied"] == total and state["errors"] == 0 and final_dst >= total - 10:
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)
    log("progress file removed (clean success)")
    log("=== migrate_to_kb_v2.py END (SUCCESS) ===")
    sys.exit(0)
else:
    log("=== migrate_to_kb_v2.py END (with discrepancies) ===")
    sys.exit(1)
