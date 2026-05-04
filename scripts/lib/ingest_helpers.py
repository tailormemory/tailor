"""TAILOR — post-ingest HNSW verifier helper.

Wraps `collection.upsert()` with a vector-path read-back that detects
the chromadb 1.x SQL-vs-HNSW persist drift documented in
`scripts/maintenance/repair_hnsw_index.md`.

The verifier is the write-time complement to that audit/repair tool:
when drift is detected, the suspect ids are appended to
`logs/hnsw_audits/pending_<run_id>.json` and a row is inserted into the
`maintenance_log` SQLite table. The next `repair_hnsw_index.py` run
picks them up and re-embeds. Ingest continues; the per-script `errors`
counter is incremented by the caller. No hard fail, no inline alerts.

v1.2.3.1: before recording drift, suspect ids are checked against
chromadb's `embeddings_queue` table. Ids still pending in the queue are
vector-consumer lag (transient), not permanent drift, and are
suppressed from pending JSON / maintenance_log. The maintenance_log
payload's `drift_summary.transient_count` records how many suspects
were filtered out for observability.

Upstream issue: chroma-core/chroma#6975 (observed on chromadb==1.5.5).
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
from typing import Any, Iterable, Sequence

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "db", "chroma.sqlite3")
PENDING_DIR = os.path.join(BASE_DIR, "logs", "hnsw_audits")

VERIFIER_RETRY_DELAY = 0.5  # seconds between first check and retry
MAINTENANCE_LOG_DRIFT_ID_CAP = 50  # cap drift_ids in the JSON payload to keep rows small


def make_run_id(source: str) -> str:
    """Conventional run id: `<source>_YYYYMMDD_HHMMSS_<pid>`.

    PID suffix avoids collision when two ingests of the same source start
    in the same second (parallel runs, manual back-to-back launches, tests).
    """
    return f"{source}_{time.strftime('%Y%m%d_%H%M%S')}_{os.getpid()}"


def _vector_path_drift(
    collection: Any,
    ids: Sequence[str],
    embeddings: Sequence[Sequence[float]],
) -> list[str]:
    """Return the subset of `ids` whose HNSW top-1 self-match is not itself.

    For each just-upserted chunk, query with its own embedding and check the
    top-1 id matches. If HNSW received the write, the chunk dominates its
    own neighborhood (cosine distance ~0). If not, the top-1 is some other
    chunk and we have drift between SQL metadata and the HNSW pickle.

    Uses a single batched `query()` call (cheaper than per-chunk queries on
    the live 155k-chunk collection — ~14ms vs ~19ms for batch=10).
    """
    if not ids:
        return []
    result = collection.query(
        query_embeddings=list(embeddings),
        n_results=1,
        include=[],
    )
    returned = result.get("ids") or []
    drift: list[str] = []
    for i, expected in enumerate(ids):
        try:
            top = returned[i][0] if returned[i] else None
        except (IndexError, TypeError):
            top = None
        if top != expected:
            drift.append(expected)
    return drift


def _check_queue_membership(db_path: str, ids: Sequence[str]) -> set[str]:
    """Return the subset of `ids` currently present in chromadb's
    `embeddings_queue` table.

    These ids are writes the vector segment has not yet consumed —
    so a verifier observation of "missing from HNSW" is expected lag,
    not permanent drift.

    Read-only against the chromadb sqlite. On any sqlite error returns
    an empty set (degrades to pre-v1.2.3.1 behavior: caller treats all
    drift as permanent)."""
    if not ids:
        return set()
    try:
        uri = f"file:{db_path}?mode=ro"
        con = sqlite3.connect(uri, uri=True)
        try:
            placeholders = ",".join("?" * len(ids))
            cur = con.execute(
                f"SELECT id FROM embeddings_queue WHERE id IN ({placeholders})",
                list(ids),
            )
            return {row[0] for row in cur.fetchall()}
        finally:
            con.close()
    except Exception:
        return set()


def _record_pending_drift(
    run_id: str,
    source: str,
    drift_ids: Sequence[str],
    pending_dir: str | None = None,
) -> str:
    """Append the drifted ids to logs/hnsw_audits/pending_<run_id>.json.

    File is created on first call within the run; subsequent calls append
    to the `entries` list so a single run can record drift from multiple
    batches without clobbering."""
    target_dir = pending_dir if pending_dir is not None else PENDING_DIR
    os.makedirs(target_dir, exist_ok=True)
    path = os.path.join(target_dir, f"pending_{run_id}.json")
    if os.path.exists(path):
        with open(path) as f:
            data = json.load(f)
    else:
        data = {"run_id": run_id, "created": int(time.time()), "entries": []}
    data["entries"].append({
        "source": source,
        "timestamp": int(time.time()),
        "ids": list(drift_ids),
    })
    data["last_updated"] = int(time.time())
    with open(path, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    return path


def _record_maintenance_log(
    db_path: str,
    run_id: str,
    source: str,
    drift_ids: Sequence[str],
    *,
    transient_count: int = 0,
) -> bool:
    """Insert one row into maintenance_log. Mirrors the convention from
    `repair_hnsw_index.py:_ensure_maintenance_log_row`. Best-effort: failure
    here must not propagate (drift logging is observability, not the fix)."""
    try:
        con = sqlite3.connect(db_path)
        try:
            cur = con.cursor()
            cur.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM maintenance_log")
            next_id = cur.fetchone()[0]
            drift_total = len(drift_ids)
            capped = list(drift_ids)[:MAINTENANCE_LOG_DRIFT_ID_CAP]
            payload = {
                "tool": "ingest_helpers.py",
                "mode": "verify",
                "run_id": run_id,
                "source": source,
                "drift_summary": {
                    "drift_total": drift_total,
                    "drift_ids_in_payload": len(capped),
                    "drift_ids_capped_at": MAINTENANCE_LOG_DRIFT_ID_CAP,
                    "transient_count": transient_count,
                },
                "action_taken": "logged for next audit",
                "drift_ids": capped,
            }
            cur.execute(
                "INSERT INTO maintenance_log (id, timestamp, operation) VALUES (?, ?, ?)",
                (next_id, int(time.time()), json.dumps(payload, sort_keys=True)),
            )
            con.commit()
        finally:
            con.close()
        return True
    except Exception as e:
        # Pending JSON is the source of truth for recovery; the maintenance_log
        # row is the surfacing path for the dashboard / end-of-run telegram.
        # Stay non-fatal but make the failure visible immediately rather than
        # discovered later when the dashboard shows zero drift entries.
        print(
            f"[ingest_helpers] WARN: maintenance_log write failed for run_id={run_id}: {e}. "
            f"Drift recorded in pending JSON only.",
            file=sys.stderr,
        )
        return False


def verified_upsert(
    collection: Any,
    ids: Sequence[str],
    embeddings: Sequence[Sequence[float]],
    documents: Sequence[str],
    metadatas: Sequence[dict],
    run_id: str,
    source: str,
    *,
    db_path: str | None = None,
    pending_dir: str | None = None,
    retry_delay: float = VERIFIER_RETRY_DELAY,
) -> bool:
    """Upsert into ChromaDB and verify all ids are reachable via HNSW.

    Returns True on clean upsert (verifier passed, possibly after a single
    retry that handled vector-consumer lag, or after the v1.2.3.1
    queue-membership check classified all drift as transient). Returns
    False on persistent drift, after recording the suspect ids for the
    next audit run. Does not raise: the caller's existing error-handling
    path (increment counter, continue loop) stays intact.

    `run_id` is the caller-supplied identifier shared across all batches in
    one ingest run; use `make_run_id(source)` at the top of the script.
    """
    id_list = list(ids)
    collection.upsert(
        ids=id_list,
        embeddings=list(embeddings),
        documents=list(documents),
        metadatas=list(metadatas),
    )
    # v1.2.6.1: chromadb 1.5.5 SIGSEGVs in collection.query() when called
    # immediately after upsert() of fresh ids (chroma-core/chroma#6975).
    # The kill is at the C/Rust level — uncatchable from Python — and takes
    # the whole ingest process with it, losing buffered stdout and the final
    # save_registry() call. Skip the inline drift verifier; nightly
    # repair_hnsw_index.py (--apply) is the canonical drift detector per
    # this module's docstring. Re-enable once chromadb is pinned past 1.5.5.
    #
    # v1.2.6.3: chromadb 1.5.8 introduced a NEW silent-drop mode unrelated
    # to the 1.5.5 SIGSEGV: bindings.upsert() returns True even when nothing
    # is persisted (collection in "frozen" state — repro: audit forense BIS
    # 2026-05-04). The previous return-True-unconditional left this drop
    # invisible. Replace it with a cheap post-upsert get() of the first id
    # in the batch: if the just-upserted id is not readable, the entire
    # batch is treated as failed (caller increments errors, surfacing the
    # condition in the nightly log + Telegram). This is a safer substitute
    # for _vector_path_drift (which still SIGSEGVs on 1.5.5) — get() is
    # the simplest read path that doesn't trigger the Rust query() bug.
    if id_list:
        try:
            sample_id = id_list[0]
            result = collection.get(ids=[sample_id])
            if not result.get("ids") or sample_id not in result["ids"]:
                print(
                    f"[ingest_helpers] SILENT DROP detected: upsert returned "
                    f"OK but get(ids=[{sample_id!r}]) is empty "
                    f"(run_id={run_id}, source={source}, batch_size={len(id_list)}). "
                    f"chromadb 1.5.8 frozen-collection bug — see audit forense BIS.",
                    file=sys.stderr,
                )
                return False
        except Exception as e:
            print(
                f"[ingest_helpers] post-upsert sample check raised "
                f"{type(e).__name__}: {e} (run_id={run_id}, source={source}). "
                f"Treating batch as failed.",
                file=sys.stderr,
            )
            return False
    return True

    if retry_delay > 0:  # pragma: no cover — disabled above
        time.sleep(retry_delay)
    drift = _vector_path_drift(collection, ids, embeddings)
    if not drift:
        return True

    # Vector consumer may simply be behind: writes from this upsert can
    # still be in embeddings_queue rather than missing from HNSW
    # permanently. Filter pending-in-queue ids before raising the alarm.
    target_db = db_path if db_path is not None else DB_PATH
    try:
        in_queue = _check_queue_membership(target_db, drift)
    except Exception as e:
        # Belt-and-suspenders: helper already swallows sqlite errors; if
        # something else explodes we still must not break the never-raise
        # contract. Fall back to pre-v1.2.3.1 behaviour (treat all as drift).
        print(
            f"[ingest_helpers] WARN: queue-membership check failed for "
            f"run_id={run_id}: {e}. Treating all drift as permanent.",
            file=sys.stderr,
        )
        in_queue = set()

    real_drift = [eid for eid in drift if eid not in in_queue]
    transient_count = len(drift) - len(real_drift)

    if not real_drift:
        print(
            f"[ingest_helpers] transient drift suppressed: "
            f"{transient_count} chunk(s) pending in embeddings_queue "
            f"(run_id={run_id}, source={source})",
            file=sys.stderr,
        )
        return True

    _record_pending_drift(run_id, source, real_drift, pending_dir=pending_dir)
    _record_maintenance_log(target_db, run_id, source, real_drift,
                            transient_count=transient_count)
    return False
