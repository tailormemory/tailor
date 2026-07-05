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


def _resolve_backoff(
    backoff: float | tuple[float, ...],
    attempt_idx: int,
) -> float:
    """Resolve sleep duration before the (attempt_idx+1)-th retry attempt.

    `backoff` may be a float (constant delay between every attempt pair)
    or a tuple of per-position delays. With a tuple, `backoff[i]` is the
    sleep between attempt `i+1` and attempt `i+2`; if `attempt_idx`
    exceeds `len(backoff)-1`, the last value is reused (clamp). Empty
    tuple yields 0.0.
    """
    if isinstance(backoff, tuple):
        if not backoff:
            return 0.0
        return float(backoff[min(attempt_idx, len(backoff) - 1)])
    return float(backoff)


def _resolve_collection(collection: Any) -> Any:
    """Support both a live Collection and a zero-arg getter.

    (C) The 3 interactive mcp_server callsites (retry>0) pass the bound
    `get_collection` function so the retry loop can RE-RESOLVE the handle on
    each attempt: if the first resolution landed inside a maintenance window
    (`get_collection()` → None because SIGUSR1 released ChromaDB), a later
    attempt after SIGUSR2 re-fetches a live handle and the write persists,
    instead of looping forever on a frozen None.

    The 4 batch callsites (retry=0) keep passing a live Collection instance,
    which is NOT callable → returned as-is (bit-identical to prior behaviour).
    """
    return collection() if callable(collection) else collection


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
    retry: int = 0,
    retry_backoff: float | tuple[float, ...] = 0.0,
    report: dict | None = None,
) -> bool:
    """Upsert into ChromaDB and verify the first batch id is reachable
    via a post-upsert sample-get sentinel (chromadb 1.5.8 frozen-collection
    silent-drop detection — see v1.2.6.3 commit).

    Two modes:

    retry=0 (default — used by all 4 batch callsites)
        Single-shot, bit-identical to the v1.2.6.3 behaviour:
          - `collection.upsert()` exceptions PROPAGATE to the caller (the
            batch loops in ingest_docs / ingest_conversations / ingest_upload
            / ingest_gmail rely on this to increment their `errors` counter
            via their own try/except).
          - `collection.get()` exceptions are swallowed; the function
            returns False with a stderr log.
          - Empty sample-get result → return False.
          - No retry, no backoff.

    retry=N>0 (A1, opt-in — used by the 3 interactive mcp_server callsites)
        Up to `1 + N` attempts. Each attempt re-runs BOTH `collection.upsert()`
        AND the sample-get (the chromadb 1.5.8 frozen-collection state is
        not cured by re-reading; only a fresh upsert may break out of it).
        Exceptions from the getter (reopen/lazy-init), upsert OR get are
        caught and logged as a failed attempt (reason="error") — with
        retry>0, verified_upsert NEVER raises (differs from retry=0). A
        getter that RETURNS None is a distinct case (reason="maintenance"),
        not an error. Returns True on the first successful attempt;
        if attempt > 0, logs "recovered on attempt N". Returns False
        if all attempts exhaust, with a FINAL_FAILURE stderr line that
        includes `last_error`.

        Between attempts, sleeps for `_resolve_backoff(retry_backoff, i)`
        seconds; NO sleep follows the last attempt.

    Rationale for retry (NOT a cure for frozen-collection):
        The chromadb 1.5.8 frozen-collection state is non-transient — once
        a collection is in that state, every attempt fails. Retry serves
        two distinct purposes:
          (1) anti-false-positive filter: N consecutive sample-get misses
              confirm a real silent-drop rather than a transient blip;
          (2) recovery for the non-frozen transient case (embeddings_queue
              lag, momentary vector-consumer stall).
        On a true frozen collection, all attempts fail and the function
        returns False — by design, not a bug. The caller surfaces this
        to the user as an explicit error (see mcp_server.py callsites).

    Known limitation (parity with the 4 batch callsites — not a regression):
        the sample-get reads from the same Collection object that just
        wrote. If chromadb serves the read from an in-memory cache before
        the on-disk persist, a true persist failure can be masked. Out
        of scope for A1; the canonical drift detector remains
        `repair_hnsw_index.py` (nightly).

    Args:
        collection: a live Collection OR a zero-arg getter returning
            `Collection | None` (see `_resolve_collection`). With retry>0 the
            getter is re-resolved on EVERY attempt so a None captured during a
            maintenance window recovers on a later attempt (fix C).
        retry: number of EXTRA attempts after the initial one (0 = single-shot).
        retry_backoff: sleep between consecutive attempts. A float applies
            uniformly; a tuple `(s0, s1, ...)` specifies per-position delays
            with clamp to the last value when there are more retries than
            tuple entries.
        report: optional dict, populated ONLY on the retry>0 path. On exit it
            carries `reason` ∈ {"ok", "maintenance", "silent_drop", "error"}
            and `last_error`. `"maintenance"` means every attempt found the
            collection unavailable (SIGUSR1 released it) → the caller must
            surface an HONEST "MCP in maintenance" error, NOT the misleading
            "chromadb silent-drop" (fix B). `"silent_drop"` is the genuine
            chromadb 1.5.8 frozen-collection case (upsert OK, sample-get miss).

    `run_id` is the caller-supplied identifier shared across all batches in
    one ingest run; use `make_run_id(source)` at the top of the script.
    """
    id_list = list(ids)

    if retry > 0:
        # ===== retry>0 path (A1 opt-in): loop with full upsert+sample-get =====
        last_error: str | None = None
        last_reason: str = "error"   # ok | maintenance | silent_drop | error
        total_attempts = retry + 1
        for attempt in range(total_attempts):
            attempt_label = f"attempt {attempt + 1}/{total_attempts}"
            try:
                # (C) re-resolve the handle on EVERY attempt. A None captured
                # during a maintenance window (SIGUSR1 → get_collection() returns
                # None) is no longer frozen for the whole loop: once SIGUSR2 fires
                # during the backoff, the next attempt re-fetches a live
                # collection. The resolve is INSIDE the try so a getter that
                # RAISES during reopen/lazy-init is caught as reason="error" and
                # retried, NOT propagated (retry>0 contract: never raises). A
                # getter that RETURNS None is a different case → maintenance below.
                col = _resolve_collection(collection)
                if col is None:
                    # (B) maintenance, NOT a chromadb silent-drop. Do not call
                    # None.upsert() — that raised the misleading AttributeError
                    # that got re-labelled "chromadb silent-drop" every flush cycle.
                    last_reason = "maintenance"
                    last_error = "collection unavailable (maintenance mode)"
                    print(
                        f"[ingest_helpers] {attempt_label}: collection unavailable "
                        f"(maintenance) — will re-resolve on next attempt "
                        f"(run_id={run_id}, source={source}).",
                        file=sys.stderr,
                    )
                else:
                    col.upsert(
                        ids=id_list,
                        embeddings=list(embeddings),
                        documents=list(documents),
                        metadatas=list(metadatas),
                    )
                    if not id_list:
                        if report is not None:
                            report["reason"] = "ok"
                        return True
                    sample_id = id_list[0]
                    result = col.get(ids=[sample_id])
                    if result.get("ids") and sample_id in result["ids"]:
                        if attempt > 0:
                            print(
                                f"[ingest_helpers] verified_upsert recovered on "
                                f"{attempt_label} (run_id={run_id}, source={source}).",
                                file=sys.stderr,
                            )
                        if report is not None:
                            report["reason"] = "ok"
                        return True
                    last_reason = "silent_drop"
                    last_error = f"silent_drop sample_id={sample_id!r}"
                    print(
                        f"[ingest_helpers] SILENT DROP observed on {attempt_label}: "
                        f"upsert returned OK but get(ids=[{sample_id!r}]) is empty "
                        f"(run_id={run_id}, source={source}, batch_size={len(id_list)}).",
                        file=sys.stderr,
                    )
            except Exception as e:
                # Covers getter-raise (reopen/lazy-init), upsert-raise, get-raise.
                last_reason = "error"
                last_error = f"{type(e).__name__}: {e}"
                print(
                    f"[ingest_helpers] {attempt_label} raised "
                    f"{type(e).__name__}: {e} (run_id={run_id}, source={source}).",
                    file=sys.stderr,
                )
            if attempt < retry:
                sleep_for = _resolve_backoff(retry_backoff, attempt)
                if sleep_for > 0:
                    time.sleep(sleep_for)
        if report is not None:
            report["reason"] = last_reason
            report["last_error"] = last_error
        print(
            f"[ingest_helpers] verified_upsert FAILED after {total_attempts} "
            f"attempts (run_id={run_id}, source={source}, reason={last_reason}, "
            f"last_error={last_error}).",
            file=sys.stderr,
        )
        return False

    # ===== retry=0 path: v1.2.6.3 single-shot, body preserved verbatim =====
    # (C) getter support: batch callsites pass a live Collection (not callable
    # → returned as-is, bit-identical); resolved once here if ever given a getter.
    collection = _resolve_collection(collection)
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
