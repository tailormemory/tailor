"""Tests for scripts.lib.ingest_helpers.

verified_upsert (post-v1.2.6.3) wraps collection.upsert() with a
sample-get sentinel: reads back the first id of each batch via
collection.get(); if missing, returns False. Surfaces the chromadb
1.5.8 frozen-collection silent-drop. No HNSW probing, no retry,
no pending-drift recording — the canonical drift detector is
repair_hnsw_index.py (nightly).

The pre-1.2.6.3 inline drift verifier (_vector_path_drift +
embeddings_queue check + pending-drift JSON + maintenance_log) is
dead code disabled by the SIGSEGV bypass introduced in v1.2.6.1.
Tests that exercised it were removed; their coverage either lives
in repair_hnsw_index.py (canonical drift detector) or will be
re-introduced by A1 with the new retry-opt-in semantics.
"""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "scripts", "lib"))

import ingest_helpers as ih  # noqa: E402


# ============================================================
# FIXTURES
# ============================================================


def _is_exception_value(v) -> bool:
    """True if v is an Exception instance or an Exception subclass.

    Shared validator for FakeCollection.program_get / program_upsert: both
    methods accept the same exception forms (instance or class) and route
    them through the same raise path. Anything matching this is suitable
    as a `raise` target — Python's `raise` syntax accepts both forms.
    """
    return isinstance(v, BaseException) or (
        isinstance(v, type) and issubclass(v, BaseException)
    )


class FakeCollection:
    """Fake chromadb.Collection for verified_upsert tests.

    Truth model:
        upsert(ids=[i], embeddings=[e], ...) writes i -> e to self.sql.
        get(ids=[i]) returns {"ids": [i]} iff i is in self.sql,
        else {"ids": []}.

    Silent-drop / failure simulation via .program_get(*responses) and
    .program_upsert(*outcomes). Both enqueue FIFO entries consumed
    one-per-call by the corresponding method; once a queue is exhausted,
    the method reverts to the truthful default.

    Queue entry types:
        FakeCollection.DEFAULT
            sentinel — use truthful behaviour for this single call
            (upsert writes to self.sql; get reads from self.sql).
        Exception INSTANCE or SUBCLASS (e.g. RuntimeError or RuntimeError("boom"))
            the method `raise`s it. Used to simulate chromadb-side
            failures (e.g. SIGSEGV-recovered exception, frozen-collection
            raise variants). Validated by _is_exception_value.
        dict with key "ids" (program_get only)
            returned verbatim from get(). Used to simulate silent-drop
            (`{"ids": []}` = sample missing).

    program_upsert does NOT accept dict outcomes — upsert has no
    user-facing return value, only side-effects.

    Common patterns (used by this file and by A1 retry/backoff tests):
        coll.program_get({"ids": []})
            -> silent-drop only on the first get() call.
        coll.program_get({"ids": []}, FakeCollection.DEFAULT)
            -> drop on call 1, truthful on call 2 (retry recovery).
        coll.program_get({"ids": []}, {"ids": []}, {"ids": []})
            -> drop on first 3 calls (retry=2 exhausted, 3 attempts).
        coll.program_upsert(RuntimeError("boom"), FakeCollection.DEFAULT)
            -> first upsert raises, second succeeds (retry recovery).

    Attributes for assertions:
        sql           dict[id -> embedding]
        upsert_calls  list[dict] — one entry per upsert() with {"ids": [...]},
                      recorded BEFORE the queue is consumed so raising
                      attempts are counted too.
        get_calls     list[list[str]] — one entry per get() with the ids requested,
                      recorded BEFORE the queue is consumed (same rule).
    """

    DEFAULT = object()  # sentinel: queue entry meaning "use truthful default"

    def __init__(self):
        self.sql: dict[str, list[float]] = {}
        self.upsert_calls: list[dict] = []
        self.get_calls: list[list[str]] = []
        self._get_queue: list = []
        self._upsert_queue: list = []

    def program_get(self, *responses) -> None:
        """Enqueue FIFO responses for the next get() calls.

        Each response must be a dict with key "ids", FakeCollection.DEFAULT,
        or an Exception instance/subclass. Anything else raises TypeError
        — fail-fast on malformed test setup.
        """
        for r in responses:
            if r is FakeCollection.DEFAULT:
                self._get_queue.append(r)
                continue
            if isinstance(r, dict) and "ids" in r:
                self._get_queue.append(r)
                continue
            if _is_exception_value(r):
                self._get_queue.append(r)
                continue
            raise TypeError(
                f"program_get: each response must be a dict with key 'ids', "
                f"FakeCollection.DEFAULT, or an Exception instance/subclass; "
                f"got {type(r).__name__}: {r!r}"
            )

    def program_upsert(self, *outcomes) -> None:
        """Enqueue FIFO outcomes for the next upsert() calls.

        Each outcome must be FakeCollection.DEFAULT (truthful write) or
        an Exception instance/subclass (raised from upsert()). Anything
        else raises TypeError — fail-fast on malformed test setup.
        """
        for o in outcomes:
            if o is FakeCollection.DEFAULT or _is_exception_value(o):
                self._upsert_queue.append(o)
                continue
            raise TypeError(
                f"program_upsert: each outcome must be FakeCollection.DEFAULT "
                f"or an Exception instance/subclass; "
                f"got {type(o).__name__}: {o!r}"
            )

    def upsert(self, *, ids, embeddings, documents, metadatas):
        self.upsert_calls.append({"ids": list(ids)})
        if self._upsert_queue:
            outcome = self._upsert_queue.pop(0)
            if outcome is not FakeCollection.DEFAULT:
                raise outcome
        for i, eid in enumerate(ids):
            self.sql[eid] = list(embeddings[i])

    def get(self, *, ids):
        ids_list = list(ids)
        self.get_calls.append(ids_list)
        if self._get_queue:
            resp = self._get_queue.pop(0)
            if resp is FakeCollection.DEFAULT:
                pass  # fall through to truthful default
            elif _is_exception_value(resp):
                raise resp
            else:
                return resp
        present = [i for i in ids_list if i in self.sql]
        return {"ids": present}


def _ensure_maintenance_log(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    con.execute(
        "CREATE TABLE IF NOT EXISTS maintenance_log ("
        "id INT PRIMARY KEY, timestamp INT NOT NULL, operation TEXT NOT NULL)"
    )
    con.commit()
    con.close()


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Synthetic environment: tmp DB with maintenance_log, tmp pending dir.

    Used by the clean-upsert sanity check to prove verified_upsert does
    not write to legacy locations (pending JSON dir / maintenance_log
    table). Those writes belonged to the pre-1.2.6.3 inline drift
    verifier and must remain dormant.
    """
    db_path = tmp_path / "chroma.sqlite3"
    pending_dir = tmp_path / "logs" / "hnsw_audits"
    _ensure_maintenance_log(str(db_path))
    monkeypatch.setattr(ih, "DB_PATH", str(db_path))
    monkeypatch.setattr(ih, "PENDING_DIR", str(pending_dir))
    return {
        "db_path": str(db_path),
        "pending_dir": str(pending_dir),
    }


def _payload(ids: list[str]) -> dict:
    """Build matching ids/embeddings/documents/metadatas of the same length.

    Embeddings are derived from `hash(eid)` so distinct ids get distinct
    vectors. The shape is preserved for parity with the real chromadb
    upsert contract even though verified_upsert post-1.2.6.3 only
    reads back via collection.get() (no vector probing)."""
    embs: list[list[float]] = []
    for eid in ids:
        h = hash(eid)
        embs.append([
            float((h >> 0) & 0xFFFF) / 65535.0,
            float((h >> 16) & 0xFFFF) / 65535.0,
            float((h >> 32) & 0xFFFF) / 65535.0,
            float((h >> 48) & 0xFFFF) / 65535.0,
        ])
    return {
        "ids": ids,
        "embeddings": embs,
        "documents": [f"doc body {eid}" for eid in ids],
        "metadatas": [{"source": "test", "chunk_index": i} for i in range(len(ids))],
    }


# ============================================================
# make_run_id
# ============================================================


def test_make_run_id_includes_source_timestamp_pid():
    rid = ih.make_run_id("conversations")
    assert rid.startswith("conversations_")
    parts = rid.split("_")
    # source_YYYYMMDD_HHMMSS_PID -> 4 underscore-separated tokens
    assert len(parts) == 4
    assert parts[0] == "conversations"
    assert parts[1].isdigit() and len(parts[1]) == 8  # YYYYMMDD
    assert parts[2].isdigit() and len(parts[2]) == 6  # HHMMSS
    assert parts[3] == str(os.getpid())


def test_make_run_id_is_unique_across_pids(monkeypatch):
    """Different pids in the same second yield different run ids."""
    fixed_time = "20260427_153000"
    monkeypatch.setattr(ih.time, "strftime", lambda fmt: fixed_time)
    monkeypatch.setattr(ih.os, "getpid", lambda: 1111)
    rid_a = ih.make_run_id("docs")
    monkeypatch.setattr(ih.os, "getpid", lambda: 2222)
    rid_b = ih.make_run_id("docs")
    assert rid_a != rid_b
    assert rid_a.endswith("_1111")
    assert rid_b.endswith("_2222")


# ============================================================
# verified_upsert — post-1.2.6.3 sample-get sentinel
# ============================================================


def test_verified_upsert_clean_returns_true(env):
    """Clean upsert: sentinel reads back the first batch id and finds it."""
    coll = FakeCollection()
    p = _payload(["a_chunk_0", "a_chunk_1", "a_chunk_2"])
    ok = ih.verified_upsert(
        coll, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="test_run_001", source="test",
    )
    assert ok is True
    assert len(coll.upsert_calls) == 1
    # Post-1.2.6.3 sentinel: a single get() of the first batch id.
    assert len(coll.get_calls) == 1
    assert coll.get_calls[0] == ["a_chunk_0"]
    # Sanity: verified_upsert must not write to legacy locations (those
    # belonged to the pre-1.2.6.3 inline drift verifier — dead code).
    pending_dir = Path(env["pending_dir"])
    assert not pending_dir.exists() or not list(pending_dir.glob("pending_*.json"))
    con = sqlite3.connect(env["db_path"])
    rows = con.execute("SELECT * FROM maintenance_log").fetchall()
    con.close()
    assert rows == []


def test_verified_upsert_empty_ids_short_circuits():
    """Empty batch: upsert is called (mirrors real chromadb contract) but
    the sentinel get() is skipped because there is no first id to sample."""
    coll = FakeCollection()
    ok = ih.verified_upsert(
        coll, [], [], [], [], run_id="test_run_007", source="test",
    )
    assert ok is True
    assert coll.get_calls == []


# ============================================================
# A1 — retry opt-in (retry kwarg, backoff, never-raise extension)
# ============================================================


def test_retry_zero_is_single_shot_legacy():
    """Regression guard for the 4 batch callsites: retry not passed →
    1 upsert, 1 get, single-shot (identical to pre-A1 batch behaviour)."""
    coll = FakeCollection()
    p = _payload(["a_chunk_0"])
    ok = ih.verified_upsert(
        coll, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="t_retry_zero_legacy", source="test",
    )
    assert ok is True
    assert len(coll.upsert_calls) == 1
    assert len(coll.get_calls) == 1
    assert coll.get_calls[0] == ["a_chunk_0"]


def test_retry_zero_get_raises_swallowed():
    """retry=0 with collection.get() raising → return False, no propagation
    (contract preserved for the 4 batch callsites)."""
    coll = FakeCollection()
    coll.program_get(RuntimeError("simulated get failure"))
    p = _payload(["a_chunk_0"])
    ok = ih.verified_upsert(
        coll, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="t_retry_zero_get_raise", source="test",
    )
    assert ok is False
    assert len(coll.upsert_calls) == 1  # upsert succeeded
    assert len(coll.get_calls) == 1     # get attempted before raising


def test_retry_zero_upsert_raises_propagates():
    """retry=0 with collection.upsert() raising → exception PROPAGATES.
    The 4 batch callsites depend on this: their outer try/except catches
    chromadb exceptions and increments their `errors` counter."""
    coll = FakeCollection()
    coll.program_upsert(RuntimeError("simulated upsert failure"))
    p = _payload(["a_chunk_0"])
    with pytest.raises(RuntimeError, match="simulated upsert failure"):
        ih.verified_upsert(
            coll, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
            run_id="t_retry_zero_upsert_raise", source="test",
        )
    assert len(coll.upsert_calls) == 1
    assert coll.get_calls == []  # never reached get


def test_retry_recovers_on_second_attempt(monkeypatch):
    """retry=2: first sample-get sees silent-drop, second attempt is
    truthful → True. Both upsert and get are re-run for the recovery."""
    coll = FakeCollection()
    coll.program_get({"ids": []}, FakeCollection.DEFAULT)
    monkeypatch.setattr(ih.time, "sleep", lambda d: None)
    p = _payload(["a_chunk_0"])
    ok = ih.verified_upsert(
        coll, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="t_retry_recover", source="test",
        retry=2,
    )
    assert ok is True
    assert len(coll.upsert_calls) == 2  # full re-run, not get-only retry
    assert len(coll.get_calls) == 2


def test_retry_exhausted_returns_false(monkeypatch):
    """retry=2, all 3 attempts see silent-drop → False, 3 full attempts,
    no exception leaks (retry>0 contract: never raises)."""
    coll = FakeCollection()
    coll.program_get({"ids": []}, {"ids": []}, {"ids": []})
    monkeypatch.setattr(ih.time, "sleep", lambda d: None)
    p = _payload(["a_chunk_0"])
    ok = ih.verified_upsert(
        coll, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="t_retry_exhausted", source="test",
        retry=2,
    )
    assert ok is False
    assert len(coll.upsert_calls) == 3
    assert len(coll.get_calls) == 3


def test_retry_upsert_raise_recovered(monkeypatch):
    """retry=2, upsert raises on attempt 1, succeeds on attempt 2 → True.
    Verifies the retry loop catches upsert exceptions (differs from retry=0
    where they propagate)."""
    coll = FakeCollection()
    coll.program_upsert(
        RuntimeError("attempt-1 upsert boom"),
        FakeCollection.DEFAULT,
    )
    monkeypatch.setattr(ih.time, "sleep", lambda d: None)
    p = _payload(["a_chunk_0"])
    ok = ih.verified_upsert(
        coll, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="t_retry_upsert_recover", source="test",
        retry=2,
    )
    assert ok is True
    assert len(coll.upsert_calls) == 2  # both attempts recorded (raise + success)
    assert len(coll.get_calls) == 1     # only attempt 2 reached the sentinel


def test_retry_upsert_raise_exhausted(monkeypatch):
    """retry=2, upsert raises on all 3 attempts → False, NO RAISE
    (key behavioural divergence from retry=0 which propagates)."""
    coll = FakeCollection()
    coll.program_upsert(
        RuntimeError("a1"), RuntimeError("a2"), RuntimeError("a3"),
    )
    monkeypatch.setattr(ih.time, "sleep", lambda d: None)
    p = _payload(["a_chunk_0"])
    ok = ih.verified_upsert(
        coll, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="t_retry_upsert_exhausted", source="test",
        retry=2,
    )
    assert ok is False
    assert len(coll.upsert_calls) == 3
    assert coll.get_calls == []  # no upsert ever succeeded, never reached get


def test_backoff_float(monkeypatch):
    """retry_backoff as float → uniform sleep between every attempt pair."""
    sleeps: list[float] = []
    monkeypatch.setattr(ih.time, "sleep", lambda d: sleeps.append(d))
    coll = FakeCollection()
    coll.program_get({"ids": []}, {"ids": []}, {"ids": []})
    p = _payload(["a_chunk_0"])
    ih.verified_upsert(
        coll, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="t_backoff_float", source="test",
        retry=2, retry_backoff=0.3,
    )
    assert sleeps == [0.3, 0.3]


def test_backoff_tuple(monkeypatch):
    """retry_backoff as tuple → per-position sleeps."""
    sleeps: list[float] = []
    monkeypatch.setattr(ih.time, "sleep", lambda d: sleeps.append(d))
    coll = FakeCollection()
    coll.program_get({"ids": []}, {"ids": []}, {"ids": []})
    p = _payload(["a_chunk_0"])
    ih.verified_upsert(
        coll, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="t_backoff_tuple", source="test",
        retry=2, retry_backoff=(0.5, 1.0),
    )
    assert sleeps == [0.5, 1.0]


def test_backoff_tuple_clamp(monkeypatch):
    """retry_backoff tuple shorter than the number of retries → clamp to
    the last value for further positions."""
    sleeps: list[float] = []
    monkeypatch.setattr(ih.time, "sleep", lambda d: sleeps.append(d))
    coll = FakeCollection()
    coll.program_get({"ids": []}, {"ids": []}, {"ids": []}, {"ids": []})
    p = _payload(["a_chunk_0"])
    ih.verified_upsert(
        coll, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="t_backoff_clamp", source="test",
        retry=3, retry_backoff=(0.5, 1.0),
    )
    assert sleeps == [0.5, 1.0, 1.0]


def test_no_sleep_after_last_attempt(monkeypatch):
    """Sleep happens BETWEEN attempts, not after the final one. retry=1 →
    2 attempts total → at most 1 sleep."""
    sleeps: list[float] = []
    monkeypatch.setattr(ih.time, "sleep", lambda d: sleeps.append(d))
    coll = FakeCollection()
    coll.program_get({"ids": []}, {"ids": []})
    p = _payload(["a_chunk_0"])
    ih.verified_upsert(
        coll, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="t_no_sleep_last", source="test",
        retry=1, retry_backoff=5.0,
    )
    assert sleeps == [5.0]
