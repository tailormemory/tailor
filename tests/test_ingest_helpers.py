"""Tests for scripts.lib.ingest_helpers.

The helper wraps `collection.upsert()` with a vector-path readback that
detects chromadb 1.x SQL-vs-HNSW persist drift. Tests build a fake
collection that simulates HNSW state explicitly, so we exercise:
  - clean upsert path (verifier passes first try)
  - retry-then-success (vector consumer lag)
  - persistent drift -> pending_<run_id>.json + maintenance_log row
  - never-raise contract
without requiring chromadb to be installed.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "scripts", "lib"))

import ingest_helpers as ih  # noqa: E402


# ============================================================
# FIXTURES
# ============================================================


class FakeCollection:
    """Fake chromadb.Collection that explicitly tracks HNSW vs SQL state.

    `sql` mirrors the metadata segment (always written on upsert).
    `hnsw` mirrors the vector segment (configurable: drop writes to
    simulate persist-on-shutdown drift). `query()` returns top-1 from
    `hnsw` only — exactly what the verifier reads.
    """

    def __init__(self, *, drop_hnsw_for: list[str] | None = None, drop_after_n_calls: int | None = None):
        self.sql: dict[str, list[float]] = {}
        self.hnsw: dict[str, list[float]] = {}
        self.upsert_calls: list[dict] = []
        self.query_calls: int = 0
        self._drop_for = set(drop_hnsw_for or [])
        # If set, only drop on upsert calls AFTER the Nth call has happened
        # (used to simulate "first attempt drifted, retry succeeded")
        self._drop_after = drop_after_n_calls
        self._upsert_count = 0

    def upsert(self, *, ids, embeddings, documents, metadatas):
        self._upsert_count += 1
        self.upsert_calls.append({"ids": list(ids), "n": self._upsert_count})
        # SQL always succeeds
        for i, eid in enumerate(ids):
            self.sql[eid] = list(embeddings[i])
        # HNSW: drop the configured ids only on the configured upsert call
        for i, eid in enumerate(ids):
            if eid in self._drop_for:
                if self._drop_after is None or self._upsert_count <= self._drop_after:
                    continue
            self.hnsw[eid] = list(embeddings[i])

    def query(self, *, query_embeddings, n_results=1, include=None):
        self.query_calls += 1
        results_ids: list[list[str]] = []
        for q in query_embeddings:
            best_id = None
            best_dist = float("inf")
            for hid, hemb in self.hnsw.items():
                # squared euclidean — sufficient for ranking
                d = sum((a - b) ** 2 for a, b in zip(q, hemb))
                if d < best_dist:
                    best_dist = d
                    best_id = hid
            results_ids.append([best_id] if best_id else [])
        return {"ids": results_ids}


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
    """Synthetic environment: tmp DB with maintenance_log, tmp pending dir."""
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

    Embeddings are derived from `hash(eid)` so distinct ids across distinct
    batches still get distinct vectors — important for the multi-batch test
    where the verifier must not match a chunk against a same-position chunk
    from a prior batch."""
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
# verified_upsert success path
# ============================================================


def test_verified_upsert_clean_returns_true(env):
    coll = FakeCollection()
    p = _payload(["a_chunk_0", "a_chunk_1", "a_chunk_2"])
    ok = ih.verified_upsert(
        coll, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="test_run_001", source="test",
    )
    assert ok is True
    assert len(coll.upsert_calls) == 1
    assert coll.query_calls == 1  # single batched verify, no retry
    # No pending file written
    assert not Path(env["pending_dir"]).exists() or not list(Path(env["pending_dir"]).glob("pending_*.json"))
    # No maintenance_log row written
    con = sqlite3.connect(env["db_path"])
    rows = con.execute("SELECT * FROM maintenance_log").fetchall()
    con.close()
    assert rows == []


# ============================================================
# verified_upsert retry-then-success
# ============================================================


def test_verified_upsert_retry_then_success(env, monkeypatch):
    """Vector-consumer lag: first verify fails, retry succeeds."""
    # Drop hnsw write for 'a_chunk_0' on call #1, but not on call #2.
    # We simulate the lag by having FakeCollection upsert once, then
    # us patching its hnsw map between the two _vector_path_drift calls.
    coll = FakeCollection(drop_hnsw_for=["a_chunk_0"])
    p = _payload(["a_chunk_0", "a_chunk_1"])

    # Patch sleep to avoid real delay; while sleeping, flip hnsw to "caught up"
    sleeps: list[float] = []
    def fake_sleep(d):
        sleeps.append(d)
        # Vector consumer "catches up" while we sleep
        coll.hnsw["a_chunk_0"] = list(p["embeddings"][0])
    monkeypatch.setattr(ih.time, "sleep", fake_sleep)

    ok = ih.verified_upsert(
        coll, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="test_run_002", source="test",
    )
    assert ok is True
    assert len(coll.upsert_calls) == 1  # single upsert, two verify calls
    assert coll.query_calls == 2
    assert sleeps == [ih.VERIFIER_RETRY_DELAY]


# ============================================================
# verified_upsert persistent drift
# ============================================================


def test_verified_upsert_drift_logs_pending_and_maintenance_log(env, monkeypatch):
    """Persistent drift: pending JSON + maintenance_log written, returns False."""
    # Drop hnsw for two ids permanently
    coll = FakeCollection(drop_hnsw_for=["a_chunk_0", "a_chunk_2"])
    p = _payload(["a_chunk_0", "a_chunk_1", "a_chunk_2"])
    monkeypatch.setattr(ih.time, "sleep", lambda d: None)

    ok = ih.verified_upsert(
        coll, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="test_run_003", source="docs",
    )
    assert ok is False
    assert len(coll.upsert_calls) == 1
    assert coll.query_calls == 2  # primary + retry

    # Pending JSON
    pending_path = Path(env["pending_dir"]) / "pending_test_run_003.json"
    assert pending_path.exists()
    data = json.loads(pending_path.read_text())
    assert data["run_id"] == "test_run_003"
    assert len(data["entries"]) == 1
    entry = data["entries"][0]
    assert entry["source"] == "docs"
    assert sorted(entry["ids"]) == ["a_chunk_0", "a_chunk_2"]

    # maintenance_log row
    con = sqlite3.connect(env["db_path"])
    rows = con.execute("SELECT id, timestamp, operation FROM maintenance_log").fetchall()
    con.close()
    assert len(rows) == 1
    payload = json.loads(rows[0][2])
    assert payload["tool"] == "ingest_helpers.py"
    assert payload["mode"] == "verify"
    assert payload["run_id"] == "test_run_003"
    assert payload["source"] == "docs"
    assert payload["drift_summary"]["drift_total"] == 2
    assert payload["drift_summary"]["drift_ids_in_payload"] == 2
    assert sorted(payload["drift_ids"]) == ["a_chunk_0", "a_chunk_2"]


def test_verified_upsert_drift_caps_payload_ids(env, monkeypatch):
    """Drift > MAINTENANCE_LOG_DRIFT_ID_CAP → cap the payload, drift_total still
    reports the true count."""
    big_ids = [f"chunk_{i:04d}" for i in range(75)]
    coll = FakeCollection(drop_hnsw_for=big_ids)
    p = _payload(big_ids)
    monkeypatch.setattr(ih.time, "sleep", lambda d: None)

    ok = ih.verified_upsert(
        coll, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="test_run_004", source="docs",
    )
    assert ok is False

    # All 75 ids in pending JSON
    pending_path = Path(env["pending_dir"]) / "pending_test_run_004.json"
    data = json.loads(pending_path.read_text())
    assert len(data["entries"][0]["ids"]) == 75

    # maintenance_log capped at 50, but drift_total = 75
    con = sqlite3.connect(env["db_path"])
    rows = con.execute("SELECT operation FROM maintenance_log").fetchall()
    con.close()
    payload = json.loads(rows[0][0])
    assert payload["drift_summary"]["drift_total"] == 75
    assert payload["drift_summary"]["drift_ids_in_payload"] == 50
    assert payload["drift_summary"]["drift_ids_capped_at"] == ih.MAINTENANCE_LOG_DRIFT_ID_CAP
    assert len(payload["drift_ids"]) == 50


def test_verified_upsert_multiple_batches_same_run_id_appends(env, monkeypatch):
    """Two batches in one run that both drift → single pending file with two entries."""
    coll = FakeCollection(drop_hnsw_for=["a_chunk_0", "b_chunk_0"])
    monkeypatch.setattr(ih.time, "sleep", lambda d: None)

    p1 = _payload(["a_chunk_0", "a_chunk_1"])
    ih.verified_upsert(
        coll, p1["ids"], p1["embeddings"], p1["documents"], p1["metadatas"],
        run_id="test_run_005", source="docs",
    )
    p2 = _payload(["b_chunk_0", "b_chunk_1"])
    ih.verified_upsert(
        coll, p2["ids"], p2["embeddings"], p2["documents"], p2["metadatas"],
        run_id="test_run_005", source="docs",
    )

    pending_path = Path(env["pending_dir"]) / "pending_test_run_005.json"
    data = json.loads(pending_path.read_text())
    assert len(data["entries"]) == 2
    assert data["entries"][0]["ids"] == ["a_chunk_0"]
    assert data["entries"][1]["ids"] == ["b_chunk_0"]


# ============================================================
# never-raise contract
# ============================================================


def test_verified_upsert_never_raises_on_log_write_failure(env, monkeypatch, capsys):
    """If maintenance_log write fails, verified_upsert returns False with stderr
    warning — never raises. Pending JSON is still written."""
    # Break the DB by pointing at a non-existent path
    monkeypatch.setattr(ih, "DB_PATH", "/nonexistent/path/should_not_be_writable.sqlite3")
    monkeypatch.setattr(ih.time, "sleep", lambda d: None)

    coll = FakeCollection(drop_hnsw_for=["x_chunk_0"])
    p = _payload(["x_chunk_0", "x_chunk_1"])
    ok = ih.verified_upsert(
        coll, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="test_run_006", source="docs",
    )
    assert ok is False

    # Pending JSON is still written
    pending_path = Path(env["pending_dir"]) / "pending_test_run_006.json"
    assert pending_path.exists()

    captured = capsys.readouterr()
    assert "maintenance_log write failed" in captured.err
    assert "test_run_006" in captured.err


def test_verified_upsert_empty_ids_short_circuits(env):
    coll = FakeCollection()
    ok = ih.verified_upsert(
        coll, [], [], [], [], run_id="test_run_007", source="test",
    )
    assert ok is True
    # Upsert is still called (empty batch is upsert-callee's call), but query is skipped
    assert coll.query_calls == 0


# ============================================================
# defensive: malformed query result
# ============================================================


def test_verifier_handles_malformed_result_shape(env, monkeypatch):
    """If chromadb returns fewer result lists than queries, missing positions
    are treated as drift."""
    class WeirdCollection(FakeCollection):
        def query(self, *, query_embeddings, n_results=1, include=None):
            self.query_calls += 1
            # Return only one result for two queries — malformed
            return {"ids": [["a_chunk_0"]]}

    coll = WeirdCollection()
    p = _payload(["a_chunk_0", "a_chunk_1"])
    monkeypatch.setattr(ih.time, "sleep", lambda d: None)

    ok = ih.verified_upsert(
        coll, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="test_run_008", source="test",
    )
    # a_chunk_1 has no result row -> treated as drift
    assert ok is False
    pending_path = Path(env["pending_dir"]) / "pending_test_run_008.json"
    data = json.loads(pending_path.read_text())
    assert "a_chunk_1" in data["entries"][0]["ids"]
