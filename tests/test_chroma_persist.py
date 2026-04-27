"""Tests for scripts.lib.chroma_persist.

The helper triggers chromadb's HNSW persist by issuing many idempotent
re-upserts. Tests use a fake collection that records upserts and
optionally simulates chroma's pickle-touch behavior, so the mtime
advancement path is exercised without needing chromadb installed.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "scripts", "lib"))

import chroma_persist as cp  # noqa: E402


# ============================================================
# FIXTURES
# ============================================================


class FakePersistCollection:
    """Records upsert calls; optionally touches a pickle file to simulate
    chromadb's persist-on-write behavior."""

    def __init__(
        self,
        *,
        data: dict[str, dict] | None = None,
        pickle_path: str | None = None,
        touch_pickle: bool = False,
        touch_after_n_writes: int = 0,
    ):
        # data: {id -> {"embedding": list[float], "metadata": dict, "document": str}}
        self._data = data or {}
        self.upsert_calls = 0
        self.upsert_id_counts: dict[str, int] = {}
        self._pickle_path = pickle_path
        self._touch = touch_pickle
        self._touch_after = touch_after_n_writes
        self._cumulative_writes = 0

    def get(self, *, ids, include):
        out = {"ids": [], "embeddings": [], "metadatas": [], "documents": []}
        for eid in ids:
            if eid in self._data:
                out["ids"].append(eid)
                out["embeddings"].append(self._data[eid]["embedding"])
                out["metadatas"].append(self._data[eid]["metadata"])
                out["documents"].append(self._data[eid]["document"])
        return out

    def upsert(self, *, ids, embeddings, documents, metadatas):
        self.upsert_calls += 1
        self._cumulative_writes += len(ids)
        for eid in ids:
            self.upsert_id_counts[eid] = self.upsert_id_counts.get(eid, 0) + 1
        # Simulate chromadb's HNSW persist crossing sync_threshold
        if self._touch and self._pickle_path and self._cumulative_writes >= self._touch_after:
            # Bump mtime to "now+1s" to ensure strictly-greater than the pre-loop value
            new_mtime = time.time() + 1.0
            os.utime(self._pickle_path, (new_mtime, new_mtime))

    def count(self):
        return len(self._data)


def _seed(n: int) -> dict[str, dict]:
    return {
        f"chunk_{i:04d}": {
            "embedding": [float(i), 0.0, 0.0, 0.0],
            "metadata": {"source": "document", "chunk_index": i},
            "document": f"doc body {i}",
        }
        for i in range(n)
    }


# ============================================================
# TESTS
# ============================================================


def test_empty_sample_is_noop_success():
    coll = FakePersistCollection(data=_seed(5))
    result = cp.force_chroma_persist(coll, [])
    assert result["success"] is True
    assert result["iterations"] == 0
    assert result["total_writes"] == 0
    assert coll.upsert_calls == 0


def test_iterations_math_target_1100_n_10():
    """target=1100, N=10 → 110 iterations × 10 ids = 1100 writes."""
    seed = _seed(10)
    coll = FakePersistCollection(data=seed)
    sample_ids = list(seed.keys())
    result = cp.force_chroma_persist(coll, sample_ids, target_writes=1100)
    assert result["success"] is True
    assert result["iterations"] == 110
    assert result["total_writes"] == 1100
    assert result["ids_persisted"] == 10
    assert coll.upsert_calls == 110


def test_iterations_math_target_1100_n_66():
    """target=1100, N=66 → ceil(1100/66) = 17 iterations × 66 = 1122 writes (≥ target)."""
    seed = _seed(66)
    coll = FakePersistCollection(data=seed)
    sample_ids = list(seed.keys())
    result = cp.force_chroma_persist(coll, sample_ids, target_writes=1100)
    assert result["iterations"] == 17
    assert result["total_writes"] == 17 * 66
    assert result["total_writes"] >= 1100
    assert coll.upsert_calls == 17


def test_idempotency_count_unchanged():
    """Re-upserting same ids does not change the collection's count.

    On a real chromadb collection, upsert with the same ids replaces the
    rows (no growth). FakePersistCollection's count() reflects the source
    data, which never grows."""
    seed = _seed(5)
    coll = FakePersistCollection(data=seed)
    before = coll.count()
    cp.force_chroma_persist(coll, list(seed.keys()), target_writes=200)
    after = coll.count()
    assert before == after == 5
    # Each id was upserted iterations times — real chromadb dedupes, our fake doesn't grow data
    assert all(c == 40 for c in coll.upsert_id_counts.values())  # ceil(200/5)=40


def test_no_resolved_ids_returns_failure_with_warning(capsys):
    """If get() returns nothing (sample ids don't exist), return success=False
    with a stderr warning. No upserts attempted."""
    coll = FakePersistCollection(data={})
    result = cp.force_chroma_persist(coll, ["nonexistent_a", "nonexistent_b"])
    assert result["success"] is False
    assert result["iterations"] == 0
    assert coll.upsert_calls == 0
    captured = capsys.readouterr()
    assert "none of the 2 sample ids resolved" in captured.err


def test_partial_resolution_uses_what_get_returned():
    """If only some ids resolve, persist with the resolved subset."""
    seed = _seed(3)
    coll = FakePersistCollection(data=seed)
    # 3 real + 2 fake → fake collection's get() returns only the 3 real
    sample = list(seed.keys()) + ["fake_x", "fake_y"]
    result = cp.force_chroma_persist(coll, sample, target_writes=99)
    assert result["success"] is True
    assert result["ids_persisted"] == 3
    assert result["iterations"] == 33  # ceil(99/3)
    assert result["total_writes"] == 99


def test_mtime_advances_when_touched(tmp_path):
    """When the fake collection touches the pickle on upsert, mtime_advanced=True."""
    pickle_path = tmp_path / "index_metadata.pickle"
    pickle_path.write_bytes(b"fake")
    # Set mtime to 1 hour ago so any "now" touch is strictly greater
    old_mtime = time.time() - 3600
    os.utime(str(pickle_path), (old_mtime, old_mtime))

    seed = _seed(10)
    coll = FakePersistCollection(
        data=seed,
        pickle_path=str(pickle_path),
        touch_pickle=True,
        touch_after_n_writes=1000,  # only after crossing sync_threshold
    )
    result = cp.force_chroma_persist(
        coll, list(seed.keys()),
        target_writes=1100,
        pickle_path=str(pickle_path),
    )
    assert result["success"] is True
    assert result["mtime_advanced"] is True
    assert result["mtime_before"] == pytest.approx(old_mtime, abs=0.5)
    assert result["mtime_after"] is not None
    assert result["mtime_after"] > result["mtime_before"]


def test_mtime_did_not_advance_warns_but_returns_success(tmp_path, capsys):
    """When the pickle's mtime doesn't move (persist didn't fire), warn to stderr
    but still return success=True (defensive: next audit catches drift)."""
    pickle_path = tmp_path / "index_metadata.pickle"
    pickle_path.write_bytes(b"fake")
    seed = _seed(10)
    coll = FakePersistCollection(data=seed, touch_pickle=False)
    result = cp.force_chroma_persist(
        coll, list(seed.keys()),
        target_writes=200,
        pickle_path=str(pickle_path),
    )
    assert result["success"] is True
    assert result["mtime_advanced"] is False
    captured = capsys.readouterr()
    assert "HNSW pickle mtime did not advance" in captured.err
    assert "200 idempotent writes" in captured.err


def test_no_pickle_path_skips_mtime_check(capsys):
    """No pickle_path → mtime_advanced stays None, no warning."""
    seed = _seed(10)
    coll = FakePersistCollection(data=seed)
    result = cp.force_chroma_persist(coll, list(seed.keys()), target_writes=50)
    assert result["mtime_advanced"] is None
    assert result["mtime_before"] is None
    assert result["mtime_after"] is None
    captured = capsys.readouterr()
    assert "WARN" not in captured.err


def test_pickle_path_missing_file_treated_as_no_advance(tmp_path, capsys):
    """A pickle_path pointing at a non-existent file → mtime_before is None,
    mtime_advanced=False, warning emitted."""
    missing = tmp_path / "does_not_exist.pickle"
    seed = _seed(10)
    coll = FakePersistCollection(data=seed)
    result = cp.force_chroma_persist(
        coll, list(seed.keys()),
        target_writes=50,
        pickle_path=str(missing),
    )
    assert result["success"] is True
    assert result["mtime_advanced"] is False
    captured = capsys.readouterr()
    assert "mtime did not advance" in captured.err
