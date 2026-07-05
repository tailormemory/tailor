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
from tests.helpers.chromadb_fake import FakeCollection  # noqa: E402


# ============================================================
# FIXTURES
# ============================================================


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


# ============================================================
# (C)+(B) — getter re-resolution across maintenance + honest reason
# ============================================================
# Fix per il silent-drop post-maintenance (auto-flush SIGUSR1/SIGUSR2).
# (C) collection ora accetta un getter, ri-risolto a OGNI attempt: un None
#     catturato durante la finestra di maintenance recupera all'attempt
#     successivo (dopo SIGUSR2 mid-backoff).
# (B) report["reason"] distingue "maintenance" (collection None per l'intero
#     budget) da "silent_drop" (chromadb frozen-collection reale).


def _flipping_getter(coll, live_from_attempt):
    """Getter che ritorna None (maintenance) finché la chiamata N-esima
    (1-based) < live_from_attempt, poi `coll`. Traccia il conteggio in
    getter.calls per le assert di ri-risoluzione."""
    state = {"n": 0}

    def getter():
        state["n"] += 1
        return None if state["n"] < live_from_attempt else coll

    getter.calls = state
    return getter


def test_getter_reresolves_and_recovers_after_maintenance(monkeypatch):
    """(C): attempt 1 → None (maintenance), attempt 2 re-risolve → live →
    persiste. La locale NON è più congelata a None per tutto il loop."""
    monkeypatch.setattr(ih.time, "sleep", lambda d: None)
    coll = FakeCollection()
    getter = _flipping_getter(coll, live_from_attempt=2)
    p = _payload(["a_chunk_0"])
    report: dict = {}
    ok = ih.verified_upsert(
        getter, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="t_maint_recover", source="test", retry=2, report=report,
    )
    assert ok is True
    assert report["reason"] == "ok"
    assert getter.calls["n"] == 2          # re-risolto: None poi live
    assert len(coll.upsert_calls) == 1     # solo l'attempt live ha scritto
    assert "a_chunk_0" in coll.sql


def test_maintenance_whole_budget_reports_maintenance_not_silent_drop(monkeypatch):
    """(B): collection None per tutti e 3 gli attempt → False con
    reason='maintenance' (NON 'silent_drop'), NESSUN upsert tentato,
    NESSUN None.upsert() → nessun AttributeError mis-attribuito."""
    monkeypatch.setattr(ih.time, "sleep", lambda d: None)
    getter = _flipping_getter(FakeCollection(), live_from_attempt=99)  # mai live
    p = _payload(["a_chunk_0"])
    report: dict = {}
    ok = ih.verified_upsert(
        getter, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="t_maint_exhausted", source="test", retry=2, report=report,
    )
    assert ok is False
    assert report["reason"] == "maintenance"
    assert "maintenance" in report["last_error"]
    assert getter.calls["n"] == 3          # ri-risolto a ogni attempt (retry=2 → 3)


def test_real_silent_drop_still_reports_silent_drop(monkeypatch):
    """(B): collection VALIDA ma sample-get sempre vuoto → il vero
    frozen-collection resta etichettato 'silent_drop', non 'maintenance'."""
    monkeypatch.setattr(ih.time, "sleep", lambda d: None)
    coll = FakeCollection()
    coll.program_get({"ids": []}, {"ids": []}, {"ids": []})
    p = _payload(["a_chunk_0"])
    report: dict = {}
    ok = ih.verified_upsert(
        coll, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="t_real_drop", source="test", retry=2, report=report,
    )
    assert ok is False
    assert report["reason"] == "silent_drop"
    assert len(coll.upsert_calls) == 3


def test_maintenance_then_persistent_drop_reports_silent_drop(monkeypatch):
    """La classificazione riflette l'ULTIMO attempt: maintenance sul primo,
    poi collection live che droppa → reason='silent_drop' (l'ultimo attempt
    ha raggiunto una collection reale)."""
    monkeypatch.setattr(ih.time, "sleep", lambda d: None)
    coll = FakeCollection()
    coll.program_get({"ids": []}, {"ids": []})  # attempt 2 e 3 droppano
    getter = _flipping_getter(coll, live_from_attempt=2)
    p = _payload(["a_chunk_0"])
    report: dict = {}
    ok = ih.verified_upsert(
        getter, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="t_maint_then_drop", source="test", retry=2, report=report,
    )
    assert ok is False
    assert report["reason"] == "silent_drop"
    assert getter.calls["n"] == 3
    assert len(coll.upsert_calls) == 2      # attempt 1 = maintenance (no upsert)


def test_retry_zero_accepts_getter():
    """(C) retry=0: il getter è risolto una volta (parità batch-callsite:
    l'istanza Collection non-callable resta invariata, il getter callable
    viene chiamato)."""
    coll = FakeCollection()
    p = _payload(["a_chunk_0"])
    ok = ih.verified_upsert(
        lambda: coll, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="t_getter_retry0", source="test",
    )
    assert ok is True
    assert len(coll.upsert_calls) == 1
    assert "a_chunk_0" in coll.sql


def test_report_reason_ok_on_first_attempt_success(monkeypatch):
    """report['reason'] == 'ok' quando il primo attempt persiste (istanza
    Collection diretta, non getter — retro-compat)."""
    monkeypatch.setattr(ih.time, "sleep", lambda d: None)
    coll = FakeCollection()
    p = _payload(["a_chunk_0"])
    report: dict = {}
    ok = ih.verified_upsert(
        coll, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="t_report_ok", source="test", retry=2, report=report,
    )
    assert ok is True
    assert report["reason"] == "ok"


def test_getter_raises_is_caught_as_error_never_propagates(monkeypatch):
    """(Codex blocker) un getter che SOLLEVA durante reopen/lazy-init deve
    essere catturato dal retry loop come reason='error', ritentato, e chiudere
    con False + report popolato — MAI propagato (contratto retry>0: never
    raises). Distinto dal getter che RITORNA None (→ maintenance)."""
    monkeypatch.setattr(ih.time, "sleep", lambda d: None)
    calls = {"n": 0}

    def raising_getter():
        calls["n"] += 1
        raise RuntimeError("reopen boom")

    p = _payload(["a_chunk_0"])
    report: dict = {}
    ok = ih.verified_upsert(
        raising_getter, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="t_getter_raise", source="test", retry=2, report=report,
    )
    assert ok is False
    assert report["reason"] == "error"          # non 'maintenance', non {}
    assert "RuntimeError" in report["last_error"]
    assert calls["n"] == 3                       # ri-risolto a ogni attempt


def test_getter_raise_then_live_recovers(monkeypatch):
    """Getter che solleva sull'attempt 1 (transiente) e ritorna live sul 2 →
    il retry recupera e persiste (reason='ok')."""
    monkeypatch.setattr(ih.time, "sleep", lambda d: None)
    coll = FakeCollection()
    state = {"n": 0}

    def getter():
        state["n"] += 1
        if state["n"] < 2:
            raise RuntimeError("transient reopen failure")
        return coll

    p = _payload(["a_chunk_0"])
    report: dict = {}
    ok = ih.verified_upsert(
        getter, p["ids"], p["embeddings"], p["documents"], p["metadatas"],
        run_id="t_getter_raise_recover", source="test", retry=2, report=report,
    )
    assert ok is True
    assert report["reason"] == "ok"
    assert state["n"] == 2
    assert "a_chunk_0" in coll.sql
