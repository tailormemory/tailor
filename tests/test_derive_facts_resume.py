"""Tests for scripts.enrichment.derive_facts durable-resume logic.

Covers the structural fix to nightly fact derivation (model "A con priorità ai
nuovi"). The four non-negotiable invariants:

  1. Durable progress — a run killed mid-way (SIGTERM) does NOT throw away the
     work already done; the next night resumes from the right point.
  2. last_run advances ONLY on a fully drained queue, NEVER on SIGTERM/partial.
  3. Priority to new — facts created after last_run go first.
  4. Two distinct cursors — last_run (boundary) vs derived_watermarks (per-entity
     durable progress), never collapsed.

No real HTTP / LLM: derive_for_entity and BackendManager are patched.
"""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from scripts.enrichment import derive_facts as df  # noqa: E402


# ── helpers ───────────────────────────────────────────────────

def _build_db(path, entities):
    """entities: list of (name, created_at, n_facts). Creates a minimal facts
    table with the columns derive_facts touches."""
    db = sqlite3.connect(path)
    db.execute("""
        CREATE TABLE facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chunk_id TEXT, fact TEXT, category TEXT, entity_tags TEXT,
            event_date TEXT, confidence REAL, created_at TEXT,
            relation_type TEXT, derived_from TEXT, superseded_by TEXT DEFAULT ''
        )
    """)
    for name, created_at, n_facts in entities:
        for i in range(n_facts):
            db.execute(
                "INSERT INTO facts (chunk_id, fact, category, entity_tags, event_date, "
                "confidence, created_at, relation_type, derived_from, superseded_by) "
                "VALUES (?,?,?,?,?,?,?,?,?,'')",
                (f"c_{name}_{i}", f"{name} fact {i}", "cat", json.dumps([name]),
                 "", 0.9, created_at, "extracted", ""),
            )
    db.commit()
    db.close()


class _FakeMgr:
    def __init__(self, *a, **k):
        self.backends = [{"name": "anthropic", "model": "m"}]


@pytest.fixture
def patched(monkeypatch, tmp_path):
    """Wire derive_facts to a temp DB + checkpoint, no network, no throttle."""
    db_path = str(tmp_path / "facts.sqlite3")
    ckpt = str(tmp_path / "checkpoints" / "derives_checkpoint.json")
    monkeypatch.setattr(df, "FACTS_DB", db_path)
    monkeypatch.setattr(df, "CHECKPOINT_FILE", ckpt)
    monkeypatch.setattr(df, "BackendManager", _FakeMgr)
    monkeypatch.setattr(df, "RATE_LIMIT_DELAY", 0)

    calls = []
    state = {"n": 0, "kill_at": None}

    def fake_derive(norm, data, existing, manager):
        state["n"] += 1
        calls.append(norm)
        if state["kill_at"] is not None and state["n"] == state["kill_at"]:
            signal.raise_signal(signal.SIGTERM)  # exercise the real SIGTERM path
        return []

    monkeypatch.setattr(df, "derive_for_entity", fake_derive)
    return {"db": db_path, "ckpt": ckpt, "calls": calls, "state": state}


def _run_nightly(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["derive_facts.py", "--nightly"])
    df.main()


# ── 1. killed-mid-run resumes from the right point ────────────

def test_killed_run_resumes_without_reprocessing(patched, monkeypatch):
    # 6 entities, distinct created_at → deterministic newest-first order E6..E1
    ents = [(f"E{i}", f"2026-06-10T00:0{i}:00", 5) for i in range(1, 7)]
    _build_db(patched["db"], ents)
    ALL = {f"e{i}" for i in range(1, 7)}

    # ── Night 1: kill on the 3rd entity ──
    patched["state"]["kill_at"] = 3
    with pytest.raises(SystemExit):
        _run_nightly(monkeypatch)

    cp1 = json.load(open(patched["ckpt"]))
    done1 = set(cp1["derived_watermarks"])
    # Killed mid-way: some done, not all (invariant 1: progress is durable)
    assert 0 < len(done1) < 6
    # Invariant 2: last_run NOT advanced on a partial/killed run
    assert cp1["last_run"] == ""
    assert cp1.get("last_run_completed") is False
    # Reporting reflects real count, not 0 (the old tail-parse artefact)
    assert cp1["entities_processed"] == len(done1)

    # ── Night 2: completes ──
    patched["calls"].clear()
    patched["state"]["n"] = 0
    patched["state"]["kill_at"] = None
    _run_nightly(monkeypatch)

    run2 = set(patched["calls"])
    # Invariant 1 + 3: night 2 processes EXACTLY the remainder, never re-doing
    # what night 1 already watermarked.
    assert run2.isdisjoint(done1)
    assert run2 == ALL - done1
    cp2 = json.load(open(patched["ckpt"]))
    assert set(cp2["derived_watermarks"]) == ALL          # whole queue drained
    # Invariant 2: NOW last_run advances (full drain on a completed run)
    assert cp2["last_run"] != ""
    assert cp2["last_run_completed"] is True

    # ── Night 3: nothing due — all entities up to date ──
    patched["calls"].clear()
    patched["state"]["n"] = 0
    _run_nightly(monkeypatch)
    assert patched["calls"] == []


# ── 2. new facts re-open a watermarked entity (2026-05-25 guard) ──

def test_new_facts_reopen_watermarked_entity(patched, monkeypatch):
    """The watermark-dict must NOT silently skip an entity that received new
    facts after being processed — that was the cumulative-set bug the old 1h
    reset tried to paper over."""
    _build_db(patched["db"], [("Acme", "2026-06-10T00:01:00", 5)])

    # Night 1: derive Acme to completion → watermarked
    patched["state"]["kill_at"] = None
    _run_nightly(monkeypatch)
    assert patched["calls"] == ["acme"]

    # A new fact arrives for Acme, newer than its watermark
    db = sqlite3.connect(patched["db"])
    db.execute(
        "INSERT INTO facts (chunk_id, fact, category, entity_tags, event_date, "
        "confidence, created_at, relation_type, derived_from, superseded_by) "
        "VALUES (?,?,?,?,?,?,?,?,?,'')",
        ("c_acme_new", "Acme brand-new fact", "cat", json.dumps(["Acme"]),
         "", 0.9, "2026-06-20T12:00:00", "extracted", ""),
    )
    db.commit()
    db.close()

    # Night 2: Acme is "due" again (max_created_at > watermark) → re-processed
    patched["calls"].clear()
    patched["state"]["n"] = 0
    _run_nightly(monkeypatch)
    assert patched["calls"] == ["acme"]


# ── 3. the script's own derived rows must NOT re-open the entity ──

def test_derived_output_does_not_reopen_entity(patched, monkeypatch):
    """Blocker fix: the watermark is computed over SOURCE facts only. An entity
    whose derivation produced a derived row (created_at=now(), newer than any
    source fact) must NOT re-open the next night solely because of that row."""
    _build_db(patched["db"], [("Acme", "2026-06-10T00:01:00", 5)])

    calls = []

    def derive_with_output(norm, data, existing, manager):
        calls.append(norm)
        sid = data["facts"][0]["id"]
        return [{"fact": "Acme derived insight", "category": "derived",
                 "confidence": 0.9, "_source_fact_ids": [sid]}]

    monkeypatch.setattr(df, "derive_for_entity", derive_with_output)

    # Night 1: derive → a derived row is inserted with created_at=now()
    _run_nightly(monkeypatch)
    assert calls == ["acme"]
    db = sqlite3.connect(patched["db"])
    n_derived = db.execute(
        "SELECT COUNT(*) FROM facts WHERE relation_type='derived'").fetchone()[0]
    db.close()
    assert n_derived >= 1  # the now()-stamped derived row exists

    # Night 2: must stay closed — the only "new" row is its own derived output
    calls.clear()
    _run_nightly(monkeypatch)
    assert calls == []


# ── 4. manual runs are isolated from the nightly checkpoint ────

def test_manual_run_does_not_persist_watermarks(patched, monkeypatch):
    """--top/--entity/full must not touch the nightly state (derived_watermarks
    or last_run)."""
    _build_db(patched["db"], [(f"E{i}", f"2026-06-10T00:0{i}:00", 5) for i in range(1, 4)])

    # Seed an existing nightly checkpoint
    os.makedirs(os.path.dirname(patched["ckpt"]), exist_ok=True)
    seed = {"last_run": "2026-06-01T00:00:00",
            "derived_watermarks": {"preexisting": "2026-05-01T00:00:00"},
            "entities_processed": 99, "facts_derived": 99}
    with open(patched["ckpt"], "w") as f:
        json.dump(seed, f)

    monkeypatch.setattr(sys, "argv", ["derive_facts.py", "--top", "5"])
    df.main()

    # Checkpoint untouched by the manual run
    cp = json.load(open(patched["ckpt"]))
    assert cp == seed
