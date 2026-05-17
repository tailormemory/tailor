"""Tests for the mcp_server.py write paths that wrap verified_upsert.

A1 Blocco 2 — these tests exercise the 3 callsites that wrap
verified_upsert with retry=2:
  - kb_add                  → string return (MCP tool contract)
  - kb_update_session       → string return (MCP tool contract)
  - /api/ingest-live        → JSON response (HTTP endpoint)

ASGI end-to-end deferred — follow-up, non coperto da A1. The
/api/ingest-live coverage here is a source-level guard against
regression of the wrap pattern (verified_upsert call shape +
failure response shape); the actual ASGI handler invocation
will be added in a follow-up that introduces a Starlette
TestClient against the mcp_server app.
"""

from __future__ import annotations

import datetime as _dt
import os
import re
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts", "lib"))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import mcp_server  # noqa: E402
import ingest_helpers as ih  # noqa: E402
from tests.helpers.chromadb_fake import FakeCollection  # noqa: E402


# ============================================================
# Common fixture: patch mcp_server's chromadb + embedding + entity
# dependencies so kb_add and kb_update_session run against a
# FakeCollection-backed env without touching the real KB.
# ============================================================


@pytest.fixture
def fake_kb(monkeypatch):
    """Patch mcp_server's collection / embedding / entity helpers.

    Returns a dict with:
      coll                 — the FakeCollection backing get_collection
      entity_index_calls   — list of (chunk_id, entities, types) tuples
                             captured from _update_entity_index calls
    Also patches ingest_helpers.time.sleep so retry backoff does not
    add real wallclock to the silent-drop tests.
    """
    coll = FakeCollection()
    monkeypatch.setattr(mcp_server, "get_collection", lambda: coll)
    monkeypatch.setattr(mcp_server, "get_embedding", lambda text: [0.1, 0.2, 0.3, 0.4])
    monkeypatch.setattr(
        mcp_server, "_extract_entities_inline",
        lambda text: (["TestEntity"], ["misc"]),
    )
    entity_index_calls: list = []
    monkeypatch.setattr(
        mcp_server, "_update_entity_index",
        lambda cid, ents, typs: entity_index_calls.append(
            (cid, list(ents), list(typs))
        ),
    )
    monkeypatch.setattr(ih.time, "sleep", lambda d: None)
    return {"coll": coll, "entity_index_calls": entity_index_calls}


# ============================================================
# kb_add
# ============================================================


def test_kb_add_silent_drop_returns_explicit_error(fake_kb):
    """verified_upsert returns False → kb_add returns explicit string error
    that the LLM sees verbatim."""
    fake_kb["coll"].program_get(
        {"ids": []}, {"ids": []}, {"ids": []},  # 3 attempts all see drop
    )
    result = mcp_server.kb_add(content="something to save", title="My fact")
    assert isinstance(result, str)
    assert result.startswith("Error saving: KB write verification failed")
    assert "3 attempts" in result
    assert "silent-drop" in result
    assert "NOT persisted" in result


def test_kb_add_success_calls_entity_index(fake_kb):
    """Happy path: verified_upsert succeeds → kb_add returns 'Saved to KB:'
    AND _update_entity_index is called for the persisted chunk."""
    result = mcp_server.kb_add(content="x", title="y")
    assert result.startswith("Saved to KB:")
    assert len(fake_kb["entity_index_calls"]) == 1
    cid, ents, typs = fake_kb["entity_index_calls"][0]
    assert ents == ["TestEntity"]
    assert typs == ["misc"]
    # The chunk_id passed to entity_index matches what was written to chromadb
    assert cid in fake_kb["coll"].sql


def test_kb_add_silent_drop_skips_entity_index(fake_kb):
    """verified_upsert failure → _update_entity_index NOT called.

    This is the test that proves A1 does not open a twin dangling-
    reference hole in the entity index when persistence fails. The
    side-effect ordering moved _update_entity_index INSIDE the success
    branch for exactly this reason."""
    fake_kb["coll"].program_get(
        {"ids": []}, {"ids": []}, {"ids": []},
    )
    result = mcp_server.kb_add(content="x", title="y")
    assert result.startswith("Error saving:")
    assert fake_kb["entity_index_calls"] == []


# ============================================================
# kb_update_session
# ============================================================


def test_kb_update_session_silent_drop_returns_explicit_error(fake_kb):
    """verified_upsert failure → 'Error saving session: ...' string."""
    fake_kb["coll"].program_get(
        {"ids": []}, {"ids": []}, {"ids": []},
    )
    result = mcp_server.kb_update_session(summary="s", key_facts="k")
    assert isinstance(result, str)
    assert result.startswith("Error saving session: KB write verification failed")
    assert "3 attempts" in result
    assert "silent-drop" in result
    assert "Session NOT persisted" in result


def test_kb_update_session_silent_drop_skips_entity_index(fake_kb):
    """Twin hole guard for kb_update_session (same as kb_add)."""
    fake_kb["coll"].program_get(
        {"ids": []}, {"ids": []}, {"ids": []},
    )
    mcp_server.kb_update_session(summary="s", key_facts="k")
    assert fake_kb["entity_index_calls"] == []


def test_kb_update_session_duplicate_id_overwrites(fake_kb, monkeypatch):
    """Two kb_update_session calls in the same second collide on
    chunk_id (which is {prefix}_session_YYYYMMDD_HHMMSS — second
    resolution, NO content hash).

    Pre-A1 with collection.add(): the 2nd call raised
    IDAlreadyExistsError → kb_update_session returned
    'Error saving session: ...'.
    Post-A1 with verified_upsert wrapping collection.upsert(): the 2nd
    call silently OVERWRITES the 1st → 'Sessione salvata in KB ...'.

    This test cements that behaviour change as INTENTIONAL. Root cause
    of the collision is chunk_id granularity (second-resolution
    without a content hash), not upsert(); see the follow-up comment
    above the kb_update_session callsite. A finer-grained chunk_id is
    the real fix and is out of A1 scope.
    """
    # Freeze datetime.now() so both calls produce the same chunk_id.
    fixed_now = _dt.datetime(2026, 5, 16, 21, 0, 0)

    class FrozenDatetime(_dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr(mcp_server, "datetime", FrozenDatetime)

    r1 = mcp_server.kb_update_session(summary="first summary", key_facts="f1")
    r2 = mcp_server.kb_update_session(summary="second summary", key_facts="f2")

    # Both calls return success — the 2nd is NOT an error.
    assert r1.startswith("Sessione salvata in KB"), r1
    assert r2.startswith("Sessione salvata in KB"), r2

    # Only one chunk_id present in chromadb state — overwrite confirmed.
    assert len(fake_kb["coll"].sql) == 1
    only_id = next(iter(fake_kb["coll"].sql))
    assert only_id.endswith("_session_20260516_210000")

    # Both upsert attempts were recorded by the fake (proves the 2nd
    # call actually went through, didn't short-circuit on duplicate).
    assert len(fake_kb["coll"].upsert_calls) == 2


# ============================================================
# /api/ingest-live — ASGI end-to-end deferred
# ============================================================
# The /api/ingest-live wrap mirrors the kb_add / kb_update_session
# pattern with retry=2, retry_backoff=0.3 and a JSON response on
# failure. Full ASGI end-to-end testing (Starlette TestClient against
# the mcp_server app) is deferred — follow-up, non coperto da A1.
# Below is a source-level guard against pattern regression: the wrap
# shape and failure response shape stay consistent with the design.


def test_ingest_live_callsite_wraps_verified_upsert():
    """Source-level guard for the /api/ingest-live wrap.

    ASGI end-to-end deferred — follow-up, non coperto da A1.

    This test reads mcp_server.py and asserts the /api/ingest-live
    block contains:
      - the verified_upsert wrap with retry=2, retry_backoff=0.3,
        source="ingest_live";
      - the failure response shape ('persisted': False, 500,
        message including 'silent-drop' and 'verification failed').
    Regression here means the wrap was undone or the JSON contract
    with the browser extension drifted.
    """
    with open(os.path.join(ROOT, "mcp_server.py"), encoding="utf-8") as f:
        src = f.read()
    # Locate the /api/ingest-live branch.
    # Tolerant to quoting/spacing reformat of mcp_server.py.
    anchor_match = re.search(r'path\s*==\s*[\'"]/api/ingest-live[\'"]', src)
    assert anchor_match is not None, "ingest-live handler not found in mcp_server.py"
    anchor = anchor_match.start()
    block = src[anchor:]
    # Cut at the next elif route (heuristic but stable for this branch).
    next_elif = block.find("elif path ==")
    if next_elif != -1:
        block = block[:next_elif]

    # Wrap shape
    assert "verified_upsert(" in block
    assert re.search(r'source\s*=\s*[\'"]ingest_live[\'"]', block), \
        'verified_upsert call missing source="ingest_live"'
    assert re.search(r'retry\s*=\s*2\b', block), \
        "verified_upsert call missing retry=2"
    assert re.search(r'retry_backoff\s*=\s*0\.3\b', block), \
        "verified_upsert call missing retry_backoff=0.3"

    # Failure response shape (contract with the browser extension)
    assert re.search(r'[\'"]persisted[\'"]\s*:\s*False\b', block), \
        'failure response missing "persisted": False'
    assert "silent-drop" in block
    assert "verification failed" in block
