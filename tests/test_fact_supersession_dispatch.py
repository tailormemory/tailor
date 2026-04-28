"""Tests for scripts.enrichment.fact_supersession dispatch + rotation logic.

Covers the new BackendManager-driven backend dispatch and the
run_batch() retry-on-rotation behaviour. Provider-specific call_*
functions are monkeypatched so no real HTTP happens.
"""

from __future__ import annotations

import asyncio
import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from scripts.lib import backend_manager as bm  # noqa: E402
from scripts.enrichment import fact_supersession as fs  # noqa: E402


def _patch_cfg(monkeypatch, backends, limit=100):
    monkeypatch.setattr(bm, "get_enrichment_backends", lambda role: list(backends))
    monkeypatch.setattr(bm, "get_enrichment_daily_limit", lambda role: limit)


def _pair(id_a: int = 1, id_b: int = 2):
    """Build a (new, old, sim) tuple in the shape run_batch expects."""
    return (
        {"id": id_a, "fact": "new fact", "event_date": "2026-04-28", "category": "x"},
        {"id": id_b, "fact": "old fact", "event_date": "2026-01-01", "category": "x"},
        0.6,
    )


# ============================================================
# 1. call_backend dispatches to the right call_* per backend['name']
# ============================================================


def test_dispatch_routes_to_provider_per_backend(monkeypatch):
    invoked: list[str] = []

    async def fake_anthropic(session, semaphore, *args, **kwargs):
        invoked.append("anthropic")
        return {"result": "INDEPENDENT", "reason": "ok"}

    async def fake_gemini(session, semaphore, *args, **kwargs):
        invoked.append("google")
        return {"result": "SUPERSEDES", "reason": "ok"}

    async def fake_openai(session, semaphore, *args, **kwargs):
        invoked.append("openai")
        return {"result": "INDEPENDENT", "reason": "ok"}

    monkeypatch.setattr(fs, "call_anthropic", fake_anthropic)
    monkeypatch.setattr(fs, "call_gemini", fake_gemini)
    monkeypatch.setattr(fs, "call_openai", fake_openai)

    async def go():
        for name in ["anthropic", "google", "openai"]:
            backend = {"name": name, "model": "m", "api_key": "k",
                       "workers": 1, "calls": 0, "limit": 100, "exhausted": False}
            r = await fs.call_backend(None, asyncio.Semaphore(1), backend,
                                      "new", "old", "2026-04-28", "2026-01-01")
            assert isinstance(r, dict)
        # Unknown provider returns None
        backend = {"name": "deepseek", "model": "m", "api_key": "k",
                   "workers": 1, "calls": 0, "limit": 100, "exhausted": False}
        r = await fs.call_backend(None, asyncio.Semaphore(1), backend,
                                  "new", "old", "", "")
        assert r is None

    asyncio.run(go())
    assert invoked == ["anthropic", "google", "openai"]


# ============================================================
# 2. run_batch: rate-limit on backend A → rotate, retry on backend B
# ============================================================


def test_rate_limit_rotates_and_retries_once(monkeypatch):
    _patch_cfg(monkeypatch, [
        {"provider": "google", "model": "g", "workers": 5},
        {"provider": "anthropic", "model": "a", "workers": 15},
    ])
    mgr = bm.BackendManager("fact_supersession",
                            api_keys={"google": "k1", "anthropic": "k2"})

    call_log: list[str] = []

    async def fake_call_backend(session, semaphore, backend, *args):
        call_log.append(backend["name"])
        if backend["name"] == "google":
            return "RATE_LIMITED"
        return {"result": "INDEPENDENT", "reason": "from anthropic"}

    monkeypatch.setattr(fs, "call_backend", fake_call_backend)

    async def go():
        return await fs.run_batch(None, asyncio.Semaphore(1), mgr,
                                  [_pair(1, 2)], max_retry=1)

    results = asyncio.run(go())

    assert len(results) == 1
    assert results[0] == {"result": "INDEPENDENT", "reason": "from anthropic"}
    # Google was tried first, then anthropic on retry
    assert call_log == ["google", "anthropic"]
    # Manager state: google exhausted (rate-limited), anthropic has 1 success
    g = next(b for b in mgr.backends if b["name"] == "google")
    a = next(b for b in mgr.backends if b["name"] == "anthropic")
    assert g["exhausted"] is True
    assert a["calls"] == 1
    assert a["exhausted"] is False
