"""Tests for scripts.lib.backend_manager.BackendManager.

Tests the rotation/exhaustion bookkeeping in isolation: get_enrichment_backends
and get_enrichment_daily_limit are monkeypatched so no real config is loaded,
and api_keys are passed explicitly so no env/plist lookup happens.
"""

from __future__ import annotations

import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from scripts.lib import backend_manager as bm  # noqa: E402


def _patch_cfg(monkeypatch, backends, limit=100):
    monkeypatch.setattr(bm, "get_enrichment_backends", lambda role: list(backends))
    monkeypatch.setattr(bm, "get_enrichment_daily_limit", lambda role: limit)


# ============================================================
# 1. Init: filtering by missing API key
# ============================================================


def test_init_filters_backends_without_api_key(monkeypatch, capsys):
    _patch_cfg(monkeypatch, [
        {"provider": "google", "model": "g", "workers": 5},
        {"provider": "anthropic", "model": "a", "workers": 15},
    ])
    # Only google has a key
    mgr = bm.BackendManager("test_role", api_keys={"google": "g-key", "anthropic": ""})
    assert [b["name"] for b in mgr.backends] == ["google"]
    assert mgr.backends[0]["api_key"] == "g-key"
    err = capsys.readouterr().err
    assert "skipping anthropic" in err


# ============================================================
# 2. current() rotation skips exhausted entries
# ============================================================


def test_current_returns_first_non_exhausted(monkeypatch):
    _patch_cfg(monkeypatch, [
        {"provider": "google", "model": "g", "workers": 5},
        {"provider": "anthropic", "model": "a", "workers": 15},
    ])
    mgr = bm.BackendManager("test_role", api_keys={"google": "k1", "anthropic": "k2"})
    # Manually exhaust google
    mgr.backends[0]["exhausted"] = True
    cur = mgr.current()
    assert cur is not None
    assert cur["name"] == "anthropic"
    assert mgr.current_idx == 1


# ============================================================
# 3. current() returns None when all exhausted
# ============================================================


def test_current_returns_none_when_all_exhausted(monkeypatch):
    _patch_cfg(monkeypatch, [
        {"provider": "google", "model": "g", "workers": 5},
        {"provider": "anthropic", "model": "a", "workers": 15},
    ])
    mgr = bm.BackendManager("test_role", api_keys={"google": "k1", "anthropic": "k2"})
    mgr.mark_rate_limited()  # exhausts google, advances to anthropic
    mgr.mark_rate_limited()  # exhausts anthropic, advances back
    assert mgr.current() is None
    assert mgr.all_exhausted() is True


# ============================================================
# 4. mark_success exhausts on limit (atomic via n=)
# ============================================================


def test_mark_success_exhausts_on_limit(monkeypatch, capsys):
    _patch_cfg(monkeypatch, [
        {"provider": "google", "model": "g", "workers": 5},
        {"provider": "anthropic", "model": "a", "workers": 15},
    ], limit=10)
    mgr = bm.BackendManager("test_role", api_keys={"google": "k1", "anthropic": "k2"})
    # Single calls
    for _ in range(5):
        mgr.mark_success()
    assert mgr.backends[0]["calls"] == 5
    assert mgr.backends[0]["exhausted"] is False
    assert mgr.current_idx == 0
    # Atomic batch: 5 more pushes us to limit (10), exhaust + rotate
    mgr.mark_success(n=5)
    assert mgr.backends[0]["calls"] == 10
    assert mgr.backends[0]["exhausted"] is True
    assert mgr.current_idx == 1
    err = capsys.readouterr().err
    assert "google reached daily limit (10)" in err


# ============================================================
# 5. mark_error rotates without exhausting
# ============================================================


def test_mark_error_rotates_without_exhausting(monkeypatch):
    _patch_cfg(monkeypatch, [
        {"provider": "google", "model": "g", "workers": 5},
        {"provider": "anthropic", "model": "a", "workers": 15},
    ])
    mgr = bm.BackendManager("test_role", api_keys={"google": "k1", "anthropic": "k2"})
    assert mgr.current_idx == 0
    mgr.mark_error()
    assert mgr.current_idx == 1
    # google not exhausted — still eligible
    assert mgr.backends[0]["exhausted"] is False
    # Rotate again — wraps around
    mgr.mark_error()
    assert mgr.current_idx == 0


# ============================================================
# 6. status() formatting
# ============================================================


def test_status_format(monkeypatch):
    _patch_cfg(monkeypatch, [
        {"provider": "google", "model": "g", "workers": 5},
        {"provider": "anthropic", "model": "a", "workers": 15},
    ], limit=100)
    mgr = bm.BackendManager("test_role", api_keys={"google": "k1", "anthropic": "k2"})
    mgr.mark_success(n=7)
    s = mgr.status()
    assert "google:7/100OK" in s
    assert "anthropic:0/100OK" in s
    assert " | " in s
    # Exhaust google
    mgr.backends[0]["exhausted"] = True
    s = mgr.status()
    assert "google:7/100EXH" in s
