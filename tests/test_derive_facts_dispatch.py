"""Tests for scripts.enrichment.derive_facts dispatch + rotation logic.

Covers:
- call_provider routing by backend['name'] to the correct provider HTTP endpoint
- derive_for_entity rotates on transient error (provider A 503 → provider B 200)

Uses requests_mock so no real HTTP happens.
"""

from __future__ import annotations

import os
import sys

import pytest
import requests_mock as rm_module  # noqa: F401 — needed for the requests_mock fixture

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from scripts.lib import backend_manager as bm  # noqa: E402
from scripts.enrichment import derive_facts as df  # noqa: E402


def _patch_cfg(monkeypatch, backends, limit=100):
    monkeypatch.setattr(bm, "get_enrichment_backends", lambda role: list(backends))
    monkeypatch.setattr(bm, "get_enrichment_daily_limit", lambda role: limit)


def _backend(name: str, model: str = "m"):
    return {"name": name, "model": model, "api_key": "k",
            "workers": 5, "calls": 0, "limit": 100, "exhausted": False}


# ============================================================
# 1. call_provider routes anthropic vs google to the right endpoints
# ============================================================


def test_call_provider_routes_to_correct_endpoint(requests_mock):
    requests_mock.post(
        "https://api.anthropic.com/v1/messages",
        json={"content": [{"text": "anthropic-text"}]},
    )
    requests_mock.post(
        "https://generativelanguage.googleapis.com/v1beta/models/gflash:generateContent",
        json={"candidates": [{"content": {"parts": [{"text": "google-text"}]}}]},
    )

    r_a = df.call_provider(_backend("anthropic", "claude-haiku"), "user", "system")
    assert r_a == "anthropic-text"
    last_anthropic = requests_mock.request_history[-1]
    assert last_anthropic.url == "https://api.anthropic.com/v1/messages"
    assert last_anthropic.headers.get("x-api-key") == "k"
    assert last_anthropic.json()["system"] == "system"
    assert last_anthropic.json()["model"] == "claude-haiku"

    r_g = df.call_provider(_backend("google", "gflash"), "user", "system")
    assert r_g == "google-text"
    last_google = requests_mock.request_history[-1]
    assert "generativelanguage.googleapis.com" in last_google.url
    assert "key=k" in last_google.url
    body = last_google.json()
    assert body["systemInstruction"]["parts"][0]["text"] == "system"
    assert body["contents"][0]["parts"][0]["text"] == "user"

    # Unknown provider returns None without HTTP
    n_before = len(requests_mock.request_history)
    r_none = df.call_provider(_backend("deepseek"), "user", "system")
    assert r_none is None
    assert len(requests_mock.request_history) == n_before


# ============================================================
# 2. derive_for_entity rotates on transient error and retries once
# ============================================================


def test_derive_for_entity_rotates_on_error(monkeypatch, requests_mock):
    _patch_cfg(monkeypatch, [
        {"provider": "google", "model": "gflash", "workers": 5},
        {"provider": "anthropic", "model": "claude-haiku", "workers": 15},
    ])
    mgr = bm.BackendManager("fact_derivation",
                            api_keys={"google": "g-key", "anthropic": "a-key"})

    # Google returns 503 (transient); Anthropic returns valid JSON array
    requests_mock.post(
        "https://generativelanguage.googleapis.com/v1beta/models/gflash:generateContent",
        status_code=503,
    )
    requests_mock.post(
        "https://api.anthropic.com/v1/messages",
        json={"content": [{"text": '[{"fact":"derived statement","category":"derived","confidence":0.9,"source_ids":[0]}]'}]},
    )

    entity_data = {
        "names": {"X"},
        "facts": [
            {"id": 100, "fact": "first fact", "category": "c", "event_date": ""},
            {"id": 101, "fact": "second fact", "category": "c", "event_date": ""},
        ],
    }
    result = df.derive_for_entity("x", entity_data, set(), mgr)

    assert len(result) == 1
    assert result[0]["fact"] == "derived statement"
    # Both endpoints should have been hit, in order
    history = requests_mock.request_history
    assert len(history) == 2
    assert "generativelanguage.googleapis.com" in history[0].url
    assert "anthropic.com" in history[1].url
    # google not exhausted (transient error → mark_error, not mark_rate_limited)
    g = next(b for b in mgr.backends if b["name"] == "google")
    a = next(b for b in mgr.backends if b["name"] == "anthropic")
    assert g["exhausted"] is False
    assert a["calls"] == 1
