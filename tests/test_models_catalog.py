"""Tests for scripts/lib/models_catalog.py.

Covers the four per-provider fetchers (Anthropic, OpenAI, Google, DeepSeek),
the 1-hour in-memory cache, upstream-error translation, and the 503-missing-key
path. Uses requests_mock so no real network calls are made.
"""

from __future__ import annotations

import os
import sys

import pytest
import requests_mock as rm_module  # noqa: F401 — needed so the requests_mock fixture is available

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from scripts.lib import models_catalog  # noqa: E402


def _cfg_get(data: dict):
    """Build a cfg_get callable mirroring scripts.lib.config.get's signature."""

    def cfg_get(section, key=None, default=None):
        s = data.get(section, {})
        if key is None:
            return s if s else default
        return s.get(key, default) if isinstance(s, dict) else default

    return cfg_get


@pytest.fixture(autouse=True)
def _clear_cache_and_isolate_keys(monkeypatch):
    """Every test starts with an empty cache, zero provider env vars, and a
    stubbed secrets-DB lookup. That way the explicit ``api_key`` we put in
    the test cfg is the ONLY source of keys — no real DB or env bleed-through.
    """
    models_catalog.clear_cache()
    for var in (
        "ANTHROPIC_API_KEY",
        "OPENAI_API_KEY",
        "GOOGLE_API_KEY",
        "GEMINI_API_KEY",
        "DEEPSEEK_API_KEY",
    ):
        monkeypatch.delenv(var, raising=False)
    from scripts.lib import api_keys
    monkeypatch.setattr(api_keys, "_db_lookup", lambda _p: None)
    yield
    models_catalog.clear_cache()


# ── 1. Anthropic happy path ────────────────────────────────────

def test_anthropic_happy_path_uses_api_display_name(requests_mock):
    requests_mock.get(
        "https://api.anthropic.com/v1/models",
        json={
            "data": [
                {"id": "claude-opus-4-7", "display_name": "Claude Opus 4.7"},
                {"id": "claude-sonnet-4-6", "display_name": "Claude Sonnet 4.6"},
                {"id": "claude-haiku-4-5", "display_name": "Claude Haiku 4.5"},
            ]
        },
    )
    cfg = _cfg_get({"llm": {"provider": "anthropic", "api_key": "sk-ant-test"}})
    out = models_catalog.get_models("anthropic", cfg_get=cfg)

    assert out["provider"] == "anthropic"
    assert out["cached"] is False
    ids = [m["id"] for m in out["models"]]
    # Sorted descending by ID — opus > sonnet > haiku lexicographically.
    assert ids == ["claude-sonnet-4-6", "claude-opus-4-7", "claude-haiku-4-5"]
    display_by_id = {m["id"]: m["display_name"] for m in out["models"]}
    assert display_by_id["claude-opus-4-7"] == "Claude Opus 4.7"


# ── 2. OpenAI filter ───────────────────────────────────────────

def test_openai_filter_keeps_only_chat_models(requests_mock):
    raw = [
        "gpt-4o",
        "gpt-4o-mini",
        "gpt-4o-audio-preview",
        "gpt-4o-realtime-preview",
        "gpt-4o-transcribe",
        "gpt-image-1",
        "gpt-4o-search-preview",
        "gpt-3.5-turbo",
        "o1",
        "o1-pro",
        "o3-mini",
        "tts-1",
        "tts-1-hd",
        "whisper-1",
        "text-embedding-3-small",
        "text-embedding-3-large",
        "omni-moderation-latest",
        "dall-e-3",
        "chatgpt-4o-latest",      # does not start with gpt- or o<digit> → filtered
        "babbage-002",            # same — filtered
    ]
    requests_mock.get(
        "https://api.openai.com/v1/models",
        json={"data": [{"id": i} for i in raw]},
    )
    cfg = _cfg_get({"llm": {"provider": "openai", "api_key": "sk-oai-test"}})
    out = models_catalog.get_models("openai", cfg_get=cfg)

    ids = {m["id"] for m in out["models"]}
    assert ids == {"gpt-4o", "gpt-4o-mini", "gpt-3.5-turbo", "o1", "o1-pro", "o3-mini"}
    # Spot-check display name derivation.
    by_id = {m["id"]: m["display_name"] for m in out["models"]}
    assert by_id["gpt-4o-mini"] == "GPT 4o Mini"
    assert by_id["o1-pro"] == "O1 Pro"


# ── 3. Google filter ───────────────────────────────────────────

def test_google_filter_keeps_only_chat_gemini(requests_mock):
    raw = [
        "models/gemini-2.5-pro",
        "models/gemini-2.5-flash",
        "models/gemini-2.0-flash",
        "models/gemini-1.5-pro",
        "models/gemini-1.5-flash",
        "models/gemini-2.0-flash-image-generation",
        "models/gemini-2.5-pro-preview-tts",
        "models/gemini-2.5-flash-preview-tts",
        "models/gemini-robotics-er-1.5-preview",
        "models/gemini-2.5-computer-use-preview",
        "models/gemini-embedding-001",
        "models/embedding-001",               # no gemini- prefix → filtered
        "models/text-bison-001",              # no gemini- prefix → filtered
        "models/imagen-3.0-generate-002",     # no gemini- prefix → filtered
        "models/aqa",                         # no gemini- prefix → filtered
    ]
    requests_mock.get(
        "https://generativelanguage.googleapis.com/v1beta/models",
        json={"models": [{"name": n} for n in raw]},
    )
    cfg = _cfg_get({"llm": {"provider": "google", "api_key": "ai-studio-key"}})
    out = models_catalog.get_models("google", cfg_get=cfg)

    ids = {m["id"] for m in out["models"]}
    assert ids == {
        "gemini-2.5-pro",
        "gemini-2.5-flash",
        "gemini-2.0-flash",
        "gemini-1.5-pro",
        "gemini-1.5-flash",
    }
    # "models/" prefix stripped.
    assert all(not i.startswith("models/") for i in ids)
    by_id = {m["id"]: m["display_name"] for m in out["models"]}
    assert by_id["gemini-2.5-pro"] == "Gemini 2.5 Pro"


# ── 4. DeepSeek filter ─────────────────────────────────────────

def test_deepseek_filter_excludes_embedding(requests_mock):
    requests_mock.get(
        "https://api.deepseek.com/v1/models",
        json={
            "data": [
                {"id": "deepseek-chat"},
                {"id": "deepseek-reasoner"},
                {"id": "deepseek-embedding"},   # synthetic — must be filtered
            ]
        },
    )
    cfg = _cfg_get({"llm": {"provider": "deepseek", "api_key": "ds-test"}})
    out = models_catalog.get_models("deepseek", cfg_get=cfg)

    ids = {m["id"] for m in out["models"]}
    assert ids == {"deepseek-chat", "deepseek-reasoner"}
    assert "deepseek-embedding" not in ids
    by_id = {m["id"]: m["display_name"] for m in out["models"]}
    assert by_id["deepseek-chat"] == "DeepSeek Chat"


# ── 5. Cache hit ──────────────────────────────────────────────

def test_cache_hit_within_ttl_avoids_second_upstream_call(requests_mock):
    mocked = requests_mock.get(
        "https://api.anthropic.com/v1/models",
        json={"data": [{"id": "claude-opus-4-7", "display_name": "Claude Opus 4.7"}]},
    )
    cfg = _cfg_get({"llm": {"provider": "anthropic", "api_key": "sk-ant-test"}})

    first = models_catalog.get_models("anthropic", cfg_get=cfg, now=lambda: 1000.0)
    second = models_catalog.get_models("anthropic", cfg_get=cfg, now=lambda: 1500.0)

    assert mocked.call_count == 1
    assert first["cached"] is False
    assert second["cached"] is True
    # Cached timestamp equals the first-call timestamp, not the second.
    assert second["cached_at"] == first["cached_at"]


# ── 6. Cache expiry ───────────────────────────────────────────

def test_cache_expiry_refetches_after_ttl(requests_mock):
    mocked = requests_mock.get(
        "https://api.anthropic.com/v1/models",
        json={"data": [{"id": "claude-opus-4-7", "display_name": "Claude Opus 4.7"}]},
    )
    cfg = _cfg_get({"llm": {"provider": "anthropic", "api_key": "sk-ant-test"}})

    models_catalog.get_models("anthropic", cfg_get=cfg, now=lambda: 1000.0)
    # 1 second past the 1-hour TTL.
    models_catalog.get_models("anthropic", cfg_get=cfg, now=lambda: 1000.0 + 3601)

    assert mocked.call_count == 2


# ── 7. Upstream error → 502 ───────────────────────────────────

def test_upstream_error_translates_to_502(requests_mock):
    requests_mock.get(
        "https://api.anthropic.com/v1/models",
        status_code=401,
        json={"error": {"type": "authentication_error", "message": "invalid x-api-key"}},
    )
    cfg = _cfg_get({"llm": {"provider": "anthropic", "api_key": "sk-ant-bad"}})

    body, status = models_catalog.handle_request("provider=anthropic", cfg_get=cfg)

    assert status == 502
    assert body["error"] == "upstream anthropic returned 401"
    assert "detail" in body
    assert "authentication_error" in body["detail"]


# ── 8. Missing API key → 503 ──────────────────────────────────

def test_missing_api_key_returns_503(requests_mock):
    # Config does not reference anthropic anywhere, and env vars are scrubbed
    # by the autouse fixture, so resolve_api_key returns "".
    cfg = _cfg_get({"llm": {"provider": "openai", "api_key": "sk-oai-test"}})

    body, status = models_catalog.handle_request("provider=anthropic", cfg_get=cfg)

    assert status == 503
    assert body == {"error": "API key for anthropic not configured"}
    # Ensure we never even attempted the upstream call.
    assert requests_mock.call_count == 0
