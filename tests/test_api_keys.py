"""Tests for scripts.lib.api_keys — the cross-module key resolver.

This is the integration point between the secrets DB (commits 1-7) and
the LLM client + embedding modules. It's the only place where decryption
errors are allowed to degrade gracefully, so those paths matter.

Hermetic: TAILOR_HOME and TAILOR_SECRETS_DB_PATH point at tmp_path, and
the provider env vars are explicitly delenv'd so inherited shell state
can't leak into assertions.
"""

from __future__ import annotations

import logging
import os
import sys
from unittest.mock import patch

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from scripts.lib import api_keys, secrets_crypto, secrets_store  # noqa: E402
from scripts.lib.api_keys import (  # noqa: E402
    PROVIDER_API_KEY_ENVS,
    env_var_names,
    resolve_api_key,
)

ALL_ENV_VARS = sorted({v for tup in PROVIDER_API_KEY_ENVS.values() for v in tup})


@pytest.fixture(autouse=True)
def hermetic_env(tmp_path, monkeypatch):
    monkeypatch.setenv("TAILOR_HOME", str(tmp_path / "tailor_home"))
    monkeypatch.setenv("TAILOR_SECRETS_DB_PATH", str(tmp_path / "secrets.sqlite3"))
    # Wipe any inherited provider-key env vars so tests control the state.
    for name in ALL_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    secrets_store.reset_master_key_cache()
    yield tmp_path
    secrets_store.reset_master_key_cache()


# ── precedence ────────────────────────────────────────────────────────


def test_resolve_api_key_prefers_db(monkeypatch):
    secrets_crypto.ensure_master_key()
    secrets_store.set_secret("anthropic", "sk-ant-FROM-DB-12345678")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-FROM-ENV-ignored")

    assert resolve_api_key("anthropic") == "sk-ant-FROM-DB-12345678"


def test_resolve_api_key_falls_back_to_env(monkeypatch):
    # DB has no row — env wins.
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-env-only")
    assert resolve_api_key("anthropic") == "sk-ant-env-only"


def test_resolve_api_key_returns_empty_when_neither():
    # No DB key, no env var (fixture delenv'd them).
    assert resolve_api_key("anthropic") == ""
    assert resolve_api_key("openai") == ""
    assert resolve_api_key("google") == ""
    assert resolve_api_key("deepseek") == ""


def test_resolve_api_key_falls_back_on_decryption_error(monkeypatch, caplog):
    secrets_crypto.ensure_master_key()
    secrets_store.set_secret("anthropic", "sk-ant-stored")

    # Simulate master-key-was-rotated-out: get_secret raises.
    def boom(provider):
        raise secrets_crypto.DecryptionError("decryption failed")

    monkeypatch.setattr(secrets_store, "get_secret", boom)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-env-rescue")

    caplog.set_level(logging.ERROR, logger="scripts.lib.api_keys")
    out = resolve_api_key("anthropic")

    assert out == "sk-ant-env-rescue"
    # The error was logged by class name only — no plaintext / no key bytes.
    messages = [r.getMessage() for r in caplog.records]
    assert any("decryption failed" in m for m in messages)
    for record in caplog.records:
        assert "sk-ant-stored" not in record.getMessage()
        for arg in record.args or ():
            assert "sk-ant-stored" not in str(arg)


def test_resolve_api_key_explicit_literal_wins(monkeypatch):
    # A hard-coded key in YAML (what users with `api_key: sk-abc...` have
    # today) must still work without touching the DB/env.
    secrets_crypto.ensure_master_key()
    secrets_store.set_secret("anthropic", "sk-ant-from-db-ignored")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-from-env-ignored")

    out = resolve_api_key("anthropic", explicit="sk-ant-YAML-LITERAL")
    assert out == "sk-ant-YAML-LITERAL"


def test_resolve_api_key_ignores_unresolved_placeholder(monkeypatch):
    # When config.load_config() can't expand ${ANTHROPIC_API_KEY}, it
    # leaves the literal in place. That must NOT block the DB/env path.
    secrets_crypto.ensure_master_key()
    secrets_store.set_secret("anthropic", "sk-ant-from-db-wins")

    out = resolve_api_key("anthropic", explicit="${ANTHROPIC_API_KEY}")
    assert out == "sk-ant-from-db-wins"


def test_resolve_api_key_google_accepts_gemini_env(monkeypatch):
    # New .env import convention uses GEMINI_API_KEY.
    monkeypatch.setenv("GEMINI_API_KEY", "ai-gemini")
    assert resolve_api_key("google") == "ai-gemini"


def test_resolve_api_key_google_falls_back_to_google_env(monkeypatch):
    # Legacy LaunchDaemon plists ship with GOOGLE_API_KEY.
    monkeypatch.setenv("GOOGLE_API_KEY", "ai-google")
    assert resolve_api_key("google") == "ai-google"


def test_resolve_api_key_google_prefers_gemini_over_google(monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "from-gemini")
    monkeypatch.setenv("GOOGLE_API_KEY", "from-google")
    # GEMINI is first in the tuple so it wins — documented precedence.
    assert resolve_api_key("google") == "from-gemini"


def test_resolve_api_key_ollama_always_empty(monkeypatch):
    # Ollama is a local endpoint with no key concept.
    monkeypatch.setenv("ANTHROPIC_API_KEY", "should-not-match")
    assert resolve_api_key("ollama") == ""


def test_resolve_api_key_unknown_provider_passes_explicit_through():
    # We don't know about 'bogus' — if the caller gave a literal, echo it;
    # otherwise return empty. No DB lookup, no env lookup.
    assert resolve_api_key("bogus") == ""
    assert resolve_api_key("bogus", explicit="hard-coded-123") == "hard-coded-123"


# ── coverage guard ────────────────────────────────────────────────────


@pytest.mark.parametrize("provider", sorted(PROVIDER_API_KEY_ENVS.keys()))
def test_resolve_api_key_for_all_providers(provider, monkeypatch):
    """Every listed provider must be resolvable end-to-end from the env
    (or return "" for ollama which has no env var list).
    """
    envs = PROVIDER_API_KEY_ENVS[provider]
    if not envs:
        # Ollama — no key path.
        assert resolve_api_key(provider) == ""
        return
    # Set the first env var in the tuple; resolver must return it.
    monkeypatch.setenv(envs[0], f"key-for-{provider}")
    assert resolve_api_key(provider) == f"key-for-{provider}"


def test_env_var_names_is_public():
    assert list(env_var_names("anthropic")) == ["ANTHROPIC_API_KEY"]
    assert set(env_var_names("google")) == {"GEMINI_API_KEY", "GOOGLE_API_KEY"}
    assert list(env_var_names("ollama")) == []
