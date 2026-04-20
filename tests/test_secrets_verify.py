"""Tests for scripts.lib.secrets_verify.

All HTTP is mocked — no real provider traffic. Tests prove both the
happy path and that failure modes (4xx / timeout / network error) never
surface the key in messages and that each provider in ALLOWED_PROVIDERS
has a verifier wired up.
"""

from __future__ import annotations

import logging
import os
import sys
from unittest.mock import MagicMock, patch

import pytest
import requests

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from scripts.lib import secrets_verify  # noqa: E402
from scripts.lib.secrets_store import ALLOWED_PROVIDERS  # noqa: E402
from scripts.lib.secrets_verify import (  # noqa: E402
    VERIFIERS,
    verify_anthropic,
    verify_deepseek,
    verify_google,
    verify_openai,
)


def _ok(status: int = 200, body: dict | None = None) -> MagicMock:
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status
    resp.json.return_value = body if body is not None else {}
    return resp


# ── happy paths ───────────────────────────────────────────────────────


def test_verify_anthropic_success():
    with patch.object(secrets_verify.requests, "post", return_value=_ok(200)) as m:
        verified, err, ms = verify_anthropic("sk-ant-test", timeout=5.0)
    assert verified is True
    assert err is None
    assert ms >= 0
    # Key goes in header, not URL or body.
    kwargs = m.call_args.kwargs
    assert kwargs["headers"]["x-api-key"] == "sk-ant-test"
    assert kwargs["json"]["max_tokens"] == 1
    assert kwargs["json"]["model"] == "claude-haiku-4-5-20251001"


def test_verify_openai_success():
    with patch.object(secrets_verify.requests, "post", return_value=_ok(200)) as m:
        verified, err, _ = verify_openai("sk-oai-test", timeout=5.0)
    assert verified is True
    assert err is None
    assert m.call_args.kwargs["headers"]["Authorization"] == "Bearer sk-oai-test"
    assert m.call_args.kwargs["json"]["model"] == "gpt-4o-mini"


def test_verify_google_success():
    with patch.object(secrets_verify.requests, "post", return_value=_ok(200)) as m:
        verified, err, _ = verify_google("AIzaSyTest", timeout=5.0)
    assert verified is True
    assert err is None
    # Gemini auth is via query param, not header.
    assert m.call_args.kwargs["params"] == {"key": "AIzaSyTest"}
    assert "Authorization" not in m.call_args.kwargs["headers"]


def test_verify_deepseek_success():
    with patch.object(secrets_verify.requests, "post", return_value=_ok(200)) as m:
        verified, err, _ = verify_deepseek("sk-deep-test", timeout=5.0)
    assert verified is True
    assert err is None
    assert m.call_args.kwargs["headers"]["Authorization"] == "Bearer sk-deep-test"
    assert m.call_args.kwargs["json"]["model"] == "deepseek-chat"


# ── failure modes ─────────────────────────────────────────────────────


def test_verify_anthropic_401_returns_error_message():
    body = {"error": {"type": "authentication_error", "message": "invalid x-api-key"}}
    with patch.object(secrets_verify.requests, "post", return_value=_ok(401, body)):
        verified, err, ms = verify_anthropic("sk-bad", timeout=5.0)
    assert verified is False
    assert err is not None
    assert "invalid x-api-key" in err
    assert "sk-bad" not in err
    assert ms >= 0


def test_verify_anthropic_timeout_returns_timeout_message():
    with patch.object(
        secrets_verify.requests,
        "post",
        side_effect=requests.exceptions.Timeout(),
    ):
        verified, err, ms = verify_anthropic("sk-ant-test", timeout=7.0)
    assert verified is False
    assert err == "Timeout after 7s"
    assert ms >= 0


def test_verify_network_error_returns_generic_message():
    with patch.object(
        secrets_verify.requests,
        "post",
        side_effect=requests.exceptions.ConnectionError("connection refused"),
    ):
        verified, err, _ = verify_openai("sk-oai-test", timeout=5.0)
    assert verified is False
    # Message must not include the key or the URL repr — just the class name.
    assert "ConnectionError" in err
    assert "sk-oai-test" not in err


def test_verify_handles_non_json_error_body():
    resp = MagicMock(spec=requests.Response)
    resp.status_code = 500
    resp.json.side_effect = ValueError("no json")
    with patch.object(secrets_verify.requests, "post", return_value=resp):
        verified, err, _ = verify_openai("sk-oai-test")
    assert verified is False
    assert err == "HTTP 500"


# ── security / coverage ───────────────────────────────────────────────


def test_verifier_never_logs_key(caplog):
    """No log record should contain the key as a substring, for any outcome."""
    caplog.set_level(logging.DEBUG, logger=secrets_verify.log.name)

    key = "sk-ULTRA-SECRET-should-never-appear-in-logs"

    # Success
    with patch.object(secrets_verify.requests, "post", return_value=_ok(200)):
        verify_anthropic(key)
    # 4xx
    with patch.object(
        secrets_verify.requests,
        "post",
        return_value=_ok(401, {"error": {"message": "bad"}}),
    ):
        verify_openai(key)
    # Timeout
    with patch.object(
        secrets_verify.requests,
        "post",
        side_effect=requests.exceptions.Timeout(),
    ):
        verify_google(key)
    # Network error
    with patch.object(
        secrets_verify.requests,
        "post",
        side_effect=requests.exceptions.ConnectionError(f"connect to https://x?key={key}"),
    ):
        verify_deepseek(key)

    for record in caplog.records:
        assert key not in record.getMessage()
        for arg in record.args or ():
            assert key not in str(arg)


def test_verify_all_providers_have_entry_in_VERIFIERS():
    assert set(VERIFIERS.keys()) == ALLOWED_PROVIDERS
    for fn in VERIFIERS.values():
        assert callable(fn)


@pytest.mark.parametrize("provider", sorted(ALLOWED_PROVIDERS))
def test_verifier_returns_three_tuple(provider):
    with patch.object(secrets_verify.requests, "post", return_value=_ok(200)):
        result = VERIFIERS[provider]("fake-key-long-enough")
    assert isinstance(result, tuple)
    assert len(result) == 3
    verified, err, ms = result
    assert isinstance(verified, bool)
    assert err is None
    assert isinstance(ms, int)
