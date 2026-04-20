"""Per-provider "test connection" helpers.

Each verifier hits the provider's cheapest completion endpoint with
``max_tokens: 1`` and a one-token prompt. The goal is to answer a single
question: does this key authenticate? Latency and response body shape are
only interesting in so far as we need to distinguish "HTTP auth failure"
from "transport failure".

Security:
  * Keys go in headers (``x-api-key`` / ``Authorization``) or, for Gemini,
    a query string. ``requests`` does not log either by default; we also
    never build an error message that includes the key bytes.
  * The only thing a verifier logs is a provider-level debug line with
    HTTP status and elapsed ms — never the key, never the URL (the Gemini
    URL embeds the key).
  * On non-2xx responses we parse the JSON body for a human message if
    possible, truncating to 200 chars so a wall of HTML can't leak into
    the UI toast.

Each function returns ``(verified: bool, error: str | None, latency_ms: int)``.
``error`` is ``None`` on success and a short human-readable message otherwise
(UI renders it directly). A ``None`` error with ``verified=False`` would be
a bug — callers can assert on it.
"""

from __future__ import annotations

import logging
import time
from typing import Callable

import requests


log = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 10.0


def _elapsed_ms(start: float) -> int:
    return int((time.monotonic() - start) * 1000)


def _parse_error_message(resp: "requests.Response", fallback: str) -> str:
    """Extract a short, user-facing error from a non-2xx response.

    Providers differ on where the message lives:
      * Anthropic / OpenAI / DeepSeek → ``error.message``
      * Google Gemini                → ``error.message`` (same shape)
    We also handle plain-string ``error`` fields and fall back to the HTTP
    status line. Output is capped at 200 chars to keep UI toasts sane.
    """
    try:
        data = resp.json()
    except Exception:
        return fallback
    err = data.get("error") if isinstance(data, dict) else None
    msg: str | None = None
    if isinstance(err, dict):
        msg = err.get("message") or err.get("type")
    elif isinstance(err, str):
        msg = err
    if not msg:
        msg = fallback
    return str(msg)[:200]


def _run(
    provider: str,
    request_fn: Callable[[], "requests.Response"],
    timeout: float,
) -> tuple[bool, str | None, int]:
    """Shared control flow: time the call, classify the outcome.

    ``request_fn`` is a zero-arg callable that returns a ``requests.Response``
    (or raises). Factoring it this way keeps each provider helper to just
    the URL/headers/body assembly — no duplicated try/except ladders.
    """
    start = time.monotonic()
    try:
        resp = request_fn()
    except requests.exceptions.Timeout:
        ms = _elapsed_ms(start)
        log.debug("verify %s timeout after %dms", provider, ms)
        return False, f"Timeout after {int(timeout)}s", ms
    except requests.exceptions.RequestException as e:
        ms = _elapsed_ms(start)
        log.debug("verify %s network error after %dms", provider, ms)
        # Strip the URL from the exception repr — Gemini's URL carries the key.
        return False, f"Network error: {e.__class__.__name__}", ms

    ms = _elapsed_ms(start)
    log.debug("verify %s http=%s in %dms", provider, resp.status_code, ms)
    if 200 <= resp.status_code < 300:
        return True, None, ms
    fallback = f"HTTP {resp.status_code}"
    return False, _parse_error_message(resp, fallback), ms


# ── providers ─────────────────────────────────────────────────────────


def verify_anthropic(
    key: str, timeout: float = _DEFAULT_TIMEOUT
) -> tuple[bool, str | None, int]:
    """POST /v1/messages with claude-haiku-4-5 and max_tokens=1."""

    def call() -> "requests.Response":
        return requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 1,
                "messages": [{"role": "user", "content": "ok"}],
            },
            timeout=timeout,
        )

    return _run("anthropic", call, timeout)


def verify_openai(
    key: str, timeout: float = _DEFAULT_TIMEOUT
) -> tuple[bool, str | None, int]:
    """POST /v1/chat/completions with gpt-4o-mini and max_tokens=1."""

    def call() -> "requests.Response":
        return requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {key}",
                "content-type": "application/json",
            },
            json={
                "model": "gpt-4o-mini",
                "max_tokens": 1,
                "messages": [{"role": "user", "content": "ok"}],
            },
            timeout=timeout,
        )

    return _run("openai", call, timeout)


def verify_google(
    key: str, timeout: float = _DEFAULT_TIMEOUT
) -> tuple[bool, str | None, int]:
    """POST gemini-2.5-flash-lite:generateContent with maxOutputTokens=1.

    Gemini's auth is a ``?key=`` query param rather than a header — the
    key must never leak into logs. ``_run`` converts exceptions to class
    names, and we don't log the URL anywhere.
    """

    def call() -> "requests.Response":
        return requests.post(
            "https://generativelanguage.googleapis.com/v1beta/models/"
            "gemini-2.5-flash-lite:generateContent",
            params={"key": key},
            headers={"content-type": "application/json"},
            json={
                "contents": [{"parts": [{"text": "ok"}]}],
                "generationConfig": {"maxOutputTokens": 1},
            },
            timeout=timeout,
        )

    return _run("google", call, timeout)


def verify_deepseek(
    key: str, timeout: float = _DEFAULT_TIMEOUT
) -> tuple[bool, str | None, int]:
    """POST /chat/completions with deepseek-chat and max_tokens=1."""

    def call() -> "requests.Response":
        return requests.post(
            "https://api.deepseek.com/chat/completions",
            headers={
                "Authorization": f"Bearer {key}",
                "content-type": "application/json",
            },
            json={
                "model": "deepseek-chat",
                "max_tokens": 1,
                "messages": [{"role": "user", "content": "ok"}],
            },
            timeout=timeout,
        )

    return _run("deepseek", call, timeout)


VERIFIERS: dict[str, Callable[..., tuple[bool, str | None, int]]] = {
    "anthropic": verify_anthropic,
    "openai": verify_openai,
    "google": verify_google,
    "deepseek": verify_deepseek,
}


__all__ = [
    "VERIFIERS",
    "verify_anthropic",
    "verify_deepseek",
    "verify_google",
    "verify_openai",
]
