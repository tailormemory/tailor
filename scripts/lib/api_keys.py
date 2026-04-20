"""Single entry point for provider API-key resolution.

Callers (:mod:`scripts.lib.llm_client`, :mod:`scripts.lib.embedding`)
use :func:`resolve_api_key` every time they need a key — no caching
here beyond what :mod:`scripts.lib.secrets_store` already keeps around
the master key. Resolution is intentionally cheap (one SQLite read +
AES-GCM decrypt), so per-request resolution always reflects the latest
DB state without a stale-cache class of bugs.

Precedence:

1. Explicit literal from config — if YAML contains a hard-coded key,
   honour it. Unresolved ``${NAME}`` placeholders (the ones our config
   loader leaves in place when an env var is unset) do NOT count as
   literal; those fall through to the DB/env path.
2. Encrypted secrets DB — the dashboard-managed source of truth.
3. Env var — last-resort fallback. Non-negotiable: current installs
   ship with LaunchDaemon plist env vars and must keep working.

Decryption failures (bad master key, tampering, rotation mismatch)
are caught and logged by *class name only*; the request gracefully
degrades to the env var rather than crashing so a broken master key
never takes the whole system down.
"""

from __future__ import annotations

import logging
import os
from typing import Iterable


log = logging.getLogger(__name__)


# Per-provider env vars we consult as the last-resort fallback.
#
# Google is tricky: the legacy install uses ``GOOGLE_API_KEY`` (what
# llm_client.py has always used) while the new .env import UI expects
# ``GEMINI_API_KEY`` (the name Google's own docs now recommend). We
# accept both — GEMINI first so a new user who followed the import UI
# wins, GOOGLE second so existing LaunchDaemon plist setups keep
# working. Ollama has no key — it's a local endpoint.
PROVIDER_API_KEY_ENVS: dict[str, tuple[str, ...]] = {
    "anthropic": ("ANTHROPIC_API_KEY",),
    "openai":    ("OPENAI_API_KEY",),
    "google":    ("GEMINI_API_KEY", "GOOGLE_API_KEY"),
    "deepseek":  ("DEEPSEEK_API_KEY",),
    "ollama":    (),
}


def _looks_like_unresolved_placeholder(value: str) -> bool:
    """True for ``${NAME}`` strings our config loader left in place.

    The loader substitutes env refs at load time and falls back to the
    original ``${NAME}`` literal when the env var is missing. Treat that
    case the same as "no key" so the DB/env path gets a chance.
    """
    return value.startswith("${") and value.endswith("}")


def _env_lookup(provider: str) -> str:
    for name in PROVIDER_API_KEY_ENVS.get(provider, ()):
        val = os.environ.get(name)
        if val:
            return val
    return ""


def _db_lookup(provider: str) -> str | None:
    """Best-effort DB read. Import locally so this module is cheap to
    load even when the secrets stack is cold.

    Returns:
      * the plaintext key on success
      * ``None`` when no row exists or the DB is missing
      * ``None`` when decryption fails (logged, not raised)
    """
    try:
        from scripts.lib import secrets_store
    except Exception:
        return None
    try:
        return secrets_store.get_secret(provider)
    except Exception as e:
        # Class name only — never the master key state, row bytes, or
        # anything that could help an attacker distinguish failure modes.
        log.error(
            "secrets decryption failed for provider=%s error=%s",
            provider, type(e).__name__,
        )
        return None


def resolve_api_key(provider: str, explicit: str = "") -> str:
    """Return the best-available API key for ``provider``, or ``""``.

    ``explicit`` is whatever ``cfg["api_key"]`` held at client-construction
    time — if it's a resolved literal (i.e. not an ``${NAME}`` placeholder
    that didn't expand), we trust it and return immediately so users who
    hard-coded a key in YAML keep working exactly as before.
    """
    if provider not in PROVIDER_API_KEY_ENVS:
        # Unknown provider — nothing to resolve. Caller (LLMClient) is
        # responsible for raising a meaningful error if the key is needed.
        return explicit if explicit else ""

    if explicit and not _looks_like_unresolved_placeholder(explicit):
        return explicit

    db_key = _db_lookup(provider)
    if db_key:
        return db_key

    return _env_lookup(provider)


def env_var_names(provider: str) -> Iterable[str]:
    """Expose the env var list so callers (e.g. error messages) can cite
    the exact names to set without hard-coding them in two places."""
    return PROVIDER_API_KEY_ENVS.get(provider, ())


__all__ = [
    "PROVIDER_API_KEY_ENVS",
    "env_var_names",
    "resolve_api_key",
]
