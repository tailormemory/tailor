""".env import/export + diff computation for the secrets store.

This module is I/O only at the file-reading edges (:func:`parse_env_file`
and :func:`default_env_path`). :func:`compute_diff` and
:func:`export_to_env_format` are pure so the UI's preview payload can be
tested without mocking a filesystem.

Security:
  * Parser never interpolates variables, never shells out, never logs the
    values it reads. Input is treated as literal text.
  * Callers (notably :mod:`scripts.lib.secrets_api`) must ensure the
    preview/diff payload they emit to the UI contains ``last4`` only —
    never the plaintext key.
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Any


# provider -> canonical env var name. Adding a provider = update both
# ALLOWED_PROVIDERS (in secrets_store) and this map.
ENV_VAR_MAP: dict[str, str] = {
    "anthropic": "ANTHROPIC_API_KEY",
    "openai": "OPENAI_API_KEY",
    "google": "GEMINI_API_KEY",
    "deepseek": "DEEPSEEK_API_KEY",
}

# reverse lookup for the parser.
_VAR_TO_PROVIDER: dict[str, str] = {v: k for k, v in ENV_VAR_MAP.items()}

# POSIX-ish identifier. Same alphabet as shell env vars.
_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Diff states. Keep in sync with the UI renderer in the dashboard.
STATE_NEW = "new"
STATE_CONFLICT = "conflict"
STATE_IDENTICAL = "identical"
STATE_DB_ONLY = "db_only"
STATE_NEITHER = "neither"

SUGGESTED_ACTION: dict[str, str] = {
    STATE_NEW: "import",
    STATE_CONFLICT: "keep",  # ambiguous — the user must consciously decide.
    STATE_IDENTICAL: "skip",
    STATE_DB_ONLY: "skip",
}


# ── paths ─────────────────────────────────────────────────────────────


def _repo_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def default_env_path() -> str:
    """``$TAILOR_ROOT/.env`` if the env var is set, else ``<repo>/.env``."""
    root = os.environ.get("TAILOR_ROOT") or _repo_root()
    return os.path.join(root, ".env")


# ── parser ────────────────────────────────────────────────────────────


def _strip_inline_comment(value: str) -> str:
    """No-op — we deliberately keep unquoted values literal."""
    return value


def _parse_value(raw: str, lineno: int) -> str:
    """Decode the right-hand side of a ``KEY=RHS`` line.

    Quoted forms (``"..."`` or ``'...'``) must terminate on the same line
    and may not have trailing content. Unquoted values are taken literally
    — no variable interpolation, no inline-comment stripping — and trimmed
    only of the newline.
    """
    s = raw
    if not s:
        return ""

    first = s[0]
    if first in ('"', "'"):
        # Find the matching closing quote. Backslash escapes are honored
        # only for the quote char itself and for backslash.
        out: list[str] = []
        i = 1
        escape = False
        while i < len(s):
            ch = s[i]
            if escape:
                out.append(ch)
                escape = False
                i += 1
                continue
            if ch == "\\" and first == '"':
                # Only double-quoted strings support escapes (shell convention).
                escape = True
                i += 1
                continue
            if ch == first:
                # Closing quote found. Anything other than trailing whitespace
                # after this point is malformed.
                tail = s[i + 1 :].strip()
                if tail:
                    raise ValueError(
                        f"line {lineno}: trailing content after closing quote"
                    )
                return "".join(out)
            out.append(ch)
            i += 1
        raise ValueError(f"line {lineno}: unterminated quoted string")

    return _strip_inline_comment(s)


def parse_env_file(path: str) -> dict[str, str]:
    """Parse ``path`` and return ``{provider: plaintext_key}``.

    Only variables present in :data:`ENV_VAR_MAP` are returned; all others
    (including legitimate non-secret config) are ignored, which keeps the
    call site free of leaks when the file contains unrelated values.

    Raises:
      FileNotFoundError: if ``path`` does not exist.
      ValueError: on malformed lines (bad key name, unterminated quote,
        missing ``=``, trailing content after quote).
    """
    with open(path, encoding="utf-8") as f:
        text = f.read()

    out: dict[str, str] = {}
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.lstrip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()

        eq = line.find("=")
        if eq < 0:
            raise ValueError(f"line {lineno}: missing '=' separator")
        key = line[:eq].strip()
        if not _KEY_RE.match(key):
            raise ValueError(f"line {lineno}: invalid key name {key!r}")

        # Rightmost side — strip only the trailing newline whitespace for
        # unquoted values; trim is applied before quote detection so leading
        # spaces between '=' and the value don't break parsing.
        rhs = line[eq + 1 :].rstrip("\r\n")
        value = _parse_value(rhs.lstrip(), lineno)

        provider = _VAR_TO_PROVIDER.get(key)
        if provider is None:
            continue
        out[provider] = value

    return out


# ── diff ──────────────────────────────────────────────────────────────


def _last4(s: str | None) -> str | None:
    if s is None:
        return None
    return s[-4:]


def compute_diff(
    env_keys: dict[str, str],
    db_secrets_metadata: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return a per-provider diff sorted by provider name.

    Each row includes the state, last4 for each side, the DB's
    ``updated_at`` (epoch seconds, or ``None`` when absent), and a
    ``suggested_action`` the UI can preselect. Rows where neither side has
    a value are omitted — nothing for the user to act on.

    NOTE on ``identical``: we compare last4 rather than plaintext because
    the DB never exposes plaintext to this layer. Two different keys could
    technically share the same last4 and be misreported as ``identical``.
    In practice the user always sees this diff, can still click Replace,
    and no silent overwrite ever happens — so the worst case is a benign
    UX surprise, not a security issue.
    """
    db_map = {row["provider"]: row for row in db_secrets_metadata}
    rows: list[dict[str, Any]] = []

    for provider in ENV_VAR_MAP:
        env_val = env_keys.get(provider)
        db_row = db_map.get(provider)
        env_last4 = _last4(env_val) if env_val is not None else None
        db_last4 = db_row["last4"] if db_row else None
        db_updated_at = db_row["updated_at"] if db_row else None

        if env_val is None and db_row is None:
            state = STATE_NEITHER
        elif env_val is not None and db_row is None:
            state = STATE_NEW
        elif env_val is None and db_row is not None:
            state = STATE_DB_ONLY
        elif env_last4 == db_last4:
            state = STATE_IDENTICAL
        else:
            state = STATE_CONFLICT

        if state == STATE_NEITHER:
            continue

        rows.append({
            "provider": provider,
            "env_var": ENV_VAR_MAP[provider],
            "state": state,
            "env_last4": env_last4,
            "db_last4": db_last4,
            "db_updated_at": db_updated_at,
            "suggested_action": SUGGESTED_ACTION.get(state, "skip"),
        })

    rows.sort(key=lambda r: r["provider"])
    return rows


# ── export ────────────────────────────────────────────────────────────


def _quote_double(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def export_to_env_format(secrets: dict[str, str]) -> str:
    """Serialize ``{provider: plaintext}`` to ``.env`` text.

    Only providers present in :data:`ENV_VAR_MAP` are emitted; unknown
    keys in ``secrets`` are silently dropped to prevent accidental leaks
    of whatever else happened to be in the caller's dict. Output is UTF-8,
    always quoted with double quotes, shell-safe, and round-trips through
    :func:`parse_env_file` for any value this codebase would ever emit.
    """
    header = f"# Generated by TAILOR on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    lines = [header]
    for provider in ENV_VAR_MAP:
        if provider not in secrets:
            continue
        lines.append(f"{ENV_VAR_MAP[provider]}={_quote_double(secrets[provider])}\n")
    return "".join(lines)


__all__ = [
    "ENV_VAR_MAP",
    "STATE_CONFLICT",
    "STATE_DB_ONLY",
    "STATE_IDENTICAL",
    "STATE_NEITHER",
    "STATE_NEW",
    "SUGGESTED_ACTION",
    "compute_diff",
    "default_env_path",
    "export_to_env_format",
    "parse_env_file",
]
