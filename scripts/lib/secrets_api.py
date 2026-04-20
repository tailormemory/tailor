"""HTTP handlers for /api/secrets/*.

Thin layer between ``mcp_server._handle_rest_api`` and the storage/crypto
modules. Handlers parse already-decoded JSON, return payload dicts on
success, and raise :class:`SecretsApiError` on failure — the ASGI glue
converts both into the ``{ok, error, status}`` envelope.

This mirrors the ``scripts.lib.config_runtime`` / ``ConfigSaveError``
pattern so the contract across dashboard endpoints stays identical.
"""

from __future__ import annotations

import os
from typing import Any

from scripts.lib import secrets_crypto, secrets_store
from scripts.lib.secrets_crypto import DecryptionError
from scripts.lib.secrets_store import ALLOWED_PROVIDERS
from scripts.lib.secrets_verify import VERIFIERS


_MIN_KEY_LEN = 8
_BACKUP_SUBDIR = ".secrets_backups"


class SecretsApiError(Exception):
    """Transport-layer error with an HTTP status + human message."""

    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


def default_backups_dir() -> str:
    """Sibling of the live secrets DB (``db/.secrets_backups/``).

    Resolved at call time so tests that monkeypatch ``TAILOR_SECRETS_DB_PATH``
    automatically get a test-local backup dir too.
    """
    db_path = secrets_store.get_secrets_db_path()
    return os.path.join(
        os.path.dirname(os.path.abspath(db_path)) or ".", _BACKUP_SUBDIR
    )


def _require_provider(provider: Any) -> str:
    if not isinstance(provider, str) or provider not in ALLOWED_PROVIDERS:
        allowed = ", ".join(sorted(ALLOWED_PROVIDERS))
        raise SecretsApiError(
            400, f"unknown provider; allowed: {allowed}"
        )
    return provider


def _existing_providers() -> set[str]:
    return {entry["provider"] for entry in secrets_store.list_secrets()}


# ── handlers ──────────────────────────────────────────────────────────


def handle_list() -> dict[str, Any]:
    """GET /api/secrets/list — metadata-only listing + master key status.

    ``master_key_exists`` drives the "Setup required" banner in the UI.
    """
    return {
        "secrets": secrets_store.list_secrets(),
        "master_key_exists": secrets_crypto.master_key_exists(),
    }


def handle_set(body: dict[str, Any]) -> dict[str, Any]:
    """POST /api/secrets/set — upsert a provider key.

    Auto-backs-up the DB on overwrite (rotation safety). First-time sets
    skip the backup because there is nothing to protect yet.
    """
    provider = _require_provider(body.get("provider"))
    key = body.get("key")
    if not isinstance(key, str) or len(key) < _MIN_KEY_LEN:
        raise SecretsApiError(400, f"key must be a string of at least {_MIN_KEY_LEN} characters")

    backup: str | None = None
    if provider in _existing_providers():
        backup = os.path.basename(secrets_store.backup_db(default_backups_dir()))

    try:
        secrets_store.set_secret(provider, key)
    except ValueError as e:
        raise SecretsApiError(400, str(e)) from e

    # Read-back to surface the last4 the store actually wrote, rather than
    # recomputing it here and risking a drift between the two.
    entry = next(
        (e for e in secrets_store.list_secrets() if e["provider"] == provider),
        None,
    )
    return {
        "provider": provider,
        "last4": entry["last4"] if entry else key[-4:],
        "backup": backup,
    }


def handle_delete(body: dict[str, Any]) -> dict[str, Any]:
    """DELETE /api/secrets/delete — remove a provider key.

    Always backs up first; deletion is destructive and we want the undo
    path to exist even if the row was already empty (the backup is cheap).
    """
    provider = _require_provider(body.get("provider"))
    backup = os.path.basename(secrets_store.backup_db(default_backups_dir()))
    deleted = secrets_store.delete_secret(provider)
    return {"deleted": deleted, "backup": backup}


def handle_verify(body: dict[str, Any], timeout: float = 10.0) -> dict[str, Any]:
    """POST /api/secrets/verify — test-connection against a provider.

    Two modes: "verify my stored key" (body omits ``key``) and "verify
    this candidate key before I save it" (body has ``key``). The latter
    never touches the DB — the UI uses it to gate the Save button.
    """
    provider = _require_provider(body.get("provider"))
    candidate = body.get("key")

    if candidate is not None:
        if not isinstance(candidate, str) or not candidate:
            raise SecretsApiError(400, "key must be a non-empty string")
        key = candidate
    else:
        try:
            key = secrets_store.get_secret(provider)
        except DecryptionError:
            raise SecretsApiError(500, "failed to decrypt stored key")
        if key is None:
            raise SecretsApiError(404, f"no stored key for provider {provider}")

    verifier = VERIFIERS.get(provider)
    if verifier is None:  # belt-and-suspenders — _require_provider already filtered.
        raise SecretsApiError(400, f"no verifier for provider {provider}")

    verified, err, latency_ms = verifier(key, timeout=timeout)
    return {
        "verified": verified,
        "latency_ms": latency_ms,
        "error": err,
    }


def handle_backups() -> dict[str, Any]:
    """GET /api/secrets/backups — list snapshot files, newest first."""
    return {"backups": secrets_store.list_backups(default_backups_dir())}


def handle_restore(body: dict[str, Any]) -> dict[str, Any]:
    """POST /api/secrets/restore — replace the live DB with a snapshot.

    The underlying ``secrets_store.restore_db`` already takes a pre-restore
    backup and returns its filename — we surface it so the UI can offer
    "undo restore" as a one-click option. After the file swap we reset the
    master key cache so the very next ``get_secret`` picks up whatever the
    restored DB was encrypted against.
    """
    filename = body.get("filename")
    if not isinstance(filename, str) or not filename:
        raise SecretsApiError(400, "filename is required")

    try:
        pre = secrets_store.restore_db(filename, default_backups_dir())
    except FileNotFoundError as e:
        raise SecretsApiError(404, str(e)) from e
    except ValueError as e:
        raise SecretsApiError(400, str(e)) from e

    secrets_store.reset_master_key_cache()
    return {"restored_from": filename, "pre_restore_backup": pre}


def handle_master_key_setup() -> dict[str, Any]:
    """POST /api/secrets/master-key/setup — idempotent first-time init."""
    if secrets_crypto.master_key_exists():
        return {"created": False, "message": "Master key already exists"}
    secrets_crypto.ensure_master_key()
    return {"created": True}


__all__ = [
    "SecretsApiError",
    "default_backups_dir",
    "handle_backups",
    "handle_delete",
    "handle_list",
    "handle_master_key_setup",
    "handle_restore",
    "handle_set",
    "handle_verify",
]
