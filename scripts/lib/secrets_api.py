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

from scripts.lib import secrets_crypto, secrets_env, secrets_store
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


# ── .env import/preview ───────────────────────────────────────────────


def _resolve_env_path(body: dict[str, Any]) -> str:
    raw = body.get("path")
    if raw is None or raw == "":
        return secrets_env.default_env_path()
    if not isinstance(raw, str):
        raise SecretsApiError(400, "path must be a string")
    return raw


def _parse_env_or_raise(path: str) -> dict[str, str]:
    try:
        return secrets_env.parse_env_file(path)
    except FileNotFoundError:
        raise SecretsApiError(400, f"env file not found: {path}")
    except (UnicodeDecodeError, OSError) as e:
        raise SecretsApiError(400, f"could not read env file: {e.__class__.__name__}")
    except ValueError as e:
        raise SecretsApiError(400, f"malformed env file: {e}")


def handle_env_preview(body: dict[str, Any]) -> dict[str, Any]:
    """POST /api/secrets/env/preview — diff ``.env`` against the live DB.

    Returns only metadata (``last4``, timestamps, states, suggested actions).
    The plaintext env values stay inside this process — they are used to
    compute last4 and then dropped before the response dict is built.
    """
    path = _resolve_env_path(body)
    env_keys = _parse_env_or_raise(path)
    diff = secrets_env.compute_diff(env_keys, secrets_store.list_secrets())
    return {"env_path": path, "diff": diff}


def handle_env_import(body: dict[str, Any]) -> dict[str, Any]:
    """POST /api/secrets/env/import — apply per-provider actions.

    Re-parses the env file and re-computes the diff (client state may be
    stale). Each action is validated against the freshly observed state
    before any write; if anything is incompatible the whole request 400s
    with the offending set — never a partial write.
    """
    actions = body.get("actions")
    if not isinstance(actions, dict) or not actions:
        raise SecretsApiError(400, "actions must be a non-empty object")

    path = _resolve_env_path(body)
    env_keys = _parse_env_or_raise(path)
    diff = secrets_env.compute_diff(env_keys, secrets_store.list_secrets())
    state_by_provider = {row["provider"]: row["state"] for row in diff}

    valid_actions = {"import", "replace", "skip", "keep"}
    invalid: list[dict[str, str]] = []
    for provider, action in actions.items():
        if provider not in secrets_env.ENV_VAR_MAP:
            invalid.append({"provider": provider, "reason": "unknown provider"})
            continue
        if action not in valid_actions:
            invalid.append({"provider": provider, "action": action, "reason": "unknown action"})
            continue
        state = state_by_provider.get(provider, secrets_env.STATE_NEITHER)
        if action == "import" and state != secrets_env.STATE_NEW:
            invalid.append({
                "provider": provider, "action": action,
                "reason": f"import requires state=new, got {state}",
            })
        elif action == "replace" and state != secrets_env.STATE_CONFLICT:
            invalid.append({
                "provider": provider, "action": action,
                "reason": f"replace requires state=conflict, got {state}",
            })

    if invalid:
        raise SecretsApiError(400, f"invalid actions: {invalid}")

    # Take a single pre-import snapshot if any replace is in the batch. One
    # backup per batch is the right grain: the user clicked Import once, and
    # "undo that one click" should restore everything at once rather than
    # interleaving N snapshots with N overwrites.
    pre_import_backup: str | None = None
    if any(actions.get(p) == "replace" for p in actions):
        pre_import_backup = os.path.basename(
            secrets_store.backup_db(default_backups_dir())
        )

    applied: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for provider, action in actions.items():
        if action in ("import", "replace"):
            plaintext = env_keys[provider]
            try:
                secrets_store.set_secret(provider, plaintext)
            except ValueError as e:
                raise SecretsApiError(400, str(e)) from e
            entry = next(
                (e for e in secrets_store.list_secrets() if e["provider"] == provider),
                None,
            )
            applied.append({
                "provider": provider,
                "action": action,
                "last4": entry["last4"] if entry else plaintext[-4:],
            })
        else:
            skipped.append({
                "provider": provider,
                "action": action,
                "reason": state_by_provider.get(provider, secrets_env.STATE_NEITHER),
            })

    return {
        "env_path": path,
        "applied": applied,
        "skipped": skipped,
        "pre_import_backup": pre_import_backup,
    }


__all__ = [
    "SecretsApiError",
    "default_backups_dir",
    "handle_backups",
    "handle_delete",
    "handle_env_import",
    "handle_env_preview",
    "handle_list",
    "handle_master_key_setup",
    "handle_restore",
    "handle_set",
    "handle_verify",
]
