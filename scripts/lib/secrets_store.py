"""Encrypted SQLite store for provider API keys.

Sits on top of :mod:`scripts.lib.secrets_crypto`. One row per provider:
``(provider, ciphertext, last4, updated_at)``. The ciphertext is opaque to
this module — the crypto module owns the format.

Design:
  * Functions, not a class: callers don't need to thread a handle. The
    DB path comes from :func:`get_secrets_db_path` (env override for tests).
  * Master key is cached at module import time on first use via
    :func:`reset_master_key_cache` for invalidation (e.g., after rotation).
  * Backups use SQLite's online backup API — the live DB may be mid-write
    and the snapshot will still be consistent.
  * Never log plaintext keys. last4 is fine to log (it's already clear in
    the DB for UI purposes).
"""

from __future__ import annotations

import os
import re
import shutil
import sqlite3
import tempfile
import threading
import time
from datetime import datetime
from typing import Any

from scripts.lib import secrets_crypto
from scripts.lib.secrets_crypto import DecryptionError


ALLOWED_PROVIDERS: frozenset[str] = frozenset(
    {"anthropic", "openai", "google", "deepseek"}
)

BACKUP_KEEP: int = 20

_DB_FILENAME = "secrets.sqlite3"

_MIGRATION = """
CREATE TABLE IF NOT EXISTS secrets (
    provider   TEXT    PRIMARY KEY,
    ciphertext BLOB    NOT NULL,
    last4      TEXT    NOT NULL,
    updated_at INTEGER NOT NULL
);
"""

# Filename format we produce in backup_db. The `-N` tail is the
# same-second collision tiebreaker, matching config_runtime's convention.
_BACKUP_NAME_RE = re.compile(r"^secrets-(\d{8})-(\d{6})(?:-\d+)?\.sqlite3$")

_master_key_lock = threading.Lock()
_master_key_cache: bytes | None = None


# ── paths ─────────────────────────────────────────────────────────────


def _repo_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def get_secrets_db_path() -> str:
    """Absolute path to the secrets DB.

    Honours ``TAILOR_SECRETS_DB_PATH`` for hermetic tests; otherwise lives
    alongside the other SQLite files at ``<repo>/db/secrets.sqlite3``.
    """
    override = os.environ.get("TAILOR_SECRETS_DB_PATH")
    if override:
        return override
    return os.path.join(_repo_root(), "db", _DB_FILENAME)


# ── master key cache ──────────────────────────────────────────────────


def _get_master_key() -> bytes:
    global _master_key_cache
    with _master_key_lock:
        if _master_key_cache is None:
            _master_key_cache = secrets_crypto.ensure_master_key()
        return _master_key_cache


def reset_master_key_cache() -> None:
    """Forget the cached master key — next call reloads from disk.

    Needed whenever the master key file is rotated out from under us
    (e.g., user imported a new key via the UI).
    """
    global _master_key_cache
    with _master_key_lock:
        _master_key_cache = None


# ── connection / migrations ───────────────────────────────────────────


def _connect() -> sqlite3.Connection:
    """Open the DB, apply migrations, return a configured connection."""
    path = get_secrets_db_path()
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    conn = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(_MIGRATION)
    return conn


# ── CRUD ──────────────────────────────────────────────────────────────


def _validate_provider(provider: str) -> None:
    if provider not in ALLOWED_PROVIDERS:
        allowed = ", ".join(sorted(ALLOWED_PROVIDERS))
        raise ValueError(f"unknown provider {provider!r}; allowed: {allowed}")


def set_secret(provider: str, plaintext_key: str) -> None:
    """Encrypt ``plaintext_key`` and upsert the row for ``provider``.

    Refreshes ``last4`` and ``updated_at`` on every write.
    """
    _validate_provider(provider)
    if not isinstance(plaintext_key, str) or not plaintext_key:
        raise ValueError("plaintext_key must be a non-empty string")

    key = _get_master_key()
    blob = secrets_crypto.encrypt(plaintext_key, key)
    last4 = plaintext_key[-4:]
    now = int(time.time())

    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO secrets (provider, ciphertext, last4, updated_at) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(provider) DO UPDATE SET "
            "  ciphertext = excluded.ciphertext, "
            "  last4 = excluded.last4, "
            "  updated_at = excluded.updated_at",
            (provider, blob, last4, now),
        )
    finally:
        conn.close()


def get_secret(provider: str) -> str | None:
    """Decrypt and return the plaintext key for ``provider``.

    Returns ``None`` if no row exists. Raises :class:`DecryptionError` if
    the row is present but cannot be decrypted (master key rotated without
    re-encrypting rows, DB corruption, tampering).
    """
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT ciphertext FROM secrets WHERE provider = ?",
            (provider,),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return None
    key = _get_master_key()
    return secrets_crypto.decrypt(row["ciphertext"], key)


def delete_secret(provider: str) -> bool:
    """Remove the row for ``provider``. True if a row was deleted."""
    conn = _connect()
    try:
        cur = conn.execute("DELETE FROM secrets WHERE provider = ?", (provider,))
        return cur.rowcount > 0
    finally:
        conn.close()


def list_secrets() -> list[dict[str, Any]]:
    """Return ``[{provider, last4, updated_at}, ...]`` sorted by provider.

    Safe for UI consumption — never decrypts, never returns ciphertext.
    """
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT provider, last4, updated_at FROM secrets ORDER BY provider ASC"
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


# ── backup / restore ──────────────────────────────────────────────────


def _rotate_backups(backup_dir: str) -> None:
    snapshots = sorted(
        f for f in os.listdir(backup_dir) if _BACKUP_NAME_RE.match(f)
    )
    while len(snapshots) > BACKUP_KEEP:
        try:
            os.remove(os.path.join(backup_dir, snapshots.pop(0)))
        except OSError:
            pass


def backup_db(backup_dir: str) -> str:
    """Snapshot the live DB into ``backup_dir/secrets-YYYYMMDD-HHMMSS.sqlite3``.

    Uses SQLite's online backup API so concurrent writes produce a consistent
    snapshot. Rotates to keep at most :data:`BACKUP_KEEP` files.
    Returns the created file path.
    """
    os.makedirs(backup_dir, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    dest = os.path.join(backup_dir, f"secrets-{ts}.sqlite3")
    suffix = 1
    while os.path.exists(dest):
        dest = os.path.join(backup_dir, f"secrets-{ts}-{suffix}.sqlite3")
        suffix += 1

    src = _connect()
    try:
        dst = sqlite3.connect(dest)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()

    _rotate_backups(backup_dir)
    return dest


def _resolve_backup_path(filename: str, backup_dir: str) -> str:
    if not isinstance(filename, str) or not filename:
        raise ValueError("filename is required")
    if "/" in filename or "\\" in filename or ".." in filename:
        raise ValueError(f"invalid filename: {filename!r}")
    if not _BACKUP_NAME_RE.match(filename):
        raise ValueError(
            f"filename must match secrets-YYYYMMDD-HHMMSS.sqlite3: {filename!r}"
        )

    bdir_abs = os.path.realpath(backup_dir)
    full = os.path.realpath(os.path.join(backup_dir, filename))
    if os.path.commonpath([full, bdir_abs]) != bdir_abs:
        raise ValueError("path traversal blocked")
    if not os.path.isfile(full):
        raise FileNotFoundError(f"backup not found: {filename}")
    return full


def restore_db(backup_filename: str, backup_dir: str) -> str | None:
    """Replace the live DB with the contents of ``backup_dir/backup_filename``.

    Takes an auto-backup of the current DB into ``backup_dir`` before
    overwriting so the operation is reversible. Uses SQLite's online backup
    API to write into the live DB — safer than a file copy because any
    in-flight WAL/SHM state on the destination is superseded cleanly.

    Returns the pre-restore backup filename (basename inside ``backup_dir``)
    when one was taken, or ``None`` if there was no live DB to snapshot.
    """
    src_path = _resolve_backup_path(backup_filename, backup_dir)

    # Safety snapshot of the current state — if the restore turns out to be
    # the wrong file, the user's last-known-good state is still on disk.
    live_path = get_secrets_db_path()
    pre_backup: str | None = None
    if os.path.isfile(live_path):
        pre_backup = os.path.basename(backup_db(backup_dir))

    live_dir = os.path.dirname(os.path.abspath(live_path)) or "."
    os.makedirs(live_dir, exist_ok=True)

    # Write into a sibling temp DB first, then atomic-replace. This avoids
    # corrupt state if the copy is interrupted mid-way.
    fd, tmp_path = tempfile.mkstemp(prefix=".secrets-restore.", dir=live_dir)
    os.close(fd)
    try:
        src = sqlite3.connect(src_path)
        try:
            dst = sqlite3.connect(tmp_path)
            try:
                src.backup(dst)
            finally:
                dst.close()
        finally:
            src.close()

        # Fsync and atomic replace. Also clean up any WAL/SHM sidecars of
        # the old live DB — they belong to the pre-restore generation.
        with open(tmp_path, "rb") as f:
            os.fsync(f.fileno())
        os.replace(tmp_path, live_path)
        for sidecar in (live_path + "-wal", live_path + "-shm"):
            try:
                os.unlink(sidecar)
            except FileNotFoundError:
                pass
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    return pre_backup


def _parse_backup_iso(filename: str) -> str:
    m = _BACKUP_NAME_RE.match(filename)
    if not m:
        return ""
    try:
        return datetime.strptime(
            m.group(1) + m.group(2), "%Y%m%d%H%M%S"
        ).isoformat()
    except ValueError:
        return ""


def list_backups(backup_dir: str) -> list[dict[str, Any]]:
    """Return backup files in ``backup_dir`` sorted newest-first.

    Sort key is the filename (which is timestamped) rather than mtime, so
    the order is stable regardless of whether the files were touched by
    unrelated tools.
    """
    if not os.path.isdir(backup_dir):
        return []
    out: list[dict[str, Any]] = []
    for fname in os.listdir(backup_dir):
        if not _BACKUP_NAME_RE.match(fname):
            continue
        full = os.path.join(backup_dir, fname)
        try:
            st = os.stat(full)
        except OSError:
            continue
        out.append({
            "filename": fname,
            "created_at": _parse_backup_iso(fname),
            "size_bytes": st.st_size,
        })
    out.sort(key=lambda e: e["filename"], reverse=True)
    return out


__all__ = [
    "ALLOWED_PROVIDERS",
    "DecryptionError",
    "backup_db",
    "delete_secret",
    "get_secret",
    "get_secrets_db_path",
    "list_backups",
    "list_secrets",
    "reset_master_key_cache",
    "restore_db",
    "set_secret",
]
