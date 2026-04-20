"""Master key manager + AES-256-GCM encryption primitives.

Foundation for encrypted secret storage. Everything in this module is
pure crypto — no DB, no HTTP, no awareness of what gets encrypted.

Security invariants (enforce when extending):
  * NEVER log the master key bytes, its path contents, or any plaintext
    value passed through encrypt()/decrypt(). No print(), no logger.debug().
  * NEVER expose the raw reason a decrypt failed to callers — a timing
    or message differential could leak information. Always raise the
    opaque DecryptionError.
  * Nonces MUST come from os.urandom(12) and MUST NOT be reused with
    the same key. A fresh nonce per encrypt() call is the contract.

The master key lives at ``$TAILOR_HOME/master.key`` (default
``~/.tailor/master.key``), 32 random bytes, mode 0o600. The TAILOR_HOME
override exists so tests can run hermetically.
"""

from __future__ import annotations

import base64
import logging
import os
import stat
import tempfile

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

_MASTER_KEY_BYTES = 32
_NONCE_BYTES = 12
_MASTER_KEY_FILENAME = "master.key"

log = logging.getLogger(__name__)


class DecryptionError(Exception):
    """Decryption failed: tampering, wrong key, or corrupt blob.

    The internal cause is deliberately not exposed — surfacing it could
    help an attacker distinguish between failure modes.
    """


def _tailor_home() -> str:
    override = os.environ.get("TAILOR_HOME")
    if override:
        return override
    return os.path.join(os.path.expanduser("~"), ".tailor")


def get_master_key_path() -> str:
    """Return the absolute path to the master key file.

    Ensures the containing directory exists with mode 0o700. Does not
    create the key file itself.
    """
    home = _tailor_home()
    os.makedirs(home, mode=0o700, exist_ok=True)
    return os.path.join(home, _MASTER_KEY_FILENAME)


def master_key_exists() -> bool:
    """True iff the master key file exists and is readable by us."""
    path = os.path.join(_tailor_home(), _MASTER_KEY_FILENAME)
    return os.path.isfile(path) and os.access(path, os.R_OK)


def _atomic_write_bytes(path: str, data: bytes, mode: int = 0o600) -> None:
    directory = os.path.dirname(path) or "."
    fd, tmp_path = tempfile.mkstemp(prefix=".master.key.", dir=directory)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp_path, mode)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _warn_if_world_readable(path: str) -> None:
    try:
        st_mode = os.stat(path).st_mode
    except OSError:
        return
    if st_mode & (stat.S_IRGRP | stat.S_IROTH | stat.S_IWGRP | stat.S_IWOTH):
        log.warning(
            "master key file has overly permissive mode %o; expected 0o600",
            stat.S_IMODE(st_mode),
        )


def ensure_master_key() -> bytes:
    """Return the master key, creating it on first call.

    Idempotent: repeated calls return the same 32 bytes and do not
    rewrite the file. On first call, generates 32 bytes from os.urandom
    and writes atomically with mode 0o600.
    """
    path = get_master_key_path()
    if os.path.isfile(path):
        _warn_if_world_readable(path)
        with open(path, "rb") as f:
            data = f.read()
        if len(data) != _MASTER_KEY_BYTES:
            raise ValueError(
                f"master key at {path} is {len(data)} bytes; expected {_MASTER_KEY_BYTES}"
            )
        return data

    key = os.urandom(_MASTER_KEY_BYTES)
    _atomic_write_bytes(path, key, mode=0o600)
    return key


def encrypt(plaintext: str, master_key: bytes) -> bytes:
    """Encrypt a UTF-8 string with AES-256-GCM.

    Returns ``nonce || ciphertext || tag`` as a single bytes blob. The
    nonce is 12 random bytes and a new one is drawn every call; the
    tag is the trailing 16 bytes produced by AES-GCM authentication.
    Callers store the blob as-is.
    """
    if len(master_key) != _MASTER_KEY_BYTES:
        raise ValueError(f"master key must be {_MASTER_KEY_BYTES} bytes")
    nonce = os.urandom(_NONCE_BYTES)
    aead = AESGCM(master_key)
    ct_and_tag = aead.encrypt(nonce, plaintext.encode("utf-8"), None)
    return nonce + ct_and_tag


def decrypt(blob: bytes, master_key: bytes) -> str:
    """Decrypt a blob produced by :func:`encrypt`.

    Raises :class:`DecryptionError` on any failure (bad key, tampering,
    truncated blob, invalid UTF-8). The exception message is constant
    — do not introduce failure-specific messages.
    """
    if len(master_key) != _MASTER_KEY_BYTES:
        raise DecryptionError("decryption failed")
    if len(blob) < _NONCE_BYTES + 16:
        raise DecryptionError("decryption failed")
    nonce, ct_and_tag = blob[:_NONCE_BYTES], blob[_NONCE_BYTES:]
    aead = AESGCM(master_key)
    try:
        plaintext_bytes = aead.decrypt(nonce, ct_and_tag, None)
        return plaintext_bytes.decode("utf-8")
    except (InvalidTag, UnicodeDecodeError, ValueError):
        raise DecryptionError("decryption failed") from None


def export_master_key() -> str:
    """Return the current master key as a base64 string for user backup.

    The caller is responsible for presenting this to the user via a
    secure channel (e.g., the authenticated dashboard) and NOT logging
    it. Reads from disk rather than any cached copy.
    """
    path = get_master_key_path()
    with open(path, "rb") as f:
        data = f.read()
    if len(data) != _MASTER_KEY_BYTES:
        raise ValueError(
            f"master key at {path} is {len(data)} bytes; expected {_MASTER_KEY_BYTES}"
        )
    return base64.b64encode(data).decode("ascii")


def import_master_key(b64_string: str) -> None:
    """Overwrite the master key file with ``b64_string`` (base64 of 32 bytes).

    Used for migration/restore. Atomic: on invalid input, the existing
    key on disk is untouched. Raises :class:`ValueError` if the input
    is not valid base64 or does not decode to exactly 32 bytes.
    """
    if not isinstance(b64_string, str):
        raise ValueError("master key import must be a base64 string")
    try:
        decoded = base64.b64decode(b64_string, validate=True)
    except (ValueError, base64.binascii.Error) as e:
        raise ValueError("invalid base64 encoding") from e
    if len(decoded) != _MASTER_KEY_BYTES:
        raise ValueError(
            f"master key must decode to exactly {_MASTER_KEY_BYTES} bytes, got {len(decoded)}"
        )
    path = get_master_key_path()
    _atomic_write_bytes(path, decoded, mode=0o600)
