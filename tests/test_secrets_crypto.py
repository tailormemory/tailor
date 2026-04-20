"""Tests for scripts.lib.secrets_crypto — master key + AES-GCM primitives.

Hermetic: every test sets ``TAILOR_HOME`` to a pytest ``tmp_path`` so
the user's real ``~/.tailor/`` is never touched.
"""

from __future__ import annotations

import base64
import os
import stat
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from scripts.lib import secrets_crypto  # noqa: E402
from scripts.lib.secrets_crypto import (  # noqa: E402
    DecryptionError,
    decrypt,
    encrypt,
    ensure_master_key,
    export_master_key,
    get_master_key_path,
    import_master_key,
    master_key_exists,
)


@pytest.fixture(autouse=True)
def tailor_home(tmp_path, monkeypatch):
    """Point TAILOR_HOME at a fresh tmp dir for every test.

    secrets_crypto reads TAILOR_HOME on every call, so simply setting
    the env var is enough — no module reload required.
    """
    home = tmp_path / "tailor_home"
    monkeypatch.setenv("TAILOR_HOME", str(home))
    yield home


def test_master_key_generation_creates_file_with_correct_permissions(tailor_home):
    key = ensure_master_key()
    path = get_master_key_path()

    assert isinstance(key, bytes)
    assert len(key) == 32
    assert os.path.isfile(path)

    mode = stat.S_IMODE(os.stat(path).st_mode)
    assert mode == 0o600, f"expected 0o600, got {oct(mode)}"

    # The parent dir should also be locked down.
    dir_mode = stat.S_IMODE(os.stat(os.path.dirname(path)).st_mode)
    assert dir_mode & (stat.S_IRWXG | stat.S_IRWXO) == 0


def test_master_key_is_idempotent(tailor_home):
    key1 = ensure_master_key()
    path = get_master_key_path()
    mtime1 = os.stat(path).st_mtime_ns

    key2 = ensure_master_key()
    mtime2 = os.stat(path).st_mtime_ns

    assert key1 == key2
    assert mtime1 == mtime2, "master key file should not be rewritten on reload"


def test_encrypt_decrypt_roundtrip(tailor_home):
    key = ensure_master_key()
    plaintext = "sk-super-secret-value-42 with unicode: café 🔑"

    blob = encrypt(plaintext, key)
    assert isinstance(blob, bytes)
    assert plaintext.encode("utf-8") not in blob

    assert decrypt(blob, key) == plaintext


def test_decrypt_rejects_tampered_ciphertext(tailor_home):
    key = ensure_master_key()
    blob = bytearray(encrypt("classified", key))

    # Flip a bit somewhere in the ciphertext region (after the nonce).
    blob[15] ^= 0x01

    with pytest.raises(DecryptionError):
        decrypt(bytes(blob), key)


def test_decrypt_rejects_wrong_key(tailor_home):
    key_a = ensure_master_key()
    key_b = os.urandom(32)
    assert key_a != key_b

    blob = encrypt("for-a-eyes-only", key_a)

    with pytest.raises(DecryptionError):
        decrypt(blob, key_b)


def test_encrypt_produces_unique_nonce(tailor_home):
    key = ensure_master_key()
    plaintext = "same input every time"

    blob1 = encrypt(plaintext, key)
    blob2 = encrypt(plaintext, key)

    assert blob1 != blob2, "fresh nonce per call should make ciphertexts differ"
    # Nonce is the first 12 bytes; they should differ.
    assert blob1[:12] != blob2[:12]
    # But both still decrypt to the same plaintext.
    assert decrypt(blob1, key) == plaintext
    assert decrypt(blob2, key) == plaintext


def test_export_import_master_key_roundtrip(tmp_path, monkeypatch):
    # First dir: generate and export.
    home1 = tmp_path / "home1"
    monkeypatch.setenv("TAILOR_HOME", str(home1))

    original = ensure_master_key()
    exported = export_master_key()
    assert isinstance(exported, str)
    assert base64.b64decode(exported) == original

    # Swap to a fresh dir, import, and confirm the key matches.
    home2 = tmp_path / "home2"
    monkeypatch.setenv("TAILOR_HOME", str(home2))

    assert not master_key_exists()
    import_master_key(exported)

    imported = ensure_master_key()
    assert imported == original

    # Permissions should still be 0o600 after import.
    mode = stat.S_IMODE(os.stat(get_master_key_path()).st_mode)
    assert mode == 0o600


def test_import_master_key_rejects_invalid_length(tailor_home):
    bad = base64.b64encode(os.urandom(16)).decode("ascii")
    with pytest.raises(ValueError):
        import_master_key(bad)

    # The original key on disk (if any) should be untouched — and since
    # we never wrote one in this test, no file should exist.
    assert not os.path.isfile(get_master_key_path())
