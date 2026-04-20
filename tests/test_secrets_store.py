"""Tests for scripts.lib.secrets_store — encrypted API-key CRUD + backup/restore.

Hermetic: every test points ``TAILOR_HOME`` at a tmp dir for the master
key and ``TAILOR_SECRETS_DB_PATH`` at a tmp file for the DB. The module's
master-key cache is reset in the fixture so state doesn't leak between tests.
"""

from __future__ import annotations

import os
import sys
import time

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from scripts.lib import secrets_store  # noqa: E402
from scripts.lib.secrets_store import (  # noqa: E402
    ALLOWED_PROVIDERS,
    backup_db,
    delete_secret,
    get_secret,
    get_secrets_db_path,
    list_backups,
    list_secrets,
    restore_db,
    set_secret,
)


@pytest.fixture(autouse=True)
def hermetic_env(tmp_path, monkeypatch):
    """Isolate master key, DB, and backup dir per test."""
    monkeypatch.setenv("TAILOR_HOME", str(tmp_path / "tailor_home"))
    monkeypatch.setenv("TAILOR_SECRETS_DB_PATH", str(tmp_path / "secrets.sqlite3"))
    secrets_store.reset_master_key_cache()
    backup_dir = tmp_path / "backups"
    yield {
        "tmp": tmp_path,
        "backup_dir": str(backup_dir),
    }
    secrets_store.reset_master_key_cache()


# ── CRUD ──────────────────────────────────────────────────────────────


def test_set_and_get_secret_roundtrip():
    set_secret("anthropic", "sk-ant-test-12345")
    assert get_secret("anthropic") == "sk-ant-test-12345"


def test_set_updates_last4_and_timestamp():
    set_secret("openai", "sk-old-key-AAAA")
    first = list_secrets()[0]
    assert first["last4"] == "AAAA"
    t1 = first["updated_at"]

    # Wait a beat to guarantee a monotonic bump at second resolution.
    time.sleep(1.1)
    set_secret("openai", "sk-new-key-ZZZZ")

    second = list_secrets()[0]
    assert second["last4"] == "ZZZZ"
    assert second["updated_at"] > t1
    assert get_secret("openai") == "sk-new-key-ZZZZ"


def test_get_missing_returns_none():
    assert get_secret("anthropic") is None
    assert get_secret("google") is None


def test_delete_returns_true_when_exists_false_otherwise():
    assert delete_secret("deepseek") is False
    set_secret("deepseek", "sk-deep-1234")
    assert delete_secret("deepseek") is True
    assert get_secret("deepseek") is None
    assert delete_secret("deepseek") is False


def test_list_secrets_returns_metadata_only():
    set_secret("anthropic", "sk-ant-AAAA")
    set_secret("openai", "sk-oai-BBBB")

    entries = list_secrets()

    assert [e["provider"] for e in entries] == ["anthropic", "openai"]
    for e in entries:
        assert set(e.keys()) == {"provider", "last4", "updated_at"}
        assert "ciphertext" not in e
        # last4 must be the plaintext tail, not the full key.
        assert len(e["last4"]) == 4


def test_set_rejects_unknown_provider():
    with pytest.raises(ValueError):
        set_secret("bogus", "whatever")
    # Sanity: set of allowed providers hasn't drifted from the spec.
    assert ALLOWED_PROVIDERS == frozenset(
        {"anthropic", "openai", "google", "deepseek"}
    )


def test_ciphertext_in_db_is_not_plaintext():
    plaintext = "sk-super-secret-plaintext-XYZ-do-not-leak"
    set_secret("anthropic", plaintext)

    # Force any WAL content to the main DB file so we read the full state.
    import sqlite3

    conn = sqlite3.connect(get_secrets_db_path())
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()

    with open(get_secrets_db_path(), "rb") as f:
        raw = f.read()

    assert plaintext.encode("utf-8") not in raw
    # The round-tripped plaintext must still decrypt from the encrypted blob.
    assert get_secret("anthropic") == plaintext
    # last4 IS stored in clear by design (for UI sk-***XXXX rendering).
    assert plaintext[-4:].encode("utf-8") in raw


# ── backup / restore ──────────────────────────────────────────────────


def test_backup_and_restore_roundtrip(hermetic_env):
    bdir = hermetic_env["backup_dir"]

    set_secret("anthropic", "sk-ant-ORIGINAL")
    set_secret("openai", "sk-oai-ORIGINAL")

    snap = backup_db(bdir)
    assert os.path.isfile(snap)

    # Mutate state: rotate one key, delete another.
    set_secret("anthropic", "sk-ant-NEW")
    delete_secret("openai")
    assert get_secret("anthropic") == "sk-ant-NEW"
    assert get_secret("openai") is None

    restore_db(os.path.basename(snap), bdir)

    assert get_secret("anthropic") == "sk-ant-ORIGINAL"
    assert get_secret("openai") == "sk-oai-ORIGINAL"


def test_backup_rotation_keeps_20(hermetic_env, monkeypatch):
    bdir = hermetic_env["backup_dir"]
    set_secret("anthropic", "sk-ant-key")

    # Stub datetime.now() inside secrets_store so we can produce 25 distinct
    # filenames without sleeping for 25 real seconds.
    from datetime import datetime

    base = datetime(2026, 1, 1, 0, 0, 0)
    counter = {"i": 0}

    class FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            counter["i"] += 1
            return base.replace(second=counter["i"] % 60, minute=(counter["i"] // 60))

    monkeypatch.setattr(secrets_store, "datetime", FakeDatetime)

    for _ in range(25):
        backup_db(bdir)

    remaining = [f for f in os.listdir(bdir) if f.startswith("secrets-")]
    assert len(remaining) == 20

    # The 20 that remain must be the newest 20 (lexicographically largest).
    sorted_remaining = sorted(remaining)
    # Check that the kept set has no gaps at the top of the ordering: the
    # max filename in `remaining` should match the max we ever produced.
    entries = list_backups(bdir)
    assert len(entries) == 20
    assert entries[0]["filename"] == sorted_remaining[-1]


def test_restore_rejects_path_traversal(hermetic_env):
    bdir = hermetic_env["backup_dir"]
    os.makedirs(bdir, exist_ok=True)

    for bad in [
        "../etc/passwd",
        "..\\windows\\system32",
        "secrets-20260101-010101.sqlite3/../../../evil",
        "subdir/secrets-20260101-010101.sqlite3",
        "secrets-bad.sqlite3",  # wrong format
        "secrets-20260101-010101.txt",  # wrong extension
        "",  # empty
    ]:
        with pytest.raises(ValueError):
            restore_db(bad, bdir)


def test_list_backups_sorted_desc(hermetic_env):
    bdir = hermetic_env["backup_dir"]
    set_secret("anthropic", "sk-a")

    first = backup_db(bdir)
    time.sleep(1.1)
    second = backup_db(bdir)
    time.sleep(1.1)
    third = backup_db(bdir)

    entries = list_backups(bdir)
    filenames = [e["filename"] for e in entries]
    assert filenames == [
        os.path.basename(third),
        os.path.basename(second),
        os.path.basename(first),
    ]
    for e in entries:
        assert set(e.keys()) == {"filename", "created_at", "size_bytes"}
        assert e["size_bytes"] > 0
