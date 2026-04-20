"""Tests for scripts.lib.secrets_api — the dashboard-facing handlers.

The handlers own the response-shape contract. ASGI glue in mcp_server is
a trivial JSON-in/JSON-out wrapper, so testing the handlers directly is
enough to guarantee the endpoint behavior the UI depends on.

Hermetic: each test points TAILOR_HOME and TAILOR_SECRETS_DB_PATH at a
tmp dir so the real ~/.tailor/ and db/secrets.sqlite3 are never touched.
Verifier HTTP is mocked at the VERIFIERS dict level (no real traffic).
"""

from __future__ import annotations

import os
import sys
from unittest.mock import patch

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from scripts.lib import secrets_api, secrets_crypto, secrets_store  # noqa: E402
from scripts.lib.secrets_api import (  # noqa: E402
    SecretsApiError,
    default_backups_dir,
    handle_backup_download,
    handle_backups,
    handle_delete,
    handle_list,
    handle_master_key_export,
    handle_master_key_import,
    handle_master_key_setup,
    handle_restore,
    handle_set,
    handle_verify,
)


@pytest.fixture(autouse=True)
def hermetic_env(tmp_path, monkeypatch):
    monkeypatch.setenv("TAILOR_HOME", str(tmp_path / "tailor_home"))
    monkeypatch.setenv("TAILOR_SECRETS_DB_PATH", str(tmp_path / "secrets.sqlite3"))
    secrets_store.reset_master_key_cache()
    yield tmp_path
    secrets_store.reset_master_key_cache()


@pytest.fixture
def stub_verifiers(monkeypatch):
    """Replace the real VERIFIERS dict so no provider HTTP fires."""
    calls: list[tuple] = []

    def make(result):
        def v(key, timeout=10.0):
            calls.append(("verified", key, timeout))
            return result
        return v

    stubs = {
        "anthropic": make((True, None, 42)),
        "openai": make((True, None, 42)),
        "google": make((True, None, 42)),
        "deepseek": make((True, None, 42)),
    }
    monkeypatch.setattr(secrets_api, "VERIFIERS", stubs)
    return calls


# ── list ──────────────────────────────────────────────────────────────


def test_list_returns_master_key_status():
    # Fresh fixture: no key has been created yet.
    out = handle_list()
    assert out["master_key_exists"] is False
    assert out["secrets"] == []

    # After setup, the status flips.
    handle_master_key_setup()
    out = handle_list()
    assert out["master_key_exists"] is True


# ── set ───────────────────────────────────────────────────────────────


def test_set_skips_backup_on_first_time():
    out = handle_set({"provider": "anthropic", "key": "sk-ant-12345678"})
    assert out["provider"] == "anthropic"
    assert out["last4"] == "5678"
    assert out["backup"] is None


def test_set_creates_backup_when_key_exists():
    handle_set({"provider": "anthropic", "key": "sk-ant-ORIGINAL"})
    out = handle_set({"provider": "anthropic", "key": "sk-ant-ROTATED"})
    assert out["last4"] == "ATED"
    assert out["backup"] is not None
    assert out["backup"].startswith("secrets-") and out["backup"].endswith(".sqlite3")
    # The named backup must actually exist in the backup dir.
    assert os.path.isfile(os.path.join(default_backups_dir(), out["backup"]))


def test_set_rejects_unknown_provider():
    with pytest.raises(SecretsApiError) as exc:
        handle_set({"provider": "bogus", "key": "a-valid-key-12345"})
    assert exc.value.status == 400


def test_set_rejects_short_key():
    with pytest.raises(SecretsApiError) as exc:
        handle_set({"provider": "anthropic", "key": "short"})
    assert exc.value.status == 400

    with pytest.raises(SecretsApiError):
        handle_set({"provider": "anthropic", "key": ""})

    with pytest.raises(SecretsApiError):
        handle_set({"provider": "anthropic", "key": None})


# ── delete ────────────────────────────────────────────────────────────


def test_delete_always_creates_backup():
    # Even when the row doesn't exist, the backup is still taken — the
    # undo path must exist regardless of what the DB looked like.
    out_nonexistent = handle_delete({"provider": "anthropic"})
    assert out_nonexistent["deleted"] is False
    assert out_nonexistent["backup"] is not None

    handle_set({"provider": "anthropic", "key": "sk-ant-12345678"})
    out = handle_delete({"provider": "anthropic"})
    assert out["deleted"] is True
    assert out["backup"].startswith("secrets-")


def test_delete_rejects_unknown_provider():
    with pytest.raises(SecretsApiError) as exc:
        handle_delete({"provider": "bogus"})
    assert exc.value.status == 400


# ── verify ────────────────────────────────────────────────────────────


def test_verify_with_provided_key_doesnt_store(stub_verifiers):
    out = handle_verify({"provider": "anthropic", "key": "sk-candidate-ABCD"})
    assert out["verified"] is True
    assert out["error"] is None
    # The candidate key must NOT end up in the DB.
    assert secrets_store.get_secret("anthropic") is None


def test_verify_with_stored_key_uses_get_secret(stub_verifiers):
    handle_set({"provider": "openai", "key": "sk-oai-stored-XYZ"})
    out = handle_verify({"provider": "openai"})
    assert out["verified"] is True
    # Verifier was called with the decrypted stored key, not some placeholder.
    assert stub_verifiers[-1][1] == "sk-oai-stored-XYZ"


def test_verify_returns_payload_on_auth_failure(monkeypatch):
    """Auth failure is a successful endpoint call — error bubbles up in payload,
    not as a raised SecretsApiError. The ASGI glue will return HTTP 200.
    """
    monkeypatch.setattr(
        secrets_api,
        "VERIFIERS",
        {"anthropic": lambda key, timeout=10.0: (False, "invalid x-api-key", 55)},
    )
    out = handle_verify({"provider": "anthropic", "key": "sk-bad-candidate"})
    assert out["verified"] is False
    assert out["error"] == "invalid x-api-key"
    assert out["latency_ms"] == 55


def test_verify_404_when_no_stored_key(stub_verifiers):
    with pytest.raises(SecretsApiError) as exc:
        handle_verify({"provider": "anthropic"})
    assert exc.value.status == 404


def test_verify_rejects_unknown_provider():
    with pytest.raises(SecretsApiError) as exc:
        handle_verify({"provider": "bogus", "key": "whatever-long"})
    assert exc.value.status == 400


# ── backups / restore ─────────────────────────────────────────────────


def test_backups_lists_what_was_taken():
    handle_set({"provider": "anthropic", "key": "sk-ant-12345678"})
    # Overwrite once to force an auto-backup.
    handle_set({"provider": "anthropic", "key": "sk-ant-87654321"})

    out = handle_backups()
    assert len(out["backups"]) >= 1
    assert all("filename" in b for b in out["backups"])


def test_restore_creates_pre_restore_backup():
    handle_set({"provider": "anthropic", "key": "sk-ant-ORIGINAL"})
    # Create an initial snapshot we'll restore from.
    snap = os.path.basename(
        secrets_store.backup_db(default_backups_dir())
    )
    # Mutate state.
    handle_set({"provider": "anthropic", "key": "sk-ant-MUTATED-9876"})

    out = handle_restore({"filename": snap})
    assert out["restored_from"] == snap
    assert out["pre_restore_backup"] is not None
    assert out["pre_restore_backup"].startswith("secrets-")
    # The pre-restore backup must exist in the backup dir.
    assert os.path.isfile(
        os.path.join(default_backups_dir(), out["pre_restore_backup"])
    )
    # And the restored DB must contain the original key.
    assert secrets_store.get_secret("anthropic") == "sk-ant-ORIGINAL"


def test_restore_rejects_path_traversal():
    bdir = default_backups_dir()
    os.makedirs(bdir, exist_ok=True)
    for bad in ("../etc/passwd", "secrets-bad.sqlite3", "", "subdir/secrets-20260101-010101.sqlite3"):
        with pytest.raises(SecretsApiError) as exc:
            handle_restore({"filename": bad})
        assert exc.value.status == 400


def test_restore_missing_file_404():
    # Well-formed filename, nothing on disk.
    with pytest.raises(SecretsApiError) as exc:
        handle_restore({"filename": "secrets-20260101-010101.sqlite3"})
    assert exc.value.status == 404


def test_restore_resets_master_key_cache():
    handle_set({"provider": "anthropic", "key": "sk-ant-12345678"})
    snap = os.path.basename(secrets_store.backup_db(default_backups_dir()))

    with patch.object(
        secrets_store, "reset_master_key_cache", wraps=secrets_store.reset_master_key_cache
    ) as spy:
        handle_restore({"filename": snap})
    assert spy.called


# ── master key setup ──────────────────────────────────────────────────


def test_master_key_setup_idempotent():
    first = handle_master_key_setup()
    assert first["created"] is True

    second = handle_master_key_setup()
    assert second["created"] is False
    assert "Master key already exists" in second["message"]


def test_master_key_setup_creates_file():
    assert secrets_crypto.master_key_exists() is False
    handle_master_key_setup()
    assert secrets_crypto.master_key_exists() is True


# ── master key export / import ────────────────────────────────────────


def test_master_key_export_returns_base64():
    import base64
    handle_master_key_setup()
    out = handle_master_key_export()
    assert "key_b64" in out
    decoded = base64.b64decode(out["key_b64"])
    assert len(decoded) == 32


def test_master_key_export_404_when_missing():
    # Fixture hasn't created a key, and we never call setup.
    assert secrets_crypto.master_key_exists() is False
    with pytest.raises(SecretsApiError) as exc:
        handle_master_key_export()
    assert exc.value.status == 404


def test_master_key_import_backs_up_current_first():
    import base64, glob
    handle_master_key_setup()
    original_b64 = handle_master_key_export()["key_b64"]

    # Replace with a different 32-byte key.
    replacement = base64.b64encode(os.urandom(32)).decode("ascii")
    assert replacement != original_b64
    out = handle_master_key_import({"key_b64": replacement})

    # A backup sibling of the master.key file must exist.
    mkey_path = secrets_crypto.get_master_key_path()
    siblings = sorted(glob.glob(mkey_path + ".backup-*"))
    assert siblings, "expected a master.key.backup-* file"
    assert out["backup_path"] == siblings[-1]
    # The backup must contain the ORIGINAL 32 bytes (pre-replacement).
    with open(out["backup_path"], "rb") as f:
        assert base64.b64encode(f.read()).decode("ascii") == original_b64
    # And the live key is now the replacement.
    assert handle_master_key_export()["key_b64"] == replacement


def test_master_key_import_rejects_invalid_length():
    import base64
    handle_master_key_setup()
    # 16 bytes, not 32.
    short = base64.b64encode(os.urandom(16)).decode("ascii")
    with pytest.raises(SecretsApiError) as exc:
        handle_master_key_import({"key_b64": short})
    assert exc.value.status == 400

    # Garbage base64 also rejected.
    with pytest.raises(SecretsApiError) as exc:
        handle_master_key_import({"key_b64": "!!!not-base64!!!"})
    assert exc.value.status == 400

    # Missing / wrong-type field.
    with pytest.raises(SecretsApiError):
        handle_master_key_import({})
    with pytest.raises(SecretsApiError):
        handle_master_key_import({"key_b64": None})


def test_master_key_import_resets_cache():
    import base64
    handle_master_key_setup()
    # Prime the cache.
    secrets_store.set_secret("anthropic", "sk-ant-pre-import")
    assert secrets_store.get_secret("anthropic") == "sk-ant-pre-import"

    replacement = base64.b64encode(os.urandom(32)).decode("ascii")
    with patch.object(
        secrets_store,
        "reset_master_key_cache",
        wraps=secrets_store.reset_master_key_cache,
    ) as spy:
        handle_master_key_import({"key_b64": replacement})
    assert spy.called, "reset_master_key_cache must be called after import"


# ── backup download ───────────────────────────────────────────────────


def test_backup_download_streams_bytes():
    handle_master_key_setup()
    secrets_store.set_secret("anthropic", "sk-ant-for-download")
    snap = os.path.basename(secrets_store.backup_db(default_backups_dir()))

    data, ctype = handle_backup_download(snap)
    assert ctype == "application/octet-stream"
    # SQLite files start with the fixed "SQLite format 3" header.
    assert data.startswith(b"SQLite format 3")
    # And the on-disk file matches what we got back.
    with open(os.path.join(default_backups_dir(), snap), "rb") as f:
        assert f.read() == data


def test_backup_download_rejects_path_traversal():
    bdir = default_backups_dir()
    os.makedirs(bdir, exist_ok=True)
    for bad in ("../etc/passwd", "subdir/secrets-20260101-010101.sqlite3",
                "secrets-bad.sqlite3", "", "secrets-20260101-010101.txt"):
        with pytest.raises(SecretsApiError) as exc:
            handle_backup_download(bad)
        assert exc.value.status == 400


def test_backup_download_missing_file_404():
    # Valid filename format, nothing on disk.
    with pytest.raises(SecretsApiError) as exc:
        handle_backup_download("secrets-20260101-010101.sqlite3")
    assert exc.value.status == 404
