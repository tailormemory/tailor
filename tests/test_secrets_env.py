"""Tests for scripts.lib.secrets_env + the env preview/import handlers.

Parser + diff + export are pure, so they're tested directly.
Endpoint-level tests exercise handle_env_preview / handle_env_import via
secrets_api, with TAILOR_HOME + TAILOR_SECRETS_DB_PATH pointing into
tmp_path so no real ~/.tailor/ or db/secrets.sqlite3 is touched.
"""

from __future__ import annotations

import logging
import os
import sys
from unittest.mock import patch

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from scripts.lib import secrets_api, secrets_env, secrets_store  # noqa: E402
from scripts.lib.secrets_api import (  # noqa: E402
    SecretsApiError,
    handle_env_import,
    handle_env_preview,
    handle_master_key_setup,
)
from scripts.lib.secrets_env import (  # noqa: E402
    ENV_VAR_MAP,
    compute_diff,
    export_to_env_format,
    parse_env_file,
)


# ── fixtures ──────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def hermetic_env(tmp_path, monkeypatch):
    monkeypatch.setenv("TAILOR_HOME", str(tmp_path / "tailor_home"))
    monkeypatch.setenv("TAILOR_SECRETS_DB_PATH", str(tmp_path / "secrets.sqlite3"))
    monkeypatch.setenv("TAILOR_ROOT", str(tmp_path))
    secrets_store.reset_master_key_cache()
    yield tmp_path
    secrets_store.reset_master_key_cache()


def _write_env(tmp_path, content: str) -> str:
    path = tmp_path / "custom.env"
    path.write_text(content)
    return str(path)


# ── parser ────────────────────────────────────────────────────────────


def test_parse_basic(tmp_path):
    path = _write_env(tmp_path, "ANTHROPIC_API_KEY=sk-ant-12345\nOPENAI_API_KEY=sk-oai-67890\n")
    out = parse_env_file(path)
    assert out == {"anthropic": "sk-ant-12345", "openai": "sk-oai-67890"}


def test_parse_quoted_double(tmp_path):
    path = _write_env(tmp_path, 'ANTHROPIC_API_KEY="sk-ant with spaces"\n')
    assert parse_env_file(path) == {"anthropic": "sk-ant with spaces"}


def test_parse_quoted_single(tmp_path):
    path = _write_env(tmp_path, "ANTHROPIC_API_KEY='sk-ant-literal$VAR'\n")
    # Single quotes: literal, no $VAR interpolation.
    assert parse_env_file(path) == {"anthropic": "sk-ant-literal$VAR"}


def test_parse_double_quote_supports_escaped_quote(tmp_path):
    path = _write_env(tmp_path, 'ANTHROPIC_API_KEY="a\\"b"\n')
    assert parse_env_file(path) == {"anthropic": 'a"b'}


def test_parse_ignores_comments_and_blank_lines(tmp_path):
    path = _write_env(
        tmp_path,
        "# leading comment\n\nANTHROPIC_API_KEY=sk-1\n\n  # indented comment\n"
        "OPENAI_API_KEY=sk-2\n",
    )
    assert parse_env_file(path) == {"anthropic": "sk-1", "openai": "sk-2"}


def test_parse_ignores_unknown_vars(tmp_path):
    path = _write_env(
        tmp_path,
        "FOO=bar\nANTHROPIC_API_KEY=sk-1\nPATH=/usr/bin\nGEMINI_API_KEY=ai-g\n",
    )
    out = parse_env_file(path)
    assert "foo" not in out
    assert "path" not in out
    assert out == {"anthropic": "sk-1", "google": "ai-g"}


def test_parse_accepts_export_prefix(tmp_path):
    path = _write_env(tmp_path, "export OPENAI_API_KEY=sk-oai\n")
    assert parse_env_file(path) == {"openai": "sk-oai"}


def test_parse_malformed_raises_unterminated_quote(tmp_path):
    path = _write_env(tmp_path, 'ANTHROPIC_API_KEY="unterminated\n')
    with pytest.raises(ValueError, match="unterminated"):
        parse_env_file(path)


def test_parse_malformed_raises_bad_key(tmp_path):
    path = _write_env(tmp_path, "1BAD=value\n")
    with pytest.raises(ValueError, match="invalid key name"):
        parse_env_file(path)


def test_parse_malformed_raises_missing_equals(tmp_path):
    path = _write_env(tmp_path, "JUST_A_NAME\n")
    with pytest.raises(ValueError, match="missing '=' separator"):
        parse_env_file(path)


def test_parse_malformed_raises_trailing_after_quote(tmp_path):
    path = _write_env(tmp_path, 'ANTHROPIC_API_KEY="ok" extra-stuff\n')
    with pytest.raises(ValueError, match="trailing content"):
        parse_env_file(path)


def test_parse_missing_file_raises_filenotfounderror(tmp_path):
    with pytest.raises(FileNotFoundError):
        parse_env_file(str(tmp_path / "does-not-exist.env"))


# ── diff ──────────────────────────────────────────────────────────────


def test_diff_all_states():
    env_keys = {
        "anthropic": "sk-ant-NEWW",       # new (not in DB)
        "openai":    "sk-oai-ROTATED",    # conflict with db_last4="OLDD"
        "google":    "aa-bb-cc-IDEN",     # identical with db_last4="IDEN"
        # deepseek absent from env
    }
    db_meta = [
        {"provider": "openai",   "last4": "OLDD", "updated_at": 100},
        {"provider": "google",   "last4": "IDEN", "updated_at": 200},
        {"provider": "deepseek", "last4": "DSDS", "updated_at": 300},
    ]
    diff = compute_diff(env_keys, db_meta)
    rows = {r["provider"]: r for r in diff}

    assert [r["provider"] for r in diff] == sorted(rows.keys())
    assert rows["anthropic"]["state"] == "new"
    assert rows["anthropic"]["suggested_action"] == "import"
    assert rows["openai"]["state"] == "conflict"
    assert rows["openai"]["suggested_action"] == "keep"
    assert rows["google"]["state"] == "identical"
    assert rows["deepseek"]["state"] == "db_only"
    # Every row carries the env var name for the UI label.
    assert rows["anthropic"]["env_var"] == "ANTHROPIC_API_KEY"


def test_diff_filters_neither():
    # deepseek missing from both — must not appear.
    diff = compute_diff({"anthropic": "sk-1234"}, [])
    providers = [r["provider"] for r in diff]
    assert providers == ["anthropic"]


def test_diff_identical_when_last4_match():
    env_keys = {"anthropic": "completely-different-key-ABCD"}
    db_meta = [{"provider": "anthropic", "last4": "ABCD", "updated_at": 99}]
    diff = compute_diff(env_keys, db_meta)
    assert diff[0]["state"] == "identical"
    # Comment in the module explains why this is acceptable.


# ── export ────────────────────────────────────────────────────────────


def test_export_roundtrip(tmp_path):
    secrets = {
        "anthropic": "sk-ant-round-trip",
        "openai": 'has " quote',
        "google": "simple-gemini",
    }
    text = export_to_env_format(secrets)
    path = tmp_path / "rt.env"
    path.write_text(text)
    parsed = parse_env_file(str(path))
    assert parsed == secrets


def test_export_drops_unknown_providers():
    out = export_to_env_format({"anthropic": "sk-1", "bogus": "leak-me"})
    assert "ANTHROPIC_API_KEY" in out
    assert "bogus" not in out.lower()
    assert "leak-me" not in out


def test_export_only_emits_known_providers_in_map_order():
    # All four, shuffled — order in output is ENV_VAR_MAP order, not insertion.
    out = export_to_env_format({
        "deepseek": "dk",
        "anthropic": "ak",
        "google": "gk",
        "openai": "ok",
    })
    positions = [out.index(v) for v in ENV_VAR_MAP.values()]
    assert positions == sorted(positions)


# ── endpoint: preview ─────────────────────────────────────────────────


def _fresh_db():
    """Make sure master key + DB are initialized for endpoint tests."""
    handle_master_key_setup()


def test_preview_returns_diff_with_no_plaintext(tmp_path):
    _fresh_db()
    secret_plaintext = "sk-ant-ULTRA-SECRET-never-in-response-BEEF"
    env_path = _write_env(tmp_path, f"ANTHROPIC_API_KEY={secret_plaintext}\n")

    out = handle_env_preview({"path": env_path})
    # Serialize the whole response and make sure the plaintext isn't hiding
    # anywhere inside it.
    import json as _json
    blob = _json.dumps(out, default=str)
    assert secret_plaintext not in blob
    # last4 IS allowed — that's what the UI displays.
    row = next(r for r in out["diff"] if r["provider"] == "anthropic")
    assert row["env_last4"] == "BEEF"
    assert row["state"] == "new"
    assert out["env_path"] == env_path


def test_preview_missing_env_file_returns_400():
    with pytest.raises(SecretsApiError) as exc:
        handle_env_preview({"path": "/nope/does-not-exist.env"})
    assert exc.value.status == 400


def test_preview_uses_default_path_when_omitted(tmp_path):
    # TAILOR_ROOT is tmp_path via fixture, so default is tmp_path/.env.
    (tmp_path / ".env").write_text("OPENAI_API_KEY=sk-default-oai\n")
    _fresh_db()

    out = handle_env_preview({})
    assert out["env_path"] == str(tmp_path / ".env")
    assert any(r["provider"] == "openai" and r["state"] == "new" for r in out["diff"])


# ── endpoint: import ──────────────────────────────────────────────────


def test_import_applies_actions(tmp_path):
    _fresh_db()
    env_path = _write_env(
        tmp_path,
        "ANTHROPIC_API_KEY=sk-ant-fromenv-1234\nOPENAI_API_KEY=sk-oai-fromenv-5678\n",
    )

    out = handle_env_import({
        "path": env_path,
        "actions": {"anthropic": "import", "openai": "skip"},
    })

    # import landed in the DB.
    assert secrets_store.get_secret("anthropic") == "sk-ant-fromenv-1234"
    # skip left the DB untouched.
    assert secrets_store.get_secret("openai") is None

    applied_providers = {e["provider"] for e in out["applied"]}
    skipped_providers = {e["provider"] for e in out["skipped"]}
    assert applied_providers == {"anthropic"}
    assert skipped_providers == {"openai"}
    assert out["applied"][0]["last4"] == "1234"


def test_import_rejects_incompatible_action(tmp_path):
    _fresh_db()
    # anthropic is new (not in DB yet) → `replace` is not valid.
    env_path = _write_env(tmp_path, "ANTHROPIC_API_KEY=sk-ant-12345\n")
    with pytest.raises(SecretsApiError) as exc:
        handle_env_import({
            "path": env_path,
            "actions": {"anthropic": "replace"},
        })
    assert exc.value.status == 400
    assert "replace requires state=conflict" in exc.value.message


def test_import_rejects_import_on_identical(tmp_path):
    _fresh_db()
    # Pre-load the DB so last4 matches the env.
    secrets_store.set_secret("anthropic", "sk-ant-matching-1234")
    env_path = _write_env(tmp_path, "ANTHROPIC_API_KEY=different-key-1234\n")

    with pytest.raises(SecretsApiError) as exc:
        handle_env_import({
            "path": env_path,
            "actions": {"anthropic": "import"},
        })
    # import is only valid on 'new' — this is 'identical'.
    assert exc.value.status == 400
    assert "import requires state=new" in exc.value.message


def test_import_creates_backups_via_set_secret(tmp_path):
    """Replacing a conflicting key must go through set_secret, which already
    backs up the DB on overwrite. We spy on backup_db to confirm."""
    _fresh_db()
    secrets_store.set_secret("anthropic", "sk-ant-OLD-1111")
    env_path = _write_env(tmp_path, "ANTHROPIC_API_KEY=sk-ant-NEW-2222\n")

    with patch.object(
        secrets_store, "backup_db", wraps=secrets_store.backup_db
    ) as spy:
        out = handle_env_import({
            "path": env_path,
            "actions": {"anthropic": "replace"},
        })

    assert spy.called, "set_secret must backup on overwrite"
    assert secrets_store.get_secret("anthropic") == "sk-ant-NEW-2222"
    assert out["applied"][0]["last4"] == "2222"


def test_import_skip_is_noop(tmp_path):
    _fresh_db()
    env_path = _write_env(tmp_path, "ANTHROPIC_API_KEY=sk-ant-shouldnt-land\n")

    with patch.object(secrets_store, "set_secret") as m_set:
        out = handle_env_import({
            "path": env_path,
            "actions": {"anthropic": "skip"},
        })
    m_set.assert_not_called()
    assert out["applied"] == []
    assert out["skipped"][0]["provider"] == "anthropic"


def test_import_never_logs_plaintext(tmp_path, caplog):
    _fresh_db()
    secret = "sk-ant-NEVER-LOG-THIS-PLAINTEXT-KEY-9999"
    env_path = _write_env(tmp_path, f"ANTHROPIC_API_KEY={secret}\n")

    caplog.set_level(logging.DEBUG)
    handle_env_import({"path": env_path, "actions": {"anthropic": "import"}})

    for record in caplog.records:
        assert secret not in record.getMessage()
        for arg in record.args or ():
            assert secret not in str(arg)


def test_import_rejects_missing_actions(tmp_path):
    _fresh_db()
    env_path = _write_env(tmp_path, "ANTHROPIC_API_KEY=sk-12345\n")
    with pytest.raises(SecretsApiError) as exc:
        handle_env_import({"path": env_path})
    assert exc.value.status == 400


def test_import_rejects_unknown_action(tmp_path):
    _fresh_db()
    env_path = _write_env(tmp_path, "ANTHROPIC_API_KEY=sk-12345\n")
    with pytest.raises(SecretsApiError) as exc:
        handle_env_import({
            "path": env_path,
            "actions": {"anthropic": "nuke"},
        })
    assert exc.value.status == 400
    assert "unknown action" in exc.value.message


def test_import_rejects_unknown_provider(tmp_path):
    _fresh_db()
    env_path = _write_env(tmp_path, "ANTHROPIC_API_KEY=sk-12345\n")
    with pytest.raises(SecretsApiError) as exc:
        handle_env_import({
            "path": env_path,
            "actions": {"bogus": "import"},
        })
    assert exc.value.status == 400
