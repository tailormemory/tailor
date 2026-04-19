"""Tests for scripts.lib.config_runtime — the runtime config editor core.

These tests exercise the helpers directly (no ASGI app spin-up). The HTTP
endpoint is a thin wrapper over save_config(); testing the helper covers
both surfaces.
"""

from __future__ import annotations

import os
import sys
import types

import pytest
import yaml

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)  # for `scripts.lib.*`
sys.path.insert(0, os.path.join(ROOT, "scripts", "lib"))  # bare names

from scripts.lib import config_runtime  # noqa: E402
from scripts.lib.config_runtime import (  # noqa: E402
    BACKUP_KEEP,
    BLACKLIST_SECTIONS,
    ConfigSaveError,
    check_blacklist,
    create_backup,
    save_config,
    soft_reload,
    validate_loadable,
)


# ── Fixtures ────────────────────────────────────────────────────


@pytest.fixture
def config_path(tmp_path):
    """Fresh config path per test. Lives under tmp_path/config/ so the
    .backups/ sibling dir the code creates doesn't collide with the repo.
    """
    d = tmp_path / "config"
    d.mkdir()
    p = d / "tailor.yaml"
    p.write_text(yaml.safe_dump({
        "llm": {"provider": "anthropic", "model": "claude-haiku-4-5", "temperature": 0.3},
        "embedding": {"provider": "ollama", "model": "nomic-embed-text"},
        "user": {"name": "Test", "language": "en"},
    }))
    return str(p)


@pytest.fixture
def fake_singleton_modules():
    """Register fake modules mimicking the real singletons so soft_reload()
    has something to patch without pulling in the real scripts.lib.llm_client
    (which has a heavy import chain).

    Yields a dict of the fake modules so tests can assert on their state.
    Cleans them out of sys.modules afterwards.
    """
    # llm_client: mimic _brain / _classifier singletons
    llm = types.ModuleType("fake_llm_client_for_test.llm_client")
    llm._brain = object()
    llm._classifier = object()

    # embedding: mimic module-level config vars
    emb = types.ModuleType("fake_emb_for_test.embedding")
    emb._provider = "stale"
    emb._model = "stale"
    emb._endpoint = "stale"
    emb._dimensions = -1
    emb._api_key_env = "stale"
    emb._keep_alive = -999

    # i18n: mimic language + provider + translation caches
    i18n = types.ModuleType("fake_i18n_for_test.i18n")
    i18n._USER_LANG = "en"
    i18n._LLM_PROVIDER = "anthropic"
    i18n._mem_cache = {"stale": "cached"}
    i18n._disk_cache = {"stale": "cached"}
    i18n._disk_cache_path = "/old/path.json"
    i18n._disk_loaded = True

    registered = {
        llm.__name__: llm,
        emb.__name__: emb,
        i18n.__name__: i18n,
    }
    for name, mod in registered.items():
        sys.modules[name] = mod

    yield {"llm": llm, "emb": emb, "i18n": i18n}

    for name in registered:
        sys.modules.pop(name, None)


# ── Blacklist ───────────────────────────────────────────────────


def test_blacklist_covers_expected_sections():
    assert BLACKLIST_SECTIONS == frozenset({"auth", "paths", "database", "nightly"})


def test_check_blacklist_returns_first_offender():
    assert check_blacklist({"llm": {}, "auth": {}}) == "auth"
    assert check_blacklist({"llm": {}, "user": {}}) is None


def test_save_rejects_blacklisted_section(config_path):
    before = open(config_path).read()
    with pytest.raises(ConfigSaveError) as exc:
        save_config({"auth": {"token": "bad"}}, config_path)
    assert exc.value.status == 403
    assert "auth" in exc.value.message
    assert open(config_path).read() == before, "file must be untouched on 403"


@pytest.mark.parametrize("section", sorted(BLACKLIST_SECTIONS))
def test_save_rejects_each_blacklisted_section(config_path, section):
    with pytest.raises(ConfigSaveError) as exc:
        save_config({section: {"anything": 1}}, config_path)
    assert exc.value.status == 403


# ── Validation ──────────────────────────────────────────────────


def test_validate_loadable_accepts_sane_config():
    ok, err = validate_loadable({"llm": {"provider": "anthropic"}})
    assert ok and err == ""


def test_validate_loadable_rejects_non_mapping_root():
    ok, err = validate_loadable(["not", "a", "dict"])  # type: ignore[arg-type]
    assert not ok
    assert "mapping" in err.lower()


def test_validate_loadable_rejects_non_serializable_value():
    # An object() isn't yaml-dumpable — boot would crash trying to parse it.
    ok, err = validate_loadable({"llm": {"provider": object()}})
    assert not ok


def test_save_rejects_non_dict_body(config_path):
    before = open(config_path).read()
    with pytest.raises(ConfigSaveError) as exc:
        save_config({}, config_path)
    assert exc.value.status == 400
    assert open(config_path).read() == before


def test_save_rejects_yaml_unloadable_after_merge(config_path):
    before = open(config_path).read()
    # Value that survives JSON parsing but that yaml.safe_dump can't serialize.
    bad = {"llm": {"provider": object()}}
    with pytest.raises(ConfigSaveError) as exc:
        save_config(bad, config_path)
    assert exc.value.status == 400
    assert open(config_path).read() == before


# ── Backup + rotation ───────────────────────────────────────────


def test_create_backup_noop_for_missing_file(tmp_path):
    missing = str(tmp_path / "nope.yaml")
    assert create_backup(missing) == ""


def test_backup_created_before_each_save(config_path):
    bdir = config_runtime.backup_dir_for(config_path)
    assert not os.path.exists(bdir)

    result = save_config({"user": {"name": "New"}}, config_path)
    assert os.path.isdir(bdir)
    backups = sorted(os.listdir(bdir))
    assert len(backups) == 1
    assert backups[0].startswith("tailor-") and backups[0].endswith(".yaml")
    assert result["backup"] == backups[0]

    # Backup must contain the OLD content, not the new one.
    old_content = yaml.safe_load(open(os.path.join(bdir, backups[0])).read())
    assert old_content["user"]["name"] == "Test"


def test_backup_rotation_keeps_last_20(config_path):
    bdir = config_runtime.backup_dir_for(config_path)
    os.makedirs(bdir, exist_ok=True)
    # Pre-seed 25 older backups with sortable names so rotation has work to do.
    for i in range(25):
        name = f"tailor-20250101-{i:06d}.yaml"
        with open(os.path.join(bdir, name), "w") as f:
            f.write("seed: true\n")

    save_config({"user": {"name": "After"}}, config_path)

    remaining = sorted(f for f in os.listdir(bdir) if f.startswith("tailor-"))
    assert len(remaining) == BACKUP_KEEP
    # The oldest pre-seeded ones must have been rotated out; the newest save
    # (today's timestamp) must survive.
    assert not any(f == "tailor-20250101-000000.yaml" for f in remaining)
    assert any(f.startswith("tailor-2") for f in remaining)  # today's timestamp


def test_collision_guard_same_second_save(config_path):
    # Two saves inside the same timestamp second must both produce files.
    save_config({"user": {"name": "A"}}, config_path)
    save_config({"user": {"name": "B"}}, config_path)
    bdir = config_runtime.backup_dir_for(config_path)
    assert len(os.listdir(bdir)) == 2


# ── Happy path ──────────────────────────────────────────────────


def test_save_merges_shallow_within_sections(config_path):
    save_config({"llm": {"temperature": 0.9}}, config_path)
    reloaded = yaml.safe_load(open(config_path).read())
    # New key updated, other keys in the same section preserved.
    assert reloaded["llm"]["temperature"] == 0.9
    assert reloaded["llm"]["provider"] == "anthropic"
    assert reloaded["llm"]["model"] == "claude-haiku-4-5"
    # Untouched sections intact.
    assert reloaded["embedding"]["provider"] == "ollama"


def test_save_returns_reloaded_summary(config_path, fake_singleton_modules):
    result = save_config({"user": {"name": "Alice"}}, config_path)
    assert result["ok"] is True
    # The fake modules under our fake names should appear in the summary.
    llm_reloads = result["reloaded"].get("llm_client", [])
    assert any(n.endswith(".llm_client") for n in llm_reloads)


# ── soft_reload ─────────────────────────────────────────────────


def test_soft_reload_nulls_llm_singletons(fake_singleton_modules):
    llm = fake_singleton_modules["llm"]
    assert llm._brain is not None
    soft_reload()
    assert llm._brain is None
    assert llm._classifier is None


def test_soft_reload_reassigns_embedding_vars(
    tmp_path, monkeypatch, fake_singleton_modules
):
    emb = fake_singleton_modules["emb"]
    # Point the real config loader at our fixture file so soft_reload picks
    # up the values we want to assert on.
    cfg_path = tmp_path / "tailor.yaml"
    cfg_path.write_text(yaml.safe_dump({
        "embedding": {
            "provider": "openai", "model": "text-embedding-3-large",
            "endpoint": "https://api.openai.com/v1/embeddings",
            "dimensions": 1536, "api_key_env": "OPENAI_API_KEY", "keep_alive": 300,
        },
    }))
    monkeypatch.setenv("TAILOR_CONFIG", str(cfg_path))
    # Force the cfg loader to re-read by nuking its cache.
    from scripts.lib import config as cfg_mod
    cfg_mod._config = None

    soft_reload()

    assert emb._provider == "openai"
    assert emb._model == "text-embedding-3-large"
    assert emb._endpoint == "https://api.openai.com/v1/embeddings"
    assert emb._dimensions == 1536
    assert emb._api_key_env == "OPENAI_API_KEY"
    assert emb._keep_alive == 300


def test_soft_reload_keep_alive_defaults_when_unset(
    tmp_path, monkeypatch, fake_singleton_modules
):
    emb = fake_singleton_modules["emb"]
    cfg_path = tmp_path / "tailor.yaml"
    cfg_path.write_text(yaml.safe_dump({"embedding": {"provider": "ollama"}}))
    monkeypatch.setenv("TAILOR_CONFIG", str(cfg_path))
    from scripts.lib import config as cfg_mod
    cfg_mod._config = None

    soft_reload()

    # Spec: when keep_alive is absent, embedding defaults to -1 (hot).
    assert emb._keep_alive == -1


def test_soft_reload_invalidates_i18n_cache_on_language_change(
    tmp_path, monkeypatch, fake_singleton_modules
):
    i18n = fake_singleton_modules["i18n"]
    assert i18n._USER_LANG == "en"

    cfg_path = tmp_path / "tailor.yaml"
    cfg_path.write_text(yaml.safe_dump({
        "user": {"language": "it"},
        "llm": {"provider": "google"},
    }))
    monkeypatch.setenv("TAILOR_CONFIG", str(cfg_path))
    from scripts.lib import config as cfg_mod
    cfg_mod._config = None

    soft_reload()

    assert i18n._USER_LANG == "it"
    assert i18n._LLM_PROVIDER == "google"
    # Translation caches must have been dropped — old entries were keyed to "en".
    assert i18n._mem_cache == {}
    assert i18n._disk_cache == {}
    assert i18n._disk_loaded is False
    assert i18n._disk_cache_path == ""


def test_soft_reload_preserves_i18n_cache_when_language_unchanged(
    tmp_path, monkeypatch, fake_singleton_modules
):
    i18n = fake_singleton_modules["i18n"]
    cfg_path = tmp_path / "tailor.yaml"
    cfg_path.write_text(yaml.safe_dump({
        "user": {"language": "en"},  # same as before
        "llm": {"provider": "openai"},  # provider changed
    }))
    monkeypatch.setenv("TAILOR_CONFIG", str(cfg_path))
    from scripts.lib import config as cfg_mod
    cfg_mod._config = None

    soft_reload()

    # No language change → translation caches must survive (they're still valid).
    assert i18n._mem_cache == {"stale": "cached"}
    assert i18n._disk_loaded is True
    # Provider mirror did update.
    assert i18n._LLM_PROVIDER == "openai"
