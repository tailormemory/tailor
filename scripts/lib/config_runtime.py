"""Runtime support for editing config/tailor.yaml from the dashboard.

Factored out of mcp_server.py so the pieces (backup, validation, soft-reload)
can be exercised without spinning up the ASGI app. The existing Setup Wizard
endpoint and the new runtime editor both go through save_config() here.
"""

from __future__ import annotations

import os
import shutil
import sys
from datetime import datetime
from typing import Any

import yaml


# Top-level sections the UI must never write through. auth holds bearer tokens
# (a bad value can lock the user out of their own dashboard); paths/database
# point at on-disk state the MCP needs to find on boot; nightly configures the
# scheduler that runs unattended. Everything else is fair game.
BLACKLIST_SECTIONS: frozenset[str] = frozenset({"auth", "paths", "database", "nightly"})

# Rolling backup retention. Twenty saves is roughly "a few days of active
# tweaking" — enough to roll back from a mistake without cluttering the dir.
BACKUP_KEEP: int = 20


def backup_dir_for(config_path: str) -> str:
    return os.path.join(os.path.dirname(os.path.abspath(config_path)), ".backups")


def create_backup(config_path: str) -> str:
    """Snapshot the current config into .backups/tailor-YYYYMMDD-HHMMSS.yaml.

    No-op (returns "") if the file doesn't exist yet — a fresh install hits
    save before any config is on disk, and there's nothing to protect.
    Rotates to keep at most BACKUP_KEEP snapshots (oldest removed first).
    """
    if not os.path.exists(config_path):
        return ""

    bdir = backup_dir_for(config_path)
    os.makedirs(bdir, exist_ok=True)

    # Collision guard: two saves in the same second would otherwise overwrite.
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    dest = os.path.join(bdir, f"tailor-{ts}.yaml")
    suffix = 1
    while os.path.exists(dest):
        dest = os.path.join(bdir, f"tailor-{ts}-{suffix}.yaml")
        suffix += 1

    shutil.copy2(config_path, dest)

    snapshots = sorted(
        f for f in os.listdir(bdir)
        if f.startswith("tailor-") and f.endswith(".yaml")
    )
    while len(snapshots) > BACKUP_KEEP:
        try:
            os.remove(os.path.join(bdir, snapshots.pop(0)))
        except OSError:
            break

    return dest


def check_blacklist(incoming: dict) -> str | None:
    """Return the first blacklisted top-level key in `incoming`, else None."""
    for key in incoming:
        if key in BLACKLIST_SECTIONS:
            return key
    return None


def validate_loadable(merged: dict) -> tuple[bool, str]:
    """Shape-check: does `merged` round-trip through yaml.safe_dump/safe_load
    without error and produce a dict?

    Intentionally shallow. The rule per the spec is "the file is loadable
    without crashing the boot", not "every value is semantically valid" —
    a bogus temperature or unknown provider is caught at runtime by the
    provider itself when it's first called.
    """
    if not isinstance(merged, dict):
        return False, "Config root must be a mapping."
    try:
        dumped = yaml.safe_dump(
            merged, default_flow_style=False, allow_unicode=True, sort_keys=False
        )
    except Exception as e:
        return False, f"YAML serialization failed: {e}"
    try:
        reparsed = yaml.safe_load(dumped)
    except yaml.YAMLError as e:
        return False, f"YAML parse failed: {e}"
    if not isinstance(reparsed, dict):
        return False, "Serialized config did not parse back to a mapping."
    return True, ""


def _shallow_section_merge(existing: dict, incoming: dict) -> dict:
    """Per-top-level-key merge: nested dicts are .update()'d, anything else
    is replaced. Matches the behavior the Setup Wizard has relied on — do
    not "improve" it to a deep recursive merge without auditing that flow.
    """
    merged = dict(existing)
    for section, values in incoming.items():
        if isinstance(values, dict) and isinstance(merged.get(section), dict):
            merged[section] = {**merged[section], **values}
        else:
            merged[section] = values
    return merged


# ── Soft reload ─────────────────────────────────────────────────
#
# After writing a new config, reset every module-level singleton that caches
# a value read from tailor.yaml, so the next access reloads from disk.
#
# Discovered by direct read of scripts/lib/ (not guessed):
#   config.py:        _config                         (cfg() loader cache)
#   llm_client.py:    _brain, _classifier             (llm.*, classifier.*)
#   embedding.py:     _provider, _model, _endpoint,
#                     _dimensions, _api_key_env,
#                     _keep_alive                     (embedding.* — set at import)
#   i18n.py:          _USER_LANG, _LLM_PROVIDER       (user.language, llm.provider)
#                     _mem_cache, _disk_cache         (translation caches — keyed
#                     _disk_cache_path, _disk_loaded   to language, invalidate on change)
#
# tool_executor._mcp_module is a module-import cache, not config-derived — skip.
#
# We scan sys.modules by basename because the codebase imports the same files
# under multiple names (e.g. both `embedding` via sys.path hack and
# `scripts.lib.embedding`), which produces two distinct module objects. Missing
# one would leave a stale cached value.


def _iter_modules_by_basename(basename: str):
    for modname, mod in list(sys.modules.items()):
        if mod is None:
            continue
        if modname == basename or modname.rsplit(".", 1)[-1] == basename:
            yield modname, mod


def soft_reload() -> dict[str, list[str]]:
    """Reset all config-derived singletons so the next access reads fresh.

    Returns a {category: [module_name, ...]} map of what was reset, for
    logging and tests. Categories: "config", "llm_client", "embedding",
    "i18n". A module can appear once per category it matches.
    """
    reset: dict[str, list[str]] = {}

    # Phase 1: drop cfg() cache first, so re-reads below see the new file.
    for modname, mod in _iter_modules_by_basename("config"):
        if hasattr(mod, "_config"):
            mod._config = None
            reset.setdefault("config", []).append(modname)

    # cfg() entry point for phase-2 reloads. Import lazily so this module
    # stays importable even in odd environments where scripts.lib.config
    # isn't on sys.path yet (e.g. bare test runs).
    try:
        from scripts.lib.config import get as _cfg  # type: ignore
    except Exception:
        try:
            from lib.config import get as _cfg  # type: ignore
        except Exception:
            return reset

    # Phase 2a: llm_client — null out singletons so get_brain() / get_classifier()
    # rebuild from fresh config on next call.
    for modname, mod in _iter_modules_by_basename("llm_client"):
        changed = False
        if hasattr(mod, "_brain"):
            mod._brain = None
            changed = True
        if hasattr(mod, "_classifier"):
            mod._classifier = None
            changed = True
        if changed:
            reset.setdefault("llm_client", []).append(modname)

    # Phase 2b: embedding — these are plain module vars set at import time,
    # so we have to re-assign them rather than just null-and-rebuild.
    for modname, mod in _iter_modules_by_basename("embedding"):
        if not hasattr(mod, "_provider"):
            continue
        mod._provider = _cfg("embedding", "provider") or "ollama"
        mod._model = _cfg("embedding", "model") or "nomic-embed-text"
        mod._endpoint = (
            _cfg("embedding", "endpoint") or "http://localhost:11434/api/embed"
        )
        mod._dimensions = _cfg("embedding", "dimensions")
        mod._api_key_env = _cfg("embedding", "api_key_env") or ""
        ka = _cfg("embedding", "keep_alive")
        mod._keep_alive = -1 if ka is None else ka
        reset.setdefault("embedding", []).append(modname)

    # Phase 2c: i18n — same import-time pattern. If the language actually
    # changed, also drop the translation caches (they're keyed to the old lang).
    for modname, mod in _iter_modules_by_basename("i18n"):
        if not hasattr(mod, "_USER_LANG"):
            continue
        new_lang = (_cfg("user", "language") or "en").lower().strip()
        new_prov = (_cfg("llm", "provider") or "anthropic").lower().strip()
        if new_lang != getattr(mod, "_USER_LANG", "en"):
            mod._mem_cache = {}
            mod._disk_cache = {}
            mod._disk_cache_path = ""
            mod._disk_loaded = False
        mod._USER_LANG = new_lang
        mod._LLM_PROVIDER = new_prov
        reset.setdefault("i18n", []).append(modname)

    return reset


# ── Top-level: the save pipeline the endpoint delegates to ──────


class ConfigSaveError(Exception):
    """Structured failure from save_config(). `status` is the HTTP code the
    caller should return; `message` is UI-safe."""

    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


def save_config(incoming: dict, config_path: str) -> dict[str, Any]:
    """Validate + backup + write + soft-reload. Returns a summary dict
    describing what happened; raises ConfigSaveError on rejection.

    Pipeline:
      1. Blacklist check on incoming sections         → 403
      2. Load current file (if any), shallow-merge    → 500 on read failure
      3. Validate merged config is YAML-loadable      → 400
      4. Back up current file (if it exists)          → best-effort
      5. Write merged YAML
      6. Soft-reload singletons

    Callers (the dashboard endpoint today, /validate DRY-RUN tomorrow) should
    translate the ConfigSaveError.status into the matching HTTP response.
    """
    if not isinstance(incoming, dict) or not incoming:
        raise ConfigSaveError(400, "Empty or malformed config body")

    offender = check_blacklist(incoming)
    if offender:
        raise ConfigSaveError(
            403, f"Section '{offender}' cannot be edited via UI"
        )

    existing: dict = {}
    if os.path.exists(config_path):
        try:
            with open(config_path) as f:
                loaded = yaml.safe_load(f) or {}
            if isinstance(loaded, dict):
                existing = loaded
        except Exception as e:
            raise ConfigSaveError(500, f"Failed to read existing config: {e}")

    merged = _shallow_section_merge(existing, incoming)

    ok, err = validate_loadable(merged)
    if not ok:
        raise ConfigSaveError(400, f"Config validation failed: {err}")

    backup_path = create_backup(config_path)

    try:
        os.makedirs(os.path.dirname(os.path.abspath(config_path)), exist_ok=True)
        with open(config_path, "w") as f:
            yaml.safe_dump(
                merged, f, default_flow_style=False, allow_unicode=True, sort_keys=False
            )
    except Exception as e:
        raise ConfigSaveError(500, f"Failed to write config: {e}")

    reloaded = soft_reload()

    return {
        "ok": True,
        "backup": os.path.basename(backup_path) if backup_path else "",
        "reloaded": reloaded,
    }
