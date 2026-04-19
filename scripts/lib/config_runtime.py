"""Runtime support for editing config/tailor.yaml from the dashboard.

Factored out of mcp_server.py so the pieces (backup, validation, soft-reload)
can be exercised without spinning up the ASGI app. The existing Setup Wizard
endpoint and the new runtime editor both go through save_config() here.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
import time
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

# Marker file written next to tailor.yaml after a save. If the process dies
# within SAFETY_WINDOW_SECONDS the next boot rolls back to the backup named
# inside. See apply_pending_rollback() for the full contract.
PENDING_SAVE_FILENAME: str = ".pending-save"

# How long a save is "on probation". Must be long enough to cover import-time
# crashes + first-request lazy-init failures + KeepAlive relaunch delay;
# short enough that an unrelated crash hours later (OOM, disk full, runtime
# bug) is clearly outside the window. 180s is the sweet spot — at 600s an
# OOM five minutes post-save would false-positive.
SAFETY_WINDOW_SECONDS: int = 180


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

    We scan sys.modules by *basename* (not by canonical import path) because
    this codebase imports the same file under multiple names — `embedding`
    and `scripts.lib.embedding` produce two distinct module objects, and
    patching only one leaves the other with stale cached values. Same for
    `llm_client` / `lib.llm_client` and `config` / `scripts.lib.config`.

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


# ── Pending-save marker + boot-time rollback ────────────────────
#
# Safety net for "user saved a config that breaks boot." We mark every save
# as "on probation" by dropping a .pending-save JSON file next to tailor.yaml.
# If the process dies inside SAFETY_WINDOW_SECONDS, the next boot restores
# the backup pointed at by the marker. Older marker → save has survived the
# window, process died for unrelated reasons, no rollback.


def _pending_save_path(config_path: str) -> str:
    return os.path.join(
        os.path.dirname(os.path.abspath(config_path)), PENDING_SAVE_FILENAME
    )


def _log_rollback(msg: str) -> None:
    """Single log channel for rollback-relevant events. Grep for
    '[config-rollback]' in launchd stderr logs — if the dashboard says "saved"
    but the live config disagrees, this tells you why in one search."""
    print(f"[config-rollback] {msg}", file=sys.stderr, flush=True)


def _atomic_replace(src: str, dst: str) -> None:
    """Copy src over dst via tmp+rename. shutil.copy2 alone can leave a
    half-written file if interrupted; an atomic rename can't."""
    tmp = dst + ".rollback.tmp"
    shutil.copy2(src, tmp)
    os.replace(tmp, dst)


def write_pending_save(config_path: str, backup_name: str) -> str:
    """Drop a marker recording that a save just completed. `backup_name` is
    the basename of the pre-save snapshot inside .backups/ (empty string if
    this was the very first save — nothing to roll back to).
    """
    marker = _pending_save_path(config_path)
    payload = {
        "saved_at": time.time(),
        "backup": backup_name,
    }
    tmp = marker + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f)
    os.replace(tmp, marker)
    return marker


def _clear_pending_save(config_path: str) -> None:
    try:
        os.remove(_pending_save_path(config_path))
    except FileNotFoundError:
        pass


def apply_pending_rollback(config_path: str) -> dict | None:
    """Called once at boot, BEFORE any module that reads tailor.yaml.

    If a .pending-save marker exists AND the save is still inside the
    safety window, restore the backup it names over tailor.yaml — otherwise
    the current boot will crash the same way the previous one did and
    KeepAlive will loop forever.

    Always clears the marker on exit (rolled back, stale, or unrecoverable)
    so that whatever happens, the marker isn't still sitting there influencing
    the boot after next. Returns a summary dict for tests/logs, or None if
    there was no marker.

    IMPORTANT: do not import anything that reads tailor.yaml at module load
    (embedding, i18n, llm_client) above the call site. Reordering those
    imports is exactly the regression this function exists to prevent.
    """
    marker = _pending_save_path(config_path)
    if not os.path.exists(marker):
        return None

    try:
        with open(marker) as f:
            payload = json.load(f)
    except Exception as e:
        _log_rollback(f"corrupt .pending-save ({e}), clearing without rollback")
        _clear_pending_save(config_path)
        return {"action": "cleared_corrupt"}

    saved_at = float(payload.get("saved_at", 0))
    backup_name = payload.get("backup", "") or ""
    age = time.time() - saved_at

    if age >= SAFETY_WINDOW_SECONDS:
        # Save survived the window; any death since then is on something else.
        _clear_pending_save(config_path)
        return {"action": "stale", "age": age}

    # Inside the window → previous process probably died from this save.
    if not backup_name:
        # First-ever save: nothing to roll back to. Don't clobber the user's
        # work — best we can do is clear the marker and let them recover.
        _log_rollback(
            f"pending save aged {age:.0f}s (<{SAFETY_WINDOW_SECONDS}s window) "
            f"but no backup recorded (first save?), cannot roll back, "
            f"clearing marker"
        )
        _clear_pending_save(config_path)
        return {"action": "no_backup", "age": age}

    backup_path = os.path.join(backup_dir_for(config_path), backup_name)
    if not os.path.exists(backup_path):
        _log_rollback(
            f"pending save aged {age:.0f}s but backup file {backup_path} is "
            f"missing (rotated out?), cannot roll back, clearing marker"
        )
        _clear_pending_save(config_path)
        return {"action": "backup_missing", "age": age, "backup": backup_name}

    try:
        _atomic_replace(backup_path, config_path)
    except Exception as e:
        _log_rollback(
            f"restore from {backup_path} FAILED: {e} — manual intervention "
            f"required. Clearing marker to avoid retry crashloop."
        )
        _clear_pending_save(config_path)
        return {"action": "restore_failed", "age": age, "error": str(e)}

    _log_rollback(
        f"pending save aged {age:.0f}s (<{SAFETY_WINDOW_SECONDS}s window), "
        f"restored backup from {backup_path}"
    )
    _clear_pending_save(config_path)
    return {"action": "rolled_back", "age": age, "backup": backup_name}


def _revert_after_reload_failure(
    config_path: str, backup_path: str, reason: Exception
) -> None:
    """In-process revert when soft_reload() raises.

    Context: the new yaml passed validate_loadable() (structural check) but
    _reassigning singletons from it crashed — e.g. a nested value is the
    wrong type so `.lower()` blows up. The yaml on disk is the user's
    broken input; restore the backup so the NEXT save attempt (or next
    boot via pending-save) sees a working file, and try to resync
    singletons to that restored state.
    """
    _log_rollback(f"soft_reload failed ({reason}), reverting in-process")

    if backup_path and os.path.exists(backup_path):
        try:
            _atomic_replace(backup_path, config_path)
        except Exception as e:
            _log_rollback(f"in-process restore FAILED: {e}, yaml left as-is")
    else:
        _log_rollback(
            "no backup to restore (first save); yaml on disk is broken, "
            "next boot will use pending-save or fail"
        )

    # Best-effort resync to whatever yaml is now on disk. If THIS also
    # raises, process is in a weird state — log and move on; the caller
    # will raise ConfigSaveError so the user knows something went wrong.
    try:
        soft_reload()
    except Exception as e:
        _log_rollback(
            f"post-revert soft_reload ALSO failed: {e} — singletons may be "
            f"inconsistent until next process restart"
        )

    _clear_pending_save(config_path)


# ── Top-level: the save pipeline the endpoint delegates to ──────


class ConfigSaveError(Exception):
    """Structured failure from save_config(). `status` is the HTTP code the
    caller should return; `message` is UI-safe.

    TODO(when-cross-field-validation-added): today `message` is a single
    human string, which the form-mode UI renders as a banner at the top
    ("Some fields need attention…"). The moment we grow a check that
    spans multiple fields (e.g. "provider X requires api_key_env") or
    want to point the UI at exactly one input (instead of a generic
    scrollIntoView heuristic), evolve this into:
        {"errors": [{"field": "llm.api_key_env", "message": "..."}, ...]}
    and teach the /save endpoint to return it alongside the existing
    `error` string for backward compatibility. Don't preemptively do
    this — it's waste until we have the first multi-field check.
    """

    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


def save_config(
    incoming: dict, config_path: str, *, dry_run: bool = False
) -> dict[str, Any]:
    """Validate + backup + write + soft-reload. Returns a summary dict
    describing what happened; raises ConfigSaveError on rejection.

    Pipeline:
      1. Blacklist check on incoming sections         → 403
      2. Load current file (if any), shallow-merge    → 500 on read failure
      3. Validate merged config is YAML-loadable      → 400
      [dry_run=True returns here — no write, no reload]
      4. Back up current file (if it exists)
      5. Write merged YAML
      6. Soft-reload singletons

    `dry_run` is what /api/dashboard/config/validate rides on. Same function,
    same checks: guarantees /validate and /save cannot diverge on what
    counts as a valid payload. Do not split into two paths.

    Callers should translate ConfigSaveError.status into the matching HTTP
    response.
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

    if dry_run:
        return {"ok": True, "dry_run": True, "backup": "", "reloaded": {}}

    return _apply_validated(merged, config_path)


def _apply_validated(final_config: dict, config_path: str) -> dict[str, Any]:
    """The risky tail of save_config, shared with restore_backup.

    Assumes `final_config` is the COMPLETE target state — no further
    merging. Handles backup → write → marker → soft_reload → revert, in
    that order, so both entry points produce the same invariants:

      - a .backups/ snapshot of the pre-change state
      - a .pending-save marker protecting the transition for SAFETY_WINDOW
      - singletons resynced to the new state, OR yaml reverted and the
        marker cleared if soft_reload raised

    Keep this function single-copy. Duplicating it across save and restore
    is how subtle divergences creep in (one flow grows a check the other
    doesn't, rollback paths drift).
    """
    backup_path = create_backup(config_path)

    try:
        os.makedirs(os.path.dirname(os.path.abspath(config_path)), exist_ok=True)
        with open(config_path, "w") as f:
            yaml.safe_dump(
                final_config, f, default_flow_style=False, allow_unicode=True, sort_keys=False
            )
    except Exception as e:
        raise ConfigSaveError(500, f"Failed to write config: {e}")

    # Marker BEFORE reloading singletons. If the process dies during
    # soft_reload — or reload succeeds but a later request crashes hard
    # within SAFETY_WINDOW_SECONDS — the next boot rolls back to backup_path.
    backup_name = os.path.basename(backup_path) if backup_path else ""
    write_pending_save(config_path, backup_name)

    try:
        reloaded = soft_reload()
    except Exception as e:
        _revert_after_reload_failure(config_path, backup_path, e)
        raise ConfigSaveError(
            500, f"Soft-reload failed, config reverted: {e}"
        )

    # Leave .pending-save on disk — safety net for crashes after we return
    # (e.g. first API request hits a lazy init the new config breaks). It'll
    # be cleared by the next boot via apply_pending_rollback.

    return {
        "ok": True,
        "dry_run": False,
        "backup": backup_name,
        "reloaded": reloaded,
    }


# ── Read helpers (for GET /current, POST /validate) ─────────────


def read_current(config_path: str) -> dict[str, Any]:
    """Return current tailor.yaml as both raw text and parsed dict.

    Raw on purpose: no ${ENV_VAR} resolution. The UI needs to display and
    round-trip exactly what's on disk — resolving would leak env var values
    into the browser and re-saving would then bake them in, silently
    breaking the indirection.

    Returns {exists, path, yaml, config}. If the file is missing, returns
    empty strings/dicts so the UI can still render.
    """
    result: dict[str, Any] = {
        "exists": False,
        "path": config_path,
        "yaml": "",
        "config": {},
    }
    if not os.path.exists(config_path):
        return result
    result["exists"] = True
    with open(config_path) as f:
        raw = f.read()
    result["yaml"] = raw
    try:
        parsed = yaml.safe_load(raw)
    except yaml.YAMLError:
        parsed = None
    result["config"] = parsed if isinstance(parsed, dict) else {}
    return result


def parse_incoming(body: Any) -> dict:
    """Coerce a request body into a config dict.

    Accepted shapes (validate/save both ride on this so the UI can send
    either form without asking which endpoint accepts what):
      - a parsed dict (already decoded from JSON)
      - {"config": {...dict...}} wrapper — explicit
      - {"yaml": "..."} wrapper — raw YAML string to parse

    Raises ConfigSaveError(400, ...) on malformed input.
    """
    if not isinstance(body, dict):
        raise ConfigSaveError(400, "Body must be a JSON object")

    if isinstance(body.get("yaml"), str):
        try:
            parsed = yaml.safe_load(body["yaml"])
        except yaml.YAMLError as e:
            raise ConfigSaveError(400, f"YAML parse failed: {e}")
        if parsed is None:
            raise ConfigSaveError(400, "Empty YAML")
        if not isinstance(parsed, dict):
            raise ConfigSaveError(400, "YAML root must be a mapping")
        return parsed

    if isinstance(body.get("config"), dict):
        return body["config"]

    # Bare dict — treat the whole body as the config (preserves Setup
    # Wizard's existing contract, which posts sections at the top level).
    return body


# ── Backup listing + restore ────────────────────────────────────
#
# Both ride on the primitives defined above: list_backups reads the same
# directory create_backup writes to, and restore_backup funnels through
# _apply_validated so a restore is subject to the exact same validation,
# marker, and revert-on-reload-failure guarantees as a save.


# Filename format laid down by create_backup: tailor-YYYYMMDD-HHMMSS.yaml,
# with an optional "-N" tiebreaker for the same-second collision case.
_BACKUP_NAME_RE = re.compile(r"^tailor-(\d{8})-(\d{6})(?:-\d+)?\.yaml$")


def _parse_backup_iso(filename: str) -> str:
    """Extract ISO 8601 timestamp from a backup filename, or '' if the name
    doesn't match our format. Prefer this over mtime: shutil.copy2 preserves
    the source's mtime, so mtime reflects the last edit of the *original*
    yaml, not when the backup was created."""
    m = _BACKUP_NAME_RE.match(filename)
    if not m:
        return ""
    try:
        return datetime.strptime(
            m.group(1) + m.group(2), "%Y%m%d%H%M%S"
        ).isoformat()
    except ValueError:
        return ""


def list_backups(config_path: str) -> list[dict[str, Any]]:
    """Return backup snapshots sorted newest-first.

    Sort key is the filename (lexicographic), matching the order
    create_backup uses when rotating — so "most recent at index 0" is
    consistent between list and rotation.
    """
    bdir = backup_dir_for(config_path)
    if not os.path.isdir(bdir):
        return []
    out: list[dict[str, Any]] = []
    for fname in os.listdir(bdir):
        if not _BACKUP_NAME_RE.match(fname):
            continue
        full = os.path.join(bdir, fname)
        try:
            st = os.stat(full)
        except OSError:
            continue
        out.append({
            "filename": fname,
            "saved_at": _parse_backup_iso(fname),
            "size_bytes": st.st_size,
        })
    out.sort(key=lambda e: e["filename"], reverse=True)
    return out


def _resolve_backup_path(filename: str, config_path: str) -> str:
    """Validate `filename` refers to a real backup inside .backups/ and
    return its absolute path. Raises ConfigSaveError on anything suspicious.

    Defense in depth:
      - reject path separators and parent-dir refs (prevents `../../etc/passwd`)
      - reject hidden files / wrong prefix / wrong extension (only files
        create_backup itself produces should be restorable)
      - resolve the final path and require it to live inside .backups/
        (catches symlink tricks and any escape the regex didn't)
    """
    if not isinstance(filename, str) or not filename:
        raise ConfigSaveError(400, "filename is required")
    if "/" in filename or "\\" in filename or ".." in filename:
        raise ConfigSaveError(400, f"Invalid filename: {filename!r}")
    if not _BACKUP_NAME_RE.match(filename):
        raise ConfigSaveError(
            400,
            f"Filename must match tailor-YYYYMMDD-HHMMSS.yaml: {filename!r}",
        )

    bdir = backup_dir_for(config_path)
    bdir_abs = os.path.realpath(bdir)
    full = os.path.realpath(os.path.join(bdir, filename))

    if os.path.commonpath([full, bdir_abs]) != bdir_abs:
        raise ConfigSaveError(400, "Path traversal blocked")

    if not os.path.isfile(full):
        raise ConfigSaveError(404, f"Backup not found: {filename}")

    return full


def restore_backup(filename: str, config_path: str) -> dict[str, Any]:
    """Replace tailor.yaml with the contents of `filename` (a backup under
    .backups/). Not a merge — the backup IS the target state.

    Rides on _apply_validated, so:
      - a fresh backup of the CURRENT state is taken before the restore
        (that's what rollback would revert to if the restored content
        turns out to break something)
      - .pending-save marker points at the pre-restore backup, so a
        crash inside SAFETY_WINDOW triggers auto-rollback to where
        the user was before clicking "restore"
      - soft_reload / revert behavior is identical to save

    No blacklist check: restoring is "put me back to this exact state
    I was in before", and anything in the backup was on disk already.
    The write-side blacklist stops fresh UI edits into auth/paths/etc.,
    which is a different concern.
    """
    full = _resolve_backup_path(filename, config_path)

    try:
        with open(full) as f:
            parsed = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigSaveError(400, f"Backup is not parseable YAML: {e}")

    if not isinstance(parsed, dict):
        raise ConfigSaveError(400, "Backup root is not a mapping")

    ok, err = validate_loadable(parsed)
    if not ok:
        raise ConfigSaveError(400, f"Backup failed validation: {err}")

    result = _apply_validated(parsed, config_path)
    result["restored_from"] = filename
    return result
