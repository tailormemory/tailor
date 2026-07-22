"""Shared backend rotation manager for enrichment scripts.

Used by:
- scripts/enrichment/fact_supersession.py
- scripts/enrichment/derive_facts.py

TECHNICAL DEBT: scripts/enrichment/extract_facts_nightly.py has an
in-line BackendManager copy (introduced before this shared module
existed). Migrating it here is deferred — the in-line version works
and migrating would expand test surface beyond this PR's scope.
See docs/v1.2.4_resume_state.md "v1.2.5 backlog" for the migration
task.

Pattern (one item at a time, sync or async):

    mgr = BackendManager(role="fact_supersession")
    while items:
        backend = mgr.current()
        if backend is None:
            break  # all backends rate-limited or daily limit reached
        result = call_provider(backend, item)  # script-specific dispatch
        if result == "RATE_LIMITED":
            mgr.mark_rate_limited()  # exhaust current, advance index
            continue                  # retry same item with new backend
        if result == "TIMEOUT":
            mgr.mark_timeout()       # WARN + advance index
            continue                  # retry same item with new backend
        if result is None:
            mgr.mark_error()          # advance index without exhausting
            ...                        # script-specific: retry once or skip
        else:
            mgr.mark_success()
            save(result)

Batch pattern (supersession): pass `n` to mark_success() to record
multiple successes in one call; this avoids index drift when a single
batch's successes would cross the daily-limit boundary mid-iteration."""

from __future__ import annotations

import os
import subprocess
import sys
from typing import Optional

from scripts.lib.config import (
    get_enrichment_backends,
    get_enrichment_daily_limit,
)


_KEY_NAMES = {
    "google": "GOOGLE_API_KEY",
    "openai": "OPENAI_API_KEY",
    "anthropic": "ANTHROPIC_API_KEY",
}
_PLIST_PATH = "/Library/LaunchDaemons/com.tailor.mcp.plist"

# Consecutive HTTP timeouts on the same backend before it is considered
# down for the run. A single timeout rotates without exhausting (network
# blips happen); a sustained streak means the provider is unreachable.
TIMEOUT_CONSECUTIVE_THRESHOLD = 3


def _load_api_key(provider: str) -> str:
    """Load API key for `provider` from environment, with macOS
    LaunchDaemon plist fallback (mirrors the duplicated `get_key`
    helpers in the enrichment scripts). Returns empty string if not
    found."""
    name = _KEY_NAMES.get(provider, "")
    if not name:
        return ""
    key = os.environ.get(name, "")
    if key:
        return key
    try:
        result = subprocess.run(
            ["/usr/bin/plutil", "-extract",
             f"EnvironmentVariables.{name}", "raw", _PLIST_PATH],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    return ""


class BackendManager:
    """Provider rotation with daily-limit + rate-limit tracking.

    Backend dict shape (returned by `current()`):
        {"name": str,        # provider name from config (e.g. "google")
         "model": str,       # model name from config
         "api_key": str,     # resolved key
         "workers": int,     # parallel-call hint (5 if unset)
         "calls": int,       # successful calls so far
         "limit": int,       # daily cap before exhaustion
         "exhausted": bool,  # True when limit reached or rate-limited
         "timeouts_consecutive": int}  # HTTP-timeout streak (reset on success)
    """

    def __init__(self, role: str, api_keys: Optional[dict] = None,
                 restrict_provider: Optional[str] = None):
        """role: enrichment role name (matches config/tailor.yaml key).
        api_keys: optional {provider: key} map; if None, keys are loaded
                  from env / LaunchDaemon plist via `_load_api_key`.
        restrict_provider: if set, only that provider is loaded into the
                  rotation (used by --backend CLI override)."""
        self.role = role
        cfg = get_enrichment_backends(role)
        limit = get_enrichment_daily_limit(role)
        self.backends: list[dict] = []
        for bcfg in cfg:
            provider = bcfg.get("provider", "")
            if not provider:
                continue
            if restrict_provider and provider != restrict_provider:
                continue
            model = bcfg.get("model", "")
            workers = bcfg.get("workers", 5)
            if api_keys is not None:
                api_key = api_keys.get(provider, "")
            else:
                api_key = _load_api_key(provider)
            # Ollama doesn't need an API key
            if provider != "ollama" and not api_key:
                print(
                    f"[backend_manager:{role}] skipping {provider}/{model}"
                    f" — no API key for {_KEY_NAMES.get(provider, provider)}",
                    file=sys.stderr,
                )
                continue
            self.backends.append({
                "name": provider,
                "model": model,
                "api_key": api_key,
                "workers": workers,
                "calls": 0,
                "limit": limit,
                "exhausted": False,
                "timeouts_consecutive": 0,
            })
        self.current_idx = 0

    def current(self) -> Optional[dict]:
        """Return the first non-exhausted backend, or None if all
        exhausted. Implicitly advances `current_idx` past exhausted
        backends — call this *before* recording outcomes for a batch
        so that the dispatched-against backend stays at `current_idx`."""
        if not self.backends:
            return None
        for _ in range(len(self.backends)):
            b = self.backends[self.current_idx]
            if not b["exhausted"]:
                return b
            self.current_idx = (self.current_idx + 1) % len(self.backends)
        return None

    def mark_success(self, n: int = 1) -> None:
        """Record `n` successful calls against the current backend.
        If the daily limit is reached, exhausts current and rotates.

        Pass n>1 to atomically record a batch's worth of successes;
        avoids the mid-iteration race where calls cross the limit
        and split between current and next backend."""
        if not self.backends or n <= 0:
            return
        b = self.backends[self.current_idx]
        b["calls"] += n
        b["timeouts_consecutive"] = 0  # backend is responding again
        if b["calls"] >= b["limit"] and not b["exhausted"]:
            b["exhausted"] = True
            print(
                f"[backend_manager:{self.role}] {b['name']} reached "
                f"daily limit ({b['limit']}); rotating",
                file=sys.stderr,
            )
            self.current_idx = (self.current_idx + 1) % len(self.backends)

    def mark_rate_limited(self) -> None:
        """Record a 429 from current backend: exhaust + rotate.
        Idempotent if already exhausted (still advances index once)."""
        if not self.backends:
            return
        b = self.backends[self.current_idx]
        if not b["exhausted"]:
            b["exhausted"] = True
            print(
                f"[backend_manager:{self.role}] {b['name']} rate "
                f"limited (429); rotating",
                file=sys.stderr,
            )
        self.current_idx = (self.current_idx + 1) % len(self.backends)

    def mark_timeout(self) -> None:
        """Record an HTTP timeout from the current backend: WARN + rotate,
        so the caller can immediately retry the same item on the next
        provider. Same worker-visible treatment as a 429 (never block,
        item goes back in queue), but the backend is exhausted only after
        TIMEOUT_CONSECUTIVE_THRESHOLD consecutive timeouts — one network
        blip must not burn a provider for the whole nightly run."""
        if not self.backends:
            return
        b = self.backends[self.current_idx]
        b["timeouts_consecutive"] = b.get("timeouts_consecutive", 0) + 1
        if (b["timeouts_consecutive"] >= TIMEOUT_CONSECUTIVE_THRESHOLD
                and not b["exhausted"]):
            b["exhausted"] = True
            print(
                f"[backend_manager:{self.role}] WARN {b['name']} timed out "
                f"{b['timeouts_consecutive']}× consecutive; marking exhausted",
                file=sys.stderr,
            )
        else:
            print(
                f"[backend_manager:{self.role}] WARN {b['name']} HTTP "
                f"timeout; rotating (item back in queue)",
                file=sys.stderr,
            )
        self.current_idx = (self.current_idx + 1) % len(self.backends)

    def mark_error(self) -> None:
        """Record a transient error (timeout, 5xx, parse failure):
        rotate to next backend without exhausting current. The current
        backend remains eligible for future calls (e.g. once one
        attempt against it fails, try the next provider for this item)."""
        if not self.backends:
            return
        self.current_idx = (self.current_idx + 1) % len(self.backends)

    def all_exhausted(self) -> bool:
        if not self.backends:
            return True
        return all(b["exhausted"] for b in self.backends)

    def status(self) -> str:
        if not self.backends:
            return "(no backends)"
        parts = []
        for b in self.backends:
            mark = "OK" if not b["exhausted"] else "EXH"
            parts.append(f"{b['name']}:{b['calls']}/{b['limit']}{mark}")
        return " | ".join(parts)

    def current_workers(self) -> int:
        """Return the workers hint for the current backend, or 5 if
        none available. Useful for re-tuning an asyncio Semaphore when
        the active backend changes."""
        b = self.current()
        return b["workers"] if b else 5
