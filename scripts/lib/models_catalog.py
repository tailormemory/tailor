"""
TAILOR — Live provider model catalog.

Fetches the list of chat/tool-use-capable models from each LLM provider's
public ``GET .../models`` endpoint, filters out embedding/audio/image/etc.
entries, and caches the result in-process for one hour.

Consumers: dashboard Config page (Chat-interface row editor and Default-LLM
form). Both call ``GET /api/chat/models?provider=<name>`` to populate a
dropdown that replaces the hardcoded model lists we used to ship.

Cache semantics
---------------

* Module-level dict keyed by provider.
* 1-hour TTL — long enough that a dashboard session never pays the
  upstream latency twice, short enough that newly-announced models show
  up on the next working day.
* No disk persistence. MCP restart wipes the cache, which is fine — the
  first request after boot refills it.

API-key resolution
------------------

We reuse :func:`scripts.lib.api_keys.resolve_api_key` so DB-managed and
env-var keys both work. The *explicit* value we feed it comes from the
caller's own config: ``llm.api_key`` when ``llm.provider`` matches the
requested provider, otherwise the first enrichment backend whose
``provider`` matches. Everything else falls through to the DB/env lookup
inside :func:`resolve_api_key`.

Error model
-----------

Callers translate exceptions into HTTP codes:

* :class:`ValueError`       — unknown provider                    → 400
* :class:`LookupError`      — no API key found                    → 503
* :class:`ProviderAPIError` — upstream 4xx/5xx or network failure → 502
"""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone
from typing import Callable

import requests


log = logging.getLogger(__name__)


# ── constants ────────────────────────────────────────────────

SUPPORTED_PROVIDERS: tuple[str, ...] = ("anthropic", "openai", "google", "deepseek")

_CACHE_TTL_SECONDS = 3600
_UPSTREAM_TIMEOUT = 15

# Keep chat + reasoning-style OpenAI IDs, skip every modality / utility model.
_OPENAI_INCLUDE = re.compile(r"^(gpt-|o\d)", re.IGNORECASE)
_OPENAI_EXCLUDE = re.compile(
    r"audio|tts|realtime|image|search|transcribe|embedding|moderation",
    re.IGNORECASE,
)

# Google: keep gemini-* chat models only.
_GOOGLE_EXCLUDE = re.compile(
    r"image|tts|robotics|computer-use|embedding|preview-tts",
    re.IGNORECASE,
)

# DeepSeek: generic safety net — the catalog is small and already clean.
_DEEPSEEK_EXCLUDE = re.compile(r"embedding|vision|image", re.IGNORECASE)


# ── cache ────────────────────────────────────────────────────

_cache: dict[str, tuple[float, list[dict]]] = {}


def clear_cache() -> None:
    """Wipe the in-memory cache. Intended for tests."""
    _cache.clear()


# ── display-name heuristics ──────────────────────────────────

def _humanize_openai(model_id: str) -> str:
    """``gpt-4o-mini`` → ``GPT 4o Mini``, ``o1-pro`` → ``O1 Pro``.

    Spec calls for simple hyphen→space splitting with a handful of
    well-known-prefix fixups, not a full marketing-name table. We keep
    numeric-leading segments verbatim so ``4o`` stays ``4o`` instead of
    becoming ``4O``.
    """
    parts = model_id.split("-")
    out: list[str] = []
    for part in parts:
        if not part:
            continue
        low = part.lower()
        if low == "gpt":
            out.append("GPT")
        elif re.match(r"^o\d", low):
            out.append(part.upper())
        elif part[0].isdigit():
            out.append(part)
        else:
            out.append(part.capitalize())
    return " ".join(out)


def _humanize_google(model_id: str) -> str:
    """``gemini-2.5-pro`` → ``Gemini 2.5 Pro``."""
    parts = model_id.split("-")
    out = []
    for part in parts:
        if part and part[0].isdigit():
            out.append(part)
        else:
            out.append(part.capitalize())
    return " ".join(out)


def _humanize_deepseek(model_id: str) -> str:
    """``deepseek-chat`` → ``DeepSeek Chat``."""
    parts = model_id.split("-")
    out = []
    for part in parts:
        if part.lower() == "deepseek":
            out.append("DeepSeek")
        elif part and part[0].isdigit():
            out.append(part)
        else:
            out.append(part.capitalize())
    return " ".join(out)


def humanize(provider: str, model_id: str) -> str:
    """Pure, unit-testable display-name derivation for the three
    providers whose /models endpoint does NOT ship a clean name."""
    if provider == "openai":
        return _humanize_openai(model_id)
    if provider == "google":
        return _humanize_google(model_id)
    if provider == "deepseek":
        return _humanize_deepseek(model_id)
    return model_id


# ── per-provider fetchers ────────────────────────────────────

def _fetch_anthropic(api_key: str) -> list[dict]:
    resp = requests.get(
        "https://api.anthropic.com/v1/models",
        headers={"x-api-key": api_key, "anthropic-version": "2023-06-01"},
        timeout=_UPSTREAM_TIMEOUT,
    )
    resp.raise_for_status()
    payload = resp.json()
    out: list[dict] = []
    for m in payload.get("data", []):
        mid = m.get("id")
        if not mid:
            continue
        display = m.get("display_name") or mid
        out.append({"id": mid, "display_name": display})
    return out


def _fetch_openai(api_key: str) -> list[dict]:
    resp = requests.get(
        "https://api.openai.com/v1/models",
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=_UPSTREAM_TIMEOUT,
    )
    resp.raise_for_status()
    payload = resp.json()
    out: list[dict] = []
    for m in payload.get("data", []):
        mid = m.get("id")
        if not mid:
            continue
        if not _OPENAI_INCLUDE.match(mid):
            continue
        if _OPENAI_EXCLUDE.search(mid):
            continue
        out.append({"id": mid, "display_name": humanize("openai", mid)})
    return out


def _fetch_google(api_key: str) -> list[dict]:
    # Google expects the key as a query param, not a header.
    resp = requests.get(
        "https://generativelanguage.googleapis.com/v1beta/models",
        params={"key": api_key},
        timeout=_UPSTREAM_TIMEOUT,
    )
    resp.raise_for_status()
    payload = resp.json()
    out: list[dict] = []
    for m in payload.get("models", []):
        raw = m.get("name", "")
        mid = raw.split("/", 1)[1] if raw.startswith("models/") else raw
        if not mid or not mid.startswith("gemini-"):
            continue
        if _GOOGLE_EXCLUDE.search(mid):
            continue
        out.append({"id": mid, "display_name": humanize("google", mid)})
    return out


def _fetch_deepseek(api_key: str) -> list[dict]:
    resp = requests.get(
        "https://api.deepseek.com/v1/models",
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=_UPSTREAM_TIMEOUT,
    )
    resp.raise_for_status()
    payload = resp.json()
    out: list[dict] = []
    for m in payload.get("data", []):
        mid = m.get("id")
        if not mid:
            continue
        if _DEEPSEEK_EXCLUDE.search(mid):
            continue
        out.append({"id": mid, "display_name": humanize("deepseek", mid)})
    return out


_FETCHERS: dict[str, Callable[[str], list[dict]]] = {
    "anthropic": _fetch_anthropic,
    "openai":    _fetch_openai,
    "google":    _fetch_google,
    "deepseek":  _fetch_deepseek,
}


# ── API-key resolution ───────────────────────────────────────

def _resolve_key_for_provider(provider: str, cfg_get: Callable) -> str:
    """Find the best explicit key hint from config, then defer to
    :func:`resolve_api_key` for DB/env fallback."""
    from scripts.lib.api_keys import resolve_api_key

    explicit = ""
    llm = cfg_get("llm", None, None) or {}
    if isinstance(llm, dict) and llm.get("provider") == provider:
        explicit = llm.get("api_key", "") or ""

    if not explicit:
        enrichment = cfg_get("enrichment", None, None) or {}
        if isinstance(enrichment, dict):
            for role_cfg in enrichment.values():
                if not isinstance(role_cfg, dict):
                    continue
                # New format: backends list.
                for backend in role_cfg.get("backends", []) or []:
                    if isinstance(backend, dict) and backend.get("provider") == provider:
                        explicit = backend.get("api_key", "") or ""
                        if explicit:
                            break
                if explicit:
                    break
                # Old format: inline provider/model/api_key.
                if role_cfg.get("provider") == provider:
                    explicit = role_cfg.get("api_key", "") or ""
                    if explicit:
                        break

    return resolve_api_key(provider, explicit)


# ── public API ───────────────────────────────────────────────

class ProviderAPIError(Exception):
    """Upstream provider returned a non-2xx or the network call failed."""

    def __init__(self, status: int, detail: str = ""):
        self.status = status
        self.detail = detail
        super().__init__(f"upstream returned {status}: {detail}")


def _iso_utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_models(
    provider: str,
    cfg_get: Callable | None = None,
    now: Callable[[], float] | None = None,
) -> dict:
    """Return ``{"provider", "models", "cached", "cached_at"}`` for
    ``provider``, serving from the in-memory cache when fresh."""
    if provider not in _FETCHERS:
        raise ValueError(f"unsupported provider: {provider}")

    now_ts = now() if now else time.time()

    cached = _cache.get(provider)
    if cached and (now_ts - cached[0]) < _CACHE_TTL_SECONDS:
        return {
            "provider": provider,
            "models": cached[1],
            "cached": True,
            "cached_at": _iso_utc(cached[0]),
        }

    if cfg_get is None:
        from scripts.lib.config import get as cfg_get  # default runtime source

    api_key = _resolve_key_for_provider(provider, cfg_get)
    if not api_key:
        raise LookupError(f"API key for {provider} not configured")

    try:
        models = _FETCHERS[provider](api_key)
    except requests.HTTPError as exc:
        resp = exc.response
        status = resp.status_code if resp is not None else 0
        detail = ""
        if resp is not None:
            try:
                detail = resp.text[:500]
            except Exception:
                detail = ""
        raise ProviderAPIError(status, detail) from exc
    except requests.RequestException as exc:
        raise ProviderAPIError(0, str(exc)[:500]) from exc

    # No timestamps available — fall back to reverse lexicographic sort on ID,
    # which puts newer version numbers first for every provider's naming
    # scheme we care about today. Good enough, and cheap.
    models.sort(key=lambda m: m["id"], reverse=True)

    _cache[provider] = (now_ts, models)
    return {
        "provider": provider,
        "models": models,
        "cached": False,
        "cached_at": _iso_utc(now_ts),
    }


def handle_request(query_string: str, cfg_get: Callable | None = None) -> tuple[dict, int]:
    """HTTP adapter: parse ``?provider=...``, return ``(body, status)``.

    The mcp_server.py route handler turns the tuple into a JSONResponse.
    Extracted so the logic stays testable without an ASGI scope.
    """
    params: dict[str, str] = {}
    for part in (query_string or "").split("&"):
        if "=" in part:
            k, v = part.split("=", 1)
            params[k] = v
    provider = params.get("provider", "")
    if not provider:
        return {"error": "missing provider parameter"}, 400
    try:
        return get_models(provider, cfg_get=cfg_get), 200
    except ValueError as e:
        return {"error": str(e)}, 400
    except LookupError as e:
        return {"error": str(e)}, 503
    except ProviderAPIError as e:
        return {
            "error": f"upstream {provider} returned {e.status}",
            "detail": e.detail,
        }, 502


__all__ = [
    "ProviderAPIError",
    "SUPPORTED_PROVIDERS",
    "clear_cache",
    "get_models",
    "handle_request",
    "humanize",
]
