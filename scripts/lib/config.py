"""
TAILOR — Configuration loader.
Reads config/tailor.yaml, resolves ${ENV_VAR} references.
"""

import os, re, yaml

_DEFAULT_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "config", "tailor.yaml")

def _resolve_env(val):
    """Replace ${VAR} with os.environ[VAR], leave as-is if not set."""
    if not isinstance(val, str):
        return val
    def _sub(m):
        return os.environ.get(m.group(1), m.group(0))
    return re.sub(r'\$\{([^}]+)\}', _sub, val)

def _walk_resolve(obj):
    if isinstance(obj, dict):
        return {k: _walk_resolve(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_walk_resolve(v) for v in obj]
    return _resolve_env(obj)

_config = None

def load_config(path: str = "") -> dict:
    global _config
    if _config is not None and not path:
        return _config
    p = path or os.environ.get("TAILOR_CONFIG", _DEFAULT_PATH)
    if not os.path.exists(p):
        _config = {}
        return _config
    with open(p) as f:
        raw = yaml.safe_load(f) or {}
    _config = _walk_resolve(raw)
    # Resolve relative paths against config file directory
    config_dir = os.path.dirname(os.path.abspath(p))
    root_dir = os.path.dirname(config_dir)  # project root (parent of config/)
    for section in _config.values():
        if isinstance(section, dict):
            for k, v in section.items():
                if isinstance(v, str) and v.startswith("./"):
                    section[k] = os.path.join(root_dir, v[2:])
    return _config

def get(section: str, key: str = "", default=None):
    """Get config value. Usage: get('llm', 'provider') or get('llm') for full section."""
    cfg = load_config()
    s = cfg.get(section, {})
    if not key:
        return s if s else default
    return s.get(key, default) if isinstance(s, dict) else default


# ── Enrichment config ─────────────────────────────────────────

_ENRICHMENT_DEFAULTS = {
    "fact_extraction": [{"provider": "google", "model": "gemini-2.5-flash"}],
    "fact_derivation": [{"provider": "anthropic", "model": "claude-haiku-4-5-20251001"}],
    "entity_extraction": [{"provider": "anthropic", "model": "claude-haiku-4-5-20251001"}],
    "doc_enrichment": [{"provider": "anthropic", "model": "claude-haiku-4-5-20251001"}],
    "summaries": [{"provider": "anthropic", "model": "claude-haiku-4-5-20251001"}],
    "fact_supersession": [{"provider": "anthropic", "model": "claude-haiku-4-5-20251001"}],
}

# Default parallel workers per provider (used when not specified in config)
_DEFAULT_WORKERS = {
    "ollama": 1,
    "google": 5,
    "openai": 8,
    "anthropic": 15,
}


def get_enrichment_backends(role: str) -> list[dict]:
    """Get ordered list of backends for an enrichment role.

    Supports both formats:
      New: enrichment.role.backends = [{provider, model}, ...]
      Old: enrichment.role = {provider, model, fallback: {provider, model}}

    Returns list of {"provider": str, "model": str} dicts, ordered by priority.
    """
    enrichment = get("enrichment") or {}
    role_cfg = enrichment.get(role, {})

    if not role_cfg:
        return list(_ENRICHMENT_DEFAULTS.get(role, [{"provider": "anthropic", "model": "claude-haiku-4-5-20251001"}]))

    # New format: backends list
    if "backends" in role_cfg:
        backends = role_cfg["backends"]
        if isinstance(backends, list) and len(backends) > 0:
            return [{"provider": b.get("provider", ""), "model": b.get("model", ""),
                     "workers": b.get("workers", _DEFAULT_WORKERS.get(b.get("provider", ""), 5))}
                    for b in backends if b.get("provider")]
        # Empty backends list — fall through to defaults
        return list(_ENRICHMENT_DEFAULTS.get(role, []))

    # Old format: provider + model + optional fallback
    if "provider" in role_cfg:
        prov = role_cfg["provider"]
        result = [{"provider": prov, "model": role_cfg.get("model", ""),
                   "workers": _DEFAULT_WORKERS.get(prov, 5)}]
        fallback = role_cfg.get("fallback")
        if isinstance(fallback, dict) and fallback.get("provider"):
            fb_prov = fallback["provider"]
            result.append({"provider": fb_prov, "model": fallback.get("model", ""),
                           "workers": _DEFAULT_WORKERS.get(fb_prov, 5)})
        return result

    return list(_ENRICHMENT_DEFAULTS.get(role, []))


def get_enrichment_daily_limit(role: str) -> int:
    """Get daily limit per backend for a role. Default 4500."""
    enrichment = get("enrichment") or {}
    role_cfg = enrichment.get(role, {})
    return role_cfg.get("daily_limit_per_backend", 4500)


def get_enrichment(role: str) -> dict:
    """Get primary enrichment backend for a role (first in list).
    Backward-compatible: returns dict with 'provider' and 'model'.
    """
    backends = get_enrichment_backends(role)
    return backends[0] if backends else {"provider": "anthropic", "model": "claude-haiku-4-5-20251001"}


def get_enrichment_fallback(role: str) -> dict | None:
    """Get fallback backend for a role (second in list), or None.
    Backward-compatible with old format.
    """
    backends = get_enrichment_backends(role)
    return backends[1] if len(backends) > 1 else None


# ── Model Advisor config ──────────────────────────────────────

_MODEL_ADVISOR_DEFAULTS = {
    "enabled": False,
    "schedule_days": [1, 4],
    "hardware": {
        "ram_gb": 8,
        "max_model_size_gb": None,  # auto-calculated if None
    },
    "providers": {
        "ollama": {
            "enabled": True,
            "track_installed": True,
            "discover_new": True,
            "families": ["qwen", "llama", "gemma", "phi", "mistral", "deepseek"],
        },
        "anthropic": {"enabled": False},
        "google": {"enabled": False},
        "openai": {"enabled": False},
    },
    "notify": {
        "telegram": True,
        "dashboard": True,
    },
}


def _deep_merge(defaults: dict, overrides: dict) -> dict:
    """Recursively merge overrides into defaults."""
    result = dict(defaults)
    for k, v in overrides.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def get_model_advisor_config() -> dict:
    """Return full model_advisor config with defaults applied."""
    cfg = load_config()
    ma = cfg.get("model_advisor", {})
    return _deep_merge(_MODEL_ADVISOR_DEFAULTS, ma)


def get_model_advisor_max_size() -> int:
    """Return max model size in GB (auto-calculated from RAM if not set)."""
    cfg = get_model_advisor_config()
    hw = cfg.get("hardware", {})
    explicit = hw.get("max_model_size_gb")
    if explicit is not None:
        return int(explicit)
    ram = hw.get("ram_gb", 8)
    return int(ram * 0.6)
