"""
TAILOR — Embedding abstraction layer.
Supports multiple providers: ollama (local), openai, google.
Config from config/tailor.yaml:

embedding:
  provider: ollama          # ollama | openai | google
  model: nomic-embed-text   # provider-specific model name
  endpoint: http://localhost:11434/api/embed  # only for ollama
  dimensions: 768           # output dimensions (optional, for providers that support it)
  api_key_env: GOOGLE_API_KEY  # env var name for API key (openai/google)
"""

import os, requests
from config import get as cfg

# ── Load config ───────────────────────────────────────────────
_provider = cfg("embedding", "provider") or "ollama"
_model = cfg("embedding", "model") or "nomic-embed-text"
_endpoint = cfg("embedding", "endpoint") or "http://localhost:11434/api/embed"
_dimensions = cfg("embedding", "dimensions")
_api_key_env = cfg("embedding", "api_key_env") or ""
_keep_alive = cfg("embedding", "keep_alive")
if _keep_alive is None:
    _keep_alive = -1  # Keep model hot by default (avoids cold start)

def _get_api_key() -> str:
    if _api_key_env:
        return os.environ.get(_api_key_env, "")
    # Fallback: try common env var names based on provider
    if _provider == "openai":
        return os.environ.get("OPENAI_API_KEY", "")
    if _provider == "google":
        return os.environ.get("GOOGLE_API_KEY", "")
    return ""


# ── Ollama ────────────────────────────────────────────────────
def _embed_ollama(texts: list[str]) -> list[list[float]]:
    payload = {"model": _model, "input": [t[:4000] for t in texts], "keep_alive": _keep_alive}
    r = requests.post(_endpoint, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()["embeddings"]


# ── OpenAI ────────────────────────────────────────────────────
def _embed_openai(texts: list[str]) -> list[list[float]]:
    api_key = _get_api_key()
    if not api_key:
        raise ValueError("OpenAI API key not found. Set OPENAI_API_KEY or embedding.api_key_env in config.")
    
    payload = {
        "model": _model,
        "input": [t[:8000] for t in texts],
    }
    if _dimensions:
        payload["dimensions"] = _dimensions
    
    r = requests.post(
        "https://api.openai.com/v1/embeddings",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload,
        timeout=30
    )
    r.raise_for_status()
    data = r.json()["data"]
    # Sort by index to preserve order
    data.sort(key=lambda x: x["index"])
    return [d["embedding"] for d in data]


# ── Google (Gemini Embedding) ─────────────────────────────────
def _embed_google(texts: list[str]) -> list[list[float]]:
    api_key = _get_api_key()
    if not api_key:
        raise ValueError("Google API key not found. Set GOOGLE_API_KEY or embedding.api_key_env in config.")
    
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{_model}:batchEmbedContents?key={api_key}"
    
    requests_body = []
    for t in texts:
        req = {"model": f"models/{_model}", "content": {"parts": [{"text": t[:4000]}]}}
        if _dimensions:
            req["outputDimensionality"] = _dimensions
        requests_body.append(req)
    
    r = requests.post(url, json={"requests": requests_body}, timeout=30)
    r.raise_for_status()
    return [e["values"] for e in r.json()["embeddings"]]


# ── Provider dispatch ─────────────────────────────────────────
_PROVIDERS = {
    "ollama": _embed_ollama,
    "openai": _embed_openai,
    "google": _embed_google,
}

def get_embeddings(texts: list[str]) -> list[list[float]]:
    """Get embeddings for multiple texts using configured provider."""
    fn = _PROVIDERS.get(_provider)
    if not fn:
        raise ValueError(f"Unknown embedding provider: {_provider}. Supported: {list(_PROVIDERS.keys())}")
    return fn(texts)

def get_embedding(text: str) -> list[float]:
    """Get embedding for a single text using configured provider."""
    return get_embeddings([text])[0]

# ── Info ──────────────────────────────────────────────────────
def info() -> dict:
    """Return current embedding configuration."""
    return {
        "provider": _provider,
        "model": _model,
        "endpoint": _endpoint if _provider == "ollama" else "cloud API",
        "dimensions": _dimensions,
    }
