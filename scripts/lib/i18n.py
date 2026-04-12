"""
TAILOR — AI-powered i18n
Translates user-facing strings to the configured language using LLM.

Usage:
    from scripts.lib.i18n import t
    message = t("Pipeline completed with errors")  # → "Pipeline notturna con errori" (if lang=it)

How it works:
- English text is the source of truth in code
- If user language is "en", returns text as-is (zero cost)
- Persistent disk cache (db/translations_{lang}.json): each string translated only once ever
- In-memory cache on top for speed
- Translates via best available LLM backend (Haiku > Gemini > GPT-4o-mini > Ollama)
- Falls back to original English if all backends fail

Configuration:
    user.language in config/tailor.yaml (default: "en")
"""

import os
import sys
import json
import time

# ── Config ────────────────────────────────────────────────────
try:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
    from scripts.lib.config import get as cfg
    _USER_LANG = (cfg("user", "language") or "en").lower().strip()
    _LLM_PROVIDER = (cfg("llm", "provider") or "anthropic").lower().strip()
except Exception:
    _USER_LANG = "en"
    _LLM_PROVIDER = "anthropic"

DB_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "db")

# ── Caches ────────────────────────────────────────────────────
_mem_cache: dict[str, str] = {}
_disk_cache: dict[str, str] = {}
_disk_cache_path: str = ""
_disk_loaded: bool = False


def _load_disk_cache(lang: str) -> None:
    """Load persistent translation cache from disk."""
    global _disk_cache, _disk_cache_path, _disk_loaded
    _disk_cache_path = os.path.join(DB_DIR, f"translations_{lang}.json")
    try:
        if os.path.exists(_disk_cache_path):
            with open(_disk_cache_path, "r") as f:
                _disk_cache = json.load(f)
    except Exception:
        _disk_cache = {}
    _disk_loaded = True


def _save_to_disk(key: str, value: str) -> None:
    """Save a single translation to persistent cache."""
    _disk_cache[key] = value
    try:
        os.makedirs(os.path.dirname(_disk_cache_path), exist_ok=True)
        with open(_disk_cache_path, "w") as f:
            json.dump(_disk_cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def t(text: str, lang: str | None = None) -> str:
    """Translate text to the user's configured language.
    
    Args:
        text: English source string
        lang: Override language (default: from config)
    
    Returns:
        Translated string, or original if lang is "en" or translation fails
    """
    target = (lang or _USER_LANG).lower().strip()
    
    # No translation needed
    if target in ("en", "english", ""):
        return text
    
    # Load disk cache on first call
    if not _disk_loaded:
        _load_disk_cache(target)
    
    # Check memory cache
    if text in _mem_cache:
        return _mem_cache[text]
    
    # Check disk cache
    if text in _disk_cache:
        _mem_cache[text] = _disk_cache[text]
        return _disk_cache[text]
    
    # Translate via LLM
    translated = _translate(text, target)
    if translated and translated != text:
        _mem_cache[text] = translated
        _save_to_disk(text, translated)
        return translated
    
    return text


# Map language codes to full names for better LLM understanding
_LANG_NAMES = {
    "it": "Italian", "es": "Spanish", "fr": "French", "de": "German",
    "pt": "Portuguese", "nl": "Dutch", "ja": "Japanese", "zh": "Chinese",
    "ko": "Korean", "ru": "Russian", "ar": "Arabic", "pl": "Polish",
    "sv": "Swedish", "da": "Danish", "no": "Norwegian", "fi": "Finnish",
    "tr": "Turkish", "cs": "Czech", "ro": "Romanian", "hu": "Hungarian",
}

def _translate(text: str, target_lang: str) -> str | None:
    """Translate via best available backend."""
    lang_name = _LANG_NAMES.get(target_lang, target_lang)
    
    prompt = (
        f"Translate the following text to {lang_name}. "
        "Reply ONLY with the translation, nothing else. No quotes, no explanation. "
        "Keep emoji, numbers, technical terms (MCP, KB, API, etc.), and proper nouns unchanged. "
        "Keep placeholders like {variable} unchanged.\n\n"
        f"{text}"
    )
    
    # Order backends: user's configured provider first, then others as fallback
    _BACKEND_MAP = {
        "anthropic": _try_anthropic,
        "google": _try_google,
        "openai": _try_openai,
        "ollama": _try_ollama,
    }
    all_backends = [_try_anthropic, _try_google, _try_openai, _try_ollama]
    primary = _BACKEND_MAP.get(_LLM_PROVIDER)
    if primary:
        ordered = [primary] + [b for b in all_backends if b is not primary]
    else:
        ordered = all_backends
    
    for backend_fn in ordered:
        try:
            result = backend_fn(prompt, text)
            if result:
                return result
        except Exception:
            continue
    
    return None


def _try_anthropic(prompt: str, original: str) -> str | None:
    """Translate via Claude Haiku."""
    import requests
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        # Try loading from plist (macOS)
        try:
            import subprocess
            result = subprocess.run(
                ["plutil", "-extract", "EnvironmentVariables.ANTHROPIC_API_KEY", "raw",
                 "/Library/LaunchDaemons/com.tailor.mcp.plist"],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                api_key = result.stdout.strip()
        except Exception:
            pass
    if not api_key:
        return None
    
    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
        json={"model": "claude-haiku-4-5-20251001", "max_tokens": len(original) * 4, "temperature": 0.1,
              "messages": [{"role": "user", "content": prompt}]},
        timeout=15
    )
    if resp.status_code == 200:
        content = resp.json().get("content", [{}])
        if content:
            result = content[0].get("text", "").strip()
            if result and len(result) < len(original) * 5:
                return result
    return None


def _try_google(prompt: str, original: str) -> str | None:
    """Translate via Gemini Flash."""
    import requests
    api_key = os.environ.get("GOOGLE_API_KEY", "")
    if not api_key:
        return None
    
    resp = requests.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}",
        json={"contents": [{"parts": [{"text": prompt}]}],
              "generationConfig": {"temperature": 0.1, "maxOutputTokens": len(original) * 4}},
        timeout=15
    )
    if resp.status_code == 200:
        candidates = resp.json().get("candidates", [{}])
        if candidates:
            parts = candidates[0].get("content", {}).get("parts", [{}])
            if parts:
                result = parts[0].get("text", "").strip()
                if result and len(result) < len(original) * 5:
                    return result
    return None


def _try_openai(prompt: str, original: str) -> str | None:
    """Translate via GPT-4o-mini."""
    import requests
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        return None
    
    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={"model": "gpt-4o-mini", "temperature": 0.1, "max_tokens": len(original) * 4,
              "messages": [{"role": "user", "content": prompt}]},
        timeout=15
    )
    if resp.status_code == 200:
        choices = resp.json().get("choices", [{}])
        if choices:
            result = choices[0].get("message", {}).get("content", "").strip()
            if result and len(result) < len(original) * 5:
                return result
    return None


def _try_ollama(prompt: str, original: str) -> str | None:
    """Translate via local Ollama (fallback)."""
    import requests
    ollama_url = os.environ.get("OLLAMA_URL", "http://localhost:11434")
    model = os.environ.get("TRANSLATE_MODEL", "qwen2.5:7b")
    
    resp = requests.post(
        f"{ollama_url}/api/chat",
        json={"model": model, "stream": False, "options": {"temperature": 0.1, "num_predict": len(original) * 4},
              "messages": [{"role": "user", "content": prompt}]},
        timeout=30
    )
    if resp.status_code == 200:
        result = resp.json().get("message", {}).get("content", "").strip()
        if result and len(result) < len(original) * 5:
            return result
    return None


# ── CLI helper (for bash scripts) ─────────────────────────────
# Usage: python3 scripts/lib/i18n.py "Text to translate"
# Returns translated text to stdout

if __name__ == "__main__":
    if len(sys.argv) > 1:
        text = " ".join(sys.argv[1:])
        print(t(text))
    else:
        # Test mode
        test_strings = [
            "Pipeline completed with errors",
            "Duration: 3600s",
            "🔗 Derives: 150 inferred from 42 entities",
            "MCP server not restarted after 30s!",
            "Reply with a number.",
            "Date (15/04/2026 09:00) is in the past.",
            "Save error: timeout",
            "⚠️ Nightly pipeline (v9)",
            "🏷 Entities: 350 chunks extracted",
            "Pipeline crashed — MCP server NOT restarted. Manual intervention required.",
        ]
        print(f"Language: {_USER_LANG}")
        print(f"Cache: {_disk_cache_path}\n")
        for s in test_strings:
            start = time.time()
            translated = t(s)
            elapsed = time.time() - start
            cached = "cache" if elapsed < 0.01 else f"{elapsed:.1f}s"
            if translated != s:
                print(f"  ✅ [{cached}] {s}\n     → {translated}\n")
            else:
                print(f"  ── [{cached}] {s} (unchanged)\n")
