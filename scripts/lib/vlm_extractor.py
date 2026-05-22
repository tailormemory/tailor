"""VLM fallback extraction for OCR-garbage pages in financial PDFs.

Uses `requests` raw + BackendManager for provider rotation. No SDK.
Compatible providers: google (Gemini), anthropic (Claude).
Daily cap enforced via file counter under ~/tailor/logs/.

Public API:
    rasterize_page(filepath, page_num, dpi) -> bytes
    call_provider_vlm(backend, image_png, system_prompt, user_prompt, timeout) -> str | None
    extract_page_via_vlm(filepath, page_num, total_pages, daily_cap) -> dict | None
    vlm_daily_count_get() -> int
    vlm_daily_count_increment(n) -> int
"""
from __future__ import annotations

import base64
import fcntl
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

try:
    from scripts.lib.backend_manager import BackendManager, _load_api_key  # noqa: F401
except ImportError:
    from backend_manager import BackendManager, _load_api_key  # noqa: F401

logger = logging.getLogger(__name__)

_LOG_DIR = Path(os.path.expanduser("~/tailor/logs"))


SYSTEM_PROMPT_VLM = (
    "Sei un OCR semantico per documenti finanziari italiani. "
    "Estrai il contenuto della pagina che ricevi in input come "
    "Markdown strutturato e fedele."
)

USER_PROMPT_VLM = """Estrai TUTTO il contenuto testuale di questa pagina in formato Markdown.

Regole strette:
1. Preserva le tabelle in markdown standard (pipe + dashes). NON omettere righe né colonne.
2. Numeri: copia esattamente come appaiono (incluse separatori migliaia e decimali italiani: 1.234,56).
3. Codici ISIN, ticker, sigle (ETF, OICR, BTP, ecc.): copia esattamente.
4. Intestazioni in ## o ###. Sezioni separate da riga vuota.
5. NON inventare contenuti non visibili. Se un valore è illeggibile, scrivi `[ILLEGGIBILE]`.
6. Output: SOLO il markdown, senza preamboli, senza spiegazioni, senza note finali."""


def rasterize_page(filepath: str, page_num: int, dpi: int = 144) -> bytes:
    """Rasterize page N (1-indexed) of PDF to PNG bytes at given DPI."""
    import fitz
    doc = None
    try:
        doc = fitz.open(filepath)
        if page_num < 1 or page_num > len(doc):
            raise ValueError(f"page_num {page_num} out of range 1..{len(doc)}")
        page = doc[page_num - 1]
        zoom = dpi / 72.0
        pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom))
        return pix.tobytes("png")
    finally:
        if doc is not None:
            doc.close()


def _vlm_daily_count_file() -> Path:
    return _LOG_DIR / f"vlm_daily_count_{datetime.now().strftime('%Y%m%d')}.txt"


def vlm_daily_count_get() -> int:
    p = _vlm_daily_count_file()
    if not p.exists():
        return 0
    try:
        return int(p.read_text().strip() or "0")
    except (ValueError, OSError):
        return 0


def vlm_daily_count_increment(n: int = 1) -> int:
    """Atomic increment under fcntl.flock. Returns new count."""
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    p = _vlm_daily_count_file()
    with open(p, "a+") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            f.seek(0)
            raw = f.read().strip()
            try:
                current = int(raw) if raw else 0
            except ValueError:
                current = 0
            new = current + n
            f.seek(0)
            f.truncate()
            f.write(str(new))
            f.flush()
            os.fsync(f.fileno())
            return new
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def _call_google_vlm(model: str, api_key: str, image_png: bytes,
                     system_prompt: str, user_prompt: str,
                     timeout: int) -> Optional[str]:
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{model}:generateContent?key={api_key}")
    body = {
        "system_instruction": {"parts": [{"text": system_prompt}]},
        "contents": [{
            "role": "user",
            "parts": [
                {"inline_data": {
                    "mime_type": "image/png",
                    "data": base64.b64encode(image_png).decode("ascii"),
                }},
                {"text": user_prompt},
            ],
        }],
        "generationConfig": {
            "temperature": 0.0,
            "maxOutputTokens": 8192,
        },
    }
    try:
        resp = requests.post(url, headers={"content-type": "application/json"},
                             json=body, timeout=timeout)
    except (requests.Timeout, requests.ConnectionError) as e:
        logger.warning("google VLM transient error: %s", e)
        return None
    if resp.status_code == 429:
        return "RATE_LIMITED"
    if resp.status_code != 200:
        try:
            body_text = resp.text[:500]
        except Exception:
            body_text = "<no body>"
        if "RESOURCE_EXHAUSTED" in body_text:
            return "RATE_LIMITED"
        logger.warning("google VLM non-200: %d %s", resp.status_code, body_text)
        return None
    try:
        data = resp.json()
        if "RESOURCE_EXHAUSTED" in (resp.text or ""):
            return "RATE_LIMITED"
        return data["candidates"][0]["content"]["parts"][0]["text"]
    except (KeyError, IndexError, ValueError) as e:
        logger.warning("google VLM parse error: %s", e)
        return None


def _call_anthropic_vlm(model: str, api_key: str, image_png: bytes,
                        system_prompt: str, user_prompt: str,
                        timeout: int) -> Optional[str]:
    body = {
        "model": model,
        "max_tokens": 8192,
        "system": system_prompt,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "image",
                 "source": {"type": "base64",
                            "media_type": "image/png",
                            "data": base64.b64encode(image_png).decode("ascii")}},
                {"type": "text", "text": user_prompt},
            ],
        }],
        "temperature": 0.0,
    }
    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=body, timeout=timeout,
        )
    except (requests.Timeout, requests.ConnectionError) as e:
        logger.warning("anthropic VLM transient error: %s", e)
        return None
    if resp.status_code == 429:
        return "RATE_LIMITED"
    if resp.status_code != 200:
        try:
            body_text = resp.text[:500]
        except Exception:
            body_text = "<no body>"
        logger.warning("anthropic VLM non-200: %d %s", resp.status_code, body_text)
        return None
    try:
        data = resp.json()
        return data["content"][0]["text"]
    except (KeyError, IndexError, ValueError) as e:
        logger.warning("anthropic VLM parse error: %s", e)
        return None


def call_provider_vlm(backend: dict, image_png: bytes,
                      system_prompt: str, user_prompt: str,
                      timeout: int = 90) -> Optional[str]:
    """Dispatch one VLM call. Returns markdown str, 'RATE_LIMITED', or None.

    Raises ValueError on missing api_key (config error, not transient).
    """
    provider = backend.get("name", "")
    model = backend.get("model", "")
    api_key = backend.get("api_key", "")
    if not api_key:
        raise ValueError(f"VLM backend {provider}/{model} missing api_key")
    if provider == "google":
        return _call_google_vlm(model, api_key, image_png,
                                system_prompt, user_prompt, timeout)
    if provider == "anthropic":
        return _call_anthropic_vlm(model, api_key, image_png,
                                   system_prompt, user_prompt, timeout)
    logger.warning("VLM provider %r not supported (skip)", provider)
    return None


def extract_page_via_vlm(filepath: str, page_num: int, total_pages: int,
                         daily_cap: int = 100,
                         raster_dpi: int = 144) -> Optional[dict]:
    """Rasterize page N and call VLM via BackendManager rotation.

    Returns paddle-schema-compatible section dict on success, or None on:
      - daily cap reached
      - all backends exhausted
      - 2 attempts both failed
    """
    count = vlm_daily_count_get()
    if count >= daily_cap:
        logger.warning("VLM daily cap reached (%d >= %d), skip page %d of %s",
                       count, daily_cap, page_num, filepath)
        return None

    try:
        image_png = rasterize_page(filepath, page_num, dpi=raster_dpi)
    except Exception as e:
        logger.error("VLM rasterize failed for %s page %d: %s",
                     filepath, page_num, e)
        return None

    manager = BackendManager("vlm_extraction")
    if not manager.backends:
        logger.warning("VLM no backends configured for role vlm_extraction")
        return None

    markdown: Optional[str] = None
    used_backend: Optional[dict] = None
    for _attempt in range(2):
        backend = manager.current()
        if backend is None:
            break
        try:
            result = call_provider_vlm(backend, image_png,
                                       SYSTEM_PROMPT_VLM, USER_PROMPT_VLM)
        except ValueError as e:
            logger.error("VLM config error: %s", e)
            manager.mark_error()
            continue
        if result == "RATE_LIMITED":
            manager.mark_rate_limited()
            continue
        if result is None:
            manager.mark_error()
            continue
        manager.mark_success()
        vlm_daily_count_increment()
        markdown = result
        used_backend = backend
        break

    if markdown is None or used_backend is None:
        return None

    return {
        "text": markdown,
        "markdown": markdown,
        "metadata": {
            "page": page_num,
            "total_pages": total_pages,
            "ocr": True,
            "extractor": f"vlm-{used_backend['name']}-{used_backend['model']}",
        },
    }
