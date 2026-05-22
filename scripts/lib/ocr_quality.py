"""OCR text quality assessment — deterministic indicators.

Standalone module (no chromadb / no requests / no I/O at import time beyond
the embedded dictionary file). Used to gate VLM fallback for per-page
financial-doc extraction in the ingest pipeline.

Public API:
    assess_text_quality(text: str, **thresholds) -> dict

Pure function: input str -> output dict. No side effects, no logging,
no network. Dictionary file loaded lazily on first call and cached.
"""
import os
import re
from typing import Optional

_TOKEN_RE = re.compile(r"[a-zA-Zàèéìòù]+")
_ALPHANUM_RE = re.compile(r"[a-zA-Z0-9àèéìòù]", re.IGNORECASE)
_WHITESPACE_RE = re.compile(r"\s")

_DICT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ocr_data", "italian_top10k.txt")
_DICT_CACHE: Optional[frozenset] = None


def _load_dict() -> frozenset:
    global _DICT_CACHE
    if _DICT_CACHE is None:
        with open(_DICT_PATH, encoding="utf-8") as f:
            _DICT_CACHE = frozenset(line.strip().lower() for line in f if line.strip())
    return _DICT_CACHE


def assess_text_quality(
    text: str,
    t_mwl: float = 2.5,
    t_dwr: float = 0.50,
    min_token_len: int = 3,
) -> dict:
    """Return deterministic OCR quality indicators for a text block.

    Keys returned:
      - alpha_ratio:     float, alphanumeric chars / non-whitespace chars
                          (TELEMETRY ONLY — not in is_garbage decision; kept
                          for audit logs to surface anomalies unrelated to
                          OCR garbage, e.g. formula-heavy pages)
      - mean_word_len:   float, avg length of whitespace-split tokens
                          (raw .split(), all tokens, no len filter)
      - dict_word_ratio: float, recognized italian words / total qualifying
                          tokens (qualifying = len >= min_token_len, regex
                          [a-zA-Zàèéìòù]+)
      - char_count:      int, len(text)
      - is_garbage:      bool, True iff (mean_word_len < t_mwl) AND
                          (dict_word_ratio < t_dwr)
      - thresholds:      dict echoing the thresholds applied

    Edge cases:
      - empty or len(text) < 50: all metrics 0.0, is_garbage=True
      - 50 <= len(text) < 200: metrics computed normally but is_garbage
        forced to False (short text not enough signal — avoid false positive
        on TOC pages, section dividers, etc.)
    """
    thresholds = {"mean_word_len": t_mwl, "dict_word_ratio": t_dwr}
    char_count = len(text) if text else 0

    if not text or char_count < 50:
        return {
            "alpha_ratio": 0.0,
            "mean_word_len": 0.0,
            "dict_word_ratio": 0.0,
            "char_count": char_count,
            "is_garbage": True,
            "thresholds": thresholds,
        }

    non_ws_chars = _WHITESPACE_RE.sub("", text)
    n_non_ws = len(non_ws_chars)
    n_alpha = len(_ALPHANUM_RE.findall(text))
    alpha_ratio = (n_alpha / n_non_ws) if n_non_ws else 0.0

    raw_tokens = text.split()
    if raw_tokens:
        mean_word_len = sum(len(t) for t in raw_tokens) / len(raw_tokens)
    else:
        mean_word_len = 0.0

    word_tokens = [t for t in _TOKEN_RE.findall(text.lower()) if len(t) >= min_token_len]
    if word_tokens:
        vocab = _load_dict()
        hits = sum(1 for t in word_tokens if t in vocab)
        dict_word_ratio = hits / len(word_tokens)
    else:
        dict_word_ratio = 0.0

    is_garbage = (mean_word_len < t_mwl) and (dict_word_ratio < t_dwr)

    if char_count < 200:
        is_garbage = False

    return {
        "alpha_ratio": alpha_ratio,
        "mean_word_len": mean_word_len,
        "dict_word_ratio": dict_word_ratio,
        "char_count": char_count,
        "is_garbage": is_garbage,
        "thresholds": thresholds,
    }
