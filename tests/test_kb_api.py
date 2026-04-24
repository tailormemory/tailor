"""Tests for KB search output enrichment.

Covers format_document_fileref and _resolve_download_base_url, the two
helpers kb_search / kb_hybrid_search / kb_find_document use to surface
file_path / file_type / folder / download_url on document-sourced
results. Exercising the helpers is enough — the search tools splice their
return values verbatim into their rendered output.
"""

from __future__ import annotations

import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts", "lib"))

from kb_document_api import (  # noqa: E402
    _resolve_download_base_url,
    format_document_fileref,
)


# ── format_document_fileref ────────────────────────────────────


def test_document_source_emits_relative_url_when_no_base_url():
    meta = {
        "source": "document",
        "file_path": "Finance/2024/report.pdf",
        "file_type": ".pdf",
        "folder": "Finance",
        "title": "report.pdf",
    }
    out = format_document_fileref(meta)
    assert "file_path: Finance/2024/report.pdf" in out
    assert "file_type: .pdf" in out
    assert "folder: Finance" in out
    assert (
        "download_url: /api/kb/document?path=Finance%2F2024%2Freport.pdf" in out
    )


def test_document_source_emits_absolute_url_when_base_url_set():
    meta = {
        "source": "document",
        "file_path": "Finance/2024/report.pdf",
        "file_type": ".pdf",
        "folder": "Finance",
    }
    out = format_document_fileref(meta, base_url="https://tailor.example.com")
    assert (
        "download_url: https://tailor.example.com"
        "/api/kb/document?path=Finance%2F2024%2Freport.pdf" in out
    )


def test_base_url_trailing_slash_is_stripped():
    # Defense in depth: server-side validation rejects trailing slashes,
    # but if one ever slips in we still want a clean URL, not a doubled
    # slash between origin and path.
    meta = {
        "source": "document",
        "file_path": "a.pdf",
        "file_type": ".pdf",
        "folder": "",
    }
    out = format_document_fileref(meta, base_url="https://example.com/")
    assert "download_url: https://example.com/api/kb/document?path=a.pdf" in out


def test_non_document_source_emits_no_fields():
    meta = {
        "source": "chatgpt",
        "title": "Some conversation",
        "file_path": "should/not/appear.pdf",
        "file_type": ".pdf",
        "folder": "whatever",
    }
    out = format_document_fileref(meta, base_url="https://example.com")
    assert out == ""
    for token in ("file_path:", "file_type:", "folder:", "download_url:"):
        assert token not in out


def test_url_encoding_handles_spaces_and_special_chars():
    meta = {
        "source": "document",
        "file_path": "Salute & Fitness/piano aprile 2026.docx",
        "file_type": ".docx",
        "folder": "Salute & Fitness",
    }
    out = format_document_fileref(meta, base_url="https://tailor.example.com")
    expected_encoded = (
        "Salute%20%26%20Fitness%2Fpiano%20aprile%202026.docx"
    )
    assert (
        f"download_url: https://tailor.example.com"
        f"/api/kb/document?path={expected_encoded}" in out
    )
    # The raw file_path value should still appear unchanged on its own line.
    assert "file_path: Salute & Fitness/piano aprile 2026.docx" in out


# ── _resolve_download_base_url ─────────────────────────────────


class _FakeRequest:
    """Minimal stand-in for starlette.requests.Request — the helper only
    pokes .headers.get(), so anything exposing that dict-like contract is
    enough. Using a typing.cast to avoid a heavy real Request build.
    """

    def __init__(self, headers: dict[str, str] | None = None):
        self.headers = headers or {}


@pytest.fixture
def no_config(monkeypatch):
    """Isolate the helper from whatever tailor.yaml happens to be on disk.

    The helper imports scripts.lib.config.get lazily, which consults
    _config (module singleton). Forcing _config={} short-circuits the
    disk read and guarantees the auto-detect branch is exercised.
    """
    from scripts.lib import config as _cfg_mod
    monkeypatch.setattr(_cfg_mod, "_config", {})
    yield


@pytest.fixture
def configured_base_url(monkeypatch):
    """Inject a non-empty server.public_base_url into the config cache."""
    from scripts.lib import config as _cfg_mod
    monkeypatch.setattr(
        _cfg_mod, "_config",
        {"server": {"public_base_url": "https://configured.example.com"}},
    )
    yield "https://configured.example.com"


def test_resolve_prefers_configured_over_request(configured_base_url):
    req = _FakeRequest({"host": "other.host", "x-forwarded-proto": "https"})
    base, source = _resolve_download_base_url(req)
    assert base == configured_base_url
    assert source == "config"


def test_resolve_strips_trailing_slash_on_configured(monkeypatch):
    # Validation rejects trailing slashes on save, but the helper still
    # defends itself — a stray slash from a hand-edited yaml shouldn't
    # produce `https://host//api/...`.
    from scripts.lib import config as _cfg_mod
    monkeypatch.setattr(
        _cfg_mod, "_config",
        {"server": {"public_base_url": "https://configured.example.com/"}},
    )
    base, source = _resolve_download_base_url(None)
    assert base == "https://configured.example.com"
    assert source == "config"


def test_resolve_empty_config_falls_through_to_auto(monkeypatch):
    from scripts.lib import config as _cfg_mod
    monkeypatch.setattr(
        _cfg_mod, "_config",
        {"server": {"public_base_url": ""}},
    )
    req = _FakeRequest({"host": "auto.example.com", "x-forwarded-proto": "https"})
    base, source = _resolve_download_base_url(req)
    assert base == "https://auto.example.com"
    assert source == "auto"


def test_resolve_autodetect_with_host_and_forwarded_proto(no_config):
    req = _FakeRequest({
        "host": "tailor.example.com",
        "x-forwarded-proto": "https",
    })
    base, source = _resolve_download_base_url(req)
    assert base == "https://tailor.example.com"
    assert source == "auto"


def test_resolve_autodetect_without_forwarded_proto_defaults_to_http(no_config):
    req = _FakeRequest({"host": "tailor.example.com"})
    base, source = _resolve_download_base_url(req)
    assert base == "http://tailor.example.com"
    assert source == "auto"


def test_resolve_autodetect_missing_host_falls_back_to_localhost(no_config):
    req = _FakeRequest({})  # no Host, no X-Forwarded-Proto
    base, source = _resolve_download_base_url(req)
    assert base == "http://localhost:8787"
    assert source == "auto"


def test_resolve_autodetect_without_request_returns_localhost(no_config):
    base, source = _resolve_download_base_url(None)
    assert base == "http://localhost:8787"
    assert source == "auto"


def test_resolve_null_config_falls_through_to_auto(monkeypatch):
    # YAML null comes through as Python None — the "leave null to
    # auto-detect" contract from tailor.yaml.example must keep working.
    from scripts.lib import config as _cfg_mod
    monkeypatch.setattr(
        _cfg_mod, "_config",
        {"server": {"public_base_url": None}},
    )
    req = _FakeRequest({"host": "auto.example.com"})
    base, source = _resolve_download_base_url(req)
    assert base == "http://auto.example.com"
    assert source == "auto"
