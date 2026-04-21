"""Tests for KB search output enrichment.

Covers format_document_fileref, the helper kb_search and kb_hybrid_search
call to surface file_path / file_type / folder / download_url on
document-sourced results. Exercising the helper is enough — the search
tools splice its return value verbatim into their rendered output.
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts", "lib"))

from kb_document_api import format_document_fileref  # noqa: E402


def test_document_source_includes_all_four_fields():
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


def test_non_document_source_emits_no_fields():
    meta = {
        "source": "chatgpt",
        "title": "Some conversation",
        "file_path": "should/not/appear.pdf",  # present but ignored
        "file_type": ".pdf",
        "folder": "whatever",
    }
    out = format_document_fileref(meta)
    assert out == ""
    # Defensive: none of the four field labels should appear.
    for token in ("file_path:", "file_type:", "folder:", "download_url:"):
        assert token not in out


def test_url_encoding_handles_spaces_and_special_chars():
    meta = {
        "source": "document",
        "file_path": "Salute & Fitness/piano aprile 2026.docx",
        "file_type": ".docx",
        "folder": "Salute & Fitness",
    }
    out = format_document_fileref(meta)
    # Path component must be fully URL-encoded (safe='') — no raw spaces,
    # '&' or '/' left over in the download_url value.
    expected_encoded = (
        "Salute%20%26%20Fitness%2Fpiano%20aprile%202026.docx"
    )
    assert f"download_url: /api/kb/document?path={expected_encoded}" in out
    # The raw file_path value should still appear unchanged on its own line.
    assert "file_path: Salute & Fitness/piano aprile 2026.docx" in out
