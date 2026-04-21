"""Unit tests for scripts.lib.kb_find_document.find_documents.

The pure function takes a list of (chunk_id, metadata) tuples, so tests
can exercise ranking, dedup, supersession filtering, and n_results caps
without a live ChromaDB. The MCP tool wrapper in mcp_server.py just
pages the collection and hands rows to this helper.
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts", "lib"))

from kb_find_document import find_documents  # noqa: E402


def _chunk(cid: str, **meta) -> tuple[str, dict]:
    base = {"source": "document"}
    base.update(meta)
    return (cid, base)


def test_filename_exact_match_ranks_first():
    chunks = [
        _chunk(
            "c1", title="report.pdf", file_path="Finance/report.pdf",
            folder="Finance", file_type=".pdf",
        ),
        _chunk(
            "c2", title="budget.pdf", file_path="Finance/budget.pdf",
            folder="Finance", file_type=".pdf",
        ),
        _chunk(
            "c3", title="other.pdf", file_path="Work/other.pdf",
            folder="Work", file_type=".pdf",
        ),
    ]
    results = find_documents("report.pdf", chunks)
    assert results[0]["file_path"] == "Finance/report.pdf"
    assert results[0]["title"] == "report.pdf"
    assert results[0]["download_url"] == (
        "/api/kb/document?path=Finance%2Freport.pdf"
    )


def test_filename_partial_match_with_separator_normalization():
    # Underscores in the filename normalize to spaces so "integrazione aprile"
    # matches inside "piano_integrazione_aprile2026.docx".
    chunks = [
        _chunk(
            "c1", title="piano_integrazione_aprile2026.docx",
            file_path="Progetti/piano_integrazione_aprile2026.docx",
            folder="Progetti", file_type=".docx",
        ),
        _chunk(
            "c2", title="note.docx", file_path="Progetti/note.docx",
            folder="Progetti", file_type=".docx",
        ),
    ]
    results = find_documents("integrazione aprile", chunks)
    assert results[0]["file_path"] == (
        "Progetti/piano_integrazione_aprile2026.docx"
    )


def test_folder_match_returns_all_docs_under_folder():
    chunks = [
        _chunk(
            "c1", title="piano.docx",
            file_path="Salute & Fitness/piano.docx",
            folder="Salute & Fitness", file_type=".docx",
        ),
        _chunk(
            "c2", title="scheda.pdf",
            file_path="Salute & Fitness/scheda.pdf",
            folder="Salute & Fitness", file_type=".pdf",
        ),
        _chunk(
            "c3", title="unrelated.pdf",
            file_path="Finance/unrelated.pdf",
            folder="Finance", file_type=".pdf",
        ),
    ]
    results = find_documents("Salute", chunks)
    paths = {r["file_path"] for r in results[:2]}
    assert paths == {
        "Salute & Fitness/piano.docx",
        "Salute & Fitness/scheda.pdf",
    }
    # The Finance doc should rank strictly lower than either Salute hit.
    finance = next(r for r in results if r["file_path"].startswith("Finance/"))
    salute_scores_come_first = results.index(finance) >= 2
    assert salute_scores_come_first


def test_dedup_single_document_with_many_chunks():
    shared = {
        "title": "big.pdf",
        "file_path": "Docs/big.pdf",
        "folder": "Docs",
        "file_type": ".pdf",
        "date": "2026-01-15",
    }
    chunks = [_chunk(f"c{i}", **shared) for i in range(12)]
    results = find_documents("big", chunks)
    assert len(results) == 1
    assert results[0]["chunk_count"] == 12
    assert results[0]["last_indexed_date"] == "2026-01-15"


def test_fully_superseded_document_excluded():
    chunks = [
        _chunk(
            "c1", title="stale.pdf", file_path="Docs/stale.pdf",
            folder="Docs", file_type=".pdf", superseded_by="other",
        ),
        _chunk(
            "c2", title="stale.pdf", file_path="Docs/stale.pdf",
            folder="Docs", file_type=".pdf", superseded_by="other",
        ),
        _chunk(
            "c3", title="fresh.pdf", file_path="Docs/fresh.pdf",
            folder="Docs", file_type=".pdf",
        ),
    ]
    results = find_documents("stale", chunks)
    assert [r["file_path"] for r in results] == ["Docs/fresh.pdf"]


def test_n_results_cap_respected():
    chunks = [
        _chunk(
            f"c{i}", title=f"doc{i}.pdf",
            file_path=f"Docs/doc{i}.pdf",
            folder="Docs", file_type=".pdf",
        )
        for i in range(25)
    ]
    results = find_documents("doc", chunks, n_results=5)
    assert len(results) == 5
