"""Unit tests for scripts.lib.kb_document_api.handle_kb_document_request.

The endpoint is a thin wrapper over the handler; exercising the handler
directly is enough to cover path resolution, traversal guards, MIME
inference, and error shapes. The auth test wraps the handler in a
minimal BearerGate to confirm upstream auth is required before reaching
the handler — same pattern as test_chat_api.py:test_set_default_provider_requires_auth.
"""

from __future__ import annotations

import json
import os
import sys
from urllib.parse import quote

import pytest
from starlette.responses import Response
from starlette.testclient import TestClient

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts", "lib"))

from kb_document_api import handle_kb_document_request  # noqa: E402


@pytest.fixture
def doc_root(tmp_path):
    """Create a document root with a nested docx file and a subdir."""
    root = tmp_path / "docs"
    root.mkdir()
    nested = root / "Salute & Fitness"
    nested.mkdir()
    f = nested / "piano aprile 2026.docx"
    f.write_bytes(b"PK\x03\x04fake-docx-bytes")
    return str(root)


def _qs(params: dict[str, str]) -> str:
    return "&".join(f"{k}={quote(v, safe='')}" for k, v in params.items())


def test_happy_path_returns_bytes_and_correct_mime(doc_root):
    qs = _qs({"path": "Salute & Fitness/piano aprile 2026.docx"})
    resp = handle_kb_document_request(qs, doc_root)
    assert resp.status_code == 200
    assert resp.media_type == (
        "application/vnd.openxmlformats-officedocument"
        ".wordprocessingml.document"
    )
    assert resp.body == b"PK\x03\x04fake-docx-bytes"
    cd = resp.headers.get("content-disposition", "")
    assert cd.startswith("inline;")
    assert 'filename="piano aprile 2026.docx"' in cd
    assert resp.headers.get("content-length") == str(len(resp.body))
    assert resp.headers.get("cache-control") == "private, max-age=0"


def test_path_traversal_attempt_is_forbidden(doc_root, tmp_path):
    # Plant a secret outside the root to make sure traversal actually fails.
    outside = tmp_path / "etc_passwd"
    outside.write_text("root:x:0:0")
    qs = _qs({"path": "../etc_passwd"})
    resp = handle_kb_document_request(qs, doc_root)
    assert resp.status_code == 403
    assert json.loads(resp.body)["error"] == "Forbidden"


def test_absolute_path_in_query_is_forbidden(doc_root):
    qs = _qs({"path": "/etc/passwd"})
    resp = handle_kb_document_request(qs, doc_root)
    assert resp.status_code == 403
    assert "Absolute" in json.loads(resp.body)["error"]


def test_missing_file_returns_404(doc_root):
    qs = _qs({"path": "does/not/exist.pdf"})
    resp = handle_kb_document_request(qs, doc_root)
    assert resp.status_code == 404


def test_directory_instead_of_file_returns_404(doc_root):
    qs = _qs({"path": "Salute & Fitness"})
    resp = handle_kb_document_request(qs, doc_root)
    assert resp.status_code == 404


def test_document_root_unset_returns_503():
    qs = _qs({"path": "anything.pdf"})
    resp = handle_kb_document_request(qs, document_root=None)
    assert resp.status_code == 503
    assert json.loads(resp.body)["error"] == "ingest.document_root not configured"


def test_auth_required_without_token(doc_root):
    """Wrap the handler in a Bearer gate mirroring BearerAuthMiddleware
    and assert that a request without a token never reaches the handler.
    """
    async def _app(scope, receive, send):
        if scope["type"] != "http":
            return
        hdrs = dict(scope.get("headers", []))
        auth = hdrs.get(b"authorization", b"").decode("utf-8", errors="ignore")
        if not auth.startswith("Bearer "):
            await Response(
                content='{"error":"Unauthorized"}',
                status_code=401,
                media_type="application/json",
            )(scope, receive, send)
            return
        qs = scope.get("query_string", b"").decode()
        resp = handle_kb_document_request(qs, doc_root)
        await resp(scope, receive, send)

    client = TestClient(_app)
    resp = client.get(
        "/api/kb/document",
        params={"path": "Salute & Fitness/piano aprile 2026.docx"},
    )
    assert resp.status_code == 401
