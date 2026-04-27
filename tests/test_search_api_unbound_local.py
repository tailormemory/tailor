"""Regression test for the /api/search UnboundLocalError.

Before the fix, _handle_rest_api had a conditional `from scripts.lib.embedding
import get_embeddings` deep in the body, which caused Python to treat
`get_embeddings` as a local variable across the entire method. The
/api/search branch (which references the module-level `get_embeddings`)
therefore raised UnboundLocalError before ever reaching the import.
"""

from __future__ import annotations

import asyncio
import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts", "lib"))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import mcp_server  # noqa: E402


class _FakeCollection:
    def query(self, query_embeddings, n_results, where, include):
        return {
            "ids": [["chunk-1"]],
            "documents": [["hello world"]],
            "metadatas": [[{"source": "test"}]],
            "distances": [[0.1]],
        }


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro) if sys.version_info < (3, 10) else asyncio.run(coro)


def test_handle_rest_api_does_not_shadow_get_embeddings():
    """Structural guard: get_embeddings must resolve to the module-level
    import, not be rebound as a local by a nested `from ... import`."""
    code = mcp_server.BearerAuthMiddleware._handle_rest_api.__code__
    assert "get_embeddings" not in code.co_varnames, (
        "get_embeddings is being treated as a local variable in "
        "_handle_rest_api; a nested `from ... import get_embeddings` is "
        "shadowing the module-level binding and will cause UnboundLocalError."
    )


def test_api_search_does_not_raise_unbound_local(monkeypatch):
    """Functional guard: /api/search returns a Response, not UnboundLocalError."""
    monkeypatch.setattr(mcp_server, "get_embeddings", lambda qs: [[0.1, 0.2, 0.3]])
    monkeypatch.setattr(mcp_server, "get_collection", lambda: _FakeCollection())

    middleware = mcp_server.BearerAuthMiddleware(app=None)
    scope = {"query_string": b"q=hello&n=3", "client": ("127.0.0.1", 0)}

    response = _run(middleware._handle_rest_api("/api/search", scope))

    assert response.status_code == 200
    assert b"chunk-1" in response.body
