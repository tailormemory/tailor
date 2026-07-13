"""Regression test for /api/ingest-live embedding the FIRST CHARACTER.

Before the fix, the handler did:

    embedding = get_embeddings(text[:4000])      # <-- passes a STR
    if embedding and isinstance(embedding[0], list):
        embedding = embedding[0]

`get_embeddings(texts: list[str])` expects a LIST. Passing a string makes the
Ollama provider iterate over CHARACTERS (`[t[:4000] for t in texts]`), so a
51-char text yields 51 vectors — one per character. The "flatten" branch then
picks `embedding[0]`, i.e. the vector of the FIRST CHARACTER, and stores that.

This test uses a fake provider that reproduces the per-element semantics of
`_embed_ollama` and asserts the embedding handed to the upsert equals the
embedding of the FULL text, not of its first character.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts", "lib"))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import mcp_server  # noqa: E402


def _vec(text):
    """Deterministic, order-sensitive vector. Distinct for a single char vs
    the full text: sum-of-ordinals separates them, length disambiguates."""
    return [float(sum(ord(c) for c in text)), float(len(text))]


def _fake_get_embeddings(texts):
    """Mimics _embed_ollama: iterates the passed iterable and embeds each item.
    If handed a str (the bug), it iterates CHARACTERS."""
    return [_vec(t[:4000]) for t in texts]


class _FakeCollection:
    def get(self, ids=None, **kw):
        # No existing chunk -> is_update stays False.
        return {"ids": []}


def _run(coro):
    return asyncio.run(coro)


def test_ingest_live_embeds_full_text_not_first_char(monkeypatch):
    monkeypatch.setattr(mcp_server, "get_embeddings", _fake_get_embeddings)
    monkeypatch.setattr(mcp_server, "get_collection", lambda: _FakeCollection())

    captured = {}

    def _fake_verified_upsert(getter, ids, embeddings, documents, metadatas, **kw):
        captured["embeddings"] = embeddings
        captured["documents"] = documents
        return True

    monkeypatch.setattr(mcp_server, "verified_upsert", _fake_verified_upsert)

    payload = {
        "source": "test",
        "title": "T",
        "messages": [{"role": "user", "content": "ciao come stai oggi amico"}],
    }
    body = json.dumps(payload).encode()

    async def _receive():
        return {"body": body, "more_body": False}

    scope = {"method": "POST", "client": ("127.0.0.1", 0)}

    middleware = mcp_server.BearerAuthMiddleware(app=None)
    response = _run(middleware._handle_rest_api("/api/ingest-live", scope, _receive))

    assert response.status_code == 200, response.body

    text = captured["documents"][0]              # exactly what was stored
    stored = captured["embeddings"][0]
    expected_full = _vec(text[:4000])
    first_char = _vec(text[:4000][0])

    assert stored == expected_full, (
        f"embedding is not the full-text vector; got {stored}, "
        f"expected {expected_full}"
    )
    assert stored != first_char, (
        "embedding equals the FIRST-CHARACTER vector (the bug): "
        f"{stored} == {first_char}"
    )
