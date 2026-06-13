"""Wiring tests for kb_find_document into the web chat agent.

Pure wiring coverage (B2a-1): the tool must be (1) declared in
``llm_client.TOOLS`` with the right input schema, (2) registered in
``tool_executor.TOOL_HANDLERS``, and (3) routed to ``mcp_server.kb_find_document``
with the correct params. No network, no real MCP: the mcp module is faked.
"""

from __future__ import annotations

import json
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

from lib import llm_client  # noqa: E402
from lib import tool_executor  # noqa: E402


def _tool(name):
    return next((t for t in llm_client.TOOLS if t["name"] == name), None)


# ── 1. TOOLS declaration ───────────────────────────────────────

def test_kb_find_document_in_tools_with_schema():
    t = _tool("kb_find_document")
    assert t is not None, "kb_find_document missing from llm_client.TOOLS"
    schema = t["input_schema"]
    assert schema["type"] == "object"
    props = schema["properties"]
    assert props["query"]["type"] == "string"
    assert props["n_results"]["type"] == "integer"
    assert props["n_results"].get("default") == 10
    assert schema["required"] == ["query"]
    # Description must signal name/path fuzzy match, NOT content search.
    assert "NON sul contenuto" in t["description"]


# ── 2. handler registration ────────────────────────────────────

def test_kb_find_document_registered():
    assert "kb_find_document" in tool_executor.TOOL_HANDLERS


# ── 3. routing to mcp_server.kb_find_document ───────────────────

class _FakeMcp:
    # mcp_server.kb_find_document is -> list[dict]; the fake mirrors that real
    # signature so the test exercises the str-serialization the handler owns.
    def __init__(self):
        self.calls = []

    def kb_find_document(self, query, n_results=10):
        self.calls.append({"query": query, "n_results": n_results})
        return [{
            "title": "Integrazione Aprile",
            "file_path": "Documenti/integrazione_aprile.pdf",
            "folder": "Documenti",
            "download_url": "https://example/dl/integrazione_aprile.pdf",
        }]


def test_handler_routes_with_params(monkeypatch):
    fake = _FakeMcp()
    monkeypatch.setattr(tool_executor, "_get_mcp", lambda: fake)

    out = tool_executor.execute_tool(
        "kb_find_document", {"query": "integrazione aprile", "n_results": 3}
    )

    assert fake.calls == [{"query": "integrazione aprile", "n_results": 3}]
    # The handler must serialize list[dict] → str: tool_result["content"] is
    # contractually a string for the Anthropic API. Assert a STRING that is
    # valid JSON carrying the document fields as text.
    assert isinstance(out, str)
    parsed = json.loads(out)
    assert parsed[0]["title"] == "Integrazione Aprile"
    assert parsed[0]["file_path"] == "Documenti/integrazione_aprile.pdf"
    assert parsed[0]["download_url"] == "https://example/dl/integrazione_aprile.pdf"
    assert "Integrazione Aprile" in out
    assert "download_url" in out


def test_handler_defaults_n_results(monkeypatch):
    fake = _FakeMcp()
    monkeypatch.setattr(tool_executor, "_get_mcp", lambda: fake)

    tool_executor.execute_tool("kb_find_document", {"query": "report.pdf"})

    assert fake.calls == [{"query": "report.pdf", "n_results": 10}]
