"""Unit tests for AnthropicClient.stream_chat_with_tools — the web chat agent
tool-use loop.

These exercise the *real* streaming loop (range(5) iteration cap, final-synthesis
fallback, within-turn whitespace-block guard) by faking the Anthropic SSE HTTP
response. No network, no real MCP: ``requests.post`` and ``_mcp_call`` are
monkeypatched. Auth resolution is stubbed so no DB/env key lookup happens.
"""

from __future__ import annotations

import json
import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

from lib import llm_client  # noqa: E402
from lib.llm_client import AnthropicClient  # noqa: E402


DETERMINISTIC = "Non sono riuscito a sintetizzare una risposta dai risultati raccolti."
NUDGE_FRAGMENT = "Rispondi ora alla richiesta originale usando esclusivamente le informazioni"


def _last_user_blocks(payload):
    """Content-block type list of the payload's last message (str content → [])."""
    content = payload["messages"][-1]["content"]
    if not isinstance(content, list):
        return []
    return [b.get("type") for b in content]


def _nudge_text(payload):
    """The nudge text block anywhere in the payload's messages, or None."""
    for msg in payload["messages"]:
        content = msg.get("content")
        if not isinstance(content, list):
            continue
        for b in content:
            if isinstance(b, dict) and b.get("type") == "text" and NUDGE_FRAGMENT in b.get("text", ""):
                return b["text"]
    return None

TOOLS = [{"name": "kb_hybrid_search", "description": "d", "input_schema": {"type": "object", "properties": {}}}]


# ── fake Anthropic SSE plumbing ────────────────────────────────

def _sse(events):
    """Render [(event_name, data_dict)] into Anthropic SSE wire lines
    (as iter_lines(decode_unicode=True) would yield them: no trailing \\n)."""
    lines: list[str] = []
    for name, data in events:
        lines.append(f"event: {name}")
        lines.append(f"data: {json.dumps(data)}")
        lines.append("")  # blank line terminates the event
    return lines


class FakeResp:
    def __init__(self, lines, status=200, text="", json_obj=None):
        self._lines = lines
        self.status_code = status
        self.text = text
        self._json = json_obj or {}

    def iter_lines(self, decode_unicode=False):
        for ln in self._lines:
            yield ln

    def json(self):
        return self._json


def tool_turn(tool_id="t1", name="kb_hybrid_search", query="x", text=None):
    """A turn that ends in stop_reason=tool_use. Optionally precedes the
    tool_use block with a text block (used to test the whitespace guard)."""
    evs = []
    idx = 0
    if text is not None:
        evs.append(("content_block_start", {"index": idx, "content_block": {"type": "text", "text": ""}}))
        evs.append(("content_block_delta", {"index": idx, "delta": {"type": "text_delta", "text": text}}))
        evs.append(("content_block_stop", {"index": idx}))
        idx += 1
    evs.append(("content_block_start", {"index": idx, "content_block": {
        "type": "tool_use", "id": tool_id, "name": name, "input": {"query": query}}}))
    evs.append(("content_block_stop", {"index": idx}))
    evs.append(("message_delta", {"delta": {"stop_reason": "tool_use"}, "usage": {"output_tokens": 5}}))
    evs.append(("message_stop", {}))
    return FakeResp(_sse(evs))


def text_turn(text, stop_reason="end_turn"):
    """A turn that ends in stop_reason=end_turn with a single text block."""
    evs = [
        ("content_block_start", {"index": 0, "content_block": {"type": "text", "text": ""}}),
        ("content_block_delta", {"index": 0, "delta": {"type": "text_delta", "text": text}}),
        ("content_block_stop", {"index": 0}),
        ("message_delta", {"delta": {"stop_reason": stop_reason}, "usage": {"output_tokens": 7}}),
        ("message_stop", {}),
    ]
    return FakeResp(_sse(evs))


class _FakePost:
    """Pops a queued response per call, records the outgoing JSON payload."""

    def __init__(self, responses):
        self._it = iter(responses)
        self.payloads: list[dict] = []

    def __call__(self, url, headers=None, json=None, stream=None, timeout=None):
        self.payloads.append(json)
        return next(self._it)


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(llm_client, "resolve_api_key", lambda *a, **k: "k")
    monkeypatch.setattr(llm_client, "_mcp_call", lambda name, args: "RESULT")
    return AnthropicClient({
        "provider": "anthropic", "model": "m", "api_key": "k",
        "max_tokens": 50, "temperature": 0.3,
    })


def _run(client, monkeypatch, responses):
    post = _FakePost(responses)
    monkeypatch.setattr(llm_client.requests, "post", post)
    events = list(client.stream_chat_with_tools("sys", [{"role": "user", "content": "hi"}], tools=TOOLS))
    return events, post


def _types(events):
    return [e["type"] for e in events]


def _joined_tokens(events):
    return "".join(e["delta"] for e in events if e["type"] == "token")


# ── iteration cap → final synthesis ────────────────────────────

def test_iteration_cap_triggers_final_synthesis(client, monkeypatch):
    # 5 tool-only turns exhaust the loop; the 6th request is the tool-free synthesis.
    responses = [tool_turn() for _ in range(5)] + [text_turn("Sintesi finale.")]
    events, post = _run(client, monkeypatch, responses)

    types = _types(events)
    assert "error" not in types
    assert types[-1] == "done"
    assert "Sintesi finale." in _joined_tokens(events)

    # 5 loop requests + 1 synthesis request.
    assert len(post.payloads) == 6
    synth = post.payloads[5]
    # Loop requests carry tools; the synthesis request OMITS the field entirely
    # (passing [] would be re-defaulted to TOOLS upstream).
    assert "tools" in post.payloads[0]
    assert "tools" not in synth

    # The synthesis nudge is appended as a text block AFTER the tool_result(s),
    # in the SAME last user message.
    assert _last_user_blocks(synth) == ["tool_result", "text"]
    assert _nudge_text(synth) is not None

    # Larger token budget scoped to this call only; loop requests keep the default.
    assert synth["max_tokens"] == 1500
    assert post.payloads[0]["max_tokens"] == 50


def test_fallback_does_not_mutate_original_msgs(client, monkeypatch):
    # The nudge lives only in the synthesis COPY. The original msgs (aliased by
    # the loop request payloads) must never carry it — else future turns get
    # poisoned by an injected text block.
    responses = [tool_turn() for _ in range(5)] + [text_turn("ok")]
    events, post = _run(client, monkeypatch, responses)

    # payloads[4]["messages"] is the live `msgs` list (loop sends it by ref);
    # its last message must NOT have gained the nudge.
    original_last = post.payloads[4]["messages"][-1]
    assert all(NUDGE_FRAGMENT not in (b.get("text", "") or "")
               for b in original_last["content"] if b.get("type") == "text")
    # The synthesis copy DOES carry it.
    assert _nudge_text(post.payloads[5]) is not None
    # And exactly one nudge block exists in the synthesis last message.
    assert _last_user_blocks(post.payloads[5]).count("text") == 1


def test_final_synthesis_empty_emits_deterministic_and_error(client, monkeypatch):
    # Synthesis returns empty text → deterministic line + observable error with
    # the captured stop_reason, never mute. Safety-net branch stays intact.
    responses = [tool_turn() for _ in range(5)] + [text_turn("")]  # text_turn → stop_reason=end_turn
    events, post = _run(client, monkeypatch, responses)

    deltas = [e["delta"] for e in events if e["type"] == "token"]
    assert DETERMINISTIC in deltas

    errors = [e["error"] for e in events if e["type"] == "error"]
    assert any("final synthesis returned empty text (stop_reason=end_turn)" in e for e in errors)
    assert len(post.payloads) == 6
    # Nudge was still injected on the (empty) synthesis attempt.
    assert _nudge_text(post.payloads[5]) is not None


# ── within-turn whitespace text block guard ────────────────────

def test_within_turn_whitespace_text_block_excluded(client, monkeypatch):
    # First turn emits a whitespace-only ("\n") text block alongside a tool_use
    # block. The "\n" streams to the UI but must NOT be replayed to the API.
    responses = [tool_turn(text="\n"), text_turn("Risposta.")]
    events, post = _run(client, monkeypatch, responses)

    assert _types(events)[-1] == "done"
    assert "Risposta." in _joined_tokens(events)

    # On the 2nd request, the appended assistant turn must contain only the
    # tool_use block — the whitespace text block was dropped.
    second_msgs = post.payloads[1]["messages"]
    assistant = [m for m in second_msgs if m["role"] == "assistant"][-1]
    block_types = [b["type"] for b in assistant["content"]]
    assert "text" not in block_types
    assert "tool_use" in block_types


# ── normal flows: fallback must NOT fire below the cap ─────────

def test_one_tool_then_answer_no_fallback(client, monkeypatch):
    responses = [tool_turn(), text_turn("Ecco la risposta.")]
    events, post = _run(client, monkeypatch, responses)

    # Exactly 2 requests — no extra synthesis call.
    assert len(post.payloads) == 2
    assert _types(events)[-1] == "done"
    assert "Ecco la risposta." in _joined_tokens(events)
    assert DETERMINISTIC not in [e.get("delta") for e in events if e["type"] == "token"]
    # Convergent branch is untouched: no nudge, default token budget on every request.
    for p in post.payloads:
        assert _nudge_text(p) is None
        assert p["max_tokens"] == 50


def test_direct_answer_zero_tools(client, monkeypatch):
    responses = [text_turn("Diretta.")]
    events, post = _run(client, monkeypatch, responses)

    assert len(post.payloads) == 1
    assert _types(events)[-1] == "done"
    assert _joined_tokens(events) == "Diretta."
    # No fallback path: no nudge, default token budget.
    assert _nudge_text(post.payloads[0]) is None
    assert post.payloads[0]["max_tokens"] == 50
