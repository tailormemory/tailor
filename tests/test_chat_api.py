"""Integration tests for the /api/chat routes.

Uses Starlette's TestClient directly against the chat sub-app — no HTTP
calls to localhost, no real LLM, no real auth. Auth is applied upstream by
mcp_server.BearerAuthMiddleware and is not this module's concern.
"""

from __future__ import annotations

import json
import os
import sys
import time

import pytest
from starlette.testclient import TestClient

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "scripts", "lib"))

from chat_api import build_app  # noqa: E402
from chat_session_store import ChatSessionStore  # noqa: E402


class FakeLLM:
    """Records calls and replays a scripted event stream."""

    def __init__(self, events):
        self.events = list(events)
        self.calls: list[dict] = []

    def stream_chat_with_tools(self, system, messages, tools=None):
        self.calls.append({"system": system, "messages": list(messages), "tools": tools})
        for ev in self.events:
            yield ev


def make_cfg(overrides: dict | None = None):
    base = {
        ("chat_interface", "enabled"): True,
        ("chat_interface", "max_history_messages"): 20,
        ("chat_interface", "max_message_length"): 8000,
        ("chat_interface", "sse_keepalive_seconds"): 15,
        ("chat_interface", "system_prompt"): "Test prompt.",
    }
    if overrides:
        base.update(overrides)

    def cfg_get(section, key=None, default=None):
        if key is None:
            return {k[1]: v for k, v in base.items() if k[0] == section} or default
        return base.get((section, key), default)

    return cfg_get


@pytest.fixture
def store(tmp_path):
    return ChatSessionStore(str(tmp_path / "chat_sessions.sqlite3"))


def build(store, events, cfg_overrides=None):
    llm = FakeLLM(events)
    app = build_app(
        store=store,
        llm_factory=lambda: llm,
        cfg_get=make_cfg(cfg_overrides),
        tools=[],
    )
    return TestClient(app), llm


def parse_sse(text: str):
    """Parse SSE wire text into [(event, data_obj_or_str), ...]."""
    events = []
    current_event = None
    current_data: list[str] = []
    for line in text.splitlines() + [""]:
        if line.startswith(":"):
            continue
        if line == "":
            if current_event is not None or current_data:
                data_str = "\n".join(current_data)
                try:
                    data = json.loads(data_str) if data_str else None
                except json.JSONDecodeError:
                    data = data_str
                events.append((current_event or "", data))
            current_event = None
            current_data = []
            continue
        if line.startswith("event:"):
            current_event = line[len("event:"):].strip()
        elif line.startswith("data:"):
            current_data.append(line[len("data:"):].lstrip())
    return events


# ── POST /api/chat ─────────────────────────────────────────────

def test_post_chat_creates_session_and_streams_tokens(store):
    client, llm = build(store, [
        {"type": "token", "delta": "Ciao"},
        {"type": "token", "delta": ", mondo."},
        {"type": "done", "tokens": 12},
    ])
    resp = client.post("/api/chat", json={"session_id": None, "message": "hello"})
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/event-stream")
    events = parse_sse(resp.text)
    names = [e[0] for e in events]
    assert names[0] == "session"
    assert "token" in names
    assert names[-1] == "done"

    session_ev = events[0][1]
    assert session_ev["created"] is True
    assert session_ev["session_id"].startswith("sess_")

    tokens = [e[1]["delta"] for e in events if e[0] == "token"]
    assert "".join(tokens) == "Ciao, mondo."

    done_ev = events[-1][1]
    assert done_ev["total_tokens"] == 12
    assert done_ev["message_id"].startswith("msg_")

    # Messages persisted: user + assistant
    msgs = store.get_messages(session_ev["session_id"])
    assert [m["role"] for m in msgs] == ["user", "assistant"]
    assert msgs[0]["content"] == "hello"
    assert msgs[1]["content"] == "Ciao, mondo."


def test_post_chat_resumes_existing_session(store):
    sid = store.create_session()
    store.append_message(sid, "user", "prior turn")
    store.append_message(sid, "assistant", "prior reply")

    client, llm = build(store, [
        {"type": "token", "delta": "ok"},
        {"type": "done", "tokens": 2},
    ])
    resp = client.post("/api/chat", json={"session_id": sid, "message": "follow up"})
    assert resp.status_code == 200
    events = parse_sse(resp.text)
    session_ev = events[0][1]
    assert session_ev["session_id"] == sid
    assert session_ev["created"] is False

    sent_history = llm.calls[0]["messages"]
    assert [m["role"] for m in sent_history] == ["user", "assistant", "user"]
    assert sent_history[-1]["content"] == "follow up"


def test_post_chat_404_on_missing_session(store):
    client, _ = build(store, [])
    resp = client.post("/api/chat", json={"session_id": "sess_nope", "message": "x"})
    assert resp.status_code == 404


def test_post_chat_400_on_missing_message(store):
    client, _ = build(store, [])
    resp = client.post("/api/chat", json={"session_id": None, "message": ""})
    assert resp.status_code == 400


def test_post_chat_400_on_oversize_message(store):
    client, _ = build(store, [], cfg_overrides={("chat_interface", "max_message_length"): 10})
    resp = client.post("/api/chat", json={"session_id": None, "message": "a" * 11})
    assert resp.status_code == 400


def test_post_chat_503_when_disabled(store):
    client, _ = build(store, [], cfg_overrides={("chat_interface", "enabled"): False})
    resp = client.post("/api/chat", json={"session_id": None, "message": "x"})
    assert resp.status_code == 503


def test_post_chat_emits_tool_events_and_persists_them(store):
    client, _ = build(store, [
        {"type": "tool_start", "tool": "kb_hybrid_search", "input": {"query": "roma"}},
        {"type": "tool_end", "tool": "kb_hybrid_search", "duration_ms": 321},
        {"type": "token", "delta": "Trovato."},
        {"type": "done", "tokens": 4},
    ])
    resp = client.post("/api/chat", json={"session_id": None, "message": "cerca roma"})
    events = parse_sse(resp.text)
    names = [e[0] for e in events]
    assert "tool_start" in names
    assert "tool_end" in names
    assert names[-1] == "done"

    session_id = events[0][1]["session_id"]
    msgs = store.get_messages(session_id)
    assistant = [m for m in msgs if m["role"] == "assistant"][0]
    assert assistant["tool_calls"] is not None
    assert assistant["tool_calls"][0]["tool"] == "kb_hybrid_search"
    assert assistant["tool_results"][0]["duration_ms"] == 321


def test_post_chat_error_event_still_persists_partial(store):
    client, _ = build(store, [
        {"type": "token", "delta": "Partial "},
        {"type": "error", "error": "simulated failure"},
    ])
    resp = client.post("/api/chat", json={"session_id": None, "message": "hi"})
    events = parse_sse(resp.text)
    names = [e[0] for e in events]
    assert names[-1] == "error"
    assert events[-1][1]["error"] == "simulated failure"

    session_id = events[0][1]["session_id"]
    assistant = [m for m in store.get_messages(session_id) if m["role"] == "assistant"]
    assert assistant and assistant[0]["content"] == "Partial "


def test_post_chat_auto_titles_from_first_message(store):
    client, _ = build(store, [
        {"type": "token", "delta": "ok"},
        {"type": "done", "tokens": 1},
    ])
    first = "What did I work on yesterday afternoon in Rome?"
    resp = client.post("/api/chat", json={"session_id": None, "message": first})
    session_id = parse_sse(resp.text)[0][1]["session_id"]
    title = store.get_session(session_id)["title"]
    assert title != "New chat"
    assert title.startswith("What did I work on")


def test_post_chat_session_event_is_first(store):
    client, _ = build(store, [
        {"type": "token", "delta": "hi"},
        {"type": "done", "tokens": 1},
    ])
    resp = client.post("/api/chat", json={"session_id": None, "message": "x"})
    events = parse_sse(resp.text)
    assert events[0][0] == "session"


# ── session CRUD ───────────────────────────────────────────────

def test_list_sessions_returns_empty(store):
    client, _ = build(store, [])
    resp = client.get("/api/chat/sessions")
    assert resp.status_code == 200
    body = resp.json()
    assert body["sessions"] == []
    assert body["enabled"] is True
    assert isinstance(body["suggest_prompts"], list)


def test_list_sessions_returns_created_ordered(store):
    client, _ = build(
        store, [],
        cfg_overrides={("chat_interface", "suggest_prompts"): ["A", "B"]},
    )
    a = store.create_session()
    time.sleep(0.01)
    b = store.create_session()
    store.rename_session(b, "Second")
    store.rename_session(a, "First")  # bumps updated_at last
    resp = client.get("/api/chat/sessions")
    data = resp.json()
    assert data["sessions"][0]["id"] == a
    assert data["suggest_prompts"] == ["A", "B"]


def test_get_session_detail_returns_messages(store):
    client, _ = build(store, [])
    sid = store.create_session()
    store.append_message(sid, "user", "hi")
    store.append_message(sid, "assistant", "yo")
    resp = client.get(f"/api/chat/sessions/{sid}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["session"]["id"] == sid
    assert len(data["messages"]) == 2


def test_get_session_detail_404(store):
    client, _ = build(store, [])
    resp = client.get("/api/chat/sessions/sess_nope")
    assert resp.status_code == 404


def test_rename_session(store):
    client, _ = build(store, [])
    sid = store.create_session()
    resp = client.post(f"/api/chat/sessions/{sid}/rename", json={"title": "Renamed"})
    assert resp.status_code == 200
    assert resp.json()["session"]["title"] == "Renamed"


def test_rename_session_400_on_blank(store):
    client, _ = build(store, [])
    sid = store.create_session()
    resp = client.post(f"/api/chat/sessions/{sid}/rename", json={"title": "   "})
    assert resp.status_code == 400


def test_rename_session_404(store):
    client, _ = build(store, [])
    resp = client.post("/api/chat/sessions/sess_nope/rename", json={"title": "x"})
    assert resp.status_code == 404


def test_delete_session(store):
    client, _ = build(store, [])
    sid = store.create_session()
    store.append_message(sid, "user", "orphan")
    resp = client.delete(f"/api/chat/sessions/{sid}")
    assert resp.status_code == 200
    assert store.get_session(sid) is None


def test_delete_session_404(store):
    client, _ = build(store, [])
    resp = client.delete("/api/chat/sessions/sess_nope")
    assert resp.status_code == 404


# ── history truncation ────────────────────────────────────────

def test_history_respects_max_history(store):
    sid = store.create_session()
    for i in range(30):
        role = "user" if i % 2 == 0 else "assistant"
        store.append_message(sid, role, f"msg-{i}")

    client, llm = build(store, [
        {"type": "token", "delta": "ok"},
        {"type": "done", "tokens": 1},
    ], cfg_overrides={("chat_interface", "max_history_messages"): 6})
    resp = client.post("/api/chat", json={"session_id": sid, "message": "next"})
    assert resp.status_code == 200

    sent = llm.calls[0]["messages"]
    # max_history is the total turn window sent to the LLM (includes the current user turn).
    assert len(sent) == 6
    assert sent[-1]["content"] == "next"


# ── system prompt resolution ──────────────────────────────────

def test_system_prompt_uses_chat_interface_override(store):
    client, llm = build(store, [
        {"type": "token", "delta": "ok"},
        {"type": "done", "tokens": 1},
    ])
    client.post("/api/chat", json={"session_id": None, "message": "x"})
    assert llm.calls[0]["system"] == "Test prompt."


def test_system_prompt_falls_back_to_persona(store):
    client, llm = build(
        store,
        [{"type": "token", "delta": "ok"}, {"type": "done", "tokens": 1}],
        cfg_overrides={
            ("chat_interface", "system_prompt"): None,
            ("persona", "system_prompt"): "Legacy Telegram prompt.",
        },
    )
    client.post("/api/chat", json={"session_id": None, "message": "x"})
    assert llm.calls[0]["system"] == "Legacy Telegram prompt."
