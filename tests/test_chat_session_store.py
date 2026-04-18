"""Unit tests for scripts.lib.chat_session_store.ChatSessionStore."""

from __future__ import annotations

import os
import sys
import time

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "scripts", "lib"))

from chat_session_store import ChatSessionStore, _derive_title  # noqa: E402


@pytest.fixture
def store(tmp_path):
    return ChatSessionStore(str(tmp_path / "chat_sessions.sqlite3"))


def test_create_session_returns_prefixed_id(store):
    sid = store.create_session()
    assert sid.startswith("sess_") and len(sid) == len("sess_") + 12


def test_get_session_missing_returns_none(store):
    assert store.get_session("sess_does_not_exist") is None


def test_get_session_after_create(store):
    sid = store.create_session()
    s = store.get_session(sid)
    assert s is not None
    assert s["id"] == sid
    assert s["title"] == "New chat"
    assert s["message_count"] == 0
    assert s["created_at"].endswith("Z")
    assert s["updated_at"].endswith("Z")


def test_list_sessions_ordered_by_updated_at_desc(store):
    a = store.create_session()
    time.sleep(0.01)
    b = store.create_session()
    time.sleep(0.01)
    c = store.create_session()
    store.append_message(a, "user", "bump a")
    ids = [s["id"] for s in store.list_sessions()]
    assert ids[0] == a
    assert set(ids) == {a, b, c}


def test_list_sessions_respects_limit(store):
    for _ in range(3):
        store.create_session()
    assert len(store.list_sessions(limit=2)) == 2


def test_append_message_increments_count_and_returns_id(store):
    sid = store.create_session()
    mid = store.append_message(sid, "user", "hi")
    assert mid.startswith("msg_")
    s = store.get_session(sid)
    assert s["message_count"] == 1


def test_append_message_rejects_invalid_role(store):
    sid = store.create_session()
    with pytest.raises(ValueError):
        store.append_message(sid, "system", "nope")


def test_append_message_missing_session_raises(store):
    with pytest.raises(KeyError):
        store.append_message("sess_missing", "user", "hi")


def test_append_message_persists_tool_payloads(store):
    sid = store.create_session()
    tc = [{"tool": "kb_hybrid_search", "input": {"query": "roma"}}]
    tr = [{"tool": "kb_hybrid_search", "duration_ms": 321}]
    store.append_message(sid, "assistant", "ok", tool_calls=tc, tool_results=tr, tokens=42, duration_ms=100)
    msgs = store.get_messages(sid)
    assert len(msgs) == 1
    m = msgs[0]
    assert m["tool_calls"] == tc
    assert m["tool_results"] == tr
    assert m["tokens"] == 42
    assert m["duration_ms"] == 100


def test_get_messages_chronological(store):
    sid = store.create_session()
    store.append_message(sid, "user", "one")
    store.append_message(sid, "assistant", "two")
    store.append_message(sid, "user", "three")
    msgs = store.get_messages(sid)
    assert [m["content"] for m in msgs] == ["one", "two", "three"]


def test_get_messages_limit_returns_last_n_chronological(store):
    sid = store.create_session()
    for t in ["a", "b", "c", "d"]:
        store.append_message(sid, "user", t)
    msgs = store.get_messages(sid, limit=2)
    assert [m["content"] for m in msgs] == ["c", "d"]


def test_rename_session_updates_title(store):
    sid = store.create_session()
    assert store.rename_session(sid, "Custom Title") is True
    assert store.get_session(sid)["title"] == "Custom Title"


def test_rename_session_rejects_blank(store):
    sid = store.create_session()
    assert store.rename_session(sid, "   ") is False
    assert store.get_session(sid)["title"] == "New chat"


def test_rename_session_missing_returns_false(store):
    assert store.rename_session("sess_x", "anything") is False


def test_delete_session_cascades_to_messages(store):
    sid = store.create_session()
    store.append_message(sid, "user", "orphan me")
    assert store.delete_session(sid) is True
    assert store.get_session(sid) is None
    assert store.get_messages(sid) == []


def test_delete_session_missing_returns_false(store):
    assert store.delete_session("sess_x") is False


def test_auto_title_sets_from_first_message(store):
    sid = store.create_session()
    store.auto_title(sid, "What did I work on yesterday?")
    assert store.get_session(sid)["title"] == "What did I work on yesterday?"


def test_auto_title_does_not_overwrite_custom(store):
    sid = store.create_session()
    store.rename_session(sid, "Custom")
    store.auto_title(sid, "First user message")
    assert store.get_session(sid)["title"] == "Custom"


def test_auto_title_truncates_at_word_boundary(store):
    sid = store.create_session()
    msg = "This is a very long first message that should be cut somewhere sensible for readability"
    store.auto_title(sid, msg)
    title = store.get_session(sid)["title"]
    assert len(title) <= 60
    assert not title.endswith(" ")
    assert title.split()[-1] in msg.split()


def test_derive_title_handles_newlines_and_whitespace():
    out = _derive_title("  hello\n\nworld\tfoo  ")
    assert out == "hello world foo"


def test_derive_title_empty_falls_back_to_default():
    assert _derive_title("") == "New chat"
    assert _derive_title("   ") == "New chat"


def test_foreign_keys_enabled(store):
    sid = store.create_session()
    store.append_message(sid, "user", "will vanish")
    store.delete_session(sid)
    assert store.get_messages(sid) == []


def test_store_survives_reopen(tmp_path):
    path = str(tmp_path / "reopen.sqlite3")
    s1 = ChatSessionStore(path)
    sid = s1.create_session()
    s1.append_message(sid, "user", "persisted")
    s2 = ChatSessionStore(path)
    assert s2.get_session(sid)["message_count"] == 1
    assert s2.get_messages(sid)[0]["content"] == "persisted"
