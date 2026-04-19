"""
TAILOR — Native chat interface persistent session store.

SQLite-backed storage for the dashboard chat tab. Separate from the Telegram
session buffer (lib.session_store) because the lifecycle, auth surface and
retention model are different: Telegram sessions auto-expire after 10 minutes
and are summarized into the KB, while native-chat sessions are user-owned,
long-lived, and explicitly renamed/deleted from the UI.
"""

from __future__ import annotations

import json
import os
import secrets
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone


_MIGRATION = """
CREATE TABLE IF NOT EXISTS chat_sessions (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL DEFAULT 'New chat',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    message_count INTEGER NOT NULL DEFAULT 0,
    provider TEXT,
    model TEXT
);
CREATE TABLE IF NOT EXISTS chat_messages (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES chat_sessions(id) ON DELETE CASCADE,
    role TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'tool')),
    content TEXT NOT NULL,
    tool_calls TEXT,
    tool_results TEXT,
    created_at TEXT NOT NULL,
    tokens INTEGER,
    duration_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_messages_session ON chat_messages(session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_sessions_updated ON chat_sessions(updated_at DESC);
"""


def _ensure_provider_columns(conn) -> None:
    """Idempotent: add provider/model columns to chat_sessions if an older
    schema (pre-migration 003) is present. No-op on fresh DBs where the
    columns are already in the CREATE TABLE above."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(chat_sessions)").fetchall()}
    if "provider" not in cols:
        conn.execute("ALTER TABLE chat_sessions ADD COLUMN provider TEXT")
    if "model" not in cols:
        conn.execute("ALTER TABLE chat_sessions ADD COLUMN model TEXT")

_DEFAULT_TITLE = "New chat"


def _now() -> str:
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def _new_session_id() -> str:
    return f"sess_{secrets.token_hex(6)}"


def _new_message_id() -> str:
    return f"msg_{secrets.token_hex(6)}"


def _derive_title(first_message: str, max_len: int = 60) -> str:
    """First 60 chars, cut at word boundary, newlines stripped."""
    flat = " ".join(first_message.split())
    if not flat:
        return _DEFAULT_TITLE
    if len(flat) <= max_len:
        return flat
    cut = flat[:max_len]
    space = cut.rfind(" ")
    if space >= 20:
        cut = cut[:space]
    return cut.rstrip(" ,.;:")


class ChatSessionStore:
    """Thread-safe store backed by SQLite. One DB file per instance."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(os.path.abspath(db_path)) or ".", exist_ok=True)
        self._ensure_schema()

    @contextmanager
    def _conn(self):
        c = sqlite3.connect(self.db_path, timeout=5.0, isolation_level=None)
        c.row_factory = sqlite3.Row
        try:
            c.execute("PRAGMA foreign_keys = ON")
            c.execute("PRAGMA journal_mode = WAL")
            c.execute("PRAGMA busy_timeout = 5000")
            yield c
        finally:
            c.close()

    def _ensure_schema(self) -> None:
        with self._lock, self._conn() as c:
            c.executescript(_MIGRATION)
            _ensure_provider_columns(c)

    # ── sessions ───────────────────────────────────────────────

    def create_session(self, provider: str | None = None, model: str | None = None) -> str:
        """Create a session. When provider/model are both None the session
        inherits the default brain at call time; otherwise the pair is
        persisted and the same brain is used for every turn of this session."""
        sid = _new_session_id()
        ts = _now()
        with self._lock, self._conn() as c:
            c.execute(
                "INSERT INTO chat_sessions "
                "(id, title, created_at, updated_at, message_count, provider, model) "
                "VALUES (?, ?, ?, ?, 0, ?, ?)",
                (sid, _DEFAULT_TITLE, ts, ts, provider, model),
            )
        return sid

    def get_session(self, session_id: str) -> dict | None:
        with self._conn() as c:
            row = c.execute(
                "SELECT id, title, created_at, updated_at, message_count, provider, model "
                "FROM chat_sessions WHERE id = ?",
                (session_id,),
            ).fetchone()
        return dict(row) if row else None

    def list_sessions(self, limit: int = 50) -> list[dict]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT id, title, created_at, updated_at, message_count, provider, model "
                "FROM chat_sessions ORDER BY updated_at DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
        return [dict(r) for r in rows]

    def rename_session(self, session_id: str, title: str) -> bool:
        clean = (title or "").strip()
        if not clean:
            return False
        if len(clean) > 200:
            clean = clean[:200]
        ts = _now()
        with self._lock, self._conn() as c:
            cur = c.execute(
                "UPDATE chat_sessions SET title = ?, updated_at = ? WHERE id = ?",
                (clean, ts, session_id),
            )
            return cur.rowcount > 0

    def delete_session(self, session_id: str) -> bool:
        with self._lock, self._conn() as c:
            cur = c.execute("DELETE FROM chat_sessions WHERE id = ?", (session_id,))
            return cur.rowcount > 0

    def auto_title(self, session_id: str, first_message: str) -> None:
        """Set title from the first user message, only if still the default."""
        with self._lock, self._conn() as c:
            row = c.execute(
                "SELECT title FROM chat_sessions WHERE id = ?", (session_id,)
            ).fetchone()
            if not row or row["title"] != _DEFAULT_TITLE:
                return
            new_title = _derive_title(first_message)
            if not new_title or new_title == _DEFAULT_TITLE:
                return
            c.execute(
                "UPDATE chat_sessions SET title = ?, updated_at = ? WHERE id = ?",
                (new_title, _now(), session_id),
            )

    # ── messages ───────────────────────────────────────────────

    def append_message(
        self,
        session_id: str,
        role: str,
        content: str,
        tool_calls: list | None = None,
        tool_results: list | None = None,
        tokens: int | None = None,
        duration_ms: int | None = None,
    ) -> str:
        if role not in ("user", "assistant", "tool"):
            raise ValueError(f"invalid role: {role!r}")
        mid = _new_message_id()
        ts = _now()
        tc = json.dumps(tool_calls, ensure_ascii=False) if tool_calls else None
        tr = json.dumps(tool_results, ensure_ascii=False) if tool_results else None
        with self._lock, self._conn() as c:
            exists = c.execute(
                "SELECT 1 FROM chat_sessions WHERE id = ?", (session_id,)
            ).fetchone()
            if not exists:
                raise KeyError(f"session not found: {session_id}")
            c.execute(
                "INSERT INTO chat_messages "
                "(id, session_id, role, content, tool_calls, tool_results, created_at, tokens, duration_ms) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (mid, session_id, role, content, tc, tr, ts, tokens, duration_ms),
            )
            c.execute(
                "UPDATE chat_sessions SET updated_at = ?, message_count = message_count + 1 WHERE id = ?",
                (ts, session_id),
            )
        return mid

    def get_messages(self, session_id: str, limit: int | None = None) -> list[dict]:
        with self._conn() as c:
            if limit is None:
                rows = c.execute(
                    "SELECT id, session_id, role, content, tool_calls, tool_results, "
                    "created_at, tokens, duration_ms "
                    "FROM chat_messages WHERE session_id = ? "
                    "ORDER BY created_at ASC, rowid ASC",
                    (session_id,),
                ).fetchall()
            else:
                rows = c.execute(
                    "SELECT id, session_id, role, content, tool_calls, tool_results, "
                    "created_at, tokens, duration_ms "
                    "FROM chat_messages WHERE session_id = ? "
                    "ORDER BY created_at DESC, rowid DESC LIMIT ?",
                    (session_id, int(limit)),
                ).fetchall()
                rows = list(reversed(rows))
        out = []
        for r in rows:
            d = dict(r)
            d["tool_calls"] = json.loads(d["tool_calls"]) if d["tool_calls"] else None
            d["tool_results"] = json.loads(d["tool_results"]) if d["tool_results"] else None
            out.append(d)
        return out
