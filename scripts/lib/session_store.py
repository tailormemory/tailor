"""
TAILOR — Telegram session buffer.
Stores conversation history in SQLite, manages session lifecycle.
"""

import sqlite3, uuid, os
from datetime import datetime, timedelta
from lib.config import get

def _db_path() -> str:
    return get("sessions", "db_path", "./db/telegram_sessions.sqlite3")

def _timeout() -> int:
    return get("sessions", "timeout_minutes", 10)

def _max_history() -> int:
    return get("sessions", "max_history", 20)

def _conn() -> sqlite3.Connection:
    db = _db_path()
    os.makedirs(os.path.dirname(db), exist_ok=True)
    c = sqlite3.connect(db)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=5000")
    return c

def init_db() -> None:
    """Create tables if they don't exist."""
    c = _conn()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS sessions (
            session_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            last_message_at TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            summary TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            FOREIGN KEY (session_id) REFERENCES sessions(session_id)
        );
        CREATE INDEX IF NOT EXISTS idx_msg_session ON messages(session_id);
        CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status);
    """)
    c.commit()
    c.close()

def _now() -> str:
    return datetime.now().isoformat()

def get_or_create_session() -> str:
    """Return active session_id. Create new one if last message > timeout."""
    c = _conn()
    row = c.execute(
        "SELECT session_id, last_message_at FROM sessions WHERE status='active' ORDER BY last_message_at DESC LIMIT 1"
    ).fetchone()
    
    if row:
        last = datetime.fromisoformat(row["last_message_at"])
        if datetime.now() - last < timedelta(minutes=_timeout()):
            c.close()
            return row["session_id"]
        # Close expired session
        c.execute("UPDATE sessions SET status='expired' WHERE session_id=?", (row["session_id"],))
        c.commit()
    
    # Create new session
    sid = f"tg_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:4]}"
    c.execute(
        "INSERT INTO sessions (session_id, started_at, last_message_at) VALUES (?,?,?)",
        (sid, _now(), _now())
    )
    c.commit()
    c.close()
    return sid

def add_message(session_id: str, role: str, content: str) -> None:
    """Add a message and update session timestamp."""
    c = _conn()
    c.execute(
        "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?,?,?,?)",
        (session_id, role, content, _now())
    )
    c.execute(
        "UPDATE sessions SET last_message_at=? WHERE session_id=?",
        (_now(), session_id)
    )
    c.commit()
    c.close()

def get_history(session_id: str) -> list[dict]:
    """Get last N messages for the session, formatted for LLM."""
    c = _conn()
    rows = c.execute(
        "SELECT role, content FROM messages WHERE session_id=? ORDER BY id DESC LIMIT ?",
        (session_id, _max_history())
    ).fetchall()
    c.close()
    # Reverse to chronological order
    return [{"role": r["role"], "content": r["content"]} for r in reversed(rows)]

def get_expired_sessions() -> list[dict]:
    """Find sessions that expired (for session capture)."""
    c = _conn()
    cutoff = (datetime.now() - timedelta(minutes=_timeout())).isoformat()
    rows = c.execute(
        "SELECT session_id, started_at, last_message_at FROM sessions WHERE status='active' AND last_message_at < ?",
        (cutoff,)
    ).fetchall()
    c.close()
    return [dict(r) for r in rows]

def get_session_messages(session_id: str) -> list[dict]:
    """Get ALL messages for a session (for summary generation)."""
    c = _conn()
    rows = c.execute(
        "SELECT role, content, timestamp FROM messages WHERE session_id=? ORDER BY id",
        (session_id,)
    ).fetchall()
    c.close()
    return [dict(r) for r in rows]

def close_session(session_id: str, summary: str = "") -> None:
    """Mark session as closed, optionally store summary."""
    c = _conn()
    c.execute(
        "UPDATE sessions SET status='closed', summary=? WHERE session_id=?",
        (summary, session_id)
    )
    c.commit()
    c.close()

def get_session_count() -> dict:
    """Stats for monitoring."""
    c = _conn()
    total = c.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
    active = c.execute("SELECT COUNT(*) FROM sessions WHERE status='active'").fetchone()[0]
    msgs = c.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    c.close()
    return {"total_sessions": total, "active_sessions": active, "total_messages": msgs}
