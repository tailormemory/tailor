"""
TAILOR — Native chat interface API.

Exposes:
    POST   /api/chat                        → SSE stream of assistant events
    GET    /api/chat/sessions               → list sessions
    GET    /api/chat/sessions/{id}          → session detail + full messages
    POST   /api/chat/sessions/{id}/rename   → rename session
    DELETE /api/chat/sessions/{id}          → delete session

Auth is enforced upstream by mcp_server.BearerAuthMiddleware (cookie/token).
This module assumes callers are already authenticated. Do NOT wire these
endpoints into a public surface without the upstream gate.
"""

from __future__ import annotations

import json
import os
import time
import threading
import queue as _queue

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.responses import StreamingResponse
from starlette.routing import Route


_BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


# ── system prompt resolution ───────────────────────────────────

_FALLBACK_SYSTEM_PROMPT = (
    "Sei TAILOR, l'assistente AI personale di Emiliano Carlucci.\n\n"
    "Tono: asciutto, competente, con ironia britannica. Mai servile. Brevità massima.\n\n"
    "REGOLE:\n"
    "- Usa i tool per cercare nella Knowledge Base prima di rispondere.\n"
    "- Se il contesto KB non contiene l'informazione, dillo. Non improvvisare.\n"
    "- Risposte brevi e operative.\n"
    "- Rispondi nella lingua dell'utente. Markdown standard: **grassetto**, *corsivo*, `codice`, liste.\n"
    "- Se l'utente condivide fatti importanti, salvali nella KB con kb_add.\n"
)


def _resolve_system_prompt(cfg_get) -> str:
    """chat_interface.system_prompt wins; fall back to persona.system_prompt; then to built-in."""
    candidate = cfg_get("chat_interface", "system_prompt", None)
    if candidate and str(candidate).strip():
        return str(candidate)
    legacy = cfg_get("persona", "system_prompt", None)
    if legacy and str(legacy).strip():
        return str(legacy)
    return _FALLBACK_SYSTEM_PROMPT


# ── SSE formatting ─────────────────────────────────────────────

def _sse_frame(event: str, data: dict) -> str:
    payload = json.dumps(data, ensure_ascii=False, default=str)
    return f"event: {event}\ndata: {payload}\n\n"


def _sse_keepalive() -> str:
    return ": keepalive\n\n"


# ── factory / dependency injection ─────────────────────────────

def build_app(
    store=None,
    llm_factory=None,
    cfg_get=None,
    tools=None,
    db_path: str | None = None,
    brain_builder=None,
) -> Starlette:
    """Build the chat sub-application.

    Parameters
    ----------
    store : ChatSessionStore | None
        If None, one is constructed from `db_path` or the config value
        `chat_interface.db_path` (default: <repo>/db/chat_sessions.sqlite3).
    llm_factory : callable | None
        Returns an LLMClient instance for the *default* brain. Defaults to
        lib.llm_client.get_brain.
    cfg_get : callable | None
        (section, key=None, default=None) -> value. Defaults to lib.config.get.
    tools : list | None
        Tool schema list for the LLM. Defaults to lib.llm_client.TOOLS.
    db_path : str | None
        Optional override for the SQLite path when `store` is None.
    brain_builder : callable | None
        (provider: str, model: str) -> LLMClient, used for per-session
        provider selection. Defaults to lib.llm_client.build_brain. Tests
        inject a fake here to avoid hitting real provider SDKs.
    """
    from chat_session_store import ChatSessionStore  # local import: test isolation
    if cfg_get is None:
        from config import get as _cfg_get
        cfg_get = _cfg_get
    if llm_factory is None:
        from llm_client import get_brain as llm_factory  # type: ignore
    if tools is None:
        from llm_client import TOOLS as _TOOLS
        tools = _TOOLS
    if brain_builder is None:
        from llm_client import build_brain as brain_builder  # type: ignore
    if store is None:
        if db_path is None:
            db_path = cfg_get("chat_interface", "db_path", None) or os.path.join(
                _BASE_DIR, "db", "chat_sessions.sqlite3"
            )
        store = ChatSessionStore(db_path)

    max_history = int(cfg_get("chat_interface", "max_history_messages", 20) or 20)
    max_length = int(cfg_get("chat_interface", "max_message_length", 8000) or 8000)
    keepalive = int(cfg_get("chat_interface", "sse_keepalive_seconds", 15) or 15)

    # ── handlers ───────────────────────────────────────────────

    async def post_chat(request: Request) -> Response:
        if not _enabled(cfg_get):
            return JSONResponse({"error": "chat interface disabled"}, status_code=503)
        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "invalid JSON"}, status_code=400)
        session_id = body.get("session_id")
        message = body.get("message", "")
        if not isinstance(message, str) or not message.strip():
            return JSONResponse({"error": "message is required"}, status_code=400)
        if len(message) > max_length:
            return JSONResponse(
                {"error": f"message exceeds max_length ({max_length})"},
                status_code=400,
            )

        req_provider = body.get("provider") if isinstance(body.get("provider"), str) else None
        req_model = body.get("model") if isinstance(body.get("model"), str) else None

        created = False
        if session_id is None:
            # New session: allow the caller to pin a provider/model, but only
            # if it matches an entry in chat_interface.available_providers.
            # Validating against the config whitelist keeps "any valid
            # provider name" from becoming an open door to arbitrary models.
            pin_provider, pin_model = None, None
            if req_provider and req_model:
                if not _is_available(cfg_get, req_provider, req_model):
                    return JSONResponse(
                        {
                            "error": (
                                f"provider/model not in chat_interface.available_providers: "
                                f"{req_provider}/{req_model}"
                            )
                        },
                        status_code=400,
                    )
                pin_provider, pin_model = req_provider, req_model
            elif req_provider or req_model:
                # One without the other is always wrong — require the pair.
                return JSONResponse(
                    {"error": "provider and model must be set together"},
                    status_code=400,
                )
            session_id = store.create_session(provider=pin_provider, model=pin_model)
            created = True
        else:
            if not isinstance(session_id, str) or store.get_session(session_id) is None:
                return JSONResponse({"error": "session not found"}, status_code=404)

        # Persist the user message up-front so it survives disconnects.
        store.append_message(session_id, "user", message)
        if created:
            store.auto_title(session_id, message)

        # Resolve the brain for this turn. The DB is the source of truth —
        # provider/model in the request body are ignored for existing
        # sessions (the pair is immutable once the session is created).
        sess = store.get_session(session_id) or {}
        pinned_provider = sess.get("provider")
        pinned_model = sess.get("model")
        if pinned_provider and pinned_model:
            try:
                per_session_llm = brain_builder(pinned_provider, pinned_model)
            except Exception as e:
                return JSONResponse(
                    {"error": f"cannot build brain for {pinned_provider}/{pinned_model}: {e}"},
                    status_code=400,
                )
            turn_factory = (lambda _brain=per_session_llm: _brain)
        else:
            turn_factory = llm_factory

        # Build the LLM message history from persisted rows (truncated).
        history = _build_llm_history(store, session_id, max_history)
        system_prompt = _resolve_system_prompt(cfg_get)

        stream_iter = _run_stream(turn_factory, system_prompt, history, tools, keepalive)
        sse = _sse_generator(
            store=store,
            session_id=session_id,
            created=created,
            events=stream_iter,
        )

        headers = {
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        }
        return StreamingResponse(sse, media_type="text/event-stream", headers=headers)

    async def list_sessions(request: Request) -> Response:
        prompts = cfg_get("chat_interface", "suggest_prompts", None) or []
        if not isinstance(prompts, list):
            prompts = []
        return JSONResponse({
            "sessions": store.list_sessions(),
            "suggest_prompts": [str(p) for p in prompts][:6],
            "enabled": _enabled(cfg_get),
        })

    async def get_session_detail(request: Request) -> Response:
        sid = request.path_params["session_id"]
        s = store.get_session(sid)
        if s is None:
            return JSONResponse({"error": "not found"}, status_code=404)
        msgs = store.get_messages(sid)
        return JSONResponse({"session": s, "messages": msgs})

    async def rename_session(request: Request) -> Response:
        sid = request.path_params["session_id"]
        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "invalid JSON"}, status_code=400)
        title = (body or {}).get("title", "")
        if not isinstance(title, str) or not title.strip():
            return JSONResponse({"error": "title is required"}, status_code=400)
        ok = store.rename_session(sid, title)
        if not ok:
            return JSONResponse({"error": "not found"}, status_code=404)
        return JSONResponse({"ok": True, "session": store.get_session(sid)})

    async def delete_session(request: Request) -> Response:
        sid = request.path_params["session_id"]
        ok = store.delete_session(sid)
        if not ok:
            return JSONResponse({"error": "not found"}, status_code=404)
        return JSONResponse({"ok": True})

    async def list_providers(request: Request) -> Response:
        return JSONResponse(_providers_payload(cfg_get))

    routes = [
        Route("/api/chat", post_chat, methods=["POST"]),
        Route("/api/chat/providers", list_providers, methods=["GET"]),
        Route("/api/chat/sessions", list_sessions, methods=["GET"]),
        Route("/api/chat/sessions/{session_id}", get_session_detail, methods=["GET"]),
        Route("/api/chat/sessions/{session_id}/rename", rename_session, methods=["POST"]),
        Route("/api/chat/sessions/{session_id}", delete_session, methods=["DELETE"]),
    ]
    app = Starlette(routes=routes)
    app.state.store = store
    app.state.cfg_get = cfg_get
    return app


# ── SSE plumbing ───────────────────────────────────────────────

def _enabled(cfg_get) -> bool:
    v = cfg_get("chat_interface", "enabled", True)
    if isinstance(v, bool):
        return v
    return str(v).lower() not in ("false", "0", "no", "off")


def _available_providers_list(cfg_get) -> list[dict]:
    """Normalise chat_interface.available_providers into [{provider,model,label}].
    Returns [] if unset/empty/malformed — the API layer treats that as
    'only the default is available' and the UI hides the dropdown."""
    raw = cfg_get("chat_interface", "available_providers", None) or []
    if not isinstance(raw, list):
        return []
    out = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        p = item.get("provider")
        m = item.get("model")
        if not (isinstance(p, str) and p and isinstance(m, str) and m):
            continue
        label = item.get("label")
        if not isinstance(label, str) or not label:
            label = f"{p}/{m}"
        out.append({"provider": p, "model": m, "label": label})
    return out


def _default_brain_choice(cfg_get) -> dict | None:
    """Return {provider, model} of the default brain, or None if llm.* is
    incomplete. Used to seed the UI selection and to surface it in the
    /api/chat/providers payload."""
    llm = cfg_get("llm", None, None) or {}
    if isinstance(llm, dict):
        p = llm.get("provider")
        m = llm.get("model")
        if isinstance(p, str) and p and isinstance(m, str) and m:
            return {"provider": p, "model": m}
    return None


def _is_available(cfg_get, provider: str, model: str) -> bool:
    for item in _available_providers_list(cfg_get):
        if item["provider"] == provider and item["model"] == model:
            return True
    return False


def _providers_payload(cfg_get) -> dict:
    """Build the GET /api/chat/providers response. If available_providers
    is unset or empty, fall back to a single-item list containing the
    default so the UI always has something to select."""
    avail = _available_providers_list(cfg_get)
    default = _default_brain_choice(cfg_get)
    if not avail and default:
        avail = [{**default, "label": f"{default['provider']}/{default['model']} (default)"}]
    return {"default": default, "available": avail}


def _build_llm_history(store, session_id: str, max_history: int) -> list[dict]:
    """Flatten persisted messages into the LLM's expected [{'role', 'content'}] format.
    Tool-call metadata is not replayed to the LLM: the assistant's reasoning is
    stateless across turns and the final text is what matters."""
    rows = store.get_messages(session_id, limit=max_history)
    out = []
    for m in rows:
        role = m["role"]
        if role == "tool":
            continue
        if role not in ("user", "assistant"):
            continue
        out.append({"role": role, "content": m["content"]})
    return out


def _run_stream(llm_factory, system: str, messages: list[dict], tools, keepalive_seconds: int):
    """Adapt the (potentially blocking) LLM generator into a thread-pumped
    queue so an async SSE generator can emit keepalives while we wait.

    Yields raw event dicts (our internal streaming protocol)."""
    q: "_queue.Queue[dict]" = _queue.Queue(maxsize=256)
    sentinel = object()

    def _producer():
        try:
            brain = llm_factory()
            for ev in brain.stream_chat_with_tools(system, messages, tools=tools):
                q.put(ev)
        except Exception as e:
            q.put({"type": "error", "error": f"brain exception: {e}"})
        finally:
            q.put(sentinel)

    t = threading.Thread(target=_producer, daemon=True)
    t.start()

    def _iter():
        while True:
            try:
                ev = q.get(timeout=keepalive_seconds)
            except _queue.Empty:
                yield {"type": "_keepalive"}
                continue
            if ev is sentinel:
                return
            yield ev

    return _iter()


def _sse_generator(store, session_id: str, created: bool, events):
    """Translate internal streaming events into SSE frames and persist the
    final assistant message. Always emits `session` first and `done` last."""
    yield _sse_frame("session", {"session_id": session_id, "created": created})

    assistant_text_parts: list[str] = []
    tool_calls: list[dict] = []
    tool_results: list[dict] = []
    total_tokens: int | None = None
    started = time.monotonic()
    done_sent = False
    error_seen: str | None = None

    try:
        for ev in events:
            etype = ev.get("type")
            if etype == "_keepalive":
                yield _sse_keepalive()
                continue
            if etype == "token":
                delta = ev.get("delta", "")
                if delta:
                    assistant_text_parts.append(delta)
                    yield _sse_frame("token", {"delta": delta})
            elif etype == "tool_start":
                info = {"tool": ev.get("tool", ""), "input": ev.get("input", {})}
                tool_calls.append(info)
                yield _sse_frame("tool_start", info)
            elif etype == "tool_end":
                info = {"tool": ev.get("tool", ""), "duration_ms": int(ev.get("duration_ms", 0) or 0)}
                tool_results.append(info)
                yield _sse_frame("tool_end", info)
            elif etype == "done":
                total_tokens = ev.get("tokens")
            elif etype == "error":
                error_seen = str(ev.get("error", "unknown error"))
                break
    except GeneratorExit:
        # Client disconnected mid-stream; still persist what we have.
        pass

    duration_ms = int((time.monotonic() - started) * 1000)
    final_text = "".join(assistant_text_parts)

    message_id = None
    if final_text or tool_calls:
        try:
            message_id = store.append_message(
                session_id,
                "assistant",
                final_text,
                tool_calls=tool_calls or None,
                tool_results=tool_results or None,
                tokens=total_tokens,
                duration_ms=duration_ms,
            )
        except Exception as e:
            error_seen = error_seen or f"persist failed: {e}"

    if error_seen:
        yield _sse_frame("error", {"error": error_seen, "code": "brain_error"})
    else:
        yield _sse_frame(
            "done",
            {
                "message_id": message_id,
                "total_tokens": total_tokens,
                "duration_ms": duration_ms,
            },
        )
    done_sent = True  # noqa: F841  (retained for readability)
