"""
TAILOR MCP Server v14
Espone la Knowledge Base ChromaDB a Claude Desktop/Web/Mobile e ChatGPT via MCP.

Changes v14:
- Re-ranking post-retrieval: cross-encoder locale (ms-marco-MiniLM-L-6-v2) riordina top-N per rilevanza reale
- Fallback Jina Reranker API se modello locale non disponibile
- Over-fetch ×3 su kb_hybrid_search e kb_search per alimentare il re-ranker

Changes v13:
- Atomic facts: search tools enrich results with atomic facts from facts.sqlite3
- Each result shows structured facts extracted from the chunk (category, entity_tags, event_date)
- Superseded facts (superseded_by) excluded automatically
- Nuovo campo facts in search/fetch (ChatGPT-compatible)

Changes v12:
- Temporal supersession: search tools filter superseded chunks (superseded_by)
- Parametro include_superseded su kb_search, kb_hybrid_search, kb_search_by_topic, kb_search_docs, kb_search_by_entity
- Helper is_superseded() per filtro uniforme

Changes v8:
- exec_command: execute shell commands on the host machine
- read_file: lettura file dal filesystem
- write_file: scrittura file sul filesystem

Changes v7:
- kb_query_advanced: query con filtri avanzati (source, date_after, date_before, order_by)

Changes v6:
- Entity extraction inline on kb_update_session too (same logic as kb_add)

Changes v5:
- Entity extraction inline: each kb_add extracts entities via GPT-4o-mini and updates the SQLite index
- Environment variable: OPENAI_API_KEY (optional, if missing extraction is disabled)

Changes v4:
- Tool search/fetch for ChatGPT compatibility (deep research, company knowledge, chat)
- readOnlyHint annotations per skip confirmation on ChatGPT for read-only tools
- Parametro source in kb_add e kb_update_session ("claude", "chatgpt", "manual")

Changes v3:
- Autenticazione via API key (Bearer header o query param ?token=)
- Environment variable: TAILOR_API_KEY

Tools di lettura (Claude):
- kb_hybrid_search: hybrid semantic + entity search in a single query (main search tool)
- kb_search: search the KB by semantic query
- kb_search_by_topic: search with conversation title filter
- kb_search_docs: search only in documents
- kb_search_by_entity: search chunks mentioning a specific entity
- kb_list_conversations: elenca tutte le conversazioni indicizzate
- kb_query_advanced: query con filtri avanzati (fonte, data, ordinamento)
- kb_stats: statistiche sulla KB

Tools di lettura (ChatGPT-compatible):
- search: semantic search, output in OpenAI search/fetch format
- fetch: recupera contenuto completo di un chunk per ID

Tools di scrittura:
- kb_add: aggiunge un'informazione alla KB
- kb_update_session: salva un riassunto strutturato di fine sessione

Tools di sistema (fase "arm" — v8):
- exec_command: execute a shell command on the host machine
- read_file: legge un file dal filesystem
- write_file: scrive/sovrascrive un file sul filesystem

User profile tools (v11):
- get_user_profile: get user's full profile (static + dynamic)
- update_profile: update a fact in the profile (manual override + regeneration)

Avvio: python mcp_server.py
"""

import json
import os
import sys
import time
import subprocess
import requests
import chromadb
import signal
import anyio
from filelock import FileLock
import uvicorn
from datetime import datetime
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.responses import Response, JSONResponse

# Configurazione
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts", "lib"))
DB_DIR = os.path.join(BASE_DIR, "db")
# Embedding: loaded from lib.embedding (config-driven)
from embedding import get_embedding, get_embeddings, info as embedding_info
# Collection name from config
try:
    from scripts.lib.config import get as _cfg_get_top
    COLLECTION_NAME = _cfg_get_top("kb", "collection") or "tailor_kb"
except Exception:
    COLLECTION_NAME = "tailor_kb"
FACTS_DB_PATH = os.path.join(DB_DIR, "facts.sqlite3")

# ── Model Advisor: Ollama pull jobs ───────────────────────────
_ollama_jobs = {}  # {job_id: {"model": str, "status": "pulling"|"done"|"error", "progress": str, "error": str}}


# API Key da variabile d'ambiente
SERVER_PORT = int(os.environ.get("PORT", "8787"))
TAILOR_API_KEY = os.environ.get("TAILOR_API_KEY", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
JINA_API_KEY = os.environ.get("JINA_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
_CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Authorization, Content-Type",
}

# ── Rate limiting ─────────────────────────────────────────────
from scripts.lib.rate_limiter import AuthRateLimiter
_rl_cfg = {}
try:
    from scripts.lib.config import get as _rl_get
    _rl_cfg = _rl_get("auth", "rate_limit") or {}
    if not isinstance(_rl_cfg, dict):
        _rl_cfg = {}
except Exception:
    pass
_rate_limiter = AuthRateLimiter(
    max_attempts=_rl_cfg.get("max_attempts", 5),
    window_seconds=_rl_cfg.get("window_minutes", 15) * 60,
    ban_seconds=_rl_cfg.get("ban_minutes", 30) * 60,
)
# ── Multi-token auth ───────────────────────────────────────────
from scripts.lib.token_auth import TokenAuth
_tokens_cfg = []
try:
    _tokens_cfg = _rl_get("auth", "tokens") or []
    if not isinstance(_tokens_cfg, list):
        _tokens_cfg = []
except Exception:
    pass
_token_auth = TokenAuth(tokens_config=_tokens_cfg, legacy_key=TAILOR_API_KEY)

TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

if not TAILOR_API_KEY:
    print("\u26a0\ufe0f  WARNING: TAILOR_API_KEY not set. Server will start WITHOUT authentication.", file=sys.stderr)
if not OPENAI_API_KEY:
    print("\u26a0\ufe0f  OPENAI_API_KEY non impostata. Entity extraction inline disabilitata.", file=sys.stderr)


# ============================================================
# MIDDLEWARE AUTENTICAZIONE
# ============================================================

class BearerAuthMiddleware:
    """ASGI middleware: verify Bearer token or query param ?token=."""

    PUBLIC_PATHS = {"/health", "/", "/dashboard"}

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def _handle_rest_api(self, path: str, scope: dict, receive=None) -> Response:
        """Handle REST API calls. Dashboard API accepts cookie/token auth."""
        import json as _json, secrets
        async def _receive_body(scope, recv):
            """Read full request body."""
            if recv is None:
                return b""
            body = b""
            while True:
                msg = await recv()
                body += msg.get("body", b"")
                if not msg.get("more_body", False):
                    break
            return body
        def _json_response(data, status_code=200):
            body = _json.dumps(data, ensure_ascii=False, default=str)
            return Response(content=body, status_code=status_code, media_type="application/json", headers=_CORS_HEADERS)
        try:
            # /api/fetch/<chunk_id>
            if path.startswith("/api/fetch/"):
                chunk_id = path[len("/api/fetch/"):]
                if not chunk_id:
                    return _json_response({"error": "Missing chunk ID"}, 400)
                collection = get_collection()
                result = collection.get(ids=[chunk_id], include=["documents", "metadatas"])
                if not result or not result["ids"]:
                    return _json_response({"error": f"Chunk not found: {chunk_id}"}, 404)
                return _json_response({
                    "id": chunk_id,
                    "content": result["documents"][0],
                    "metadata": result["metadatas"][0],
                })

            # /api/search?q=<query>&n=<n>&source=<source>
            elif path == "/api/search":
                qs = scope.get("query_string", b"").decode()
                params = dict(p.split("=", 1) for p in qs.split("&") if "=" in p)
                query = params.get("q", "")
                if not query:
                    return _json_response({"error": "Missing query param q"}, 400)
                n = min(int(params.get("n", "5")), 20)
                source = params.get("source", "")
                from urllib.parse import unquote
                query = unquote(query)
                embedding = get_embeddings([query])[0]
                collection = get_collection()
                where = {"source": source} if source else None
                results = collection.query(query_embeddings=[embedding], n_results=n, where=where, include=["documents", "metadatas", "distances"])
                out = []
                for i in range(len(results["ids"][0])):
                    out.append({
                        "id": results["ids"][0][i],
                        "content": results["documents"][0][i],
                        "metadata": results["metadatas"][0][i],
                        "distance": results["distances"][0][i],
                    })
                return _json_response({"results": out})

            # ── Auth ──
            elif path == "/api/auth/login":
                # Rate limit check
                _login_ip = scope.get("client", ("", 0))[0] or ""
                if _rate_limiter.is_banned(_login_ip):
                    _rem = _rate_limiter.ban_remaining(_login_ip)
                    return _json_response({"error": "Too many attempts. Try again in %ds." % _rem}, 429)
                # POST: verify token, return session cookie
                body = b""
                while True:
                    msg = await _receive_body(scope, receive)
                    if msg:
                        body = msg
                        break
                    break
                import json as _jauth
                try:
                    data = _jauth.loads(body) if body else {}
                except Exception:
                    data = {}
                token = data.get("token", "")
                _auth_info = _token_auth.authenticate(token)
                if _auth_info:
                    import hashlib as _hashlib
                    session_id = secrets.token_hex(32)
                    session_hash = _hashlib.sha256(session_id.encode()).hexdigest()
                    # Store hashed session (simple file-based)
                    sessions_path = os.path.join(BASE_DIR, "db", "dashboard_sessions.json")
                    sessions = {}
                    if os.path.exists(sessions_path):
                        try:
                            with open(sessions_path) as _sf:
                                sessions = _jauth.load(_sf)
                        except Exception:
                            sessions = {}
                    import time as _time
                    sessions[session_hash] = {"created": _time.time(), "expires": _time.time() + 7*86400}
                    # Clean expired
                    sessions = {k: v for k, v in sessions.items() if v.get("expires", 0) > _time.time()}
                    with open(sessions_path, "w") as _sf:
                        _jauth.dump(sessions, _sf)
                    resp = _json_response({"ok": True})
                    resp.set_cookie("tailor_session", session_id, max_age=7*86400, httponly=True, samesite="lax", path="/")
                    _rate_limiter.record_success(_login_ip)
                    return resp
                else:
                    _banned = _rate_limiter.record_failure(_login_ip)
                    if _banned:
                        _rem = _rate_limiter.ban_remaining(_login_ip)
                        return _json_response({"error": "Too many attempts. Banned for %ds." % _rem}, 429)
                    return _json_response({"error": "Invalid token"}, 401)

            elif path == "/api/auth/check":
                return _json_response({"authenticated": True})

            # ── Dashboard API ──
            elif path == "/api/dashboard/stats":
                import sqlite3 as _sq3
                collection = get_collection()
                total_chunks = collection.count()
                # Source breakdown
                source_counts = {}
                for src in ["document", "email", "claude", "chatgpt"]:
                    try:
                        r = collection.get(where={"source": src}, include=[])
                        source_counts[src] = len(r["ids"])
                    except Exception:
                        source_counts[src] = 0
                # Facts
                facts_data = {"total": 0, "superseded": 0, "derived": 0, "covered": 0}
                if os.path.exists(FACTS_DB_PATH):
                    fc = _sq3.connect(f"file:{FACTS_DB_PATH}?mode=ro", uri=True)
                    facts_data["total"] = fc.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
                    facts_data["superseded"] = fc.execute("SELECT COUNT(*) FROM facts WHERE superseded_by IS NOT NULL").fetchone()[0]
                    facts_data["derived"] = fc.execute("SELECT COUNT(*) FROM facts WHERE relation_type = 'derived'").fetchone()[0]
                    facts_data["covered"] = fc.execute("SELECT COUNT(DISTINCT chunk_id) FROM facts").fetchone()[0]
                    fc.close()
                # Entities
                entity_count = 0
                epath = os.path.join(DB_DIR, "entity_index.sqlite3")
                if os.path.exists(epath):
                    ec = _sq3.connect(f"file:{epath}?mode=ro", uri=True)
                    entity_count = ec.execute("SELECT COUNT(DISTINCT entity) FROM entity_index").fetchone()[0]
                    ec.close()
                # Documents
                doc_count = 0
                reg_path = os.path.join(DB_DIR, "doc_registry.json")
                if os.path.exists(reg_path):
                    import json as _j2
                    doc_count = len(_j2.load(open(reg_path)))
                # DB size
                db_size_mb = 0
                for fname in os.listdir(DB_DIR):
                    fpath = os.path.join(DB_DIR, fname)
                    if os.path.isfile(fpath):
                        db_size_mb += os.path.getsize(fpath) / (1024 * 1024)
                return _json_response({
                    "chunks": {"total": total_chunks, **source_counts},
                    "facts": facts_data,
                    "entities": entity_count,
                    "documents": doc_count,
                    "db_size_mb": round(db_size_mb),
                })

            elif path == "/api/dashboard/services":
                import socket as _sock
                def _check_port(port):
                    try:
                        s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
                        s.settimeout(1)
                        result = s.connect_ex(("127.0.0.1", port))
                        s.close()
                        return result == 0
                    except Exception:
                        return False
                def _check_process(name):
                    """Check if process is running via /proc or ps, without pgrep (avoids deadlock)."""
                    try:
                        import subprocess as _sp
                        r = _sp.run(["ps", "aux"], capture_output=True, text=True, timeout=3)
                        return name in r.stdout
                    except Exception:
                        return False
                # Load services from YAML config, fallback to defaults
                from scripts.lib.config import get as _cfg_get
                _svc_list = _cfg_get("services") or [
                    {"name": "MCP Server", "id": "com.tailor.mcp", "check": "port", "port": 8787, "desc": "Knowledge Base API"},
                    {"name": "Telegram Bot", "id": "com.tailor.telegram", "check": "process", "process": "telegram_bot", "desc": "Chat interface"},
                    {"name": "Ollama", "id": "com.tailor.ollama", "check": "port", "port": 11434, "desc": "Local LLM & Embeddings"},
                ]
                services = []
                for svc in _svc_list:
                    name = svc["name"]
                    plist_id = svc.get("id", "")
                    port = svc.get("port")
                    proc_name = svc.get("process")
                    desc = svc.get("desc", "")
                    status = "stopped"
                    if port and _check_port(port):
                        status = "running"
                    elif proc_name and _check_process(proc_name):
                        status = "running"
                    elif plist_id == "com.tailor.mcp":
                        status = "running"
                    services.append({"name": name, "id": plist_id, "port": port, "status": status, "desc": desc})
                return _json_response({"services": services})

            elif path == "/api/dashboard/config":
                try:
                    from config import load_config
                    cfg = load_config()
                    # Sanitize — remove actual API keys
                    safe = {
                        "llm": {
                            "provider": cfg.get("llm", {}).get("provider", ""),
                            "model": cfg.get("llm", {}).get("model", ""),
                            "tool_use": cfg.get("llm", {}).get("tool_use", False),
                        },
                        "embedding": {
                            "provider": cfg.get("embedding", {}).get("provider", ""),
                            "model": cfg.get("embedding", {}).get("model", ""),
                            "dimensions": cfg.get("embedding", {}).get("dimensions", ""),
                        },
                        "classifier": {
                            "provider": cfg.get("classifier", {}).get("provider", ""),
                            "model": cfg.get("classifier", {}).get("model", ""),
                        },
                        "telegram": {
                            "chat_id": cfg.get("telegram", {}).get("chat_id", ""),
                        },
                        "user": {
                            "name": cfg.get("user", {}).get("name", ""),
                        },
                        "branding": cfg.get("branding", {}),
                        "cloud_sync": cfg.get("cloud_sync", []),
                        "conversations": cfg.get("ingest", {}).get("conversations", []),
                        "enrichment": cfg.get("enrichment", {}),
                        "pipeline": cfg.get("pipeline", {}),
                        "model_advisor": cfg.get("model_advisor", {}),
                    }
                    return _json_response(safe)
                except Exception as e:
                    return _json_response({"error": str(e)}, 500)

            elif path == "/api/dashboard/profile":
                try:
                    profile = {}
                    if os.path.exists(PROFILE_PATH):
                        import json as _j3
                        with open(PROFILE_PATH) as f:
                            profile = _j3.load(f)
                    return _json_response(profile)
                except Exception as e:
                    return _json_response({"error": str(e)}, 500)

            elif path == "/api/dashboard/pipeline":
                log_path = os.path.join(BASE_DIR, "logs", "sync_and_ingest.log")
                result = {"last_run": "", "steps": [], "log_tail": ""}
                try:
                    if os.path.exists(log_path):
                        with open(log_path) as f:
                            lines = f.readlines()
                        # Last 50 lines
                        result["log_tail"] = "".join(lines[-50:])
                        # Find last START
                        for line in reversed(lines):
                            if "START sync_and_ingest" in line:
                                result["last_run"] = line.strip()
                                break
                except Exception:
                    pass
                return _json_response(result)

            elif path == "/api/dashboard/setup":
                config_path = os.path.join(BASE_DIR, "config", "tailor.yaml")
                return _json_response({
                    "setup_complete": os.path.exists(config_path),
                    "config_exists": os.path.exists(config_path),
                    "example_exists": os.path.exists(config_path + ".example"),
                })

            elif path == "/api/dashboard/config/save":
                body = await _receive_body(scope, receive)
                import json as _jcfg, yaml as _ycfg
                try:
                    data = _jcfg.loads(body) if body else {}
                except Exception:
                    return _json_response({"error": "Invalid JSON body"}, 400)
                if not data:
                    return _json_response({"error": "Empty config"}, 400)
                config_path = os.path.join(BASE_DIR, "config", "tailor.yaml")
                # If existing config, load and merge
                existing = {}
                if os.path.exists(config_path):
                    try:
                        with open(config_path) as _cf:
                            existing = _ycfg.safe_load(_cf) or {}
                    except Exception:
                        existing = {}
                # Deep merge: incoming sections overwrite existing sections
                for section, values in data.items():
                    if isinstance(values, dict) and isinstance(existing.get(section), dict):
                        existing[section].update(values)
                    else:
                        existing[section] = values
                # Write YAML
                try:
                    with open(config_path, "w") as _cf:
                        _ycfg.dump(existing, _cf, default_flow_style=False, allow_unicode=True, sort_keys=False)
                    # Force config reload on next access
                    try:
                        from scripts.lib.config import _config
                        import scripts.lib.config as _cfg_mod
                        _cfg_mod._config = None
                    except Exception:
                        pass
                    return _json_response({"ok": True, "message": "Config saved"})
                except Exception as e:
                    return _json_response({"error": f"Failed to write config: {e}"}, 500)

            # ── Upload conversation files ──
            elif path == "/api/dashboard/upload":
                import uuid as _uuid
                from scripts.lib.multipart_parser import parse_multipart as _parse_mp
                ct = ""
                for hn, hv in [(k.decode(), v.decode()) for k, v in scope.get("headers", [])]:
                    if hn.lower() == "content-type": ct = hv; break
                if "multipart/form-data" not in ct:
                    return _json_response({"error": "Content-Type must be multipart/form-data"}, 400)
                body = await _receive_body(scope, receive)
                if not body:
                    return _json_response({"error": "Empty body"}, 400)
                mp = _parse_mp(body, ct)
                source_type = mp["fields"].get("source", "chatgpt")
                if source_type not in {"chatgpt", "claude", "gemini"}:
                    return _json_response({"error": f"Invalid source: {source_type}"}, 400)
                if "file" not in mp["files"]:
                    return _json_response({"error": "No file in upload"}, 400)
                file_name, file_data = mp["files"]["file"]
                # Save file
                upload_dir = os.path.join(BASE_DIR, "data", "uploads", source_type)
                os.makedirs(upload_dir, exist_ok=True)
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in file_name)
                dest = os.path.join(upload_dir, f"{ts}_{safe}")
                with open(dest, "wb") as uf:
                    uf.write(file_data)
                size_mb = round(len(file_data) / 1048576, 2)
                # Job tracker
                jid = _uuid.uuid4().hex[:12]
                jdir = os.path.join(BASE_DIR, "data", "uploads", "jobs")
                os.makedirs(jdir, exist_ok=True)
                jfile = os.path.join(jdir, f"{jid}.json")
                import json as _ju
                jdata = {"job_id": jid, "source": source_type, "file": dest,
                         "file_name": file_name, "file_size_mb": size_mb,
                         "status": "running", "started_at": datetime.now().isoformat(),
                         "records": 0, "chunks": 0, "chunks_processed": 0,
                         "progress_pct": 0, "errors": 0, "message": "Starting ingest..."}
                with open(jfile, "w") as jf:
                    _ju.dump(jdata, jf)
                # Launch background worker
                import subprocess
                vpy = os.path.join(BASE_DIR, ".venv", "bin", "python3")
                isc = os.path.join(BASE_DIR, "scripts", "ingest", "ingest_upload.py")
                lp = os.path.join(BASE_DIR, "logs", f"upload_{jid}.log")
                subprocess.Popen(
                    [vpy, isc, "--source", source_type, "--file", dest, "--job", jid],
                    stdout=open(lp, "w"), stderr=subprocess.STDOUT,
                    cwd=BASE_DIR, start_new_session=True)
                return _json_response({"ok": True, "job_id": jid, "source": source_type,
                    "file_name": file_name, "file_size_mb": size_mb,
                    "message": f"Upload received. Ingest started (job {jid})"})

            elif path.startswith("/api/dashboard/upload/status"):
                qs = scope.get("query_string", b"").decode()
                params = dict(p.split("=", 1) for p in qs.split("&") if "=" in p)
                jid = params.get("job", "")
                if not jid or not jid.isalnum():
                    return _json_response({"error": "Invalid job ID"}, 400)
                jfile = os.path.join(BASE_DIR, "data", "uploads", "jobs", f"{jid}.json")
                if not os.path.exists(jfile):
                    return _json_response({"error": f"Job not found: {jid}"}, 404)
                import json as _js
                with open(jfile) as jf:
                    return _json_response(_js.load(jf))

            elif path == "/api/dashboard/bulk":
                # Single endpoint returning all dashboard data in one call
                import sqlite3 as _sq3b
                import json as _jb
                bulk = {}

                # --- Stats ---
                try:
                    collection = get_collection()
                    total_chunks = collection.count()
                    source_counts = {}
                    for src in ["document", "email", "claude", "chatgpt"]:
                        try:
                            r = collection.get(where={"source": src}, include=[])
                            source_counts[src] = len(r["ids"])
                        except Exception:
                            source_counts[src] = 0
                    facts_data = {"total": 0, "superseded": 0, "derived": 0, "covered": 0}
                    if os.path.exists(FACTS_DB_PATH):
                        fc = _sq3b.connect(f"file:{FACTS_DB_PATH}?mode=ro", uri=True)
                        facts_data["total"] = fc.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
                        facts_data["superseded"] = fc.execute("SELECT COUNT(*) FROM facts WHERE superseded_by IS NOT NULL").fetchone()[0]
                        facts_data["derived"] = fc.execute("SELECT COUNT(*) FROM facts WHERE relation_type = 'derived'").fetchone()[0]
                        facts_data["covered"] = fc.execute("SELECT COUNT(DISTINCT chunk_id) FROM facts").fetchone()[0]
                        fc.close()
                    entity_count = 0
                    epath = os.path.join(DB_DIR, "entity_index.sqlite3")
                    if os.path.exists(epath):
                        ec = _sq3b.connect(f"file:{epath}?mode=ro", uri=True)
                        entity_count = ec.execute("SELECT COUNT(DISTINCT entity) FROM entity_index").fetchone()[0]
                        ec.close()
                    doc_count = 0
                    reg_path = os.path.join(DB_DIR, "doc_registry.json")
                    if os.path.exists(reg_path):
                        doc_count = len(_jb.load(open(reg_path)))
                    db_size_mb = 0
                    for fname in os.listdir(DB_DIR):
                        fpath = os.path.join(DB_DIR, fname)
                        if os.path.isfile(fpath):
                            db_size_mb += os.path.getsize(fpath) / (1024 * 1024)
                    bulk["stats"] = {
                        "chunks": {"total": total_chunks, **source_counts},
                        "facts": facts_data, "entities": entity_count,
                        "documents": doc_count, "db_size_mb": round(db_size_mb),
                    }
                except Exception as e:
                    bulk["stats"] = None

                # --- Services ---
                try:
                    import socket as _sockb
                    def _bcheck_port(port):
                        try:
                            s = _sockb.socket(_sockb.AF_INET, _sockb.SOCK_STREAM)
                            s.settimeout(1)
                            result = s.connect_ex(("127.0.0.1", port))
                            s.close()
                            return result == 0
                        except Exception:
                            return False
                    def _bcheck_process(name):
                        try:
                            import subprocess as _spb
                            r = _spb.run(["ps", "aux"], capture_output=True, text=True, timeout=3)
                            return name in r.stdout
                        except Exception:
                            return False
                    from scripts.lib.config import get as _cfg_get_b
                    _svc_list = _cfg_get_b("services") or [
                        {"name": "MCP Server", "id": "com.tailor.mcp", "check": "port", "port": 8787, "desc": "Knowledge Base API"},
                        {"name": "Telegram Bot", "id": "com.tailor.telegram", "check": "process", "process": "telegram_bot", "desc": "Chat interface"},
                        {"name": "Ollama", "id": "com.tailor.ollama", "check": "port", "port": 11434, "desc": "Local LLM & Embeddings"},
                    ]
                    services = []
                    for svc in _svc_list:
                        port = svc.get("port")
                        proc_name = svc.get("process")
                        status = "stopped"
                        if port and _bcheck_port(port):
                            status = "running"
                        elif proc_name and _bcheck_process(proc_name):
                            status = "running"
                        elif svc.get("id") == "com.tailor.mcp":
                            status = "running"
                        services.append({"name": svc["name"], "id": svc.get("id", ""), "port": port, "status": status, "desc": svc.get("desc", "")})
                    bulk["services"] = services
                except Exception:
                    bulk["services"] = None

                # --- Config ---
                try:
                    from config import load_config
                    cfg = load_config()
                    bulk["config"] = {
                        "llm": {"provider": cfg.get("llm", {}).get("provider", ""), "model": cfg.get("llm", {}).get("model", ""), "tool_use": cfg.get("llm", {}).get("tool_use", False)},
                        "embedding": {"provider": cfg.get("embedding", {}).get("provider", ""), "model": cfg.get("embedding", {}).get("model", ""), "dimensions": cfg.get("embedding", {}).get("dimensions", "")},
                        "classifier": {"provider": cfg.get("classifier", {}).get("provider", ""), "model": cfg.get("classifier", {}).get("model", "")},
                        "telegram": {"chat_id": cfg.get("telegram", {}).get("chat_id", "")},
                        "user": {"name": cfg.get("user", {}).get("name", "")},
                        "branding": cfg.get("branding", {}),
                        "cloud_sync": cfg.get("cloud_sync", []),
                        "conversations": cfg.get("ingest", {}).get("conversations", []),
                        "enrichment": cfg.get("enrichment", {}),
                        "pipeline": cfg.get("pipeline", {}),
                        "model_advisor": cfg.get("model_advisor", {}),
                    }
                except Exception:
                    bulk["config"] = None

                # --- Profile ---
                try:
                    bulk["profile"] = _jb.load(open(PROFILE_PATH)) if os.path.exists(PROFILE_PATH) else {}
                except Exception:
                    bulk["profile"] = {}

                # --- Pipeline ---
                try:
                    log_path = os.path.join(BASE_DIR, "logs", "sync_and_ingest.log")
                    pipe = {"last_run": "", "steps": [], "log_tail": ""}
                    if os.path.exists(log_path):
                        with open(log_path) as f:
                            lines = f.readlines()
                        pipe["log_tail"] = "".join(lines[-50:])
                        for line in reversed(lines):
                            if "START sync_and_ingest" in line:
                                pipe["last_run"] = line.strip()
                                break
                    bulk["pipeline"] = pipe
                except Exception:
                    bulk["pipeline"] = None


                # --- Model Advisor ---
                try:
                    import sqlite3 as _sq3mab
                    ma_db_path = os.path.join(DB_DIR, "model_advisor.sqlite3")
                    ma_bulk = {"advisories": [], "last_check": None}
                    if os.path.exists(ma_db_path):
                        mac = _sq3mab.connect(f"file:{ma_db_path}?mode=ro", uri=True)
                        mac.row_factory = _sq3mab.Row
                        rows = mac.execute(
                            """SELECT timestamp, type, provider, model_id, detail, size_gb
                               FROM advisories ORDER BY id DESC LIMIT 20"""
                        ).fetchall()
                        ma_bulk["advisories"] = [dict(r) for r in rows]
                        last = mac.execute("SELECT MAX(timestamp) FROM advisories").fetchone()
                        if last and last[0]:
                            ma_bulk["last_check"] = last[0]
                        else:
                            last_snap = mac.execute("SELECT MAX(snapshot_date) FROM snapshots").fetchone()
                            ma_bulk["last_check"] = last_snap[0] if last_snap and last_snap[0] else None
                        try:
                            recs = mac.execute("SELECT model_id, params_b, ram_q4_gb, downloads, family FROM recommendations ORDER BY downloads DESC LIMIT 10").fetchall()
                            ma_bulk["recommendations"] = [dict(r) for r in recs]
                        except Exception:
                            ma_bulk["recommendations"] = []
                        mac.close()
                    bulk["model_advisor"] = ma_bulk
                except Exception:
                    bulk["model_advisor"] = None

                return _json_response(bulk)


            elif path == "/api/dashboard/model-advisor":
                # Return latest model advisor results
                import sqlite3 as _sq3ma
                ma_data = {"advisories": [], "last_check": None, "snapshot_counts": {}}
                ma_db_path = os.path.join(DB_DIR, "model_advisor.sqlite3")
                if os.path.exists(ma_db_path):
                    try:
                        mac = _sq3ma.connect(f"file:{ma_db_path}?mode=ro", uri=True)
                        mac.row_factory = _sq3ma.Row
                        # Latest advisories (last 50)
                        rows = mac.execute(
                            """SELECT timestamp, type, provider, model_id, detail, size_gb
                               FROM advisories ORDER BY id DESC LIMIT 50"""
                        ).fetchall()
                        ma_data["advisories"] = [dict(r) for r in rows]
                        # Last check timestamp
                        last = mac.execute("SELECT MAX(timestamp) FROM advisories").fetchone()
                        if last and last[0]:
                            ma_data["last_check"] = last[0]
                        else:
                            last_snap = mac.execute("SELECT MAX(snapshot_date) FROM snapshots").fetchone()
                            ma_data["last_check"] = last_snap[0] if last_snap and last_snap[0] else None
                        # Snapshot counts per provider
                        for row in mac.execute(
                            """SELECT provider, COUNT(DISTINCT model_id) as cnt
                               FROM snapshots WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM snapshots)
                               GROUP BY provider"""
                        ).fetchall():
                            ma_data["snapshot_counts"][row["provider"]] = row["cnt"]
                        mac.close()
                    except Exception as e:
                        ma_data["error"] = str(e)
                # Recommendations
                try:
                    recs = mac.execute("SELECT model_id, params_b, ram_q4_gb, downloads, family FROM recommendations ORDER BY downloads DESC LIMIT 20").fetchall()
                    ma_data["recommendations"] = [dict(r) for r in recs]
                except Exception:
                    ma_data["recommendations"] = []
                # Include config
                try:
                    from scripts.lib.config import get_model_advisor_config as _get_ma_cfg
                    ma_data["config"] = _get_ma_cfg()
                except Exception:
                    ma_data["config"] = {}
                return _json_response(ma_data)


            elif path == "/api/dashboard/model-advisor/install":
                # Install/update an Ollama model via ollama pull
                body = await _receive_body(scope, receive)
                import json as _ji, subprocess as _spi, threading as _thi, uuid as _uui
                try:
                    req = _ji.loads(body)
                except Exception:
                    return _json_response({"error": "Invalid JSON"}, 400)
                model_name = req.get("model", "").strip()
                if not model_name:
                    return _json_response({"error": "Missing 'model' field"}, 400)
                # Sanitize: only allow alphanumeric, dots, colons, hyphens, slashes
                import re as _rei
                if not _rei.match(r'^[a-zA-Z0-9._:/-]+$', model_name):
                    return _json_response({"error": "Invalid model name"}, 400)
                # Derive ollama name from HuggingFace ID if needed
                # e.g. "Qwen/Qwen3.5-9B" -> try "qwen3.5:9b" style
                ollama_name = model_name
                if "/" in model_name and not model_name.startswith("http"):
                    # HF format: Org/Model-XB -> try lowercase base
                    parts = model_name.split("/")[-1]
                    ollama_name = parts.lower()
                    # Convert "qwen3.5-9b" to "qwen3.5:9b"
                    ollama_name = _rei.sub(r'-(\d+\.?\d*b)$', r':', ollama_name)
                job_id = str(_uui.uuid4())[:8]
                _ollama_jobs[job_id] = {"model": ollama_name, "status": "pulling", "progress": "Starting...", "error": ""}
                def _do_pull(jid, mname):
                    try:
                        proc = _spi.Popen(
                            ["ollama", "pull", mname],
                            stdout=_spi.PIPE, stderr=_spi.STDOUT, text=True
                        )
                        last_line = ""
                        for line in proc.stdout:
                            last_line = line.strip()
                            _ollama_jobs[jid]["progress"] = last_line
                        proc.wait()
                        if proc.returncode == 0:
                            _ollama_jobs[jid]["status"] = "done"
                            _ollama_jobs[jid]["progress"] = "Complete"
                        else:
                            _ollama_jobs[jid]["status"] = "error"
                            _ollama_jobs[jid]["error"] = last_line or "Pull failed"
                    except Exception as e:
                        _ollama_jobs[jid]["status"] = "error"
                        _ollama_jobs[jid]["error"] = str(e)
                t = _thi.Thread(target=_do_pull, args=(job_id, ollama_name), daemon=True)
                t.start()
                return _json_response({"ok": True, "job_id": job_id, "model": ollama_name})

            elif path.startswith("/api/dashboard/model-advisor/status/"):
                # Poll install job status
                job_id = path.split("/")[-1]
                job = _ollama_jobs.get(job_id)
                if not job:
                    return _json_response({"error": "Job not found"}, 404)
                return _json_response(job)

            elif path == "/api/ingest-live":
                # Receive live conversation messages from browser extension
                body = await _receive_body(scope, receive)
                import json as _jlive, hashlib as _hlive
                try:
                    data = _jlive.loads(body)
                except Exception:
                    return _json_response({"error": "Invalid JSON"}, 400)
                source = data.get("source", "web")
                title = data.get("title", "Untitled")
                messages = data.get("messages", [])
                conversation_id = data.get("conversation_id", "")
                timestamp = data.get("timestamp", "")
                if not messages:
                    return _json_response({"error": "No messages"}, 400)
                # Build conversation text
                lines = []
                for m in messages:
                    role = m.get("role", "unknown").capitalize()
                    content = m.get("content", "")
                    lines.append(f"{role}: {content}")
                text = "\n\n".join(lines)
                # Generate stable chunk ID from conversation_id or content hash
                if conversation_id:
                    conv_hash = _hlive.sha256(conversation_id.encode()).hexdigest()[:16]
                    chunk_id = f"live_{source}_{conv_hash}"
                else:
                    content_hash = _hlive.sha256(text[:500].encode()).hexdigest()[:16]
                    chunk_id = f"live_{source}_{content_hash}"
                # Check if this is an update
                collection = get_collection()
                is_update = False
                try:
                    existing = collection.get(ids=[chunk_id])
                    if existing and existing["ids"]:
                        is_update = True
                except Exception:
                    pass
                # Embed and upsert
                try:
                    from scripts.lib.embedding import get_embeddings
                    embedding = get_embeddings(text[:4000])
                    # Flatten if nested list (some providers return [[...]])
                    if embedding and isinstance(embedding[0], list):
                        embedding = embedding[0]
                    metadata = {
                        "source": source,
                        "title": title,
                        "type": "conversation",
                        "document_date": timestamp[:10] if timestamp else "",
                    }
                    collection.upsert(
                        ids=[chunk_id],
                        embeddings=[embedding],
                        documents=[text[:10000]],
                        metadatas=[metadata],
                    )
                    return _json_response({"ok": True, "chunks": 1, "updated": is_update})
                except Exception as e:
                    import traceback as _tb_live
                    print(f"[INGEST-LIVE ERROR] {e}", flush=True)
                    _tb_live.print_exc()
                    return _json_response({"error": str(e)}, 500)

            elif path == "/api/dashboard/rate-limit":
                return _json_response(_rate_limiter.get_stats())

            else:
                return _json_response({"error": f"Unknown API path: {path}"}, 404)
        except Exception as e:
            return _json_response({"error": str(e)}, 500)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        path = scope.get("path", "/")
        # CORS preflight — respond immediately, no auth needed
        method = scope.get("method", "GET")
        if method == "OPTIONS":
            response = Response(content="", status_code=204, headers=_CORS_HEADERS)
            await response(scope, receive, send)
            return
        if not TAILOR_API_KEY and not path.startswith("/api/") and path != "/dashboard":
            await self.app(scope, receive, send)
            return
        # Serve dashboard HTML
        if path == "/dashboard":
            dashboard_path = os.path.join(BASE_DIR, "dashboard", "index.html")
            if os.path.exists(dashboard_path):
                with open(dashboard_path, "r") as _df:
                    html = _df.read()
                response = Response(content=html, status_code=200, media_type="text/html")
                await response(scope, receive, send)
                return
        # Serve dashboard static assets (icons, vendor js, images)
        if path.startswith("/dashboard/") and path != "/dashboard" and path != "/dashboard/":
            import mimetypes
            # Resolve path relative to dashboard/ — strip leading /dashboard/
            rel = path[len("/dashboard/"):]
            # Security: reject path traversal via realpath validation
            dashboard_dir = os.path.realpath(os.path.join(BASE_DIR, "dashboard"))
            file_path = os.path.realpath(os.path.join(dashboard_dir, rel))
            if file_path.startswith(dashboard_dir + os.sep) and os.path.isfile(file_path):
                mime = mimetypes.guess_type(file_path)[0] or "application/octet-stream"
                with open(file_path, "rb") as _sf:
                    data = _sf.read()
                response = Response(content=data, status_code=200, media_type=mime,
                    headers={"Cache-Control": "public, max-age=604800"})
                await response(scope, receive, send)
                return

        if path in self.PUBLIC_PATHS:
            await self.app(scope, receive, send)
            return

        # ── REST API ──
        if path.startswith("/api/"):
            client = scope.get("client", ("", 0))
            client_host = client[0] if client else ""
            is_localhost = client_host in ("127.0.0.1", "::1", "localhost")
            is_dashboard = path.startswith("/api/dashboard/")
            is_auth_api = path.startswith("/api/auth/")
            is_ingest_api = path == "/api/ingest-live"

            # Localhost: allow all API calls without auth
            # Remote: allow dashboard + auth API + ingest-live with cookie/token auth
            _resolved_token = ""
            if is_localhost or is_dashboard or is_auth_api or is_ingest_api:
                # Remote calls need auth (except /api/auth/login which verifies itself)
                if not is_localhost and path not in ("/api/auth/login", "/api/dashboard/setup"):
                    token_ok = False
                    hdrs = dict(scope.get("headers", []))
                    # Check Bearer token
                    auth_val = hdrs.get(b"authorization", b"").decode("utf-8", errors="ignore")
                    _resolved_token = ""
                    if auth_val.startswith("Bearer ") and _token_auth.authenticate(auth_val[7:]):
                        _resolved_token = auth_val[7:]
                        token_ok = True
                    # Check query string token
                    if not token_ok:
                        qs = scope.get("query_string", b"").decode()
                        for param in qs.split("&"):
                            if param.startswith("token=") and _token_auth.authenticate(param[6:]):
                                _resolved_token = param[6:]
                                token_ok = True
                                break
                    # Check session cookie (stored as SHA-256 hash)
                    if not token_ok:
                        cookie_header = hdrs.get(b"cookie", b"").decode("utf-8", errors="ignore")
                        for part in cookie_header.split(";"):
                            part = part.strip()
                            if part.startswith("tailor_session="):
                                session_id = part[len("tailor_session="):]
                                import hashlib as _hck
                                session_hash = _hck.sha256(session_id.encode()).hexdigest()
                                sessions_path = os.path.join(BASE_DIR, "db", "dashboard_sessions.json")
                                if os.path.exists(sessions_path):
                                    import json as _jck, time as _tck
                                    try:
                                        with open(sessions_path) as _sf:
                                            sessions = _jck.load(_sf)
                                        sess = sessions.get(session_hash, {})
                                        if sess.get("expires", 0) > _tck.time():
                                            token_ok = True
                                    except Exception:
                                        pass
                                break
                    if not token_ok:
                        _rate_limiter.record_failure(client_host)
                        if _rate_limiter.is_banned(client_host):
                            _rem = _rate_limiter.ban_remaining(client_host)
                            response = Response(content='{"error":"Too many attempts","retry_after":%d}' % _rem, status_code=429, media_type="application/json", headers=_CORS_HEADERS)
                        else:
                            response = Response(content='{"error":"Unauthorized"}', status_code=401, media_type="application/json", headers=_CORS_HEADERS)
                        await response(scope, receive, send)
                        return
                # Check endpoint permission
                if _resolved_token and not is_localhost:
                    _ep_ok, _ep_reason = _token_auth.check_endpoint(_resolved_token, path)
                    if not _ep_ok:
                        response = Response(content='{"error":"Forbidden: insufficient permissions"}', status_code=403, media_type="application/json", headers=_CORS_HEADERS)
                        await response(scope, receive, send)
                        return
                response = await self._handle_rest_api(path, scope, receive)
                await response(scope, receive, send)
                return

        headers = dict(scope.get("headers", []))
        auth_value = headers.get(b"authorization", b"").decode("utf-8", errors="ignore")
        _mcp_token = ""
        if auth_value.startswith("Bearer ") and _token_auth.authenticate(auth_value[7:]):
            _mcp_token = auth_value[7:]
            scope["_tailor_token"] = _mcp_token
            await self.app(scope, receive, send)
            return
        query_string = scope.get("query_string", b"").decode("utf-8", errors="ignore")
        for param in query_string.split("&"):
            if param.startswith("token="):
                _mcp_token = param[6:]
                if _token_auth.authenticate(_mcp_token):
                    scope["_tailor_token"] = _mcp_token
                    await self.app(scope, receive, send)
                    return
                break
        response = Response(
            content='{"error": "Unauthorized"}',
            status_code=401,
            headers={"WWW-Authenticate": "Bearer", "Content-Type": "application/json"},
        )
        await response(scope, receive, send)


# ============================================================
# INIZIALIZZAZIONE MCP + DB
# ============================================================

mcp = FastMCP("TAILOR", host="0.0.0.0", port=SERVER_PORT, transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False))

_collection = None
_chromadb_client = None
_maintenance_mode = False
MAINTENANCE_LOCK = os.path.join(BASE_DIR, "maintenance.lock")


def _enter_maintenance(signum: int | None = None, frame=None) -> None:
    """SIGUSR1 handler: close ChromaDB for exclusive pipeline access."""
    global _collection, _chromadb_client, _maintenance_mode
    _maintenance_mode = True
    _collection = None
    if _chromadb_client is not None:
        del _chromadb_client
        _chromadb_client = None
    # Write lock file so pipeline knows ChromaDB is released
    with open(MAINTENANCE_LOCK, "w") as f:
        f.write(str(os.getpid()))
    print(f"[MAINTENANCE] Entered maintenance mode — ChromaDB released", flush=True)


def _exit_maintenance(signum: int | None = None, frame=None) -> None:
    """SIGUSR2 handler: reopen ChromaDB, resume normal operation."""
    global _maintenance_mode
    _maintenance_mode = False
    if os.path.exists(MAINTENANCE_LOCK):
        os.remove(MAINTENANCE_LOCK)
    # _collection will be lazy-reopened on next get_collection() call
    print(f"[MAINTENANCE] Exited maintenance mode — ChromaDB will reopen on next query", flush=True)


# Register signal handlers
signal.signal(signal.SIGUSR1, _enter_maintenance)
signal.signal(signal.SIGUSR2, _exit_maintenance)


def get_collection() -> chromadb.Collection | None:
    """Lazy init of ChromaDB collection. Returns None during maintenance."""
    global _collection, _chromadb_client
    if _maintenance_mode:
        return None
    if _collection is None:
        _chromadb_client = chromadb.PersistentClient(path=DB_DIR)
        _collection = _chromadb_client.get_or_create_collection(name=COLLECTION_NAME, metadata={"hnsw:space": "cosine"})
    return _collection


# get_embedding() imported from lib.embedding




# ============================================================
# RERANK HELPERS (ONNX Runtime + Jina fallback)
# ============================================================

import glob as _glob
_onnx_candidates = _glob.glob(os.path.join(BASE_DIR, "models", "reranker", "onnx", "*.onnx"))
RERANK_ONNX_PATH = _onnx_candidates[0] if _onnx_candidates else os.path.join(BASE_DIR, "models", "reranker", "onnx", "model.onnx")
RERANK_TOKENIZER_PATH = os.path.join(BASE_DIR, "models", "reranker", "tokenizer.json")

_rerank_session = None
_rerank_tokenizer = None
_rerank_failed = False

def _get_rerank_engine() -> tuple:
    """Lazy-load ONNX session + tokenizer. Returns (session, tokenizer) or (None, None)."""
    global _rerank_session, _rerank_tokenizer, _rerank_failed
    if _rerank_session is not None:
        return _rerank_session, _rerank_tokenizer
    if _rerank_failed:
        return None, None
    try:
        import onnxruntime as ort
        from tokenizers import Tokenizer
        _rerank_session = ort.InferenceSession(RERANK_ONNX_PATH)
        _rerank_tokenizer = Tokenizer.from_file(RERANK_TOKENIZER_PATH)
        _rerank_tokenizer.enable_truncation(max_length=512)
        _rerank_tokenizer.enable_padding(pad_id=0, pad_token="[PAD]")
        print(f"[RERANK] ONNX cross-encoder loaded ({RERANK_ONNX_PATH}).", file=sys.stderr)
        return _rerank_session, _rerank_tokenizer
    except Exception as e:
        _rerank_failed = True
        print(f"[RERANK] ONNX non disponibile: {e}. Fallback Jina.", file=sys.stderr)
        return None, None


def _rerank_local(query: str, documents: list) -> list:
    """Local re-rank with ONNX cross-encoder. Returns list of (score, index)."""
    import numpy as np
    session, tokenizer = _get_rerank_engine()
    if session is None:
        return None
    pairs = [(query, doc) for doc in documents]
    encodings = tokenizer.encode_batch(pairs)
    input_ids = np.array([e.ids for e in encodings], dtype=np.int64)
    attention_mask = np.array([e.attention_mask for e in encodings], dtype=np.int64)
    token_type_ids = np.array([e.type_ids for e in encodings], dtype=np.int64)
    outputs = session.run(None, {
        "input_ids": input_ids,
        "attention_mask": attention_mask,
        "token_type_ids": token_type_ids
    })
    scores = outputs[0].flatten()
    return sorted(enumerate(scores), key=lambda x: x[1], reverse=True)


def _rerank_jina(query: str, documents: list, top_n: int = 10) -> list:
    """Fallback: Jina Reranker API. Returns list of indices sorted by relevance."""
    api_key = JINA_API_KEY
    if not api_key:
        return list(range(min(top_n, len(documents))))
    try:
        resp = requests.post(
            "https://api.jina.ai/v1/rerank",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
            json={"query": query, "documents": documents, "top_n": top_n,
                  "model": "jina-reranker-v2-base-multilingual"},
            timeout=10
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])
        return [r["index"] for r in sorted(results, key=lambda x: x["relevance_score"], reverse=True)]
    except Exception as e:
        print(f"[RERANK] Jina fallback failed: {e}", file=sys.stderr)
        return list(range(min(top_n, len(documents))))


def rerank_results(query: str, items: list, n_results: int = 10, doc_key: str = "doc") -> list:
    """Re-rank a list of results by relevance using ONNX cross-encoder.
    Fallback: Jina Reranker API. Last fallback: no re-ranking.
    
    Args:
        query: the user's original query
        items: lista di dict, ognuno con almeno una chiave doc_key contenente il testo
        n_results: how many results to return
        doc_key: chiave del dict che contiene il testo del documento
    
    Returns:
        Re-ordered list (top n_results)
    """
    if len(items) <= 1:
        return items[:n_results]
    
    documents = [item[doc_key][:1000] for item in items]
    
    # Prova ONNX locale
    try:
        ranked = _rerank_local(query, documents)
        if ranked is not None:
            return [items[idx] for idx, _ in ranked[:n_results]]
    except Exception as e:
        print(f"[RERANK] ONNX error: {e}. Trying Jina.", file=sys.stderr)
    
    # Fallback Jina
    ranked_indices = _rerank_jina(query, documents, top_n=n_results)
    return [items[i] for i in ranked_indices if i < len(items)]


# ============================================================
# FACTS HELPERS
# ============================================================

def get_facts_for_chunks(chunk_ids: list) -> dict:
    """Recupera fatti atomici attivi per una lista di chunk_id.
    Returns {chunk_id: [{"fact": ..., "category": ..., "entity_tags": [...], "event_date": ...}, ...]}
    Excludes superseded facts (superseded_by IS NOT NULL) and expired ones (expires_at < now)."""
    if not chunk_ids or not os.path.exists(FACTS_DB_PATH):
        return {}
    import sqlite3 as _sq
    try:
        conn = _sq.connect(f"file:{FACTS_DB_PATH}?mode=ro", uri=True)
        conn.row_factory = _sq.Row
        cur = conn.cursor()
        result = {}
        # Batch query con IN clause — SQLite supporta fino a 999 parametri
        BATCH = 500
        for i in range(0, len(chunk_ids), BATCH):
            batch = chunk_ids[i:i+BATCH]
            placeholders = ",".join("?" * len(batch))
            cur.execute(f"""
                SELECT chunk_id, fact, category, entity_tags, event_date, document_date
                FROM facts
                WHERE chunk_id IN ({placeholders})
                AND superseded_by IS NULL
                AND (expires_at IS NULL OR expires_at = '' OR expires_at > datetime('now'))
                ORDER BY chunk_id, id
            """, batch)
            for row in cur.fetchall():
                cid = row["chunk_id"]
                if cid not in result:
                    result[cid] = []
                try:
                    tags = json.loads(row["entity_tags"]) if row["entity_tags"] else []
                except (json.JSONDecodeError, TypeError):
                    tags = []
                result[cid].append({
                    "fact": row["fact"],
                    "category": row["category"] or "",
                    "entity_tags": tags,
                    "event_date": row["event_date"] or "",
                    "document_date": row["document_date"] or ""
                })
        conn.close()
        return result
    except Exception:
        return {}


def format_facts_block(facts: list, max_facts: int = 10) -> str:
    """Formatta una lista di fatti per l'output testuale.
    Returns empty string if no facts."""
    if not facts:
        return ""
    lines = []
    shown = facts[:max_facts]
    for f in shown:
        parts = []
        if f["category"]:
            parts.append(f["category"])
        if f.get("event_date"):
            parts.append(f"event:{f['event_date']}")
        if f.get("document_date") and f.get("document_date") != f.get("event_date"):
            parts.append(f"doc:{f['document_date']}")
        suffix = f" ({', '.join(parts)})" if parts else ""
        lines.append(f"  • {f['fact']}{suffix}")
    header = f"\n📌 Atomic facts ({len(facts)} total):" if len(facts) > max_facts else "\n📌 Atomic facts:"
    if len(facts) > max_facts:
        lines.append(f"  ... e altri {len(facts) - max_facts} fatti")
    return header + "\n" + "\n".join(lines)


def get_derived_facts_for_query(query: str, max_results: int = 5) -> str:
    """Search derived facts relevant to a query by entity matching.
    Returns formatted block of derived inferences, or empty string."""
    if not os.path.exists(FACTS_DB_PATH):
        return ""
    import sqlite3 as _sq
    import re as _re
    try:
        conn = _sq.connect(f"file:{FACTS_DB_PATH}?mode=ro", uri=True)
        conn.row_factory = _sq.Row
        # Extract candidate entities from query (capitalized words + bigrams)
        caps = _re.findall(r'[A-Z][a-zA-Z]+', query)
        if not caps:
            conn.close()
            return ""
        words = list(caps)
        for i in range(len(caps) - 1):
            words.append(f"{caps[i]} {caps[i+1]}")
        results = []
        seen = set()
        for w in words:
            rows = conn.execute("""
                SELECT fact, category, entity_tags, confidence
                FROM facts
                WHERE relation_type = 'derived'
                AND entity_tags LIKE ?
                AND (superseded_by IS NULL OR superseded_by = '')
                ORDER BY confidence DESC
                LIMIT ?
            """, (f"%{w}%", max_results)).fetchall()
            for r in rows:
                if r["fact"] not in seen:
                    seen.add(r["fact"])
                    results.append(dict(r))
        conn.close()
        if not results:
            return ""
        results = sorted(results, key=lambda x: x.get("confidence", 0), reverse=True)[:max_results]
        lines = []
        for r in results:
            conf = r.get("confidence", 0)
            lines.append(f"  \u2022 [{conf:.0%}] {r['fact']}")
        return f"\n\U0001f517 Inferenze derivate ({len(results)}):\n" + "\n".join(lines)
    except Exception:
        return ""


def is_superseded(meta: dict) -> bool:
    """Returns True if the chunk has been superseded by a more recent one."""
    return bool(meta.get("superseded_by", ""))


def filter_superseded(ids: list, documents: list, metadatas: list, distances: list | None = None, n_results: int | None = None) -> dict:
    """Filter superseded chunks from ChromaDB results.
    Returns the same structures without chunks that have non-empty superseded_by."""
    filtered = {"ids": [], "documents": [], "metadatas": []}
    if distances is not None:
        filtered["distances"] = []
    for i in range(len(ids)):
        if is_superseded(metadatas[i]):
            continue
        filtered["ids"].append(ids[i])
        filtered["documents"].append(documents[i])
        filtered["metadatas"].append(metadatas[i])
        if distances is not None:
            filtered["distances"].append(distances[i])
        if n_results and len(filtered["ids"]) >= n_results:
            break
    return filtered


# ============================================================
# TOOLS DI LETTURA
# ============================================================

@mcp.tool()
def kb_search(query: str, n_results: int = 5, source_filter: str = "", include_superseded: bool = False) -> str:
    """Search the Knowledge Base by semantic query.

    Args:
        query: The question or topic to search (e.g. "crypto investment decisions")
        n_results: Number of results to return (default 5, max 20)
        source_filter: Filter by source: "document", "chatgpt", "claude" or "" for all
        include_superseded: If True, include chunks superseded by more recent info (default False)
    """
    n_results = min(max(n_results, 1), 20)
    try:
        embedding = get_embedding(query)
        collection = get_collection()
        fetch_n = n_results * 3 if not include_superseded else n_results * 3
        kwargs = dict(query_embeddings=[embedding], n_results=fetch_n)
        if source_filter in ("document", "chatgpt", "claude", "email"):
            kwargs["where"] = {"source": source_filter}
        results = collection.query(**kwargs)
        if not results or not results["documents"] or not results["documents"][0]:
            return "No results found in the KB."
        # Filtro superseded
        if not include_superseded:
            f = filter_superseded(
                results["ids"][0], results["documents"][0],
                results["metadatas"][0], results["distances"][0], n_results
            )
        else:
            f = {"ids": results["ids"][0][:n_results], "documents": results["documents"][0][:n_results],
                 "metadatas": results["metadatas"][0][:n_results], "distances": results["distances"][0][:n_results]}
        # Re-rank con cross-encoder
        kb_items = [{"id": cid, "doc": doc, "meta": meta, "distance": dist}
                    for cid, doc, meta, dist in zip(f["ids"], f["documents"], f["metadatas"], f["distances"])]
        if len(kb_items) > 1:
            kb_items = rerank_results(query, kb_items, n_results=n_results, doc_key="doc")
        else:
            kb_items = kb_items[:n_results]

        # Lookup fatti atomici
        reranked_ids = [item["id"] for item in kb_items]
        chunk_facts = get_facts_for_chunks(reranked_ids)

        output = []
        for i, item in enumerate(kb_items):
            cid, doc, meta, distance = item["id"], item["doc"], item["meta"], item.get("distance", 0)
            relevance = max(0, 1 - distance)
            source = meta.get("source", "chatgpt")
            folder = meta.get("folder", "")
            doc_type = meta.get("doc_type", "")
            category = meta.get("category", "")
            extra = " | ".join(filter(None, [folder, doc_type, f"[{category}]" if category else ""]))
            facts_block = format_facts_block(chunk_facts.get(cid, []))
            output.append(
                f"--- Result {i+1} (relevance: {relevance:.2f}) ---\n"
                f"Conversazione: {meta.get('title', 'N/A')}\n"
                f"Data: {meta.get('date', 'N/A')}\n"
                f"Fonte: {source}" + (f" | {extra}" if extra else "") + "\n"
                f"---\n{doc[:2000]}{facts_block}\n"
            )
        # Append derived facts (inferences) if relevant
        derived_block = get_derived_facts_for_query(query)
        if derived_block:
            output.append(derived_block)

        return "\n".join(output)
    except Exception as e:
        return f"Search error: {str(e)}"


@mcp.tool()
def kb_hybrid_search(query: str, n_results: int = 10, source_filter: str = "", include_superseded: bool = False) -> str:
    """Hybrid search: combines semantic search + entity search in a single query.

    This is the main search tool. It automatically performs:
    1. Semantic search (embedding similarity)
    2. Entity search (proper nouns, companies, projects mentioned in the query)
    3. Deduplicates and merges results, sorted by relevance + recency
    4. Filters superseded chunks unless include_superseded=True

    Usalo al posto di kb_search e kb_search_by_entity separati.

    Args:
        query: The question or topic to search (e.g. "what decisions were made about project X?")
        n_results: Number of results (default 10, max 30)
        source_filter: Filter by source: "document", "chatgpt", "claude", "email" or "" for all
        include_superseded: If True, include chunks superseded by more recent info (default False)
    """
    import json as _json
    import sqlite3 as _sqlite3
    import re as _re
    n_results = min(max(n_results, 1), 30)
    ENTITY_DB_PATH = os.path.join(DB_DIR, "entity_index.sqlite3")

    try:
        collection = get_collection()
        seen_ids = {}  # chunk_id -> {"doc", "meta", "score", "source_type"}

        # --- STEP 1: Semantic search (over-fetch for re-ranking) ---
        semantic_n = min(n_results * 3, 30)
        embedding = get_embedding(query)
        sem_kwargs = dict(query_embeddings=[embedding], n_results=semantic_n)
        if source_filter in ("document", "chatgpt", "claude", "email"):
            sem_kwargs["where"] = {"source": source_filter}
        sem_results = collection.query(**sem_kwargs)

        if sem_results and sem_results["documents"] and sem_results["documents"][0]:
            for doc, meta, dist, cid in zip(
                sem_results["documents"][0], sem_results["metadatas"][0],
                sem_results["distances"][0], sem_results["ids"][0]
            ):
                relevance = max(0, 1 - dist)
                seen_ids[cid] = {
                    "doc": doc, "meta": meta, "score": relevance,
                    "source_type": "semantic", "date": meta.get("date", "")
                }

        # --- STEP 2: Extract candidate entities from the query ---
        entity_hits = []
        if os.path.exists(ENTITY_DB_PATH):
            idx_conn = _sqlite3.connect(f"file:{ENTITY_DB_PATH}?mode=ro", uri=True)
            idx_cursor = idx_conn.cursor()

            # Extract capitalized words (2+ chars) as candidate entities
            words = query.split()
            candidates = set()

            # Parole singole capitalizzate
            for w in words:
                clean = _re.sub(r"[^\w]", "", w)
                if clean and clean[0].isupper() and len(clean) >= 2:
                    candidates.add(clean)

            # Bigrammi capitalizzati (es. "Acme Corp", "the user")
            for i in range(len(words) - 1):
                w1 = _re.sub(r"[^\w]", "", words[i])
                w2 = _re.sub(r"[^\w]", "", words[i + 1])
                if w1 and w2 and w1[0].isupper() and w2[0].isupper():
                    candidates.add(f"{w1} {w2}")

            # Trigrammi (es. "Acme Corp Inc")
            for i in range(len(words) - 2):
                w1 = _re.sub(r"[^\w]", "", words[i])
                w2 = _re.sub(r"[^\w]", "", words[i + 1])
                w3 = _re.sub(r"[^\w]", "", words[i + 2])
                if w1 and w3 and w1[0].isupper():
                    candidates.add(f"{w1} {w2} {w3}")

            # Search each candidate nell'entity index
            for candidate in candidates:
                idx_cursor.execute(
                    "SELECT DISTINCT chunk_id FROM entity_index WHERE entity LIKE ? COLLATE NOCASE LIMIT 100",
                    (f"%{candidate}%",)
                )
                chunk_ids = [r[0] for r in idx_cursor.fetchall()]
                if chunk_ids:
                    entity_hits.append({"entity": candidate, "chunk_ids": chunk_ids})

            idx_conn.close()

        # --- STEP 3: Retrieve chunks from entity hits (only those not already found) ---
        new_entity_ids = []
        for hit in entity_hits:
            for cid in hit["chunk_ids"]:
                if cid not in seen_ids:
                    new_entity_ids.append(cid)

        if new_entity_ids:
            # Limita per non esplodere
            new_entity_ids = new_entity_ids[:100]
            FETCH_BATCH = 40
            for i in range(0, len(new_entity_ids), FETCH_BATCH):
                batch_ids = new_entity_ids[i:i + FETCH_BATCH]
                try:
                    batch_result = collection.get(ids=batch_ids, include=["documents", "metadatas"])
                    for cid, doc, meta in zip(batch_result["ids"], batch_result["documents"], batch_result["metadatas"]):
                        if source_filter and meta.get("source", "") != source_filter:
                            continue
                        if cid not in seen_ids:
                            seen_ids[cid] = {
                                "doc": doc, "meta": meta, "score": 0.5,
                                "source_type": "entity", "date": meta.get("date", "")
                            }
                except Exception:
                    pass

        if not seen_ids:
            return "No results found in the KB."

        # --- STEP 3b: Filter superseded chunks ---
        if not include_superseded:
            seen_ids = {cid: v for cid, v in seen_ids.items() if not is_superseded(v["meta"])}

        if not seen_ids:
            return "No results found in the KB (all results have been superseded by more recent info)."

        # --- STEP 4: Ordina e formatta ---
        # Semantic results first (by score), then entity (by date)
        items = list(seen_ids.items())
        items.sort(key=lambda x: (
            0 if x[1]["source_type"] == "semantic" else 1,
            -x[1]["score"],
            x[1]["date"] if x[1]["date"] else ""
        ), reverse=False)

        # Re-rank i candidati con cross-encoder
        rerank_candidates = [{"key": k, **v} for k, v in items[:min(len(items), n_results * 3)]]
        if len(rerank_candidates) > 1:
            reranked = rerank_results(query, rerank_candidates, n_results=n_results, doc_key="doc")
            top_items = [(item["key"], {k: v for k, v in item.items() if k != "key"}) for item in reranked]
        else:
            top_items = items[:n_results]

        # Entity summary
        entity_summary = ""
        if entity_hits:
            entity_names = [h["entity"] for h in entity_hits]
            entity_summary = f"Entities recognized: {', '.join(entity_names)}\n"

        sem_count = sum(1 for _, v in top_items if v["source_type"] == "semantic")
        ent_count = sum(1 for _, v in top_items if v["source_type"] == "entity")

        output = [
            f"Hybrid search: {len(seen_ids)} total results (semantic: {sem_count}, "
            f"entities: {ent_count} nei top {len(top_items)})\n{entity_summary}"
        ]

        # Lookup atomic facts for chunks in results
        top_chunk_ids = [cid for cid, _ in top_items]
        chunk_facts = get_facts_for_chunks(top_chunk_ids)

        for i, (cid, item) in enumerate(top_items):
            meta = item["meta"]
            source = meta.get("source", "")
            folder = meta.get("folder", "")
            doc_type = meta.get("doc_type", "")
            category = meta.get("category", "")
            extra = " | ".join(filter(None, [folder, doc_type, f"[{category}]" if category else ""]))
            tag = "SEM" if item["source_type"] == "semantic" else "ENT"
            score_str = f"{item['score']:.2f}" if item["source_type"] == "semantic" else "entity"

            facts_block = format_facts_block(chunk_facts.get(cid, []))
            output.append(
                f"--- Result {i+1} [{tag} {score_str}] ---\n"
                f"Titolo: {meta.get('title', 'N/A')}\n"
                f"Data: {meta.get('date', 'N/A')} | Fonte: {source}"
                + (f" | {extra}" if extra else "") + "\n"
                f"---\n{item['doc'][:2000]}{facts_block}\n"
            )

        # Append derived facts (inferences) if relevant
        derived_block = get_derived_facts_for_query(query)
        if derived_block:
            output.append(derived_block)

        return "\n".join(output)

    except Exception as e:
        return f"kb_hybrid_search error: {str(e)}"


@mcp.tool()
def kb_search_by_topic(query: str, title_contains: str, n_results: int = 5, include_superseded: bool = False) -> str:
    """Search the KB filtering by conversation title.

    Args:
        query: The question or topic to search
        title_contains: Testo che deve essere contenuto nel titolo (es. "StayChic", "capelli", "MVP")
        n_results: Number of results (default 5, max 20)
        include_superseded: If True, include superseded chunks (default False)
    """
    n_results = min(max(n_results, 1), 20)
    try:
        embedding = get_embedding(query)
        collection = get_collection()
        fetch_n = n_results * 2 if not include_superseded else n_results
        results = collection.query(
            query_embeddings=[embedding], n_results=fetch_n,
            where={"title": {"$contains": title_contains}}
        )
        if not results or not results["documents"] or not results["documents"][0]:
            return f"No results found with title containing '{title_contains}'."
        if not include_superseded:
            f = filter_superseded(
                results["ids"][0], results["documents"][0],
                results["metadatas"][0], results["distances"][0], n_results
            )
        else:
            f = {"ids": results["ids"][0][:n_results], "documents": results["documents"][0][:n_results],
                 "metadatas": results["metadatas"][0][:n_results], "distances": results["distances"][0][:n_results]}
        # Lookup fatti atomici
        chunk_facts = get_facts_for_chunks(f["ids"])

        output = []
        for i, (cid, doc, meta, distance) in enumerate(zip(
            f["ids"], f["documents"], f["metadatas"], f["distances"]
        )):
            relevance = max(0, 1 - distance)
            facts_block = format_facts_block(chunk_facts.get(cid, []))
            output.append(
                f"--- Result {i+1} (relevance: {relevance:.2f}) ---\n"
                f"Conversazione: {meta.get('title', 'N/A')}\n"
                f"Data: {meta.get('date', 'N/A')}\n"
                f"---\n{doc[:2000]}{facts_block}\n"
            )
        return "\n".join(output)
    except Exception as e:
        return f"Search error: {str(e)}"


@mcp.tool()
def kb_search_docs(query: str, folder: str = "", doc_type: str = "", n_results: int = 5, include_superseded: bool = False) -> str:
    """Search only in documents OneDrive (source=document) in the KB.

    Args:
        query: What to search for (e.g. "annual report 2023", "bank statement")
        folder: OneDrive folder (e.g. "Finance", "Health", "Investments"). Empty = all.
        doc_type: Document type (e.g. "balance_sheet", "contract", "bank_statement"). Empty = all.
        n_results: Number of results (default 5, max 20)
        include_superseded: If True, include superseded chunks (default False)
    """
    n_results = min(max(n_results, 1), 20)
    try:
        embedding = get_embedding(query)
        collection = get_collection()
        conditions = [{"source": "document"}]
        if folder:
            conditions.append({"folder": folder})
        if doc_type:
            conditions.append({"doc_type": doc_type})
        where = conditions[0] if len(conditions) == 1 else {"$and": conditions}
        fetch_n = n_results * 2 if not include_superseded else n_results
        results = collection.query(query_embeddings=[embedding], n_results=fetch_n, where=where)
        if not results or not results["documents"] or not results["documents"][0]:
            msg = "No results found in documents"
            if folder or doc_type:
                msg += f" (folder={folder!r}, doc_type={doc_type!r})"
            return msg + "."
        if not include_superseded:
            f = filter_superseded(
                results["ids"][0], results["documents"][0],
                results["metadatas"][0], results["distances"][0], n_results
            )
        else:
            f = {"ids": results["ids"][0][:n_results], "documents": results["documents"][0][:n_results],
                 "metadatas": results["metadatas"][0][:n_results], "distances": results["distances"][0][:n_results]}
        # Lookup fatti atomici
        chunk_facts = get_facts_for_chunks(f["ids"])

        output = []
        for i, (cid, doc, meta, distance) in enumerate(zip(
            f["ids"], f["documents"], f["metadatas"], f["distances"]
        )):
            relevance = max(0, 1 - distance)
            facts_block = format_facts_block(chunk_facts.get(cid, []))
            output.append(
                f"--- Result {i+1} (relevance: {relevance:.2f}) ---\n"
                f"File: {meta.get('title', 'N/A')}\n"
                f"Cartella: {meta.get('folder', 'N/A')} | Tipo: {meta.get('doc_type', 'N/A')}\n"
                f"Data: {meta.get('date', 'N/A')}\n"
                f"---\n{doc[:2000]}{facts_block}\n"
            )
        return "\n".join(output)
    except Exception as e:
        return f"kb_search_docs error: {str(e)}"


@mcp.tool()
def kb_search_by_entity(entity: str, entity_type: str = "", n_results: int = 10, include_superseded: bool = False) -> str:
    """Search the KB for all chunks mentioning a specific entity\u00e0.

    Args:
        entity: Entity name to search (e.g. "Acme Corp", "John", "Berlin")
        entity_type: Entity type: "person", "company", "project", "amount", "location", "date_ref". Empty = all.
        n_results: Number of results (default 10, max 50)
        include_superseded: If True, include superseded chunks (default False)
    """
    import json as _json
    import sqlite3 as _sqlite3
    n_results = min(max(n_results, 1), 50)
    ENTITY_DB_PATH = os.path.join(DB_DIR, "entity_index.sqlite3")
    try:
        if not os.path.exists(ENTITY_DB_PATH):
            return "Error: entity index not found. Run: python3 scripts/enrichment/build_entity_index.py"
        idx_conn = _sqlite3.connect(f"file:{ENTITY_DB_PATH}?mode=ro", uri=True)
        idx_cursor = idx_conn.cursor()
        like_pattern = f"%{entity}%"
        if entity_type:
            idx_cursor.execute(
                "SELECT DISTINCT chunk_id FROM entity_index WHERE entity LIKE ? COLLATE NOCASE AND entity_type = ?",
                (like_pattern, entity_type)
            )
        else:
            idx_cursor.execute(
                "SELECT DISTINCT chunk_id FROM entity_index WHERE entity LIKE ? COLLATE NOCASE",
                (like_pattern,)
            )
        matched_chunk_ids = [row[0] for row in idx_cursor.fetchall()]
        idx_conn.close()
        if not matched_chunk_ids:
            msg = f"No chunks contain the entity\u00e0 '{entity}'"
            if entity_type:
                msg += f" di tipo '{entity_type}'"
            msg += ".\nSuggestions:\n  - Try name variants\n  - Use kb_search for semantic search"
            return msg
        total_found = len(matched_chunk_ids)
        collection = get_collection()
        all_results = {"ids": [], "documents": [], "metadatas": []}
        FETCH_BATCH = 40
        for i in range(0, len(matched_chunk_ids), FETCH_BATCH):
            batch_ids = matched_chunk_ids[i:i + FETCH_BATCH]
            try:
                batch_result = collection.get(ids=batch_ids, include=["documents", "metadatas"])
                all_results["ids"].extend(batch_result["ids"])
                all_results["documents"].extend(batch_result["documents"])
                all_results["metadatas"].extend(batch_result["metadatas"])
            except Exception:
                for single_id in batch_ids:
                    try:
                        r = collection.get(ids=[single_id], include=["documents", "metadatas"])
                        all_results["ids"].extend(r["ids"])
                        all_results["documents"].extend(r["documents"])
                        all_results["metadatas"].extend(r["metadatas"])
                    except Exception:
                        pass
        items = []
        for cid, doc, meta in zip(all_results["ids"], all_results["documents"], all_results["metadatas"]):
            try:
                entities_list = _json.loads(meta.get("entities", "[]"))
                types_list = _json.loads(meta.get("entity_types", "[]"))
            except Exception:
                entities_list, types_list = [], []
            items.append({
                "chunk_id": cid, "doc": doc, "meta": meta,
                "date": meta.get("date", ""), "source": meta.get("source", ""),
                "title": meta.get("title", ""), "entities_list": entities_list, "types_list": types_list,
            })
        # Filter superseded chunks
        if not include_superseded:
            items = [item for item in items if not is_superseded(item["meta"])]
        items.sort(key=lambda x: x["date"], reverse=True)
        top = items[:n_results]
        output = [
            f"Entit\u00e0 '{entity}'" + (f" [{entity_type}]" if entity_type else "") +
            f": found {total_found} chunks total, showing the {len(top)} most recent\u00f9 recenti\n"
        ]
        # Lookup fatti atomici
        top_chunk_ids = [item["chunk_id"] for item in top]
        chunk_facts = get_facts_for_chunks(top_chunk_ids)

        for i, item in enumerate(top):
            meta = item["meta"]
            folder = meta.get("folder", "")
            doc_type = meta.get("doc_type", "")
            extra = " | ".join(filter(None, [folder, doc_type]))
            entities_preview = ", ".join(
                f"{e} [{t}]" for e, t in zip(item["entities_list"][:5], item["types_list"][:5])
            )
            facts_block = format_facts_block(chunk_facts.get(item["chunk_id"], []))
            output.append(
                f"--- Result {i+1} ---\n"
                f"Titolo: {item['title']}\n"
                f"Data: {item['date']} | Fonte: {item['source']}"
                + (f" | {extra}" if extra else "") + "\n"
                + (f"Entit\u00e0 nel chunk: {entities_preview}\n" if entities_preview else "")
                + f"---\n{item['doc'][:2000]}{facts_block}\n"
            )
        return "\n".join(output)
    except Exception as e:
        return f"kb_search_by_entity error: {str(e)}"


@mcp.tool()
def kb_list_conversations(limit: int = 50) -> str:
    """List conversations stored in the Knowledge Base.

    Args:
        limit: Maximum number of conversations to show (default 50)
    """
    try:
        collection = get_collection()
        count = collection.count()
        BATCH = 500
        convs = {}
        offset = 0
        while offset < count:
            batch = collection.get(include=["metadatas"], limit=BATCH, offset=offset)
            if not batch["metadatas"]:
                break
            for meta in batch["metadatas"]:
                conv_id = meta.get("conv_id", "")
                if conv_id not in convs:
                    convs[conv_id] = {
                        "title": meta.get("title", "N/A"), "date": meta.get("date", "N/A"),
                        "model": meta.get("default_model", "N/A"),
                        "source": meta.get("source", "chatgpt"), "chunks": 0, "total_chars": 0
                    }
                convs[conv_id]["chunks"] += 1
                convs[conv_id]["total_chars"] += meta.get("char_count", 0)
            offset += len(batch["metadatas"])
        sorted_convs = sorted(convs.values(), key=lambda x: x["date"], reverse=True)[:limit]
        output = [f"Conversations in KB: {len(convs)} total\n"]
        for c in sorted_convs:
            source_tag = f" [{c['source']}]" if c['source'] != 'chatgpt' else ""
            output.append(
                f"  [{c['date']}] {c['title']} "
                f"({c['chunks']} chunk, {c['total_chars']:,} chars){source_tag}"
            )
        return "\n".join(output)
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool()
def kb_query_advanced(
    source: str = "", date_after: str = "", date_before: str = "",
    order_by: str = "date", sort_order: str = "desc", limit: int = 10
) -> str:
    """Advanced KB query with source, date, and sort filters.

    Args:
        source: Filter by source: "email", "document", "claude", "chatgpt" (empty = all)
        date_after: Data minima (formato: "2026-03-24")
        date_before: Data massima (formato: "2026-03-30")
        order_by: "date" (default) o "relevance" (non implementato)
        sort_order: "desc" (default) o "asc"
        limit: Max results (default 10, max 50)
    """
    import sqlite3 as _sqlite3
    limit = min(max(limit, 1), 50)
    try:
        db_path = os.path.join(DB_DIR, "chroma.sqlite3")
        if not os.path.exists(db_path):
            return "Error: ChromaDB database not found."
        conn = _sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        cursor = conn.cursor()
        where_conditions, params = [], []
        # Exclude superseded chunks by default
        where_conditions.append("e.id NOT IN (SELECT id FROM embedding_metadata WHERE key = 'superseded_by' AND string_value != '')")
        if source:
            where_conditions.append("e.id IN (SELECT id FROM embedding_metadata WHERE key = 'source' AND string_value = ?)")
            params.append(source)
        if date_after:
            where_conditions.append("e.id IN (SELECT id FROM embedding_metadata WHERE key = 'date' AND string_value >= ?)")
            params.append(date_after.replace("/", "-"))
        if date_before:
            where_conditions.append("e.id IN (SELECT id FROM embedding_metadata WHERE key = 'date' AND string_value <= ?)")
            params.append(date_before.replace("/", "-"))
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        order_dir = "DESC" if sort_order.lower() == "desc" else "ASC"
        query = f"""
            SELECT e.embedding_id, fts.c0,
                date_meta.string_value AS date_val, source_meta.string_value AS source_val,
                title_meta.string_value AS title_val, folder_meta.string_value AS folder_val,
                doctype_meta.string_value AS doctype_val
            FROM embeddings e
            LEFT JOIN embedding_fulltext_search_content fts ON fts.id = e.id
            LEFT JOIN embedding_metadata date_meta ON date_meta.id = e.id AND date_meta.key = 'date'
            LEFT JOIN embedding_metadata source_meta ON source_meta.id = e.id AND source_meta.key = 'source'
            LEFT JOIN embedding_metadata title_meta ON title_meta.id = e.id AND title_meta.key = 'title'
            LEFT JOIN embedding_metadata folder_meta ON folder_meta.id = e.id AND folder_meta.key = 'folder'
            LEFT JOIN embedding_metadata doctype_meta ON doctype_meta.id = e.id AND doctype_meta.key = 'doc_type'
            WHERE {where_clause}
            ORDER BY date_meta.string_value {order_dir}
            LIMIT ?
        """
        params.append(limit)
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        if not rows:
            msg = "No results"
            if source or date_after or date_before:
                filters = []
                if source: filters.append(f"source={source!r}")
                if date_after: filters.append(f"date>={date_after!r}")
                if date_before: filters.append(f"date<={date_before!r}")
                msg += f" con filtri: {', '.join(filters)}"
            return msg + "."
        output = [f"Found {len(rows)} results:\n"]
        for i, (chunk_id, text, date_val, source_val, title_val, folder_val, doctype_val) in enumerate(rows):
            extra = " | ".join(filter(None, [folder_val or "", doctype_val or ""]))
            output.append(
                f"--- Result {i+1} ---\n"
                f"ID: {chunk_id}\nFonte: {source_val or 'N/A'}" + (f" | {extra}" if extra else "") + "\n"
                f"Titolo: {title_val or 'N/A'}\nData: {date_val or 'N/A'}\n"
                f"---\n{(text or '')[:1000]}\n"
            )
        return "\n".join(output)
    except Exception as e:
        return f"kb_query_advanced error: {str(e)}"


@mcp.tool()
def kb_stats() -> str:
    """General Knowledge Base statistics."""
    try:
        collection = get_collection()
        count = collection.count()
        BATCH = 500
        convs = set()
        total_chars = 0
        models, sources, dates = {}, {}, []
        offset = 0
        while offset < count:
            batch = collection.get(include=["metadatas"], limit=BATCH, offset=offset)
            if not batch["metadatas"]:
                break
            for meta in batch["metadatas"]:
                convs.add(meta.get("conv_id", ""))
                total_chars += meta.get("char_count", 0)
                model = meta.get("default_model", "unknown")
                models[model] = models.get(model, 0) + 1
                source = meta.get("source", "chatgpt")
                sources[source] = sources.get(source, 0) + 1
                date = meta.get("date", "")
                if date:
                    dates.append(date)
            offset += len(batch["metadatas"])
        dates.sort()
        output = [
            "=== TAILOR KB Stats ===",
            f"Total chunks: {count:,}", f"Conversations: {len(convs)}",
            f"Total chars: {total_chars:,}", f"Estimated tokens: {total_chars // 4:,}",
            f"Periodo: {dates[0] if dates else 'N/A'} -> {dates[-1] if dates else 'N/A'}",
            f"\nFonti:",
        ]
        for source, cnt in sorted(sources.items(), key=lambda x: -x[1]):
            output.append(f"  {source}: {cnt:,} chunk")
        output.append(f"\nModelli (top 5):")
        for model, cnt in sorted(models.items(), key=lambda x: -x[1])[:5]:
            output.append(f"  {model}: {cnt:,} chunk")
        return "\n".join(output)
    except Exception as e:
        return f"Error: {str(e)}"


# ============================================================
# TOOLS COMPATIBILIT\u00c0 CHATGPT (search/fetch)
# ============================================================

@mcp.tool(annotations={"readOnlyHint": True})
def search(query: str, n_results: int = 10) -> dict:
    """Search the Knowledge Base by semantic query.

    Args:
        query: What to search for (e.g. "crypto investment decisions")
        n_results: Number of results to return (default 10, max 20)

    Returns:
        A dict with a "results" list containing id, title, url, snippet
    """
    n_results = min(max(n_results, 1), 20)
    try:
        embedding = get_embedding(query)
        collection = get_collection()
        results = collection.query(query_embeddings=[embedding], n_results=n_results * 2)
        if not results or not results["ids"] or not results["ids"][0]:
            return {"results": []}
        out = []
        for chunk_id, doc, meta, distance in zip(
            results["ids"][0], results["documents"][0], results["metadatas"][0], results["distances"][0]
        ):
            if is_superseded(meta):
                continue
            relevance = max(0, 1 - distance)
            source = meta.get("source", "chatgpt")
            folder, doc_type, category = meta.get("folder", ""), meta.get("doc_type", ""), meta.get("category", "")
            title = meta.get("title", "N/A")
            extra_parts = [p for p in [folder, doc_type, category] if p]
            if extra_parts:
                title = f"{title} ({', '.join(extra_parts)})"
            snippet = (doc[:500] + "...") if len(doc) > 500 else doc
            # Lookup facts for this chunk
            c_facts = get_facts_for_chunks([chunk_id]).get(chunk_id, [])
            facts_summary = [{"fact": ff["fact"], "category": ff["category"]} for ff in c_facts[:10]]
            entry = {
                "id": chunk_id, "title": title, "date": meta.get("date", ""),
                "source": source, "relevance": round(relevance, 3),
                "url": f"tailor://chunk/{chunk_id}", "snippet": snippet
            }
            if facts_summary:
                entry["facts"] = facts_summary
            out.append(entry)
            if len(out) >= n_results:
                break
        return {"results": out}
    except Exception as e:
        return {"results": [], "error": str(e)}


@mcp.tool(annotations={"readOnlyHint": True})
def fetch(id: str) -> dict:
    """Fetch the full content of a Knowledge Base chunk by its ID.

    Args:
        id: The chunk ID returned by the search tool
    """
    try:
        collection = get_collection()
        result = collection.get(ids=[id], include=["documents", "metadatas"])
        if not result or not result["ids"]:
            return {"error": f"Chunk '{id}' not found"}
        doc = result["documents"][0]
        meta = result["metadatas"][0]
        # Lookup atomic facts for this chunk
        c_facts = get_facts_for_chunks([id]).get(id, [])
        facts_list = [{"fact": ff["fact"], "category": ff["category"], "entity_tags": ff["entity_tags"], "event_date": ff["event_date"]} for ff in c_facts]
        result_dict = {
            "id": id, "url": f"tailor://chunk/{id}",
            "title": meta.get("title", "N/A"), "date": meta.get("date", ""),
            "source": meta.get("source", ""), "folder": meta.get("folder", ""),
            "doc_type": meta.get("doc_type", ""), "category": meta.get("category", ""),
            "content": doc
        }
        if facts_list:
            result_dict["facts"] = facts_list
        return result_dict
    except Exception as e:
        return {"error": str(e)}


# ============================================================
# ENTITY EXTRACTION INLINE
# ============================================================

ENTITY_TYPES = ["person", "company", "project", "amount", "location", "date_ref"]

EXTRACTION_PROMPT_INLINE = """Extract named entities from the following text.
Reply ONLY with a valid JSON object, nothing else.

Types: person, company, project, amount, location, date_ref
Rules: only entities clearly present in the text, max 20, no file/URL/path.
If no entities: {"entities": [], "types": []}
Format: {"entities": ["name1", "name2"], "types": ["type1", "type2"]}

Text:
"""

ENTITY_INDEX_PATH = os.path.join(DB_DIR, "entity_index.sqlite3")


def _extract_entities_inline(text: str) -> tuple[list, list]:
    """Estrae entit\u00e0 via GPT-4o-mini. Non blocca se fallisce."""
    if not OPENAI_API_KEY:
        return [], []
    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "gpt-4o-mini",
                "messages": [
                    {"role": "system", "content": "Entity extraction tool. Respond ONLY with valid JSON."},
                    {"role": "user", "content": EXTRACTION_PROMPT_INLINE + text[:1500]}
                ],
                "temperature": 0.0, "max_tokens": 512,
                "response_format": {"type": "json_object"}
            },
            timeout=15
        )
        r.raise_for_status()
        parsed = json.loads(r.json()["choices"][0]["message"]["content"].strip())
        entities = parsed.get("entities", [])
        types = parsed.get("types", [])
        if len(entities) != len(types):
            m = min(len(entities), len(types))
            entities, types = entities[:m], types[:m]
        valid = [(e, t) for e, t in zip(entities, types)
                 if t in ENTITY_TYPES and isinstance(e, str) and e.strip()]
        if not valid:
            return [], []
        ents, typs = zip(*valid)
        return list(ents), list(typs)
    except Exception:
        return [], []


def _update_entity_index(chunk_id: str, entities: list, types: list):
    """Inserisce le entit\u00e0 nell'indice SQLite."""
    import sqlite3
    if not entities or not os.path.exists(ENTITY_INDEX_PATH):
        return
    try:
        conn = sqlite3.connect(ENTITY_INDEX_PATH)
        cursor = conn.cursor()
        for ent, typ in zip(entities, types):
            cursor.execute(
                "INSERT OR IGNORE INTO entity_index (entity, entity_type, chunk_id) VALUES (?, ?, ?)",
                (ent, typ, chunk_id)
            )
        conn.commit()
        conn.close()
    except Exception:
        pass


# ============================================================
# TOOLS DI SCRITTURA
# ============================================================

@mcp.tool()
def kb_add(content: str, title: str, category: str = "general", source: str = "claude") -> str:
    """Save important information to the user's long-term memory (Knowledge Base).

    WHEN TO USE (proactively, without being asked):
    - User mentions a new fact: job change, new project, decision made, preference expressed
    - User shares information that could be useful in future sessions
    - A conversation produces insights, conclusions, or action plans worth preserving
    - User corrects previously stored information

    DO NOT use for: generic questions, small talk, throwaway code requests.

    Categories: decision, preference, fact, project, person, financial, health, legal, business, technical, personal

    Args:
        content: The content to save, clear and contextualized.
        title: Descriptive title
        category: Category (default "general")
        source: "claude", "chatgpt", "manual" (default "claude")
    """
    allowed_sources = ("claude", "chatgpt", "manual")
    if source not in allowed_sources:
        source = "claude"
    try:
        now = datetime.now()
        date_str = now.strftime("%Y-%m-%d")
        timestamp = now.timestamp()
        prefix = "chatgpt" if source == "chatgpt" else "claude"
        chunk_id = f"{prefix}_{now.strftime('%Y%m%d_%H%M%S')}_{abs(hash(content)) % 10000:04d}"
        conv_id = f"{prefix}_session_{now.strftime('%Y%m%d')}"
        embedding = get_embedding(content)
        collection = get_collection()
        meta = {
            "conv_id": conv_id, "title": title, "date": date_str,
            "create_time": timestamp, "default_model": source,
            "chunk_index": 0, "char_count": len(content), "turn_count": 1,
            "source": source, "category": category
        }
        entities, types = _extract_entities_inline(content)
        meta["entities"] = json.dumps(entities, ensure_ascii=False)
        meta["entity_types"] = json.dumps(types, ensure_ascii=False)
        meta["entities_extracted"] = 1
        meta["entities_count"] = len(entities)
        collection.add(ids=[chunk_id], embeddings=[embedding], documents=[content[:4000]], metadatas=[meta])
        if entities:
            _update_entity_index(chunk_id, entities, types)
        entity_info = f", {len(entities)} entit\u00e0" if entities else ""
        return f"Saved to KB: '{title}' [{category}] (source={source}, {len(content)} chars{entity_info}, {date_str})"
    except Exception as e:
        return f"Error saving: {str(e)}"


@mcp.tool()
def kb_update_session(summary: str, key_facts: str, decisions: str = "", action_items: str = "", source: str = "claude") -> str:
    """Save a structured summary of the current session to long-term memory.

    WHEN TO USE (proactively, at the end of every significant session):
    - The conversation produced decisions, plans, or new facts
    - A project was discussed with concrete progress
    - The user is wrapping up (goodbye, "thanks", "talk later")
    - The session is long and worth preserving context for

    Call this tool BEFORE responding to the user's closing message.

    Args:
        summary: Summary in 2-5 sentences
        key_facts: Key facts, one per line
        decisions: Decisions made (optional)
        action_items: Next steps (optional)
        source: "claude", "chatgpt", "manual" (default "claude")
    """
    allowed_sources = ("claude", "chatgpt", "manual")
    if source not in allowed_sources:
        source = "claude"
    try:
        now = datetime.now()
        date_str = now.strftime("%Y-%m-%d")
        timestamp = now.timestamp()
        source_label = "ChatGPT" if source == "chatgpt" else "Claude"
        doc_parts = [
            f"=== Sessione {source_label} {date_str} ===",
            f"\nRIASSUNTO:\n{summary}", f"\nFATTI CHIAVE:\n{key_facts}",
        ]
        if decisions:
            doc_parts.append(f"\nDECISIONI:\n{decisions}")
        if action_items:
            doc_parts.append(f"\nPROSSIMI STEP:\n{action_items}")
        full_doc = "\n".join(doc_parts)
        prefix = "chatgpt" if source == "chatgpt" else "claude"
        chunk_id = f"{prefix}_session_{now.strftime('%Y%m%d_%H%M%S')}"
        conv_id = f"{prefix}_session_{now.strftime('%Y%m%d')}"
        embedding = get_embedding(full_doc)
        collection = get_collection()
        meta = {
            "conv_id": conv_id, "title": f"Sessione {source_label} {date_str}",
            "date": date_str, "create_time": timestamp, "default_model": source,
            "chunk_index": 0, "char_count": len(full_doc), "turn_count": 1,
            "source": source, "category": "session_summary"
        }
        entities, types = _extract_entities_inline(full_doc)
        meta["entities"] = json.dumps(entities, ensure_ascii=False)
        meta["entity_types"] = json.dumps(types, ensure_ascii=False)
        meta["entities_extracted"] = 1
        meta["entities_count"] = len(entities)
        collection.add(ids=[chunk_id], embeddings=[embedding], documents=[full_doc[:4000]], metadatas=[meta])
        if entities:
            _update_entity_index(chunk_id, entities, types)
        entity_info = f", {len(entities)} entit\u00e0" if entities else ""
        return (
            f"Sessione salvata in KB ({date_str}, source={source}{entity_info}):\n"
            f"  - Riassunto: {len(summary)} chars\n  - Fatti chiave: {len(key_facts)} chars\n"
            f"  - Decisioni: {len(decisions)} chars\n  - Action items: {len(action_items)} chars\n"
            f"  - Totale: {len(full_doc)} chars"
        )
    except Exception as e:
        return f"Error saving session: {str(e)}"


# ============================================================
# TOOLS DI SISTEMA \u2014 FASE "ARM" (v8)
# ============================================================
# Allow the LLM to interact with the host filesystem and shell.
# Sicurezza: human-in-the-loop. Claude propone, l'utente approva, Claude esegue.
# L'autenticazione API key protegge da accessi esterni.

EXEC_TIMEOUT = 60
EXEC_MAX_OUTPUT = 50000


@mcp.tool()
def exec_command(command: str, working_dir: str = "") -> str:
    """Execute a shell command on the host machine.

    IMPORTANT: The LLM should always describe the command to the user
    and await confirmation before executing.

    Args:
        command: Shell command to execute (e.g. "ls -la ~/tailor/scripts/")
        working_dir: Working directory (default: project root)

    Returns:
        Command output (stdout + stderr), exit code, execution time
    """
    if not command or not command.strip():
        return "Error: empty command."

    cwd = working_dir if working_dir else BASE_DIR

    try:
        start = time.time()
        result = subprocess.run(
            command,
            shell=True,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=EXEC_TIMEOUT,
            env={**os.environ, "PATH": f"/opt/homebrew/bin:/usr/local/bin:{os.environ.get('PATH', '')}"}
        )
        elapsed = time.time() - start

        stdout = result.stdout or ""
        stderr = result.stderr or ""
        combined = stdout + ("\n--- STDERR ---\n" + stderr if stderr else "")

        if len(combined) > EXEC_MAX_OUTPUT:
            half = EXEC_MAX_OUTPUT // 2
            combined = combined[:half] + f"\n\n... [{len(combined) - EXEC_MAX_OUTPUT:,} chars troncati] ...\n\n" + combined[-half:]

        return (
            f"Exit code: {result.returncode}\n"
            f"Time: {elapsed:.1f}s\n"
            f"Working dir: {cwd}\n"
            f"---\n{combined}"
        )

    except subprocess.TimeoutExpired:
        return f"Error: command timed out after {EXEC_TIMEOUT}s. Command: {command}"
    except Exception as e:
        return f"exec_command error: {str(e)}"


@mcp.tool()
def read_file(path: str, max_chars: int = 100000, line_start: int = 0, line_end: int = 0) -> str:
    """Read a file from the host filesystem.

    Args:
        path: Absolute or relative path to project root (e.g. "scripts/rebuild_db.py")
        max_chars: Max characters to return (default 100000)
        line_start: Start line, 1-indexed (0 = from beginning)
        line_end: End line, 1-indexed (0 = to end)

    Returns:
        File content with size and line info
    """
    if not os.path.isabs(path):
        path = os.path.join(BASE_DIR, path)

    if not os.path.exists(path):
        return f"Error: file not found: {path}"
    if os.path.isdir(path):
        return f"Error: {path} is a directory. Use exec_command with 'ls'."

    try:
        file_size = os.path.getsize(path)
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            if line_start > 0 or line_end > 0:
                lines = f.readlines()
                total_lines = len(lines)
                start = max(0, line_start - 1) if line_start > 0 else 0
                end = min(total_lines, line_end) if line_end > 0 else total_lines
                selected = lines[start:end]
                content = "".join(selected)
                line_info = f"Righe: {start+1}-{end} di {total_lines}"
            else:
                content = f.read(max_chars + 1)
                total_lines = content.count("\n")
                line_info = f"Righe: ~{total_lines}"

        truncated = len(content) > max_chars
        if truncated:
            content = content[:max_chars]

        return (
            f"File: {path}\n"
            f"Dimensione: {file_size:,} bytes | {line_info}"
            + (" | TRONCATO" if truncated else "") +
            f"\n---\n{content}"
        )
    except Exception as e:
        return f"read_file error: {str(e)}"


@mcp.tool()
def write_file(path: str, content: str, mode: str = "overwrite") -> str:
    """Write or update a file on the host filesystem.

    IMPORTANT: The LLM should always describe what it will write
    and await user confirmation before using this tool.

    Args:
        path: Absolute or relative path to project root (e.g. "scripts/my_script.py")
        content: Content to write
        mode: "overwrite" (default) or "append"

    Returns:
        Confirmation with path and size
    """
    if not os.path.isabs(path):
        path = os.path.join(BASE_DIR, path)

    if mode not in ("overwrite", "append"):
        return f"Error: mode must be 'overwrite' or 'append', got: {mode!r}"

    try:
        parent = os.path.dirname(path)
        if parent and not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)

        existed = os.path.exists(path)
        old_size = os.path.getsize(path) if existed else 0

        write_mode = "a" if mode == "append" else "w"
        with open(path, write_mode, encoding="utf-8") as f:
            f.write(content)

        new_size = os.path.getsize(path)
        action = "Appended to" if mode == "append" else ("Overwritten" if existed else "Created")
        return (
            f"{action}: {path}\n"
            f"Dimensione: {old_size:,} \u2192 {new_size:,} bytes\n"
            f"Contenuto: {len(content):,} chars scritti"
        )
    except Exception as e:
        return f"write_file error: {str(e)}"




# ============================================================
# TOOL: TELEGRAM
# ============================================================

@mcp.tool()
def send_telegram(text: str, parse_mode: str = "Markdown") -> str:
    """Send a Telegram message to the user.

    Use this tool to notify or send information to the user via Telegram.

    Args:
        text: Message text (supports Markdown)
        parse_mode: Text format ("Markdown" or "HTML", default Markdown)

    Returns:
        Send confirmation or error
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return "Error: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not configured."

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": parse_mode,
    }

    try:
        resp = requests.post(url, json=payload, timeout=10)
        data = resp.json()
        if data.get("ok"):
            return f"Message sent (message_id: {data['result']['message_id']})"
        else:
            return f"Telegram API error: {data.get('description', 'unknown')}"
    except Exception as e:
        return f"Telegram send error: {str(e)}"



# ============================================================
# TOOL: REMINDERS
# ============================================================

import json as _json
from filelock import FileLock as _FileLock

_REMINDERS_FILE = os.path.join(BASE_DIR, "db", "reminders.json")
_LOCK_FILE = _REMINDERS_FILE + ".lock"

def _load_reminders() -> list:
    lock = _FileLock(_LOCK_FILE, timeout=5)
    with lock:
        if not os.path.exists(_REMINDERS_FILE):
            return []
        with open(_REMINDERS_FILE, "r") as f:
            return _json.load(f)

def _save_reminders(rem: list) -> None:
    lock = _FileLock(_LOCK_FILE, timeout=5)
    with lock:
        with open(_REMINDERS_FILE, "w") as f:
            _json.dump(rem, f, indent=2, ensure_ascii=False)


@mcp.tool()
def create_reminder(
    text: str,
    remind_at: str = "",
    recurring: bool = False,
    days: str = "",
    time: str = "",
    label: str = ""
) -> str:
    """Create a reminder for the user.

    Due modalita':
    1. One-shot: specifica remind_at (ISO 8601, es. "2026-04-05T10:00:00")
    2. Ricorrente: specifica recurring=True, days (es. "0,1,2,3,4" dove 0=lun), time (es. "09:00")

    Args:
        text: Testo del reminder
        remind_at: Data/ora per reminder one-shot (ISO 8601)
        recurring: True per reminder ricorrente
        days: Days of the week, comma-separated (0=Mon, 6=Sun). Only for recurring.
        time: Time HH:MM. Only for recurring.
        label: Etichetta breve per ricorrenti (opzionale)

    Returns:
        Confirmation with reminder details
    """
    from datetime import datetime as dt
    rem_list = _load_reminders()
    rid = int(dt.now().timestamp() * 1000)

    if recurring:
        if not days or not time:
            return "Error: recurring reminders require days and time."
        day_list = [int(d.strip()) for d in days.split(",")]
        day_names = ["LUN","MAR","MER","GIO","VEN","SAB","DOM"]
        r = {
            "id": rid, "type": "recurring", "text": text,
            "label": label or text[:40],
            "action": "send_text", "days": day_list, "time": time,
            "message": text, "created_at": dt.now().isoformat()
        }
        rem_list.append(r)
        _save_reminders(rem_list)
        day_str = ", ".join(day_names[d] for d in day_list)
        return f"Recurring reminder created (id: {rid})\nText: {text}\nDays: {day_str}\nTime: {time}"
    else:
        if not remind_at:
            return "Error: one-shot reminders require remind_at (ISO 8601)."
        r = {
            "id": rid, "type": "once", "text": text,
            "remind_at": remind_at, "created_at": dt.now().isoformat(),
            "sent": False
        }
        rem_list.append(r)
        _save_reminders(rem_list)
        return f"Reminder created (id: {rid})\nText: {text}\nWhen: {remind_at}"


@mcp.tool()
def list_reminders(include_sent: bool = False) -> str:
    """List the user's active reminders.

    Args:
        include_sent: If True, also include already-sent one-shot reminders

    Returns:
        Lista formattata dei reminder
    """
    from datetime import datetime as dt
    rem_list = _load_reminders()
    day_names = ["LUN","MAR","MER","GIO","VEN","SAB","DOM"]

    lines = []
    for r in rem_list:
        rtype = r.get("type", "once")

        if rtype == "once":
            if r.get("sent") and not include_sent:
                continue
            status = "✅ sent" if r.get("sent") else "⏳ pending"
            lines.append(f"[once] id:{r['id']} | {r['text']} | {r.get('remind_at','')} | {status}")

        elif rtype == "recurring":
            day_str = ", ".join(day_names[d] for d in r.get("days", []))
            label = r.get("label", r.get("text", "")[:40])
            action = r.get("action", "send_text")
            last = r.get("last_sent", "mai")
            lines.append(f"[recurring] id:{r['id']} | {label} | {day_str} {r.get('time','')} | action:{action} | ultimo invio: {last}")

    if not lines:
        return "No active reminders."
    return f"Reminders ({len(lines)} total):\n\n" + "\n".join(lines)


@mcp.tool()
def delete_reminder(reminder_id: int) -> str:
    """Delete a reminder by ID.

    Args:
        reminder_id: ID del reminder da cancellare (ottenuto da list_reminders)

    Returns:
        Deletion confirmation
    """
    rem_list = _load_reminders()
    original_len = len(rem_list)
    rem_list = [r for r in rem_list if r.get("id") != reminder_id]
    if len(rem_list) == original_len:
        return f"Reminder id:{reminder_id} not found."
    _save_reminders(rem_list)
    return f"Reminder id:{reminder_id} deleted."


# ============================================================

# ============================================================
# USER PROFILE
# ============================================================

PROFILE_PATH = os.path.join(DB_DIR, "user_profile.json")
OVERRIDES_PATH = os.path.join(DB_DIR, "user_profile_overrides.json")


@mcp.tool()
def get_user_profile() -> str:
    """Get the user's full profile (static + dynamic).

    Use this tool at the start of a session to get context about the user.
    l'utente, i suoi progetti attivi, decisioni recenti e preferenze.
    The profile is auto-generated from the KB + manual overrides.

    Returns:
        JSON profile with static sections (identity, family, companies, people)
        e dynamic (focus attuale, progetti, decisioni recenti, upcoming)
    """
    try:
        if not os.path.exists(PROFILE_PATH):
            return "Profile not yet generated. Run: python3 scripts/enrichment/generate_user_profile.py"
        with open(PROFILE_PATH, "r") as f:
            profile = json.load(f)

        # Format readable output
        lines = []
        static = profile.get("static", {})
        dynamic = profile.get("dynamic", {})

        lines.append("=== USER PROFILE ===")
        lines.append(f"Generated: {profile.get('generated_at', 'N/A')[:10]}")
        lines.append(f"Override manuali: {'SI' if profile.get('has_overrides') else 'NO'}")

        lines.append(f"\nIdentity: {static.get('identity', 'N/A')}")

        if static.get("family"):
            lines.append("\nFamiglia:")
            for f_item in static["family"]:
                lines.append(f"  - {f_item}")

        if static.get("companies"):
            lines.append("\nAziende:")
            for c in static["companies"]:
                lines.append(f"  - {c}")

        if static.get("key_people"):
            lines.append("\nPersone chiave (attive):")
            for p in static["key_people"]:
                lines.append(f"  - {p}")

        if static.get("former_key_people"):
            lines.append("\nFormer collaborators (no longer active):")
            for p in static["former_key_people"]:
                lines.append(f"  - {p}")

        if static.get("locations"):
            lines.append("\nLuoghi:")
            for loc in static["locations"]:
                lines.append(f"  - {loc}")

        if static.get("health"):
            lines.append(f"\nSalute: {static['health']}")

        if static.get("tech_setup"):
            lines.append(f"\nTech: {static['tech_setup']}")

        if static.get("preferences"):
            lines.append("\nPreferenze:")
            for pref in static["preferences"]:
                lines.append(f"  - {pref}")

        lines.append(f"\n=== CONTESTO ATTUALE (ultimi {profile.get('data_window_days', 30)} giorni) ===")
        lines.append(f"Focus: {dynamic.get('current_focus', 'N/A')}")

        if dynamic.get("active_projects"):
            lines.append("\nProgetti attivi:")
            for proj in dynamic["active_projects"]:
                lines.append(f"  - {proj}")

        if dynamic.get("recent_decisions"):
            lines.append("\nDecisioni recenti:")
            for dec in dynamic["recent_decisions"]:
                lines.append(f"  - {dec}")

        if dynamic.get("upcoming"):
            lines.append("\nIn arrivo:")
            for up in dynamic["upcoming"]:
                lines.append(f"  - {up}")

        return "\n".join(lines)

    except Exception as e:
        return f"get_user_profile error: {str(e)}"


@mcp.tool()
def update_profile(fact: str, action: str = "add") -> str:
    """Update the user's persistent profile with a new fact or correction.

    WHEN TO USE (proactively):
    - User says something that changes who they are: new role, relocation, new relationship, project change
    - User corrects personal data ("I no longer work at X", "I moved to Y")
    - An important fact about the user's life/work emerges in conversation

    Args:
        fact: The fact to update (e.g. "No longer works at X", "New project: Y", "Moved to Z")
        action: "add" (add/update), "remove" (remove a fact)
    """
    try:
        # Load existing overrides
        overrides = {}
        if os.path.exists(OVERRIDES_PATH):
            with open(OVERRIDES_PATH, "r") as f:
                overrides = json.load(f)

        if "static_overrides" not in overrides:
            overrides["static_overrides"] = {}
        if "manual_facts" not in overrides:
            overrides["manual_facts"] = []

        # Add the fact with timestamp
        from datetime import datetime
        entry = {
            "fact": fact,
            "action": action,
            "added_at": datetime.now().isoformat(),
        }
        overrides["manual_facts"].append(entry)
        overrides["_updated"] = datetime.now().strftime("%Y-%m-%d")

        # Salva
        with open(OVERRIDES_PATH, "w") as f:
            json.dump(overrides, f, indent=2, ensure_ascii=False)

        # Regenerate profile if possible
        regen_msg = ""
        try:
            api_key = os.environ.get("ANTHROPIC_API_KEY", "")
            if api_key:
                result = subprocess.run(
                    [os.path.join(BASE_DIR, ".venv", "bin", "python3"),
                     os.path.join(BASE_DIR, "scripts", "enrichment", "generate_user_profile.py")],
                    capture_output=True, text=True, timeout=120,
                    env={**os.environ, "ANTHROPIC_API_KEY": api_key}
                )
                if result.returncode == 0:
                    regen_msg = " Profile regenerated automatically."
                else:
                    regen_msg = f" Regeneration failed: {result.stderr[:200]}"
            else:
                regen_msg = " Fact saved. Profile will be regenerated on next script run."
        except Exception as regen_err:
            regen_msg = f" Fact saved. Regeneration failed: {str(regen_err)[:100]}"

        return f"Fact registered: \"{fact}\" (action: {action}).{regen_msg}"

    except Exception as e:
        return f"update_profile error: {str(e)}"


# AVVIO SERVER CON AUTH MIDDLEWARE
# ============================================================

if __name__ == "__main__":
    async def run():
        starlette_app = mcp.streamable_http_app()
        authed_app = BearerAuthMiddleware(starlette_app)
        config = uvicorn.Config(authed_app, host="0.0.0.0", port=SERVER_PORT, log_level="info", timeout_keep_alive=30)
        server = uvicorn.Server(config)
        await server.serve()

    anyio.run(run)