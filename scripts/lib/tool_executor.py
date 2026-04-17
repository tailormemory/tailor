"""
TAILOR — Local tool executor.
Calls KB functions directly via Python (no HTTP needed).
Imports the core functions from the MCP server module.
"""

import sys, os, json

# Add project root to path so we can import from mcp_server
_TAILOR_ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
if _TAILOR_ROOT not in sys.path:
    sys.path.insert(0, _TAILOR_ROOT)

# Lazy imports to avoid loading everything at startup
_mcp_module = None

def _get_mcp():
    global _mcp_module
    if _mcp_module is None:
        import mcp_server as m
        _mcp_module = m
    return _mcp_module

# ── Tool registry ─────────────────────────────────────────────

def _call_kb_hybrid_search(args: dict) -> str:
    m = _get_mcp()
    return m.kb_hybrid_search(
        query=args["query"],
        n_results=args.get("n_results", 5),
        source_filter=args.get("source_filter", ""),
        include_superseded=args.get("include_superseded", False),
    )

def _call_kb_search_docs(args: dict) -> str:
    m = _get_mcp()
    return m.kb_search_docs(
        query=args["query"],
        folder=args.get("folder", ""),
        doc_type=args.get("doc_type", ""),
        n_results=args.get("n_results", 5),
    )

def _call_get_user_profile(args: dict) -> str:
    m = _get_mcp()
    return m.get_user_profile()

def _call_kb_add(args: dict) -> str:
    m = _get_mcp()
    return m.kb_add(
        content=args["content"],
        title=args["title"],
        category=args.get("category", "general"),
        source="telegram",
    )


def _call_system_status(args: dict) -> str:
    """Return system operational status: KB stats, pipeline, services, fact coverage."""
    import sqlite3, subprocess, json
    from datetime import datetime
    m = _get_mcp()
    base = os.path.join(os.path.dirname(__file__), "..", "..")
    result = {}

    # KB stats
    try:
        stats_raw = m.kb_stats()
        result["kb_stats"] = stats_raw
    except Exception as e:
        result["kb_stats"] = f"Error: {e}"

    # Fact extraction coverage
    try:
        conn = sqlite3.connect(os.path.join(base, "db", "facts.sqlite3"))
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM facts")
        total_facts = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM facts WHERE superseded_by IS NOT NULL")
        superseded = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT chunk_id) FROM facts")
        chunks_covered = cur.fetchone()[0]
        conn.close()

        conn2 = sqlite3.connect(os.path.join(base, "db", "entity_index.sqlite3"))
        cur2 = conn2.cursor()
        cur2.execute("SELECT COUNT(DISTINCT entity) FROM entity_index")
        entities = cur2.fetchone()[0]
        conn2.close()

        # Get total chunks from ChromaDB
        try:
            total_chunks = m._collection.count() if hasattr(m, "_collection") and m._collection else 0
        except Exception:
            total_chunks = 0

        coverage_pct = round(chunks_covered / total_chunks * 100, 1) if total_chunks > 0 else 0
        remaining = total_chunks - chunks_covered
        nights_est = max(1, round(remaining / 2500)) if remaining > 0 else 0

        result["fact_extraction"] = {
            "total_facts": total_facts,
            "superseded": superseded,
            "chunks_covered": chunks_covered,
            "total_chunks": total_chunks,
            "coverage_percent": coverage_pct,
            "chunks_remaining": remaining,
            "estimated_nights_to_complete": nights_est,
        }
        result["entities"] = entities
    except Exception as e:
        result["fact_extraction"] = f"Error: {e}"

    # Pipeline last run
    try:
        log_path = os.path.join(base, "logs", "sync_and_ingest.log")
        if os.path.exists(log_path):
            with open(log_path) as f:
                lines = f.readlines()
            last_start = ""
            last_end = ""
            for line in reversed(lines):
                if "START sync_and_ingest" in line and not last_start:
                    last_start = line.strip()
                if ("Pipeline completata" in line or "Pipeline notturna" in line) and not last_end:
                    last_end = line.strip()
                if last_start and last_end:
                    break
            result["pipeline"] = {
                "last_start": last_start,
                "last_end": last_end,
                "last_10_lines": "".join(lines[-10:]).strip(),
            }
        else:
            result["pipeline"] = "No log file found"
    except Exception as e:
        result["pipeline"] = f"Error: {e}"

    # Services
    try:
        ps = subprocess.run(["ps", "aux"], capture_output=True, text=True, timeout=5)
        procs = ps.stdout
        services = {
            "MCP Server": "mcp_server" in procs,
            "Telegram Bot": "telegram_bot" in procs,
            "Ollama": "ollama" in procs,
        }
        result["services"] = {k: ("running" if v else "stopped") for k, v in services.items()}
    except Exception as e:
        result["services"] = f"Error: {e}"

    result["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return json.dumps(result, indent=2, ensure_ascii=False)


TOOL_HANDLERS = {
    "kb_hybrid_search": _call_kb_hybrid_search,
    "kb_search_docs": _call_kb_search_docs,
    "get_user_profile": _call_get_user_profile,
    "kb_add": _call_kb_add,
    "system_status": _call_system_status,
}

def execute_tool(name: str, args: dict) -> str:
    """Execute a tool by name. Returns string result."""
    handler = TOOL_HANDLERS.get(name)
    if not handler:
        return f"Unknown tool: {name}. Available: {list(TOOL_HANDLERS.keys())}"
    try:
        result = handler(args)
        # Truncate for context window sanity
        if isinstance(result, str) and len(result) > 6000:
            return result[:6000] + "\n\n[truncated]"
        return result if isinstance(result, str) else json.dumps(result)[:3000]
    except Exception as e:
        return f"Tool error ({name}): {e}"
