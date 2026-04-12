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

TOOL_HANDLERS = {
    "kb_hybrid_search": _call_kb_hybrid_search,
    "kb_search_docs": _call_kb_search_docs,
    "get_user_profile": _call_get_user_profile,
    "kb_add": _call_kb_add,
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
