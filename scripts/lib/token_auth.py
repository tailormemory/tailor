"""
TAILOR — Multi-token authentication with per-tool permissions.

Supports multiple API tokens with different permission levels:
  - full:  all operations (admin)
  - write: read + kb_add, kb_update_session, update_profile, send_telegram, reminders
  - read:  search, fetch, stats, profile, list (no modifications)

Backward-compatible: if no tokens configured, falls back to TAILOR_API_KEY with full access.
"""

import os
import logging

log = logging.getLogger("tailor.token_auth")

# Permission levels (higher = more access)
LEVELS = {"read": 1, "write": 2, "full": 3}

# MCP tool → minimum permission required
TOOL_PERMISSIONS = {
    # Read-only tools
    "search": "read",
    "kb_search": "read",
    "kb_hybrid_search": "read",
    "kb_search_by_entity": "read",
    "kb_search_by_topic": "read",
    "kb_search_docs": "read",
    "kb_query_advanced": "read",
    "kb_list_conversations": "read",
    "kb_stats": "read",
    "get_user_profile": "read",
    "fetch": "read",
    "list_reminders": "read",
    # Write tools
    "kb_add": "write",
    "kb_update_session": "write",
    "update_profile": "write",
    "send_telegram": "write",
    "create_reminder": "write",
    "delete_reminder": "write",
    # Full-access tools
    "exec_command": "full",
    "read_file": "full",
    "write_file": "full",
}

# REST endpoint → minimum permission (path prefix matching)
ENDPOINT_PERMISSIONS = {
    # Public (no auth needed)
    "/api/auth/login": None,
    "/api/auth/check": None,
    "/api/dashboard/setup": None,
    # Read
    "/api/dashboard/stats": "read",
    "/api/dashboard/services": "read",
    "/api/dashboard/profile": "read",
    "/api/dashboard/pipeline": "read",
    "/api/dashboard/bulk": "read",
    "/api/dashboard/model-advisor": "read",
    "/api/dashboard/rate-limit": "read",
    "/api/search": "read",
    "/api/fetch/": "read",
    # Write
    "/api/dashboard/upload": "write",
    "/api/dashboard/model-advisor/install": "write",
    # Full
    "/api/dashboard/config": "full",
    "/api/dashboard/config/save": "full",
}


class TokenAuth:
    """Multi-token authentication manager."""

    def __init__(self, tokens_config: list | None = None, legacy_key: str = ""):
        """
        Args:
            tokens_config: List of {"name": str, "token": str, "permission": str}
            legacy_key: Fallback TAILOR_API_KEY (full access)
        """
        self._tokens: dict[str, dict] = {}  # token_value -> {"name", "permission"}
        self._legacy_key = legacy_key

        if tokens_config:
            for t in tokens_config:
                token_val = t.get("token", "")
                # Resolve env vars like ${TAILOR_TOKEN_CLAUDE}
                if token_val.startswith("${") and token_val.endswith("}"):
                    token_val = os.environ.get(token_val[2:-1], "")
                if not token_val:
                    continue
                name = t.get("name", "unnamed")
                perm = t.get("permission", "read")
                if perm not in LEVELS:
                    log.warning(f"Token '{name}' has invalid permission '{perm}', defaulting to 'read'")
                    perm = "read"
                self._tokens[token_val] = {"name": name, "permission": perm}
                log.info(f"Token registered: '{name}' ({perm})")

        # Always add legacy key as full if not already in tokens
        if legacy_key and legacy_key not in self._tokens:
            self._tokens[legacy_key] = {"name": "default", "permission": "full"}

        log.info(f"TokenAuth initialized: {len(self._tokens)} token(s)")

    def authenticate(self, token: str) -> dict | None:
        """Verify token, return {"name", "permission"} or None if invalid."""
        return self._tokens.get(token)

    def has_permission(self, token: str, required: str) -> bool:
        """Check if token has at least the required permission level."""
        auth = self.authenticate(token)
        if auth is None:
            return False
        token_level = LEVELS.get(auth["permission"], 0)
        required_level = LEVELS.get(required, 999)
        return token_level >= required_level

    def check_tool(self, token: str, tool_name: str) -> tuple[bool, str]:
        """Check if token can use a specific MCP tool.
        Returns (allowed, reason)."""
        auth = self.authenticate(token)
        if auth is None:
            return False, "Invalid token"
        required = TOOL_PERMISSIONS.get(tool_name, "full")  # unknown tools require full
        if not self.has_permission(token, required):
            return False, f"Token '{auth['name']}' has '{auth['permission']}' access, '{required}' required for tool '{tool_name}'"
        return True, ""

    def check_endpoint(self, token: str, path: str) -> tuple[bool, str]:
        """Check if token can access a REST endpoint.
        Returns (allowed, reason)."""
        # Find matching permission
        required = None
        for prefix, perm in ENDPOINT_PERMISSIONS.items():
            if path == prefix or path.startswith(prefix):
                required = perm
                break
        if required is None:
            # Unknown endpoint — require full
            required = "full"

        auth = self.authenticate(token)
        if auth is None:
            return False, "Invalid token"
        if not self.has_permission(token, required):
            return False, f"Token '{auth['name']}' has '{auth['permission']}' access, '{required}' required"
        return True, ""

    def get_token_info(self, token: str) -> dict:
        """Get token info without exposing the token value."""
        auth = self.authenticate(token)
        if auth is None:
            return {}
        return {"name": auth["name"], "permission": auth["permission"]}

    def list_tokens(self) -> list[dict]:
        """List all configured tokens (names and permissions, no values)."""
        return [{"name": v["name"], "permission": v["permission"]} for v in self._tokens.values()]
