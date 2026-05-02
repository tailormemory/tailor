#!/bin/bash
# LaunchDaemon wrapper per mcp_server.py.
# Carica i secret da /etc/tailor/env (ANTHROPIC_API_KEY, OPENAI_API_KEY, ecc.)
# ed esegue MCP sotto l'uid del LaunchDaemon (UserName=jarvis nel plist).
set -euo pipefail

TAILOR_DIR="/Users/jarvis/tailor"
ENV_FILE="/etc/tailor/env"

if [ ! -r "$ENV_FILE" ]; then
    echo "FATAL: $ENV_FILE not readable by uid $(id -u) — cannot load secrets" >&2
    exit 1
fi

set -a
. "$ENV_FILE"
set +a

cd "$TAILOR_DIR"
exec "$TAILOR_DIR/.venv/bin/python3" mcp_server.py
