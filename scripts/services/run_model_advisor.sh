#!/bin/bash
# LaunchDaemon wrapper for scripts/services/model_advisor.py.
# Loads /etc/tailor/env (ANTHROPIC/OPENAI/GOOGLE keys, TELEGRAM_*).
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
exec "$TAILOR_DIR/.venv/bin/python3" scripts/services/model_advisor.py
