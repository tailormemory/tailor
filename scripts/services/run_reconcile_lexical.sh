#!/bin/bash
# LaunchDaemon wrapper for scripts/maintenance/reconcile_lexical_index.py.
# Loads /etc/tailor/env (TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID) for notify on abort/churn.
set -euo pipefail

TAILOR_DIR="${TAILOR_HOME:-$HOME/tailor}"
ENV_FILE="/etc/tailor/env"

if [ ! -r "$ENV_FILE" ]; then
    echo "FATAL: $ENV_FILE not readable by uid $(id -u) — cannot load secrets" >&2
    exit 1
fi

set -a
. "$ENV_FILE"
set +a

cd "$TAILOR_DIR"
exec "$TAILOR_DIR/.venv/bin/python3" scripts/maintenance/reconcile_lexical_index.py
