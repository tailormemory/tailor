#!/bin/bash
# LaunchDaemon wrapper per telegram_bot.py.
# Carica i secret da /etc/tailor/env (TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, ecc.)
# ed esegue il bot sotto l'uid del LaunchDaemon (UserName=jarvis nel plist).
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
exec "$TAILOR_DIR/.venv/bin/python3" scripts/services/telegram_bot.py
