#!/bin/bash
# LaunchDaemon wrapper for sync_and_ingest.sh (nightly pipeline).
# Loads /etc/tailor/env (ANTHROPIC/OPENAI/GOOGLE keys, TELEGRAM_*, TAILOR_HOME).
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
exec "$TAILOR_DIR/sync_and_ingest.sh"
