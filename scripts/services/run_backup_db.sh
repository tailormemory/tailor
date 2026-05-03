#!/bin/bash
# LaunchDaemon wrapper for scripts/maintenance/backup_db.sh.
# Loads /etc/tailor/env (TAILOR_HOME and any future secrets) before exec.
set -euo pipefail

TAILOR_DIR="/Users/jarvis/tailor"
ENV_FILE="/etc/tailor/env"

if [ ! -r "$ENV_FILE" ]; then
    echo "FATAL: $ENV_FILE not readable by uid $(id -u) — cannot load env" >&2
    exit 1
fi

set -a
. "$ENV_FILE"
set +a

cd "$TAILOR_DIR"
exec "$TAILOR_DIR/scripts/maintenance/backup_db.sh"
