#!/bin/bash
# run_model_advisor.sh — wrapper per cron, sourcia secrets prima di
# invocare model_advisor.py. Necessario perché model_advisor.py gira
# da cron line dedicata (0 10 * * *), fuori da sync_and_ingest.sh.
set -e
TAILOR_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -r /etc/tailor/env ]; then
    set -a
    . /etc/tailor/env
    set +a
fi
exec "$TAILOR_DIR/.venv/bin/python3" "$TAILOR_DIR/scripts/services/model_advisor.py" "$@"
