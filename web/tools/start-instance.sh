#!/bin/bash
# Start a detached landlord-mapper server for the byte-identity harness.
#
#   start-instance.sh <port> <db-path> [hashseed]
#
# Detached with setsid + </dev/null so it survives the ssh session that started
# it; the live instance on 8099 is never touched by this script.
set -eu
PORT="$1"
DB="$2"
SEED="${3:-0}"
LOG="$HOME/lm-typed-work/server-$PORT.log"
cd "$HOME/landlord-mapper-ui"
if [ "$SEED" = "random" ]; then
    unset PYTHONHASHSEED || true
    setsid env LM_PORT="$PORT" LM_DB="$DB" python3 -u server.py \
        > "$LOG" 2>&1 < /dev/null &
else
    setsid env PYTHONHASHSEED="$SEED" LM_PORT="$PORT" LM_DB="$DB" \
        python3 -u server.py > "$LOG" 2>&1 < /dev/null &
fi
for _ in $(seq 1 60); do
    code=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:$PORT/health" || true)
    if [ "$code" = "200" ]; then
        echo "port $PORT up on $DB (seed=$SEED)"
        exit 0
    fi
    sleep 1
done
echo "port $PORT FAILED to come up; log tail:"
tail -20 "$LOG"
exit 1
