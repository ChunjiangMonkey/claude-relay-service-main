#!/bin/sh
set -eu

APP_PID=""
COLLECTOR_PID=""

shutdown() {
  echo "[zeabur-start] shutting down"

  if [ -n "$APP_PID" ]; then
    kill "$APP_PID" 2>/dev/null || true
  fi

  if [ -n "$COLLECTOR_PID" ]; then
    kill "$COLLECTOR_PID" 2>/dev/null || true
  fi
}

trap shutdown INT TERM

export NODE_OPTIONS="--require /app/extensions/upstream-capture/bootstrap.js"
sh /app/extensions/upstream-capture/setup-capture-links.sh || true

if [ "${COLLECTOR_ENABLED:-true}" = "true" ]; then
  (
    cd /app/extensions/anthropic-capture/collector
    test -d node_modules || npm install --omit=dev --no-audit --no-fund

    set +e
    CHILD_PID=""

    stop_child() {
      if [ -n "$CHILD_PID" ]; then
        kill "$CHILD_PID" 2>/dev/null || true
      fi
      exit 0
    }

    trap stop_child INT TERM

    while true; do
      node src/index.js &
      CHILD_PID=$!
      wait "$CHILD_PID"
      STATUS=$?
      CHILD_PID=""
      echo "[collector] exited with status $STATUS, restart in 2s"
      sleep 2
    done
  ) &
  COLLECTOR_PID=$!
  echo "[zeabur-start] collector supervisor pid=$COLLECTOR_PID"
else
  echo "[zeabur-start] collector disabled"
fi

cd /app
/usr/local/bin/docker-entrypoint.sh node /app/src/app.js &
APP_PID=$!
echo "[zeabur-start] app pid=$APP_PID"

set +e
wait "$APP_PID"
APP_STATUS=$?
shutdown
wait "$COLLECTOR_PID" 2>/dev/null || true
exit "$APP_STATUS"
