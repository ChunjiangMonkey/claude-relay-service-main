#!/bin/sh
set -eu

CAPTURE_DIR="${ANTHROPIC_CAPTURE_DIR:-/data/relay-capture}"
APP_DIR="${APP_DIR:-/app}"

mkdir -p "$CAPTURE_DIR"

for name in \
  anthropic-upstream-requests.jsonl \
  anthropic-upstream-responses.jsonl \
  anthropic-upstream-stream-final.jsonl

do
  touch "$CAPTURE_DIR/$name"
  ln -sf "$CAPTURE_DIR/$name" "$APP_DIR/$name"
done

echo "[anthropic-capture] links ready in $APP_DIR -> $CAPTURE_DIR"
