#!/bin/sh
set -eu

ANTHROPIC_DIR="${ANTHROPIC_CAPTURE_DIR:-}"
OPENAI_DIR="${OPENAI_CAPTURE_DIR:-}"

if [ -n "$ANTHROPIC_DIR" ] && [ -n "$OPENAI_DIR" ] && [ "$ANTHROPIC_DIR" != "$OPENAI_DIR" ]; then
  echo "[upstream-capture] ANTHROPIC_CAPTURE_DIR and OPENAI_CAPTURE_DIR must match when using shared links" >&2
  exit 1
fi

CAPTURE_DIR="${ANTHROPIC_DIR:-${OPENAI_DIR:-/data/relay-capture}}"
APP_DIR="${APP_DIR:-/app}"

mkdir -p "$CAPTURE_DIR"
mkdir -p "$APP_DIR"

for name in \
  anthropic-upstream-requests.jsonl \
  anthropic-upstream-responses.jsonl \
  anthropic-upstream-stream-final.jsonl \
  openai-upstream-requests.jsonl \
  openai-upstream-responses.jsonl \
  openai-upstream-stream-final.jsonl
do
  touch "$CAPTURE_DIR/$name"
  ln -sf "$CAPTURE_DIR/$name" "$APP_DIR/$name"
done

echo "[upstream-capture] links ready in $APP_DIR -> $CAPTURE_DIR"
