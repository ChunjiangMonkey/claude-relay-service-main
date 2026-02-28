# Anthropic Upstream Capture (Non-invasive)

This extension captures **Anthropic upstream** requests/responses (including streaming response reconstruction) without changing relay business code.

## What it adds

- Runtime hook: `hook/anthropic-hook.js`
  - Injected via `NODE_OPTIONS=--require ...`
  - Captures only configured upstream hosts (default `api.anthropic.com`)
  - Writes JSONL files:
    - `anthropic-upstream-requests.jsonl`
    - `anthropic-upstream-responses.jsonl`
    - `anthropic-upstream-stream-final.jsonl`
- Collector: `collector/src/index.js`
  - Tails JSONL files with offsets
  - De-duplicates by SHA-256 line hash
  - Persists to PostgreSQL (`upstream_events_raw`, `anthropic_interactions`)

## 1) Relay service setup (Zeabur)

Mount a persistent volume, e.g. `/data/relay-capture`.

Set env vars on relay service:

```bash
ANTHROPIC_CAPTURE_ENABLED=true
ANTHROPIC_CAPTURE_DIR=/data/relay-capture
ANTHROPIC_CAPTURE_HOSTS=api.anthropic.com
ANTHROPIC_CAPTURE_MAX_RECORD_BYTES=16777216
ANTHROPIC_CAPTURE_MAX_FILE_BYTES=268435456
ANTHROPIC_CAPTURE_BACKUP_FILES=3
ANTHROPIC_CAPTURE_INCLUDE_THINKING=false
```

Inject hook at runtime:

```bash
NODE_OPTIONS=--require /app/extensions/anthropic-capture/hook/anthropic-hook.js
```

Optional symlink helper (if you want files visible under `/app` as well):

```bash
sh /app/extensions/anthropic-capture/scripts/setup-capture-links.sh
```

Example startup command in Zeabur:

```bash
sh -lc 'sh /app/extensions/anthropic-capture/scripts/setup-capture-links.sh && node src/app.js'
```

## 2) Collector service setup

Create a separate service for collector and mount the same capture volume (`/data/relay-capture`).

Install dependencies and start:

```bash
cd /app/extensions/anthropic-capture/collector
npm install
npm run start
```

Required env:

```bash
DATABASE_URL=postgres://user:pass@host:5432/dbname
ANTHROPIC_CAPTURE_DIR=/data/relay-capture
COLLECTOR_POLL_INTERVAL_MS=2000
```

Optional:

```bash
COLLECTOR_FILES=anthropic-upstream-requests.jsonl,anthropic-upstream-responses.jsonl,anthropic-upstream-stream-final.jsonl
COLLECTOR_DEBUG=false
COLLECTOR_DB_POOL_MAX=10
```

## 3) PostgreSQL schema

Collector auto-creates schema at startup.

Reference SQL: `sql/schema.sql`

## Data notes

- Non-stream responses are stored in `response_json`.
- Stream responses are reconstructed into `assistant_text_full`, with `tool_calls`, `usage`, and `stop_reason`.
- Sensitive headers (authorization/cookie/api keys) are masked before writing JSONL.
