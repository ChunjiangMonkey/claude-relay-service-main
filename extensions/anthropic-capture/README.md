# Anthropic Upstream Capture (Non-invasive)

This extension captures **Anthropic upstream** requests/responses (including streaming response reconstruction) without changing relay business code.

## What it adds

- Runtime hook: `hook/anthropic-hook.js`
  - Injected via `NODE_OPTIONS=--require ...`
  - Captures only configured upstream hosts/methods/paths (default `api.anthropic.com`, `POST`, `/v1/messages`)
  - Auto-decompresses upstream payload (`gzip`/`br`/`deflate`, with gzip magic fallback)
  - Writes JSONL files:
    - `anthropic-upstream-requests.jsonl`
    - `anthropic-upstream-responses.jsonl`
    - `anthropic-upstream-stream-final.jsonl`
- Collector: `collector/src/index.js`
  - Tails JSONL files with offsets
  - De-duplicates by SHA-256 line hash
  - Persists to MySQL (default) or PostgreSQL (`upstream_events_raw`, `anthropic_interactions`)

## 1) Zeabur quick deploy (single service, recommended)

Use this mode when you run one Zeabur service for relay and want collector to run in the same container.

### Step A: mount storage

- Mount a persistent volume to `/data/relay-capture`.

### Step B: set env vars (same service)

```bash
# Capture hook
ANTHROPIC_CAPTURE_ENABLED=true
ANTHROPIC_CAPTURE_DIR=/data/relay-capture
ANTHROPIC_CAPTURE_HOSTS=api.anthropic.com
ANTHROPIC_CAPTURE_METHODS=POST
ANTHROPIC_CAPTURE_PATH_PREFIXES=/v1/messages
ANTHROPIC_CAPTURE_MAX_RECORD_BYTES=16777216
ANTHROPIC_CAPTURE_MAX_FILE_BYTES=268435456
ANTHROPIC_CAPTURE_BACKUP_FILES=3
ANTHROPIC_CAPTURE_INCLUDE_THINKING=false
ANTHROPIC_CAPTURE_DEBUG=false

# Collector + MySQL
COLLECTOR_DB_BACKEND=mysql
MYSQL_HOST=your-mysql-host
MYSQL_PORT=3306
MYSQL_USER=your-user
MYSQL_PASSWORD=your-password
MYSQL_DATABASE=your-database
MYSQL_SSL=false
COLLECTOR_POLL_INTERVAL_MS=2000
COLLECTOR_DB_POOL_MAX=10
COLLECTOR_DEBUG=false
```

Notes:
- Do **not** set `NODE_OPTIONS` as a global Zeabur env var, because it may affect build-time `npm ci`.
- `COLLECTOR_FILES` is optional. If omitted, collector reads all three capture files by default.

### Step C: set Start Command (Zeabur)

```bash
sh -lc 'set -e; export NODE_OPTIONS="--require /app/extensions/anthropic-capture/hook/anthropic-hook.js"; sh /app/extensions/anthropic-capture/scripts/setup-capture-links.sh || true; cd /app/extensions/anthropic-capture/collector; test -d node_modules || npm install --omit=dev --no-audit --no-fund; while true; do node src/index.js; echo "[collector] exited, restart in 2s"; sleep 2; done & cd /app; exec /usr/local/bin/docker-entrypoint.sh node /app/src/app.js'
```

### Step D: verify logs

- Relay: `[anthropic-capture] Anthropic capture hook installed`
- Collector: `[collector] collector_started`

## 2) Relay service setup (Zeabur, split-service mode)

Mount a persistent volume, e.g. `/data/relay-capture`.

Set env vars on relay service:

```bash
ANTHROPIC_CAPTURE_ENABLED=true
ANTHROPIC_CAPTURE_DIR=/data/relay-capture
ANTHROPIC_CAPTURE_HOSTS=api.anthropic.com
ANTHROPIC_CAPTURE_METHODS=POST
ANTHROPIC_CAPTURE_PATH_PREFIXES=/v1/messages
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
sh -lc 'export NODE_OPTIONS="--require /app/extensions/anthropic-capture/hook/anthropic-hook.js"; sh /app/extensions/anthropic-capture/scripts/setup-capture-links.sh; exec node src/app.js'
```

## 3) Collector service setup

Create a separate service for collector and mount the same capture volume (`/data/relay-capture`).

Install dependencies and start:

```bash
cd /app/extensions/anthropic-capture/collector
npm install
npm run start
```

Required env (MySQL default):

```bash
COLLECTOR_DB_BACKEND=mysql
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=change-me
MYSQL_DATABASE=anthropic_capture
ANTHROPIC_CAPTURE_DIR=/data/relay-capture
COLLECTOR_POLL_INTERVAL_MS=2000
```

Optional:

```bash
COLLECTOR_FILES=anthropic-upstream-requests.jsonl,anthropic-upstream-responses.jsonl,anthropic-upstream-stream-final.jsonl
COLLECTOR_DEBUG=false
COLLECTOR_DB_POOL_MAX=10
```

PostgreSQL compatibility mode:

```bash
COLLECTOR_DB_BACKEND=postgres
DATABASE_URL=postgres://user:pass@host:5432/dbname
ANTHROPIC_CAPTURE_DIR=/data/relay-capture
COLLECTOR_POLL_INTERVAL_MS=2000
```

## 4) Schema

Collector auto-creates schema at startup.

Reference SQL:

- MySQL: `collector/src/db/sql/mysql.sql`
- PostgreSQL: `collector/src/db/sql/postgres.sql`

## Data notes

- Non-stream responses are stored in `response_json`.
- Stream responses are reconstructed into `assistant_text_full`, with `tool_calls`, `usage`, and `stop_reason`.
- Sensitive headers (authorization/cookie/api keys) are masked before writing JSONL.
