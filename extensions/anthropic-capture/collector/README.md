# Collector

Reads Anthropic capture JSONL files and persists into PostgreSQL.

## Run

```bash
cd /app/extensions/anthropic-capture/collector
npm install
DATABASE_URL=postgres://user:pass@host:5432/dbname \
ANTHROPIC_CAPTURE_DIR=/data/relay-capture \
npm run start
```

## Env

- `DATABASE_URL` (required)
- `ANTHROPIC_CAPTURE_DIR` (default `/data/relay-capture`)
- `COLLECTOR_POLL_INTERVAL_MS` (default `2000`)
- `COLLECTOR_FILES` (optional CSV, defaults to three upstream files)
- `COLLECTOR_DB_POOL_MAX` (default `10`)
- `COLLECTOR_DEBUG` (default `false`)
