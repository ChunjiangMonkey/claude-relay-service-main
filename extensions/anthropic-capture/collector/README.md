# Collector

Reads Anthropic capture JSONL files and persists into **MySQL (default)** or PostgreSQL.

## Run

```bash
cd /app/extensions/anthropic-capture/collector
npm install
npm run start
```

## Env (default: MySQL)

```bash
COLLECTOR_DB_BACKEND=mysql
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=change-me
MYSQL_DATABASE=anthropic_capture
MYSQL_SSL=false
ANTHROPIC_CAPTURE_DIR=/data/relay-capture
COLLECTOR_POLL_INTERVAL_MS=2000
```

Optional:

- `COLLECTOR_FILES` (defaults to three upstream files)
- `COLLECTOR_DB_POOL_MAX` (default `10`)
- `COLLECTOR_DEBUG` (default `false`)

## PostgreSQL compatibility mode

```bash
COLLECTOR_DB_BACKEND=postgres
DATABASE_URL=postgres://user:pass@host:5432/dbname
ANTHROPIC_CAPTURE_DIR=/data/relay-capture
COLLECTOR_POLL_INTERVAL_MS=2000
```

## Schema references

- MySQL: `src/db/sql/mysql.sql`
- PostgreSQL: `src/db/sql/postgres.sql`
