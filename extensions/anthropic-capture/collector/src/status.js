'use strict'

const DEFAULT_TIMEOUT_MS = 15000
const TABLES = [
  'anthropic_interactions',
  'openai_interactions',
  'upstream_events_raw',
  'collector_offsets',
  'ingest_errors'
]

const options = parseArgs(process.argv.slice(2))

async function main() {
  if (options.help) {
    printHelp()
    return
  }

  const databaseUrl = process.env.DATABASE_URL || ''
  if (!databaseUrl) {
    throw new Error('DATABASE_URL is required')
  }

  const { Client } = require('pg')
  const client = new Client({
    connectionString: databaseUrl,
    application_name: 'claude-relay-capture-status',
    connectionTimeoutMillis: options.timeoutMs
  })

  try {
    await client.connect()
    await setStatementTimeout(client, options.timeoutMs)

    const result = {
      generatedAt: new Date().toISOString(),
      connection: await getConnectionStatus(client),
      estimatedRows: await getEstimatedRows(client),
      latest: await getLatestInteractions(client),
      offsets: await getOffsets(client)
    }

    if (options.exact) {
      result.exactRows = await getExactRows(client)
    }

    if (options.sizes) {
      result.sizes = await getTableSizes(client)
      result.databaseSize = await getDatabaseSize(client)
    }

    if (options.errors) {
      result.errors = await getErrorSummary(client)
    }

    printResult(result, options)
  } finally {
    await client.end().catch(() => {})
  }
}

async function setStatementTimeout(client, timeoutMs) {
  await client.query(`SET statement_timeout = '${timeoutMs}ms'`)
}

async function getConnectionStatus(client) {
  const result = await client.query(`
    SELECT
      NOW() AS db_now,
      CURRENT_DATABASE() AS database,
      CURRENT_USER AS "user"
  `)
  return result.rows[0]
}

async function getEstimatedRows(client) {
  const result = await client.query(
    `
    SELECT
      c.relname AS table_name,
      COALESCE(s.n_live_tup, 0)::bigint AS estimated_rows,
      s.last_vacuum,
      s.last_autovacuum,
      s.last_analyze,
      s.last_autoanalyze
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_catalog.pg_stat_user_tables s ON s.relid = c.oid
    WHERE n.nspname = 'public'
      AND c.relkind = 'r'
      AND c.relname = ANY($1::text[])
    ORDER BY c.relname
    `,
    [TABLES]
  )
  return result.rows
}

async function getLatestInteractions(client) {
  const anthropic = await getLatestInteraction(client, 'anthropic_interactions')
  const openai = await getLatestInteraction(client, 'openai_interactions')

  return {
    anthropic,
    openai
  }
}

async function getLatestInteraction(client, tableName) {
  const result = await client.query(`
    SELECT
      trace_id,
      relay_key_id,
      model,
      status,
      last_seen_at,
      updated_at
    FROM public.${tableName}
    ORDER BY last_seen_at DESC NULLS LAST
    LIMIT 1
  `)
  return result.rows[0] || null
}

async function getOffsets(client) {
  const result = await client.query(`
    SELECT
      file_path,
      "offset",
      updated_at
    FROM public.collector_offsets
    ORDER BY updated_at DESC
  `)
  return result.rows
}

async function getExactRows(client) {
  const rows = {}

  for (const tableName of TABLES) {
    const result = await client.query(`SELECT COUNT(*)::bigint AS count FROM public.${tableName}`)
    rows[tableName] = result.rows[0].count
  }

  rows.main_interactions_total = (
    BigInt(rows.anthropic_interactions || 0) + BigInt(rows.openai_interactions || 0)
  ).toString()

  return rows
}

async function getTableSizes(client) {
  const result = await client.query(
    `
    SELECT
      c.relname AS table_name,
      pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
      pg_size_pretty(pg_relation_size(c.oid)) AS table_size,
      pg_size_pretty(pg_indexes_size(c.oid)) AS index_size,
      pg_total_relation_size(c.oid) AS total_bytes,
      pg_relation_size(c.oid) AS table_bytes,
      pg_indexes_size(c.oid) AS index_bytes,
      COALESCE(s.n_live_tup, 0)::bigint AS estimated_rows
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_catalog.pg_stat_user_tables s ON s.relid = c.oid
    WHERE n.nspname = 'public'
      AND c.relkind = 'r'
      AND c.relname = ANY($1::text[])
    ORDER BY pg_total_relation_size(c.oid) DESC
    `,
    [TABLES]
  )
  return result.rows
}

async function getDatabaseSize(client) {
  const result = await client.query(`
    SELECT
      pg_size_pretty(pg_database_size(CURRENT_DATABASE())) AS database_size,
      pg_database_size(CURRENT_DATABASE()) AS database_bytes
  `)
  return result.rows[0]
}

async function getErrorSummary(client) {
  const countResult = await client.query(
    'SELECT COUNT(*)::bigint AS count FROM public.ingest_errors'
  )
  const recentResult = await client.query(`
      SELECT
        id,
        source_file,
        LEFT(error, 500) AS error,
        created_at
      FROM public.ingest_errors
      ORDER BY id DESC
      LIMIT 10
    `)
  const groupedResult = await client.query(`
      SELECT
        source_file,
        LEFT(error, 300) AS error,
        COUNT(*)::bigint AS count
      FROM public.ingest_errors
      GROUP BY source_file, LEFT(error, 300)
      ORDER BY COUNT(*) DESC
      LIMIT 10
    `)

  return {
    count: countResult.rows[0].count,
    recent: recentResult.rows,
    grouped: groupedResult.rows
  }
}

function parseArgs(args) {
  const parsed = {
    sizes: false,
    errors: false,
    exact: false,
    json: false,
    help: false,
    timeoutMs: DEFAULT_TIMEOUT_MS
  }

  for (const arg of args) {
    if (arg === '--sizes') {
      parsed.sizes = true
    } else if (arg === '--errors') {
      parsed.errors = true
    } else if (arg === '--exact') {
      parsed.exact = true
    } else if (arg === '--json') {
      parsed.json = true
    } else if (arg === '--help' || arg === '-h') {
      parsed.help = true
    } else if (arg.startsWith('--timeout-ms=')) {
      parsed.timeoutMs = parseTimeout(arg.slice('--timeout-ms='.length))
    } else {
      throw new Error(`Unknown option: ${arg}`)
    }
  }

  return parsed
}

function parseTimeout(value) {
  const parsed = Number.parseInt(String(value), 10)
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Invalid --timeout-ms value: ${value}`)
  }
  return parsed
}

function printResult(result, printOptions) {
  if (printOptions.json) {
    console.log(JSON.stringify(result, null, 2))
    return
  }

  console.log('Capture database status')
  console.log('=======================')
  console.log(`generated_at: ${result.generatedAt}`)
  console.log(`db_now: ${formatDate(result.connection.db_now)}`)
  console.log(`database: ${result.connection.database}`)
  console.log(`user: ${result.connection.user}`)

  console.log('\nEstimated rows')
  console.table(
    result.estimatedRows.map((row) => ({
      table_name: row.table_name,
      estimated_rows: row.estimated_rows
    }))
  )

  console.log('\nLatest interactions')
  console.table([
    formatLatestRow('anthropic_interactions', result.latest.anthropic),
    formatLatestRow('openai_interactions', result.latest.openai)
  ])

  console.log('\nCollector offsets')
  console.table(
    result.offsets.map((row) => ({
      file_path: row.file_path,
      offset: row.offset,
      updated_at: formatDate(row.updated_at)
    }))
  )

  if (result.exactRows) {
    console.log('\nExact rows')
    console.table(
      Object.entries(result.exactRows).map(([tableName, count]) => ({
        table_name: tableName,
        count
      }))
    )
  }

  if (result.sizes) {
    console.log('\nTable sizes')
    console.table(
      result.sizes.map((row) => ({
        table_name: row.table_name,
        total_size: row.total_size,
        table_size: row.table_size,
        index_size: row.index_size,
        estimated_rows: row.estimated_rows
      }))
    )
    console.log(`database_size: ${result.databaseSize.database_size}`)
  }

  if (result.errors) {
    console.log('\nIngest errors')
    console.log(`error_count: ${result.errors.count}`)
    console.log('\nGrouped errors')
    console.table(result.errors.grouped)
    console.log('\nRecent errors')
    console.table(
      result.errors.recent.map((row) => ({
        id: row.id,
        source_file: row.source_file,
        error: row.error,
        created_at: formatDate(row.created_at)
      }))
    )
  }
}

function formatLatestRow(tableName, row) {
  if (!row) {
    return {
      table_name: tableName,
      trace_id: '',
      relay_key_id: '',
      model: '',
      status: '',
      last_seen_at: '',
      updated_at: ''
    }
  }

  return {
    table_name: tableName,
    trace_id: row.trace_id,
    relay_key_id: row.relay_key_id,
    model: row.model,
    status: row.status,
    last_seen_at: formatDate(row.last_seen_at),
    updated_at: formatDate(row.updated_at)
  }
}

function formatDate(value) {
  if (!value) {
    return ''
  }
  if (value instanceof Date) {
    return value.toISOString()
  }
  return String(value)
}

function printHelp() {
  console.log(`Usage: npm run status -- [options]

Options:
  --sizes             Include table and database size details.
  --errors            Include ingest error summaries.
  --exact             Run exact count(*) queries.
  --json              Print machine-readable JSON.
  --timeout-ms=<ms>   Set PostgreSQL statement timeout. Default: ${DEFAULT_TIMEOUT_MS}.
  -h, --help          Show this help.
`)
}

main().catch((error) => {
  console.error(`[capture-status] ${error.message}`)
  process.exit(1)
})
