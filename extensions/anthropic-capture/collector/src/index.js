'use strict'

const fs = require('fs')
const fsPromises = require('fs/promises')
const path = require('path')
const crypto = require('crypto')
const { Pool } = require('pg')

const DEFAULT_CAPTURE_DIR = '/data/relay-capture'
const DEFAULT_FILES = [
  'anthropic-upstream-requests.jsonl',
  'anthropic-upstream-responses.jsonl',
  'anthropic-upstream-stream-final.jsonl'
]

const config = {
  databaseUrl: process.env.DATABASE_URL || '',
  captureDir: process.env.ANTHROPIC_CAPTURE_DIR || DEFAULT_CAPTURE_DIR,
  files: parseFileList(process.env.COLLECTOR_FILES),
  pollIntervalMs: parsePositiveInt(process.env.COLLECTOR_POLL_INTERVAL_MS, 2000),
  debug: isEnabled(process.env.COLLECTOR_DEBUG, false)
}

if (!config.databaseUrl) {
  // eslint-disable-next-line no-console
  console.error('[collector] DATABASE_URL is required')
  process.exit(1)
}

const pool = new Pool({
  connectionString: config.databaseUrl,
  max: parsePositiveInt(process.env.COLLECTOR_DB_POOL_MAX, 10)
})

const states = new Map()
let polling = false

async function bootstrap() {
  await ensureSchema()
  await loadStates()

  await pollOnce()
  setInterval(() => {
    pollOnce().catch((error) => {
      logError('poll_once_failed', { message: error.message, stack: error.stack })
    })
  }, config.pollIntervalMs)

  logInfo('collector_started', {
    captureDir: config.captureDir,
    pollIntervalMs: config.pollIntervalMs,
    files: config.files
  })
}

async function ensureSchema() {
  const sql = `
CREATE TABLE IF NOT EXISTS upstream_events_raw (
  id BIGSERIAL PRIMARY KEY,
  event_hash TEXT NOT NULL UNIQUE,
  trace_id TEXT,
  event_type TEXT NOT NULL,
  event_ts TIMESTAMPTZ,
  source_file TEXT NOT NULL,
  payload JSONB NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_upstream_events_raw_trace_id ON upstream_events_raw(trace_id);
CREATE INDEX IF NOT EXISTS idx_upstream_events_raw_event_ts ON upstream_events_raw(event_ts DESC);

CREATE TABLE IF NOT EXISTS anthropic_interactions (
  trace_id TEXT PRIMARY KEY,
  upstream_request_id TEXT,
  model TEXT,
  is_stream BOOLEAN,
  request_json JSONB,
  response_json JSONB,
  assistant_text_full TEXT,
  tool_calls JSONB,
  usage JSONB,
  stop_reason TEXT,
  http_status INTEGER,
  latency_ms INTEGER,
  status TEXT,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_anthropic_interactions_status ON anthropic_interactions(status);
CREATE INDEX IF NOT EXISTS idx_anthropic_interactions_model ON anthropic_interactions(model);
CREATE INDEX IF NOT EXISTS idx_anthropic_interactions_last_seen ON anthropic_interactions(last_seen_at DESC);

CREATE TABLE IF NOT EXISTS collector_offsets (
  file_path TEXT PRIMARY KEY,
  inode TEXT,
  offset BIGINT NOT NULL,
  remainder TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ingest_errors (
  id BIGSERIAL PRIMARY KEY,
  source_file TEXT,
  raw_line TEXT,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`

  await pool.query(sql)
}

async function loadStates() {
  const result = await pool.query('SELECT file_path, inode, offset, remainder FROM collector_offsets')
  result.rows.forEach((row) => {
    states.set(row.file_path, {
      inode: row.inode || null,
      offset: Number.parseInt(row.offset, 10) || 0,
      remainder: row.remainder || ''
    })
  })
}

async function pollOnce() {
  if (polling) {
    return
  }
  polling = true

  try {
    for (const name of config.files) {
      const filePath = path.join(config.captureDir, name)
      await processFile(filePath)
    }
  } finally {
    polling = false
  }
}

async function processFile(filePath) {
  let stat
  try {
    stat = await fsPromises.stat(filePath)
  } catch (error) {
    if (error && error.code === 'ENOENT') {
      return
    }
    throw error
  }

  const key = filePath
  const prev = states.get(key) || { inode: null, offset: 0, remainder: '' }

  let offset = prev.offset
  let remainder = prev.remainder || ''
  const inode = String(stat.ino)

  if (prev.inode && prev.inode !== inode) {
    logInfo('file_rotated', { filePath, oldInode: prev.inode, newInode: inode })
    await drainRotatedTail(filePath, prev)
    offset = 0
    remainder = ''
  }

  if (offset > stat.size) {
    logInfo('file_truncated', { filePath, previousOffset: offset, currentSize: stat.size })
    offset = 0
    remainder = ''
  }

  if (offset === stat.size) {
    states.set(key, { inode, offset, remainder })
    return
  }

  const chunk = await readTextFrom(filePath, offset)
  const merged = `${remainder}${chunk}`
  const lines = merged.split('\n')
  remainder = lines.pop() || ''

  for (const line of lines) {
    const trimmed = line.trim()
    if (!trimmed) {
      continue
    }
    await processLine(filePath, trimmed)
  }

  offset = stat.size
  states.set(key, { inode, offset, remainder })
  await persistState(key, inode, offset, remainder)
}

async function drainRotatedTail(baseFilePath, prevState) {
  const prevOffset = Number.parseInt(prevState.offset, 10) || 0
  const prevRemainder = prevState.remainder || ''

  if (prevOffset === 0 && !prevRemainder) {
    return
  }

  const rotatedPath = await findRotatedFileByInode(baseFilePath, prevState.inode)
  if (!rotatedPath) {
    const message = `ROTATION_TAIL_NOT_FOUND inode=${prevState.inode} offset=${prevOffset}`
    logError('rotation_tail_not_found', { baseFilePath, oldInode: prevState.inode, prevOffset })
    await storeIngestError(baseFilePath, '', message)
    return
  }

  let stat
  try {
    stat = await fsPromises.stat(rotatedPath)
  } catch (error) {
    const message = `ROTATION_TAIL_STAT_FAILED inode=${prevState.inode}: ${error.message}`
    logError('rotation_tail_stat_failed', { rotatedPath, error: error.message })
    await storeIngestError(baseFilePath, '', message)
    return
  }

  let unreadChunk = ''
  if (prevOffset < stat.size) {
    unreadChunk = await readTextFrom(rotatedPath, prevOffset)
  } else if (prevOffset > stat.size) {
    logInfo('rotation_tail_offset_out_of_range', {
      rotatedPath,
      prevOffset,
      size: stat.size
    })
  }

  const merged = `${prevRemainder}${unreadChunk}`
  const lines = merged.split('\n')
  const tail = lines.pop() || ''
  let drainedLines = 0

  for (const line of lines) {
    const trimmed = line.trim()
    if (!trimmed) {
      continue
    }
    await processLine(rotatedPath, trimmed)
    drainedLines += 1
  }

  // Rotated file is finalized; flush trailing line even without newline terminator.
  const tailTrimmed = tail.trim()
  if (tailTrimmed) {
    await processLine(rotatedPath, tailTrimmed)
    drainedLines += 1
  }

  logInfo('rotation_tail_drained', {
    baseFilePath,
    rotatedPath,
    oldInode: prevState.inode,
    drainedLines
  })
}

async function findRotatedFileByInode(baseFilePath, inode) {
  if (!inode) {
    return null
  }

  const target = String(inode)
  const dir = path.dirname(baseFilePath)
  const base = path.basename(baseFilePath)

  const candidates = [`${baseFilePath}.bak`]

  try {
    const entries = await fsPromises.readdir(dir)
    const bakEntries = entries
      .filter((name) => name.startsWith(`${base}.bak.`))
      .sort((a, b) => parseBackupIndex(a, base) - parseBackupIndex(b, base))
      .map((name) => path.join(dir, name))
    candidates.push(...bakEntries)
  } catch (_) {
    // no-op
  }

  for (const candidate of candidates) {
    try {
      const stat = await fsPromises.stat(candidate)
      if (String(stat.ino) === target) {
        return candidate
      }
    } catch (_) {
      // candidate not found or inaccessible
    }
  }

  return null
}

function parseBackupIndex(fileName, base) {
  const prefix = `${base}.bak.`
  if (!fileName.startsWith(prefix)) {
    return Number.MAX_SAFE_INTEGER
  }
  const suffix = fileName.slice(prefix.length)
  const parsed = Number.parseInt(suffix, 10)
  if (!Number.isFinite(parsed)) {
    return Number.MAX_SAFE_INTEGER
  }
  return parsed
}

function readTextFrom(filePath, offset) {
  return new Promise((resolve, reject) => {
    let data = ''
    const stream = fs.createReadStream(filePath, {
      start: offset,
      encoding: 'utf8'
    })

    stream.on('data', (chunk) => {
      data += chunk
    })

    stream.on('error', reject)
    stream.on('end', () => resolve(data))
  })
}

async function processLine(sourceFile, line) {
  let payload

  try {
    payload = JSON.parse(line)
  } catch (error) {
    await storeIngestError(sourceFile, line, `JSON_PARSE_ERROR: ${error.message}`)
    return
  }

  const eventHash = sha256(line)
  const traceId = getTraceId(payload, eventHash)
  const eventType = String(payload.type || 'unknown')
  const eventTs = normalizeTimestamp(payload.ts || payload.timestamp)

  const rawInsert = await pool.query(
    `
    INSERT INTO upstream_events_raw (event_hash, trace_id, event_type, event_ts, source_file, payload)
    VALUES ($1, $2, $3, $4, $5, $6::jsonb)
    ON CONFLICT (event_hash) DO NOTHING
    RETURNING id
    `,
    [eventHash, traceId, eventType, eventTs, sourceFile, JSON.stringify(payload)]
  )

  if (rawInsert.rowCount === 0) {
    return
  }

  if (eventType === 'anthropic_upstream_request') {
    await upsertRequest(traceId, payload)
    return
  }

  if (eventType === 'anthropic_upstream_response_non_stream') {
    await upsertNonStreamResponse(traceId, payload)
    return
  }

  if (eventType === 'anthropic_upstream_stream_final') {
    await upsertStreamFinal(traceId, payload)
    return
  }

  if (eventType === 'anthropic_upstream_response_stream_summary') {
    await upsertStreamSummary(traceId, payload)
    return
  }

  if (eventType === 'anthropic_upstream_response_transport_error') {
    await upsertTransportError(traceId, payload)
  }
}

async function upsertRequest(traceId, payload) {
  const requestJson = payload.request_body_json || null
  const model = payload.request_model || (requestJson && requestJson.model) || null
  const isStream = payload.request_stream === true || (requestJson && requestJson.stream === true)
  const upstreamRequestId = payload.relay_request_id || null

  await pool.query(
    `
    INSERT INTO anthropic_interactions (
      trace_id,
      upstream_request_id,
      model,
      is_stream,
      request_json,
      status,
      first_seen_at,
      last_seen_at,
      created_at,
      updated_at
    ) VALUES ($1, $2, $3, $4, $5::jsonb, 'request_captured', NOW(), NOW(), NOW(), NOW())
    ON CONFLICT (trace_id) DO UPDATE SET
      upstream_request_id = COALESCE(EXCLUDED.upstream_request_id, anthropic_interactions.upstream_request_id),
      model = COALESCE(EXCLUDED.model, anthropic_interactions.model),
      is_stream = COALESCE(EXCLUDED.is_stream, anthropic_interactions.is_stream),
      request_json = COALESCE(EXCLUDED.request_json, anthropic_interactions.request_json),
      last_seen_at = NOW(),
      updated_at = NOW()
    `,
    [traceId, upstreamRequestId, model, isStream, requestJson ? JSON.stringify(requestJson) : null]
  )
}

async function upsertNonStreamResponse(traceId, payload) {
  const responseJson = payload.response && payload.response.body_json ? payload.response.body_json : null
  const usage = payload.response && payload.response.usage ? payload.response.usage : null
  const stopReason = payload.response && payload.response.stop_reason ? payload.response.stop_reason : null
  const model =
    (payload.response && payload.response.model) ||
    (responseJson && responseJson.model) ||
    (payload.request && payload.request.model) ||
    null
  const httpStatus = extractInt(payload.upstream && payload.upstream.statusCode)
  const latencyMs = extractInt(payload.timing && payload.timing.latency_ms)
  const upstreamRequestId = (payload.request && payload.request.relay_request_id) || payload.relay_request_id || null
  const hasError = Boolean(payload.error)

  await pool.query(
    `
    INSERT INTO anthropic_interactions (
      trace_id,
      upstream_request_id,
      model,
      is_stream,
      response_json,
      usage,
      stop_reason,
      http_status,
      latency_ms,
      status,
      first_seen_at,
      last_seen_at,
      created_at,
      updated_at
    ) VALUES (
      $1,
      $2,
      $3,
      false,
      $4::jsonb,
      $5::jsonb,
      $6,
      $7,
      $8,
      $9,
      NOW(),
      NOW(),
      NOW(),
      NOW()
    )
    ON CONFLICT (trace_id) DO UPDATE SET
      upstream_request_id = COALESCE(EXCLUDED.upstream_request_id, anthropic_interactions.upstream_request_id),
      model = COALESCE(EXCLUDED.model, anthropic_interactions.model),
      is_stream = FALSE,
      response_json = COALESCE(EXCLUDED.response_json, anthropic_interactions.response_json),
      usage = COALESCE(EXCLUDED.usage, anthropic_interactions.usage),
      stop_reason = COALESCE(EXCLUDED.stop_reason, anthropic_interactions.stop_reason),
      http_status = COALESCE(EXCLUDED.http_status, anthropic_interactions.http_status),
      latency_ms = COALESCE(EXCLUDED.latency_ms, anthropic_interactions.latency_ms),
      status = EXCLUDED.status,
      last_seen_at = NOW(),
      updated_at = NOW()
    `,
    [
      traceId,
      upstreamRequestId,
      model,
      responseJson ? JSON.stringify(responseJson) : null,
      usage ? JSON.stringify(usage) : null,
      stopReason,
      httpStatus,
      latencyMs,
      hasError ? 'error_non_stream' : 'completed_non_stream'
    ]
  )
}

async function upsertStreamFinal(traceId, payload) {
  const stream = payload.stream || {}
  const usage = stream.usage || null
  const toolCalls = Array.isArray(stream.tool_calls) ? stream.tool_calls : []
  const assistantText = typeof stream.assistant_text_full === 'string' ? stream.assistant_text_full : ''
  const stopReason = stream.stop_reason || null
  const model = stream.message_model || (payload.request && payload.request.model) || null
  const httpStatus = extractInt(payload.upstream && payload.upstream.statusCode)
  const latencyMs = extractInt(payload.timing && payload.timing.latency_ms)
  const upstreamRequestId = (payload.request && payload.request.relay_request_id) || payload.relay_request_id || null
  const hasError = Boolean(payload.error)

  await pool.query(
    `
    INSERT INTO anthropic_interactions (
      trace_id,
      upstream_request_id,
      model,
      is_stream,
      assistant_text_full,
      tool_calls,
      usage,
      stop_reason,
      http_status,
      latency_ms,
      status,
      first_seen_at,
      last_seen_at,
      created_at,
      updated_at
    ) VALUES (
      $1,
      $2,
      $3,
      true,
      $4,
      $5::jsonb,
      $6::jsonb,
      $7,
      $8,
      $9,
      $10,
      NOW(),
      NOW(),
      NOW(),
      NOW()
    )
    ON CONFLICT (trace_id) DO UPDATE SET
      upstream_request_id = COALESCE(EXCLUDED.upstream_request_id, anthropic_interactions.upstream_request_id),
      model = COALESCE(EXCLUDED.model, anthropic_interactions.model),
      is_stream = TRUE,
      assistant_text_full = COALESCE(EXCLUDED.assistant_text_full, anthropic_interactions.assistant_text_full),
      tool_calls = COALESCE(EXCLUDED.tool_calls, anthropic_interactions.tool_calls),
      usage = COALESCE(EXCLUDED.usage, anthropic_interactions.usage),
      stop_reason = COALESCE(EXCLUDED.stop_reason, anthropic_interactions.stop_reason),
      http_status = COALESCE(EXCLUDED.http_status, anthropic_interactions.http_status),
      latency_ms = COALESCE(EXCLUDED.latency_ms, anthropic_interactions.latency_ms),
      status = EXCLUDED.status,
      last_seen_at = NOW(),
      updated_at = NOW()
    `,
    [
      traceId,
      upstreamRequestId,
      model,
      assistantText,
      JSON.stringify(toolCalls),
      usage ? JSON.stringify(usage) : null,
      stopReason,
      httpStatus,
      latencyMs,
      hasError ? 'error_stream' : 'completed_stream'
    ]
  )
}

async function upsertStreamSummary(traceId, payload) {
  const usage = payload.usage || null
  const stopReason = payload.stop_reason || null
  const latencyMs = extractInt(payload.latency_ms)
  const status = payload.error ? 'error_stream_summary' : 'stream_summary'

  await pool.query(
    `
    INSERT INTO anthropic_interactions (
      trace_id,
      is_stream,
      usage,
      stop_reason,
      latency_ms,
      status,
      first_seen_at,
      last_seen_at,
      created_at,
      updated_at
    ) VALUES ($1, true, $2::jsonb, $3, $4, $5, NOW(), NOW(), NOW(), NOW())
    ON CONFLICT (trace_id) DO UPDATE SET
      is_stream = TRUE,
      usage = COALESCE(EXCLUDED.usage, anthropic_interactions.usage),
      stop_reason = COALESCE(EXCLUDED.stop_reason, anthropic_interactions.stop_reason),
      latency_ms = COALESCE(EXCLUDED.latency_ms, anthropic_interactions.latency_ms),
      status = COALESCE(anthropic_interactions.status, EXCLUDED.status),
      last_seen_at = NOW(),
      updated_at = NOW()
    `,
    [traceId, usage ? JSON.stringify(usage) : null, stopReason, latencyMs, status]
  )
}

async function upsertTransportError(traceId, payload) {
  const model = payload.request && payload.request.model ? payload.request.model : null
  const isStream = payload.request && payload.request.stream === true
  const latencyMs = extractInt(payload.timing && payload.timing.latency_ms)
  const httpStatus = extractInt(payload.http_status)
  const upstreamRequestId = (payload.request && payload.request.relay_request_id) || payload.relay_request_id || null

  await pool.query(
    `
    INSERT INTO anthropic_interactions (
      trace_id,
      upstream_request_id,
      model,
      is_stream,
      http_status,
      latency_ms,
      status,
      first_seen_at,
      last_seen_at,
      created_at,
      updated_at
    ) VALUES ($1, $2, $3, $4, $5, $6, 'transport_error', NOW(), NOW(), NOW(), NOW())
    ON CONFLICT (trace_id) DO UPDATE SET
      upstream_request_id = COALESCE(EXCLUDED.upstream_request_id, anthropic_interactions.upstream_request_id),
      model = COALESCE(EXCLUDED.model, anthropic_interactions.model),
      is_stream = COALESCE(EXCLUDED.is_stream, anthropic_interactions.is_stream),
      http_status = COALESCE(EXCLUDED.http_status, anthropic_interactions.http_status),
      latency_ms = COALESCE(EXCLUDED.latency_ms, anthropic_interactions.latency_ms),
      status = 'transport_error',
      last_seen_at = NOW(),
      updated_at = NOW()
    `,
    [traceId, upstreamRequestId, model, isStream, httpStatus, latencyMs]
  )
}

async function persistState(filePath, inode, offset, remainder) {
  await pool.query(
    `
    INSERT INTO collector_offsets (file_path, inode, offset, remainder, updated_at)
    VALUES ($1, $2, $3, $4, NOW())
    ON CONFLICT (file_path) DO UPDATE SET
      inode = EXCLUDED.inode,
      offset = EXCLUDED.offset,
      remainder = EXCLUDED.remainder,
      updated_at = NOW()
    `,
    [filePath, inode, offset, remainder]
  )
}

async function storeIngestError(sourceFile, rawLine, error) {
  await pool.query(
    `
    INSERT INTO ingest_errors (source_file, raw_line, error)
    VALUES ($1, $2, $3)
    `,
    [sourceFile, rawLine, error]
  )
}

function getTraceId(payload, fallback) {
  const candidate = payload.trace_id || payload.requestId || payload.request_id
  if (candidate && String(candidate).trim()) {
    return String(candidate).trim()
  }
  return `fallback_${fallback}`
}

function normalizeTimestamp(value) {
  if (!value) {
    return new Date().toISOString()
  }

  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return new Date().toISOString()
  }
  return date.toISOString()
}

function sha256(text) {
  return crypto.createHash('sha256').update(text).digest('hex')
}

function extractInt(value) {
  const parsed = Number.parseInt(String(value), 10)
  if (!Number.isFinite(parsed)) {
    return null
  }
  return parsed
}

function parseFileList(rawValue) {
  if (!rawValue) {
    return DEFAULT_FILES
  }
  const items = String(rawValue)
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean)
  return items.length > 0 ? items : DEFAULT_FILES
}

function parsePositiveInt(rawValue, fallback) {
  const parsed = Number.parseInt(String(rawValue || ''), 10)
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback
  }
  return parsed
}

function isEnabled(rawValue, fallback = false) {
  if (rawValue === undefined || rawValue === null || rawValue === '') {
    return fallback
  }
  const normalized = String(rawValue).trim().toLowerCase()
  return normalized === '1' || normalized === 'true' || normalized === 'yes'
}

function logInfo(message, payload) {
  // eslint-disable-next-line no-console
  console.log(`[collector] ${message}`, payload || '')
}

function logError(message, payload) {
  // eslint-disable-next-line no-console
  console.error(`[collector] ${message}`, payload || '')
}

process.on('SIGINT', async () => {
  await pool.end()
  process.exit(0)
})

process.on('SIGTERM', async () => {
  await pool.end()
  process.exit(0)
})

bootstrap().catch(async (error) => {
  logError('bootstrap_failed', { message: error.message, stack: error.stack })
  await pool.end().catch(() => {})
  process.exit(1)
})
