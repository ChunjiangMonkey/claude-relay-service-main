'use strict'

const fs = require('fs')
const fsPromises = require('fs/promises')
const path = require('path')
const crypto = require('crypto')
const { createDbAdapter, normalizeBackend } = require('./db')

const DEFAULT_CAPTURE_DIR = '/data/relay-capture'
const DEFAULT_FILES = [
  'anthropic-upstream-requests.jsonl',
  'anthropic-upstream-responses.jsonl',
  'anthropic-upstream-stream-final.jsonl'
]

const config = {
  dbBackend: normalizeBackend(process.env.COLLECTOR_DB_BACKEND || 'mysql'),
  databaseUrl: process.env.DATABASE_URL || '',
  mysql: {
    host: process.env.MYSQL_HOST || '',
    port: parsePositiveInt(process.env.MYSQL_PORT, 3306),
    user: process.env.MYSQL_USER || '',
    password: process.env.MYSQL_PASSWORD || '',
    database: process.env.MYSQL_DATABASE || '',
    ssl: isEnabled(process.env.MYSQL_SSL, false)
  },
  captureDir: process.env.ANTHROPIC_CAPTURE_DIR || DEFAULT_CAPTURE_DIR,
  files: parseFileList(process.env.COLLECTOR_FILES),
  pollIntervalMs: parsePositiveInt(process.env.COLLECTOR_POLL_INTERVAL_MS, 2000),
  debug: isEnabled(process.env.COLLECTOR_DEBUG, false),
  dbPoolMax: parsePositiveInt(process.env.COLLECTOR_DB_POOL_MAX, 10)
}

validateConfig(config)

const db = createDbAdapter(config)
const states = new Map()
let polling = false

async function bootstrap() {
  await db.ensureSchema()
  await loadStates()

  await pollOnce()
  setInterval(() => {
    pollOnce().catch((error) => {
      logError('poll_once_failed', { message: error.message, stack: error.stack })
    })
  }, config.pollIntervalMs)

  logInfo('collector_started', {
    dbBackend: config.dbBackend,
    captureDir: config.captureDir,
    pollIntervalMs: config.pollIntervalMs,
    files: config.files
  })
}

async function loadStates() {
  const rows = await db.loadOffsets()
  rows.forEach((row) => {
    states.set(row.file_path, {
      inode: row.inode || null,
      offset: Number.parseInt(String(row.offset || 0), 10) || 0,
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

  const inserted = await db.insertRawEvent({
    eventHash,
    traceId,
    eventType,
    eventTs,
    sourceFile,
    payload
  })

  if (!inserted) {
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

  await db.upsertRequest({
    traceId,
    upstreamRequestId,
    model,
    isStream,
    requestJson
  })
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

  await db.upsertNonStreamResponse({
    traceId,
    upstreamRequestId,
    model,
    responseJson,
    usage,
    stopReason,
    httpStatus,
    latencyMs,
    status: hasError ? 'error_non_stream' : 'completed_non_stream'
  })
}

async function upsertStreamFinal(traceId, payload) {
  const stream = payload.stream || {}
  const usage = stream.usage || null
  const toolCalls = Array.isArray(stream.tool_calls) ? stream.tool_calls : []
  const assistantTextFull = typeof stream.assistant_text_full === 'string' ? stream.assistant_text_full : ''
  const stopReason = stream.stop_reason || null
  const model = stream.message_model || (payload.request && payload.request.model) || null
  const httpStatus = extractInt(payload.upstream && payload.upstream.statusCode)
  const latencyMs = extractInt(payload.timing && payload.timing.latency_ms)
  const upstreamRequestId = (payload.request && payload.request.relay_request_id) || payload.relay_request_id || null
  const hasError = Boolean(payload.error)

  await db.upsertStreamFinal({
    traceId,
    upstreamRequestId,
    model,
    assistantTextFull,
    toolCalls,
    usage,
    stopReason,
    httpStatus,
    latencyMs,
    status: hasError ? 'error_stream' : 'completed_stream'
  })
}

async function upsertStreamSummary(traceId, payload) {
  const usage = payload.usage || null
  const stopReason = payload.stop_reason || null
  const latencyMs = extractInt(payload.latency_ms)
  const status = payload.error ? 'error_stream_summary' : 'stream_summary'

  await db.upsertStreamSummary({
    traceId,
    usage,
    stopReason,
    latencyMs,
    status
  })
}

async function upsertTransportError(traceId, payload) {
  const model = payload.request && payload.request.model ? payload.request.model : null
  const isStream = payload.request && payload.request.stream === true
  const latencyMs = extractInt(payload.timing && payload.timing.latency_ms)
  const httpStatus = extractInt(payload.http_status)
  const upstreamRequestId = (payload.request && payload.request.relay_request_id) || payload.relay_request_id || null

  await db.upsertTransportError({
    traceId,
    upstreamRequestId,
    model,
    isStream,
    httpStatus,
    latencyMs
  })
}

async function persistState(filePath, inode, offset, remainder) {
  await db.persistOffset({ filePath, inode, offset, remainder })
}

async function storeIngestError(sourceFile, rawLine, error) {
  await db.insertIngestError({ sourceFile, rawLine, error })
}

function validateConfig(runtimeConfig) {
  if (runtimeConfig.dbBackend === 'postgres') {
    if (!runtimeConfig.databaseUrl) {
      // eslint-disable-next-line no-console
      console.error('[collector] DATABASE_URL is required when COLLECTOR_DB_BACKEND=postgres')
      process.exit(1)
    }
    return
  }

  if (runtimeConfig.dbBackend === 'mysql') {
    const missing = []
    if (!runtimeConfig.mysql.host) missing.push('MYSQL_HOST')
    if (!runtimeConfig.mysql.user) missing.push('MYSQL_USER')
    if (!runtimeConfig.mysql.database) missing.push('MYSQL_DATABASE')

    if (missing.length > 0) {
      // eslint-disable-next-line no-console
      console.error(`[collector] Missing required MySQL env: ${missing.join(', ')}`)
      process.exit(1)
    }
    return
  }

  // eslint-disable-next-line no-console
  console.error(`[collector] Unsupported COLLECTOR_DB_BACKEND: ${runtimeConfig.dbBackend}`)
  process.exit(1)
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
  await db.close()
  process.exit(0)
})

process.on('SIGTERM', async () => {
  await db.close()
  process.exit(0)
})

bootstrap().catch(async (error) => {
  logError('bootstrap_failed', { message: error.message, stack: error.stack })
  await db.close().catch(() => {})
  process.exit(1)
})
