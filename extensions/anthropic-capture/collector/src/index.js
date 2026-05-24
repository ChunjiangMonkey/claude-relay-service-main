'use strict'

const fs = require('fs')
const fsPromises = require('fs/promises')
const path = require('path')
const crypto = require('crypto')
const { StringDecoder } = require('string_decoder')
const { createDbAdapter, normalizeBackend } = require('./db')
const { parseOpenaiSse, extractOutputContent } = require('./sseParser')

const DEFAULT_CAPTURE_DIR = '/data/relay-capture'
const DEFAULT_AUDIT_MAX_FILE_BYTES = 16 * 1024 * 1024
const DEFAULT_AUDIT_BACKUP_FILES = 3
const TRACE_AUDIT_CACHE_LIMIT = 10000
const DEFAULT_FILES = [
  'anthropic-upstream-requests.jsonl',
  'anthropic-upstream-responses.jsonl',
  'anthropic-upstream-stream-final.jsonl',
  'openai-upstream-requests.jsonl',
  'openai-upstream-responses.jsonl',
  'openai-upstream-stream-final.jsonl'
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
  dbPoolMax: parsePositiveInt(process.env.COLLECTOR_DB_POOL_MAX, 10),
  storeRawEvents: isEnabled(process.env.COLLECTOR_STORE_RAW_EVENTS, false),
  auditEnabled:
    isEnabled(process.env.CAPTURE_AUDIT_ENABLED, false) ||
    isEnabled(process.env.COLLECTOR_AUDIT_ENABLED, false),
  auditFile: resolveOutputPath(
    process.env.CAPTURE_COLLECTOR_AUDIT_FILE ||
      process.env.CAPTURE_AUDIT_FILE ||
      process.env.COLLECTOR_AUDIT_FILE ||
      'capture-audit.jsonl',
    process.env.ANTHROPIC_CAPTURE_DIR || DEFAULT_CAPTURE_DIR
  ),
  auditMaxFileBytes: parsePositiveInt(
    process.env.CAPTURE_AUDIT_MAX_FILE_BYTES || process.env.COLLECTOR_AUDIT_MAX_FILE_BYTES,
    DEFAULT_AUDIT_MAX_FILE_BYTES
  ),
  auditBackupFiles: parsePositiveInt(
    process.env.CAPTURE_AUDIT_BACKUP_FILES || process.env.COLLECTOR_AUDIT_BACKUP_FILES,
    DEFAULT_AUDIT_BACKUP_FILES
  )
}

validateConfig(config)

const db = createDbAdapter(config)
const states = new Map()
const traceAuditCache = new Map()
let auditWriteQueue = Promise.resolve()
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
    files: config.files,
    storeRawEvents: config.storeRawEvents,
    auditEnabled: config.auditEnabled,
    auditFile: config.auditFile
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

  let { offset, remainder } = prev
  remainder = remainder || ''
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

  const { offset: nextOffset, remainder: nextRemainder } = await processTextRange(
    filePath,
    offset,
    stat.size,
    remainder
  )
  offset = nextOffset
  remainder = nextRemainder

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

  let drainedLines = 0
  if (prevOffset < stat.size) {
    const result = await processTextRange(rotatedPath, prevOffset, stat.size, prevRemainder, {
      flushTail: true
    })
    drainedLines = result.processedLines
  } else if (prevOffset > stat.size) {
    logInfo('rotation_tail_offset_out_of_range', {
      rotatedPath,
      prevOffset,
      size: stat.size
    })
  } else if (prevRemainder) {
    const tailTrimmed = prevRemainder.trim()
    if (tailTrimmed) {
      await processLine(rotatedPath, tailTrimmed)
      drainedLines = 1
    }
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

async function processTextRange(filePath, startOffset, endOffset, initialRemainder, options = {}) {
  if (startOffset >= endOffset) {
    return {
      offset: startOffset,
      remainder: initialRemainder || '',
      processedLines: 0
    }
  }

  const stringDecoder = new StringDecoder('utf8')
  const stream = fs.createReadStream(filePath, {
    start: startOffset,
    end: endOffset - 1
  })

  let offset = startOffset
  let remainder = initialRemainder || ''
  let processedLines = 0

  for await (const buffer of stream) {
    offset += buffer.length
    const text = stringDecoder.write(buffer)
    if (!text) {
      continue
    }

    const merged = `${remainder}${text}`
    const lines = merged.split('\n')
    remainder = lines.pop() || ''

    for (const line of lines) {
      const trimmed = line.trim()
      if (!trimmed) {
        continue
      }
      await processLine(filePath, trimmed)
      processedLines += 1
    }
  }

  const trailingText = stringDecoder.end()
  if (trailingText) {
    const merged = `${remainder}${trailingText}`
    const lines = merged.split('\n')
    remainder = lines.pop() || ''

    for (const line of lines) {
      const trimmed = line.trim()
      if (!trimmed) {
        continue
      }
      await processLine(filePath, trimmed)
      processedLines += 1
    }
  }

  if (options.flushTail) {
    const tailTrimmed = remainder.trim()
    if (tailTrimmed) {
      await processLine(filePath, tailTrimmed)
      processedLines += 1
    }
    remainder = ''
  }

  return {
    offset,
    remainder,
    processedLines
  }
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
  const auditBase = buildCollectorAuditBase({
    payload,
    traceId,
    eventType,
    eventHash,
    sourceFile
  })

  if (config.storeRawEvents) {
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
  }

  if (eventType === 'anthropic_upstream_request') {
    await upsertRequest(traceId, payload)
    writeCollectorAudit('db_upserted', {
      ...auditBase,
      db_target: 'anthropic_interactions',
      operation: 'upsert_request'
    })
    return
  }

  if (eventType === 'anthropic_upstream_response_non_stream') {
    await upsertNonStreamResponse(traceId, payload)
    writeCollectorAudit('db_upserted', {
      ...auditBase,
      db_target: 'anthropic_interactions',
      operation: 'upsert_non_stream_response'
    })
    return
  }

  if (eventType === 'anthropic_upstream_stream_final') {
    await upsertStreamFinal(traceId, payload)
    writeCollectorAudit('db_upserted', {
      ...auditBase,
      db_target: 'anthropic_interactions',
      operation: 'upsert_stream_final'
    })
    return
  }

  if (eventType === 'anthropic_upstream_response_stream_summary') {
    await upsertStreamSummary(traceId, payload)
    writeCollectorAudit('db_upserted', {
      ...auditBase,
      db_target: 'anthropic_interactions',
      operation: 'upsert_stream_summary'
    })
    return
  }

  if (eventType === 'anthropic_upstream_response_transport_error') {
    await upsertTransportError(traceId, payload)
    writeCollectorAudit('db_upserted', {
      ...auditBase,
      db_target: 'anthropic_interactions',
      operation: 'upsert_transport_error'
    })
    return
  }

  if (eventType === 'openai_upstream_request') {
    await upsertOpenaiRequest(traceId, payload)
    writeCollectorAudit('db_upserted', {
      ...auditBase,
      db_target: 'openai_interactions',
      operation: 'upsert_openai_request'
    })
    return
  }

  if (eventType === 'openai_upstream_response_non_stream') {
    await upsertOpenaiNonStreamResponse(traceId, payload)
    writeCollectorAudit('db_upserted', {
      ...auditBase,
      db_target: 'openai_interactions',
      operation: 'upsert_openai_non_stream_response'
    })
    return
  }

  if (
    eventType === 'openai_upstream_stream_final' ||
    eventType === 'openai_upstream_response_stream_summary'
  ) {
    await upsertOpenaiStreamResponse(traceId, payload)
    writeCollectorAudit('db_upserted', {
      ...auditBase,
      db_target: 'openai_interactions',
      operation:
        eventType === 'openai_upstream_stream_final'
          ? 'upsert_openai_stream_final'
          : 'upsert_openai_stream_summary'
    })
    return
  }

  if (eventType === 'openai_upstream_response_transport_error') {
    await upsertOpenaiTransportError(traceId, payload)
    writeCollectorAudit('db_upserted', {
      ...auditBase,
      db_target: 'openai_interactions',
      operation: 'upsert_openai_transport_error'
    })
    return
  }

  writeCollectorAudit('unknown_event', auditBase)
}

async function upsertRequest(traceId, payload) {
  const requestJson = payload.request_body_json || null
  const model = payload.request_model || (requestJson && requestJson.model) || null
  const isStream = payload.request_stream === true || (requestJson && requestJson.stream === true)
  const upstreamRequestId = payload.relay_request_id || null
  const relayKeyId = payload.relay_key_id || null

  await db.upsertRequest({
    traceId,
    upstreamRequestId,
    model,
    isStream,
    requestJson,
    relayKeyId
  })
}

async function upsertNonStreamResponse(traceId, payload) {
  const responseJson =
    payload.response && payload.response.body_json ? payload.response.body_json : null
  const usage = payload.response && payload.response.usage ? payload.response.usage : null
  const stopReason =
    payload.response && payload.response.stop_reason ? payload.response.stop_reason : null
  const model =
    (payload.response && payload.response.model) ||
    (responseJson && responseJson.model) ||
    (payload.request && payload.request.model) ||
    null
  const httpStatus = extractInt(payload.upstream && payload.upstream.statusCode)
  const latencyMs = extractInt(payload.timing && payload.timing.latency_ms)
  const upstreamRequestId =
    (payload.request && payload.request.relay_request_id) || payload.relay_request_id || null
  const relayKeyId = payload.relay_key_id || null
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
    status: hasError ? 'error_non_stream' : 'completed_non_stream',
    relayKeyId
  })
}

async function upsertStreamFinal(traceId, payload) {
  const stream = payload.stream || {}
  const usage = stream.usage || null
  const toolCalls = Array.isArray(stream.tool_calls) ? stream.tool_calls : []
  const assistantTextFull =
    typeof stream.assistant_text_full === 'string' ? stream.assistant_text_full : ''
  const thoughtTextFull =
    typeof stream.thought_text_full === 'string' ? stream.thought_text_full : null
  const responseMessageId = typeof stream.message_id === 'string' ? stream.message_id : null
  const stopReason = stream.stop_reason || null
  const model = stream.message_model || (payload.request && payload.request.model) || null
  const httpStatus = extractInt(payload.upstream && payload.upstream.statusCode)
  const latencyMs = extractInt(payload.timing && payload.timing.latency_ms)
  const upstreamRequestId =
    (payload.request && payload.request.relay_request_id) || payload.relay_request_id || null
  const relayKeyId = payload.relay_key_id || null
  const hasError = Boolean(payload.error)

  await db.upsertStreamFinal({
    traceId,
    upstreamRequestId,
    model,
    assistantTextFull,
    thoughtTextFull,
    responseMessageId,
    toolCalls,
    usage,
    stopReason,
    httpStatus,
    latencyMs,
    status: hasError ? 'error_stream' : 'completed_stream',
    relayKeyId
  })
}

async function upsertStreamSummary(traceId, payload) {
  const usage = payload.usage || null
  const stopReason = payload.stop_reason || null
  const latencyMs = extractInt(payload.latency_ms)
  const relayKeyId = payload.relay_key_id || null
  const status = payload.error ? 'error_stream_summary' : 'stream_summary'

  await db.upsertStreamSummary({
    traceId,
    usage,
    stopReason,
    latencyMs,
    status,
    relayKeyId
  })
}

async function upsertTransportError(traceId, payload) {
  const model = payload.request && payload.request.model ? payload.request.model : null
  const isStream = payload.request && payload.request.stream === true
  const latencyMs = extractInt(payload.timing && payload.timing.latency_ms)
  const httpStatus = extractInt(payload.http_status)
  const upstreamRequestId =
    (payload.request && payload.request.relay_request_id) || payload.relay_request_id || null
  const relayKeyId = payload.relay_key_id || null

  await db.upsertTransportError({
    traceId,
    upstreamRequestId,
    model,
    isStream,
    httpStatus,
    latencyMs,
    relayKeyId
  })
}

async function upsertOpenaiRequest(traceId, payload) {
  const requestJson = payload.request_body_json || null
  const model = payload.request_model || (requestJson && requestJson.model) || null
  const isStream = payload.request_stream === true || (requestJson && requestJson.stream === true)
  const providerKind = payload.provider_kind || null
  const relayKeyId = payload.relay_key_id || null

  await db.upsertOpenaiRequest({
    traceId,
    providerKind,
    model,
    isStream,
    requestJson,
    relayKeyId
  })
}

async function upsertOpenaiNonStreamResponse(traceId, payload) {
  const resp = payload.response || {}
  const providerKind = payload.provider_kind || null
  const relayKeyId = payload.relay_key_id || null

  // Backward compatibility for old JSONL rows that only had SSE text in body_raw.
  const sseParsed = resp.body_raw ? parseOpenaiSse(resp.body_raw) : null

  let model = resp.model || null
  let responseId = resp.response_id || null
  let responseStatus = resp.status || null
  let usage = resp.usage || null
  let assistantTextFull = resp.assistant_text_full || ''
  let reasoningTextFull = resp.reasoning_text_full || ''
  let toolCalls = Array.isArray(resp.tool_calls) ? resp.tool_calls : []

  if (sseParsed) {
    model = sseParsed.model || model
    responseId = sseParsed.responseId || responseId
    responseStatus = sseParsed.status || responseStatus
    usage = sseParsed.usage || usage

    const {
      assistantTextFull: parsedAssistantTextFull,
      reasoningTextFull: parsedReasoningTextFull,
      toolCalls: parsedToolCalls
    } = extractOutputContent(sseParsed.output)
    assistantTextFull = parsedAssistantTextFull || assistantTextFull
    reasoningTextFull = parsedReasoningTextFull || reasoningTextFull
    if (parsedToolCalls.length > 0) {
      toolCalls = parsedToolCalls
    }
  }

  const httpStatus = extractInt(payload.upstream && payload.upstream.statusCode)

  if (!model && !usage && !sseParsed && httpStatus >= 200 && httpStatus < 300) {
    logInfo('openai_ingest_warning', {
      traceId,
      message: 'SSE parse returned null and response fields are empty'
    })
  }
  const latencyMs = extractInt(payload.timing && payload.timing.latency_ms)
  const hasError = Boolean(payload.error)
  const status = hasError ? 'error' : responseStatus || 'completed'

  const inputTokens = extractInt(usage && usage.input_tokens)
  const outputTokens = extractInt(usage && usage.output_tokens)
  const totalTokens = extractInt(usage && usage.total_tokens)
  const cachedTokens = extractInt(
    usage && usage.input_tokens_details && usage.input_tokens_details.cached_tokens
  )
  const reasoningTokens = extractInt(
    usage && usage.output_tokens_details && usage.output_tokens_details.reasoning_tokens
  )

  await db.upsertOpenaiResponse({
    traceId,
    providerKind,
    model,
    isStream: false,
    responseId,
    assistantTextFull: assistantTextFull || null,
    reasoningTextFull: reasoningTextFull || null,
    toolCalls: toolCalls.length > 0 ? toolCalls : null,
    usageJson: usage,
    inputTokens,
    outputTokens,
    totalTokens,
    cachedTokens,
    reasoningTokens,
    httpStatus,
    latencyMs,
    status,
    relayKeyId
  })
}

async function upsertOpenaiStreamResponse(traceId, payload) {
  const providerKind = payload.provider_kind || null
  const relayKeyId = payload.relay_key_id || null
  const stream = payload.stream || {}

  // stream_final has full data in .stream; stream_summary has flat fields
  const model = stream.response_model || payload.response_model || null
  const responseId = stream.response_id || payload.response_id || null
  const responseStatus = stream.status || payload.status || null
  const usage = stream.usage || payload.usage || null
  const assistantTextFull = stream.assistant_text_full || ''
  const reasoningTextFull = stream.reasoning_text_full || ''
  const toolCalls = Array.isArray(stream.tool_calls) ? stream.tool_calls : []

  const httpStatus = extractInt(
    (payload.upstream && payload.upstream.statusCode) || payload.statusCode
  )
  const latencyMs = extractInt((payload.timing && payload.timing.latency_ms) || payload.latency_ms)
  const hasError = Boolean(payload.error)
  const status = hasError ? 'error' : responseStatus || 'completed'

  const inputTokens = extractInt(usage && usage.input_tokens)
  const outputTokens = extractInt(usage && usage.output_tokens)
  const totalTokens = extractInt(usage && usage.total_tokens)
  const cachedTokens = extractInt(
    usage && usage.input_tokens_details && usage.input_tokens_details.cached_tokens
  )
  const reasoningTokens = extractInt(
    usage && usage.output_tokens_details && usage.output_tokens_details.reasoning_tokens
  )

  await db.upsertOpenaiResponse({
    traceId,
    providerKind,
    model,
    isStream: true,
    responseId,
    assistantTextFull: assistantTextFull || null,
    reasoningTextFull: reasoningTextFull || null,
    toolCalls: toolCalls.length > 0 ? toolCalls : null,
    usageJson: usage,
    inputTokens,
    outputTokens,
    totalTokens,
    cachedTokens,
    reasoningTokens,
    httpStatus,
    latencyMs,
    status,
    relayKeyId
  })
}

async function upsertOpenaiTransportError(traceId, payload) {
  const providerKind = payload.provider_kind || null
  const relayKeyId = payload.relay_key_id || null
  const request = payload.request || {}
  const model = request.request_model || request.model || null
  const isStream = request.request_stream === true || request.stream === true
  const httpStatus = extractInt(
    payload.http_status || payload.statusCode || (payload.upstream && payload.upstream.statusCode)
  )
  const latencyMs = extractInt((payload.timing && payload.timing.latency_ms) || payload.latency_ms)

  await db.upsertOpenaiResponse({
    traceId,
    providerKind,
    model,
    isStream,
    responseId: null,
    assistantTextFull: null,
    reasoningTextFull: null,
    toolCalls: null,
    usageJson: null,
    inputTokens: null,
    outputTokens: null,
    totalTokens: null,
    cachedTokens: null,
    reasoningTokens: null,
    httpStatus,
    latencyMs,
    status: 'transport_error',
    relayKeyId
  })
}

async function persistState(filePath, inode, offset, remainder) {
  await db.persistOffset({ filePath, inode, offset, remainder })
}

async function storeIngestError(sourceFile, rawLine, error) {
  await db.insertIngestError({ sourceFile, rawLine, error })
  writeCollectorAudit('ingest_error', {
    provider: 'unknown',
    event_type: 'ingest_error',
    trace_id: null,
    relay_key_id: null,
    source_file: sourceFile,
    event_hash: rawLine ? sha256(rawLine) : null,
    warnings: ['ingest_error'],
    error
  })
}

function buildCollectorAuditBase({ payload, traceId, eventType, eventHash, sourceFile }) {
  const relayKeyId = payload.relay_key_id || null
  const semantic = summarizeCollectorSemantics(payload, eventType)
  const warnings = [
    ...buildTraceAuditWarnings(traceId, relayKeyId, eventType),
    ...buildSemanticAuditWarnings(payload, eventType, semantic)
  ]

  return {
    provider: providerFromEventType(eventType),
    event_type: eventType,
    trace_id: traceId,
    relay_key_id: relayKeyId,
    source_file: path.basename(sourceFile),
    event_hash: eventHash,
    model: semantic.model,
    status: semantic.status,
    http_status: semantic.httpStatus,
    request_json_present: semantic.requestJsonPresent,
    request_json_bytes: semantic.requestJsonBytes,
    response_semantic_present: semantic.responseSemanticPresent,
    assistant_text_len: semantic.assistantTextLen,
    thinking_text_len: semantic.thinkingTextLen,
    reasoning_text_len: semantic.reasoningTextLen,
    tool_call_count: semantic.toolCallCount,
    usage_present: semantic.usagePresent,
    response_id: semantic.responseId,
    decode_error: semantic.decodeError,
    warnings
  }
}

function providerFromEventType(eventType) {
  if (eventType.startsWith('openai_')) {
    return 'openai'
  }
  if (eventType.startsWith('anthropic_')) {
    return 'anthropic'
  }
  return 'unknown'
}

function buildTraceAuditWarnings(traceId, relayKeyId, eventType) {
  const warnings = []
  const isFallbackTrace = String(traceId || '').startsWith('fallback_')
  if (isFallbackTrace) {
    warnings.push('missing_trace_id')
  }

  const cached = traceAuditCache.get(traceId)
  if (cached && cached.relayKeyId && relayKeyId && cached.relayKeyId !== relayKeyId) {
    warnings.push('relay_key_mismatch')
  }
  if (!cached && isResponseEvent(eventType)) {
    warnings.push('response_without_seen_request_in_current_collector')
  }

  if (!cached || relayKeyId) {
    rememberTraceAuditKey(traceId, relayKeyId, eventType)
  }

  return warnings
}

function rememberTraceAuditKey(traceId, relayKeyId, eventType) {
  if (!traceId) {
    return
  }
  if (traceAuditCache.size >= TRACE_AUDIT_CACHE_LIMIT) {
    const oldestKey = traceAuditCache.keys().next().value
    traceAuditCache.delete(oldestKey)
  }
  const existing = traceAuditCache.get(traceId) || {}
  traceAuditCache.set(traceId, {
    relayKeyId: relayKeyId || existing.relayKeyId || null,
    lastEventType: eventType,
    updatedAt: new Date().toISOString()
  })
}

function isResponseEvent(eventType) {
  return eventType.includes('_response_') || eventType.endsWith('_stream_final')
}

function buildSemanticAuditWarnings(payload, eventType, semantic) {
  const warnings = []
  if (isRequestEvent(eventType) && !semantic.requestJsonPresent) {
    warnings.push('missing_request_json')
  }
  if (semantic.decodeError) {
    warnings.push('decode_error')
  }
  if (
    isFinalResponseEvent(eventType) &&
    semantic.httpStatus >= 200 &&
    semantic.httpStatus < 300 &&
    !semantic.responseSemanticPresent
  ) {
    warnings.push('empty_success_response_semantics')
  }
  if (payload.error) {
    warnings.push('payload_error')
  }
  return warnings
}

function isRequestEvent(eventType) {
  return eventType === 'anthropic_upstream_request' || eventType === 'openai_upstream_request'
}

function isFinalResponseEvent(eventType) {
  return (
    eventType === 'anthropic_upstream_response_non_stream' ||
    eventType === 'anthropic_upstream_stream_final' ||
    eventType === 'openai_upstream_response_non_stream' ||
    eventType === 'openai_upstream_stream_final'
  )
}

function summarizeCollectorSemantics(payload, eventType) {
  const base = {
    model: null,
    status: null,
    httpStatus: extractInt(
      (payload.upstream && payload.upstream.statusCode) || payload.statusCode || payload.http_status
    ),
    requestJsonPresent: false,
    requestJsonBytes: 0,
    responseSemanticPresent: false,
    assistantTextLen: 0,
    thinkingTextLen: 0,
    reasoningTextLen: 0,
    toolCallCount: 0,
    usagePresent: false,
    responseId: null,
    decodeError:
      (payload.response && payload.response.decode_error) ||
      payload.decode_error ||
      (payload.stream &&
      Array.isArray(payload.stream.parse_errors) &&
      payload.stream.parse_errors.length > 0
        ? payload.stream.parse_errors.join(';')
        : null)
  }

  if (eventType === 'anthropic_upstream_request' || eventType === 'openai_upstream_request') {
    base.model =
      payload.request_model ||
      (payload.request_body_json && payload.request_body_json.model) ||
      null
    base.requestJsonPresent = Boolean(payload.request_body_json)
    base.requestJsonBytes = jsonByteLength(payload.request_body_json)
    return base
  }

  if (eventType === 'anthropic_upstream_response_non_stream') {
    const response = payload.response || {}
    const responseJson = response.body_json || null
    const contentSummary = summarizeAnthropicResponseJson(responseJson)
    base.model = response.model || (responseJson && responseJson.model) || null
    base.status = payload.error ? 'error_non_stream' : 'completed_non_stream'
    base.responseId = response.message_id || (responseJson && responseJson.id) || null
    base.assistantTextLen = contentSummary.assistantTextLen
    base.thinkingTextLen = contentSummary.thinkingTextLen
    base.toolCallCount = contentSummary.toolCallCount
    base.usagePresent = Boolean(response.usage || (responseJson && responseJson.usage))
    base.responseSemanticPresent = Boolean(
      responseJson ||
        base.assistantTextLen > 0 ||
        base.thinkingTextLen > 0 ||
        base.toolCallCount > 0 ||
        base.usagePresent
    )
    return base
  }

  if (eventType === 'anthropic_upstream_stream_final') {
    const stream = payload.stream || {}
    base.model = stream.message_model || (payload.request && payload.request.model) || null
    base.status = payload.error ? 'error_stream' : 'completed_stream'
    base.responseId = stream.message_id || null
    base.assistantTextLen = String(stream.assistant_text_full || '').length
    base.thinkingTextLen = String(stream.thought_text_full || '').length
    base.toolCallCount = Array.isArray(stream.tool_calls) ? stream.tool_calls.length : 0
    base.usagePresent = Boolean(stream.usage && Object.keys(stream.usage).length > 0)
    base.responseSemanticPresent = Boolean(
      base.assistantTextLen > 0 ||
        base.thinkingTextLen > 0 ||
        base.toolCallCount > 0 ||
        base.usagePresent
    )
    return base
  }

  if (eventType === 'openai_upstream_response_non_stream') {
    const response = payload.response || {}
    base.model = response.model || null
    base.status = response.status || (payload.error ? 'error' : 'completed')
    base.responseId = response.response_id || null
    base.assistantTextLen = String(response.assistant_text_full || '').length
    base.reasoningTextLen = String(response.reasoning_text_full || '').length
    base.toolCallCount = Array.isArray(response.tool_calls) ? response.tool_calls.length : 0
    base.usagePresent = Boolean(response.usage)
    base.responseSemanticPresent = Boolean(
      response.body_json ||
        base.assistantTextLen > 0 ||
        base.reasoningTextLen > 0 ||
        base.toolCallCount > 0 ||
        base.usagePresent
    )
    return base
  }

  if (eventType === 'openai_upstream_stream_final') {
    const stream = payload.stream || {}
    base.model = stream.response_model || null
    base.status = stream.status || (payload.error ? 'error' : 'completed')
    base.responseId = stream.response_id || null
    base.assistantTextLen = String(stream.assistant_text_full || '').length
    base.reasoningTextLen = String(stream.reasoning_text_full || '').length
    base.toolCallCount = Array.isArray(stream.tool_calls) ? stream.tool_calls.length : 0
    base.usagePresent = Boolean(stream.usage)
    base.responseSemanticPresent = Boolean(
      stream.response_json ||
        base.assistantTextLen > 0 ||
        base.reasoningTextLen > 0 ||
        base.toolCallCount > 0 ||
        base.usagePresent
    )
    return base
  }

  if (
    eventType === 'anthropic_upstream_response_stream_summary' ||
    eventType === 'openai_upstream_response_stream_summary'
  ) {
    base.model = payload.response_model || null
    base.status = payload.status || (payload.error ? 'error_stream_summary' : 'stream_summary')
    base.responseId = payload.response_id || null
    base.usagePresent = Boolean(payload.usage)
    base.responseSemanticPresent = Boolean(payload.usage || payload.stop_reason || payload.status)
    return base
  }

  return base
}

function summarizeAnthropicResponseJson(responseJson) {
  const summary = {
    assistantTextLen: 0,
    thinkingTextLen: 0,
    toolCallCount: 0
  }
  const content = Array.isArray(responseJson && responseJson.content) ? responseJson.content : []
  for (const block of content) {
    if (!block || typeof block !== 'object') {
      continue
    }
    if (block.type === 'text' && typeof block.text === 'string') {
      summary.assistantTextLen += block.text.length
      continue
    }
    if (
      (block.type === 'thinking' || block.type === 'redacted_thinking') &&
      typeof block.thinking === 'string'
    ) {
      summary.thinkingTextLen += block.thinking.length
      continue
    }
    if (
      (block.type === 'thinking' || block.type === 'redacted_thinking') &&
      typeof block.text === 'string'
    ) {
      summary.thinkingTextLen += block.text.length
      continue
    }
    if (block.type === 'tool_use') {
      summary.toolCallCount += 1
    }
  }
  return summary
}

function jsonByteLength(value) {
  if (value === undefined || value === null) {
    return 0
  }
  try {
    return Buffer.byteLength(JSON.stringify(value), 'utf8')
  } catch (_) {
    return 0
  }
}

function writeCollectorAudit(event, payload) {
  if (!config.auditEnabled) {
    return
  }

  const line = `${JSON.stringify({
    ts: new Date().toISOString(),
    source: 'collector',
    event,
    ...payload
  })}\n`

  auditWriteQueue = auditWriteQueue
    .then(() => appendAuditLine(line))
    .catch((error) => {
      logError('audit_write_failed', { message: error.message })
    })
}

async function appendAuditLine(line) {
  await fsPromises.mkdir(path.dirname(config.auditFile), { recursive: true })
  const maxFileBytes = config.auditMaxFileBytes
  const backupFiles = config.auditBackupFiles
  const nextSize = Buffer.byteLength(line, 'utf8')

  let currentSize = 0
  try {
    const stat = await fsPromises.stat(config.auditFile)
    currentSize = stat.size
  } catch (_) {
    currentSize = 0
  }

  if (currentSize + nextSize > maxFileBytes) {
    await rotateAuditFile(config.auditFile, backupFiles)
  }

  await fsPromises.appendFile(config.auditFile, line, 'utf8')
}

async function rotateAuditFile(filePath, backupFiles) {
  if (backupFiles <= 0) {
    await fsPromises.unlink(filePath).catch(() => {})
    return
  }

  for (let i = backupFiles - 1; i >= 1; i -= 1) {
    const src = i === 1 ? `${filePath}.bak` : `${filePath}.bak.${i - 1}`
    const dest = `${filePath}.bak.${i}`
    await fsPromises.rename(src, dest).catch(() => {})
  }

  await fsPromises.rename(filePath, `${filePath}.bak`).catch(() => {})
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
    if (!runtimeConfig.mysql.host) {
      missing.push('MYSQL_HOST')
    }
    if (!runtimeConfig.mysql.user) {
      missing.push('MYSQL_USER')
    }
    if (!runtimeConfig.mysql.database) {
      missing.push('MYSQL_DATABASE')
    }

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

function resolveOutputPath(rawPath, captureDir) {
  const outputPath = String(rawPath || '').trim()
  if (!outputPath) {
    return path.join(captureDir, 'capture-audit.jsonl')
  }
  return path.isAbsolute(outputPath) ? outputPath : path.join(captureDir, outputPath)
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
