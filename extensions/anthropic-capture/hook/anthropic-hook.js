'use strict'

const https = require('https')
const fs = require('fs/promises')
const path = require('path')
const crypto = require('crypto')

const PATCH_SENTINEL = Symbol.for('claude_relay.anthropic_capture_hook.installed')

if (global[PATCH_SENTINEL]) {
  return
}

global[PATCH_SENTINEL] = true

const REQUESTS_FILE = 'anthropic-upstream-requests.jsonl'
const RESPONSES_FILE = 'anthropic-upstream-responses.jsonl'
const STREAM_FINAL_FILE = 'anthropic-upstream-stream-final.jsonl'

const DEFAULT_CAPTURE_DIR = '/data/relay-capture'
const DEFAULT_MAX_RECORD_BYTES = 16 * 1024 * 1024
const DEFAULT_MAX_FILE_BYTES = 256 * 1024 * 1024
const DEFAULT_BACKUP_FILES = 3

const config = {
  enabled: isEnabled(process.env.ANTHROPIC_CAPTURE_ENABLED, true),
  captureDir: process.env.ANTHROPIC_CAPTURE_DIR || DEFAULT_CAPTURE_DIR,
  hosts: parseHosts(process.env.ANTHROPIC_CAPTURE_HOSTS || 'api.anthropic.com'),
  maxRecordBytes: parsePositiveInt(
    process.env.ANTHROPIC_CAPTURE_MAX_RECORD_BYTES,
    DEFAULT_MAX_RECORD_BYTES
  ),
  maxFileBytes: parsePositiveInt(process.env.ANTHROPIC_CAPTURE_MAX_FILE_BYTES, DEFAULT_MAX_FILE_BYTES),
  backupFiles: parsePositiveInt(process.env.ANTHROPIC_CAPTURE_BACKUP_FILES, DEFAULT_BACKUP_FILES),
  includeThinking: isEnabled(process.env.ANTHROPIC_CAPTURE_INCLUDE_THINKING, false),
  debug: isEnabled(process.env.ANTHROPIC_CAPTURE_DEBUG, false)
}

const writeQueues = new Map()
let initPromise = null

if (!config.enabled) {
  logDebug('Anthropic capture hook loaded but disabled')
  return
}

patchHttpsRequest()
logDebug('Anthropic capture hook installed', {
  captureDir: config.captureDir,
  hosts: Array.from(config.hosts),
  maxRecordBytes: config.maxRecordBytes,
  maxFileBytes: config.maxFileBytes
})

function isEnabled(rawValue, defaultValue = false) {
  if (rawValue === undefined || rawValue === null || rawValue === '') {
    return defaultValue
  }
  const normalized = String(rawValue).trim().toLowerCase()
  return normalized === '1' || normalized === 'true' || normalized === 'yes'
}

function parsePositiveInt(rawValue, fallback) {
  const parsed = Number.parseInt(String(rawValue || ''), 10)
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback
  }
  return parsed
}

function parseHosts(rawValue) {
  return new Set(
    String(rawValue || '')
      .split(',')
      .map((item) => item.trim().toLowerCase())
      .filter(Boolean)
  )
}

function maskSecret(value) {
  if (value === undefined || value === null) {
    return value
  }
  const str = String(value)
  if (str.length <= 8) {
    return '***'
  }
  return `${str.slice(0, 4)}...${str.slice(-4)}`
}

function sanitizeHeaders(headers) {
  const source = headers || {}
  const sensitive = new Set([
    'authorization',
    'proxy-authorization',
    'x-api-key',
    'cookie',
    'set-cookie',
    'x-forwarded-for',
    'x-real-ip'
  ])

  const output = {}
  for (const [rawKey, rawValue] of Object.entries(source)) {
    const key = String(rawKey).toLowerCase()
    if (sensitive.has(key)) {
      output[key] = maskSecret(rawValue)
      continue
    }
    output[key] = rawValue
  }
  return output
}

function parseMaybeJson(text) {
  if (!text || typeof text !== 'string') {
    return null
  }
  try {
    return JSON.parse(text)
  } catch (_) {
    return null
  }
}

function safeJsonStringify(payload, maxBytes, eventType) {
  let json = ''
  try {
    json = JSON.stringify(payload)
  } catch (error) {
    return JSON.stringify({
      type: `${eventType}_stringify_error`,
      error: 'JSON.stringify_failed',
      message: error && error.message ? error.message : String(error)
    })
  }

  const size = Buffer.byteLength(json, 'utf8')
  if (size <= maxBytes) {
    return json
  }

  const partial = Buffer.from(json, 'utf8').subarray(0, maxBytes).toString('utf8')
  return JSON.stringify({
    type: `${eventType}_truncated`,
    truncated: true,
    maxBytes,
    originalBytes: size,
    partialJson: partial
  })
}

function normalizeRequestMeta(firstArg, secondArg) {
  const defaults = {
    protocol: 'https:',
    method: 'GET',
    hostname: '',
    host: '',
    path: '/',
    headers: {}
  }

  let options = null

  if (typeof firstArg === 'string') {
    try {
      const url = new URL(firstArg)
      options = {
        protocol: url.protocol || defaults.protocol,
        method: defaults.method,
        hostname: url.hostname,
        host: url.host,
        path: `${url.pathname || '/'}${url.search || ''}`,
        headers: {}
      }
    } catch (_) {
      options = { ...defaults }
    }

    if (secondArg && typeof secondArg === 'object') {
      options = { ...options, ...secondArg }
    }
  } else if (firstArg instanceof URL) {
    options = {
      protocol: firstArg.protocol || defaults.protocol,
      method: defaults.method,
      hostname: firstArg.hostname,
      host: firstArg.host,
      path: `${firstArg.pathname || '/'}${firstArg.search || ''}`,
      headers: {}
    }

    if (secondArg && typeof secondArg === 'object') {
      options = { ...options, ...secondArg }
    }
  } else if (firstArg && typeof firstArg === 'object') {
    options = { ...defaults, ...firstArg }
  } else {
    options = { ...defaults }
  }

  const hostFromOptions = options.host || options.hostname || ''
  const hostname = String(
    options.hostname || String(hostFromOptions).split(':')[0] || ''
  ).toLowerCase()

  const pathValue =
    options.path ||
    (options.pathname ? `${options.pathname}${options.search || ''}` : defaults.path)

  return {
    protocol: options.protocol || defaults.protocol,
    method: String(options.method || defaults.method).toUpperCase(),
    hostname,
    host: hostFromOptions || hostname,
    path: typeof pathValue === 'string' ? pathValue : defaults.path,
    headers: options.headers || {}
  }
}

function shouldCaptureRequest(meta) {
  if (!meta || !meta.hostname) {
    return false
  }
  return config.hosts.has(meta.hostname)
}

function createTraceId() {
  const uuid = typeof crypto.randomUUID === 'function' ? crypto.randomUUID() : crypto.randomBytes(16).toString('hex')
  return `trc_${uuid}`
}

function mergeUsage(target, source) {
  if (!source || typeof source !== 'object') {
    return target
  }

  const numericFields = [
    'input_tokens',
    'output_tokens',
    'cache_creation_input_tokens',
    'cache_read_input_tokens'
  ]

  numericFields.forEach((key) => {
    if (Number.isFinite(source[key])) {
      target[key] = source[key]
    }
  })

  if (source.cache_creation && typeof source.cache_creation === 'object') {
    target.cache_creation = {
      ...(target.cache_creation || {}),
      ...source.cache_creation
    }
  }

  return target
}

function createStreamState(includeThinking) {
  return {
    eventCount: 0,
    eventTypes: new Set(),
    messageId: null,
    messageModel: null,
    stopReason: null,
    usage: {},
    assistantTextFull: '',
    thoughtTextFull: '',
    blocks: new Map(),
    toolCalls: [],
    parseErrors: []
  }
}

function ensureBlock(state, index) {
  if (!state.blocks.has(index)) {
    state.blocks.set(index, {
      index,
      type: null,
      id: null,
      name: null,
      text: '',
      input: null,
      inputJsonFragments: []
    })
  }
  return state.blocks.get(index)
}

function flushToolBlock(block, state) {
  if (!block || block.type !== 'tool_use') {
    return
  }

  let resolvedInput = block.input
  if (!resolvedInput && block.inputJsonFragments.length > 0) {
    const raw = block.inputJsonFragments.join('')
    const parsed = parseMaybeJson(raw)
    resolvedInput = parsed !== null ? parsed : raw
  }

  state.toolCalls.push({
    index: block.index,
    id: block.id || null,
    name: block.name || null,
    input: resolvedInput || null
  })
}

function applySsePayload(state, payload, eventName, includeThinking) {
  if (!payload || typeof payload !== 'object') {
    return
  }

  state.eventCount += 1
  state.eventTypes.add(eventName || payload.type || 'unknown')

  if (payload.type === 'message_start' && payload.message) {
    if (payload.message.id) {
      state.messageId = payload.message.id
    }
    if (payload.message.model) {
      state.messageModel = payload.message.model
    }
    mergeUsage(state.usage, payload.message.usage)
    return
  }

  if (payload.type === 'message_delta') {
    if (payload.delta && payload.delta.stop_reason !== undefined) {
      state.stopReason = payload.delta.stop_reason
    }
    mergeUsage(state.usage, payload.usage)
    return
  }

  if (payload.type === 'content_block_start') {
    const index = Number.isFinite(payload.index) ? payload.index : 0
    const block = ensureBlock(state, index)
    const contentBlock = payload.content_block || {}

    block.type = contentBlock.type || block.type
    block.id = contentBlock.id || block.id
    block.name = contentBlock.name || block.name

    if (contentBlock.input && typeof contentBlock.input === 'object') {
      block.input = contentBlock.input
    }

    if (typeof contentBlock.text === 'string') {
      block.text += contentBlock.text
      if (block.type === 'text') {
        state.assistantTextFull += contentBlock.text
      } else if (includeThinking && (block.type === 'thinking' || block.type === 'redacted_thinking')) {
        state.thoughtTextFull += contentBlock.text
      }
    }
    return
  }

  if (payload.type === 'content_block_delta') {
    const index = Number.isFinite(payload.index) ? payload.index : 0
    const block = ensureBlock(state, index)
    const delta = payload.delta || {}

    const deltaText = typeof delta.text === 'string' ? delta.text : ''
    if (deltaText) {
      block.text += deltaText
      if (block.type === 'text' || delta.type === 'text_delta') {
        state.assistantTextFull += deltaText
      } else if (
        includeThinking &&
        (block.type === 'thinking' || block.type === 'redacted_thinking' || delta.type === 'thinking_delta')
      ) {
        state.thoughtTextFull += deltaText
      }
    }

    if (delta.type === 'input_json_delta' && typeof delta.partial_json === 'string') {
      block.inputJsonFragments.push(delta.partial_json)
    }

    return
  }

  if (payload.type === 'content_block_stop') {
    const index = Number.isFinite(payload.index) ? payload.index : 0
    const block = state.blocks.get(index)
    if (!block) {
      return
    }
    flushToolBlock(block, state)
    state.blocks.delete(index)
    return
  }

  if (payload.type === 'error' && payload.error && payload.error.message) {
    state.parseErrors.push(String(payload.error.message))
  }
}

function buildStreamRecord(traceId, requestRecord, requestMeta, responseMeta, streamState, timing, error) {
  for (const block of streamState.blocks.values()) {
    flushToolBlock(block, streamState)
  }

  const requestJson = requestRecord.requestBodyJson || null
  return {
    ts: new Date().toISOString(),
    type: 'anthropic_upstream_stream_final',
    trace_id: traceId,
    capture_version: 1,
    upstream: {
      protocol: requestMeta.protocol,
      host: requestMeta.host,
      hostname: requestMeta.hostname,
      path: requestMeta.path,
      method: requestMeta.method,
      statusCode: responseMeta.statusCode,
      headers: responseMeta.headers
    },
    request: {
      relay_request_id: requestRecord.relayRequestId,
      model:
        requestRecord.requestModel ||
        (requestJson && typeof requestJson.model === 'string' ? requestJson.model : null),
      stream: true
    },
    stream: {
      event_count: streamState.eventCount,
      event_types: Array.from(streamState.eventTypes),
      message_id: streamState.messageId,
      message_model: streamState.messageModel,
      assistant_text_full: streamState.assistantTextFull,
      thought_text_full: config.includeThinking ? streamState.thoughtTextFull : undefined,
      tool_calls: streamState.toolCalls,
      usage: streamState.usage,
      stop_reason: streamState.stopReason,
      parse_errors: streamState.parseErrors
    },
    timing: {
      started_at: timing.startedAt,
      ended_at: timing.endedAt,
      latency_ms: timing.latencyMs
    },
    error: error
      ? {
          message: error.message || String(error),
          code: error.code || null
        }
      : null
  }
}

function buildNonStreamRecord(traceId, requestRecord, requestMeta, responseMeta, responseBodyRaw, timing, error) {
  const responseJson = parseMaybeJson(responseBodyRaw)
  const requestJson = requestRecord.requestBodyJson || null

  return {
    ts: new Date().toISOString(),
    type: 'anthropic_upstream_response_non_stream',
    trace_id: traceId,
    capture_version: 1,
    upstream: {
      protocol: requestMeta.protocol,
      host: requestMeta.host,
      hostname: requestMeta.hostname,
      path: requestMeta.path,
      method: requestMeta.method,
      statusCode: responseMeta.statusCode,
      headers: responseMeta.headers
    },
    request: {
      relay_request_id: requestRecord.relayRequestId,
      model:
        requestRecord.requestModel ||
        (requestJson && typeof requestJson.model === 'string' ? requestJson.model : null),
      stream: Boolean(requestRecord.requestStream)
    },
    response: {
      body_raw: responseBodyRaw,
      body_json: responseJson,
      usage: responseJson && responseJson.usage ? responseJson.usage : null,
      stop_reason: responseJson && responseJson.stop_reason ? responseJson.stop_reason : null,
      message_id: responseJson && responseJson.id ? responseJson.id : null,
      model: responseJson && responseJson.model ? responseJson.model : null
    },
    timing: {
      started_at: timing.startedAt,
      ended_at: timing.endedAt,
      latency_ms: timing.latencyMs
    },
    error: error
      ? {
          message: error.message || String(error),
          code: error.code || null
        }
      : null
  }
}

function patchHttpsRequest() {
  const originalRequest = https.request

  https.request = function patchedRequest(...args) {
    const requestMeta = normalizeRequestMeta(args[0], args[1])
    if (!shouldCaptureRequest(requestMeta)) {
      return originalRequest.apply(this, args)
    }

    const traceId = createTraceId()
    const startedAt = new Date().toISOString()

    const requestChunks = []
    const requestRecord = {
      requestBodyRaw: '',
      requestBodyJson: null,
      requestModel: null,
      requestStream: false,
      relayRequestId: null
    }

    let requestWritten = false
    let responseFinished = false

    const req = originalRequest.apply(this, args)

    const originalWrite = req.write.bind(req)
    req.write = function patchedWrite(chunk, encoding, callback) {
      captureChunk(requestChunks, chunk, encoding)
      return originalWrite(chunk, encoding, callback)
    }

    const originalEnd = req.end.bind(req)
    req.end = function patchedEnd(chunk, encoding, callback) {
      captureChunk(requestChunks, chunk, encoding)
      if (!requestWritten) {
        requestWritten = true
        persistRequestRecord(traceId, requestMeta, requestChunks, requestRecord)
      }
      return originalEnd(chunk, encoding, callback)
    }

    const attachResponseCapture = (res) => {
      if (res.__anthropicCaptureAttached) {
        return
      }
      res.__anthropicCaptureAttached = true

      const responseHeaders = sanitizeHeaders(res.headers || {})
      const contentTypeRaw = responseHeaders['content-type'] || responseHeaders['Content-Type'] || ''
      const contentType = String(contentTypeRaw || '').toLowerCase()
      const isStreamResponse = contentType.includes('text/event-stream')

      let sseBuffer = ''
      let currentEventName = ''
      const streamState = createStreamState(config.includeThinking)
      const responseChunks = []

      res.on('data', (chunk) => {
        if (isStreamResponse) {
          const chunkText = bufferToUtf8(chunk)
          sseBuffer += chunkText
          const lines = sseBuffer.split('\n')
          sseBuffer = lines.pop() || ''

          for (const rawLine of lines) {
            const line = rawLine.replace(/\r$/, '')
            if (!line) {
              currentEventName = ''
              continue
            }

            if (line.startsWith('event:')) {
              currentEventName = line.slice(6).trim()
              continue
            }

            if (!line.startsWith('data:')) {
              continue
            }

            const payloadRaw = line.slice(5).trimStart()
            if (!payloadRaw || payloadRaw === '[DONE]') {
              continue
            }

            const parsed = parseMaybeJson(payloadRaw)
            if (!parsed) {
              streamState.parseErrors.push('invalid_json_data_line')
              continue
            }

            applySsePayload(streamState, parsed, currentEventName, config.includeThinking)
          }
          return
        }

        responseChunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk), 'utf8'))
      })

      const finalize = (error) => {
        if (responseFinished) {
          return
        }
        responseFinished = true

        const endedAt = new Date().toISOString()
        const latencyMs = Date.now() - Date.parse(startedAt)

        const responseMeta = {
          statusCode: res.statusCode || null,
          headers: responseHeaders
        }

        if (isStreamResponse) {
          if (sseBuffer.trim()) {
            const tailLines = sseBuffer.split('\n')
            for (const rawLine of tailLines) {
              const line = rawLine.replace(/\r$/, '')
              if (!line.startsWith('data:')) {
                continue
              }
              const payloadRaw = line.slice(5).trimStart()
              const parsed = parseMaybeJson(payloadRaw)
              if (parsed) {
                applySsePayload(streamState, parsed, currentEventName, config.includeThinking)
              }
            }
          }

          const streamRecord = buildStreamRecord(
            traceId,
            requestRecord,
            requestMeta,
            responseMeta,
            streamState,
            { startedAt, endedAt, latencyMs },
            error
          )

          writeJsonl(STREAM_FINAL_FILE, streamRecord)
          writeJsonl(RESPONSES_FILE, {
            ts: new Date().toISOString(),
            type: 'anthropic_upstream_response_stream_summary',
            trace_id: traceId,
            statusCode: responseMeta.statusCode,
            stop_reason: streamRecord.stream.stop_reason,
            usage: streamRecord.stream.usage,
            event_count: streamRecord.stream.event_count,
            latency_ms: latencyMs,
            error: streamRecord.error
          })
          return
        }

        const responseBodyRaw = Buffer.concat(responseChunks).toString('utf8')
        const nonStreamRecord = buildNonStreamRecord(
          traceId,
          requestRecord,
          requestMeta,
          responseMeta,
          responseBodyRaw,
          { startedAt, endedAt, latencyMs },
          error
        )
        writeJsonl(RESPONSES_FILE, nonStreamRecord)
      }

      res.on('end', () => finalize(null))
      res.on('error', (error) => finalize(error))
    }

    req.on('response', attachResponseCapture)

    req.on('error', (error) => {
      if (!requestWritten) {
        requestWritten = true
        persistRequestRecord(traceId, requestMeta, requestChunks, requestRecord)
      }

      if (responseFinished) {
        return
      }

      const endedAt = new Date().toISOString()
      const latencyMs = Date.now() - Date.parse(startedAt)

      writeJsonl(RESPONSES_FILE, {
        ts: new Date().toISOString(),
        type: 'anthropic_upstream_response_transport_error',
        trace_id: traceId,
        capture_version: 1,
        upstream: {
          protocol: requestMeta.protocol,
          host: requestMeta.host,
          hostname: requestMeta.hostname,
          path: requestMeta.path,
          method: requestMeta.method
        },
        request: {
          relay_request_id: requestRecord.relayRequestId,
          model: requestRecord.requestModel,
          stream: Boolean(requestRecord.requestStream)
        },
        timing: {
          started_at: startedAt,
          ended_at: endedAt,
          latency_ms: latencyMs
        },
        error: {
          message: error && error.message ? error.message : String(error),
          code: error && error.code ? error.code : null
        }
      })

      responseFinished = true
    })

    return req
  }
}

function persistRequestRecord(traceId, requestMeta, requestChunks, requestRecord) {
  const requestBodyRaw = Buffer.concat(requestChunks).toString('utf8')
  const requestBodyJson = parseMaybeJson(requestBodyRaw)

  requestRecord.requestBodyRaw = requestBodyRaw
  requestRecord.requestBodyJson = requestBodyJson
  requestRecord.requestModel =
    requestBodyJson && typeof requestBodyJson.model === 'string' ? requestBodyJson.model : null
  requestRecord.requestStream = requestBodyJson && requestBodyJson.stream === true
  requestRecord.relayRequestId =
    getHeaderCaseInsensitive(requestMeta.headers, 'x-request-id') ||
    getHeaderCaseInsensitive(requestMeta.headers, 'x-requestid') ||
    null

  writeJsonl(REQUESTS_FILE, {
    ts: new Date().toISOString(),
    type: 'anthropic_upstream_request',
    trace_id: traceId,
    capture_version: 1,
    upstream: {
      protocol: requestMeta.protocol,
      host: requestMeta.host,
      hostname: requestMeta.hostname,
      path: requestMeta.path,
      method: requestMeta.method
    },
    relay_request_id: requestRecord.relayRequestId,
    headers: sanitizeHeaders(requestMeta.headers),
    request_body_raw: requestBodyRaw,
    request_body_json: requestBodyJson,
    request_model: requestRecord.requestModel,
    request_stream: requestRecord.requestStream
  })
}

function getHeaderCaseInsensitive(headers, targetKey) {
  if (!headers || typeof headers !== 'object') {
    return undefined
  }
  const lowerTarget = String(targetKey).toLowerCase()
  for (const [key, value] of Object.entries(headers)) {
    if (String(key).toLowerCase() === lowerTarget) {
      return value
    }
  }
  return undefined
}

function captureChunk(chunks, chunk, encoding) {
  if (chunk === undefined || chunk === null) {
    return
  }

  // req.end(callback) / req.write(chunk, callback): callback must not be treated as payload
  if (typeof chunk === 'function') {
    return
  }

  if (Buffer.isBuffer(chunk)) {
    chunks.push(chunk)
    return
  }

  if (chunk instanceof ArrayBuffer) {
    chunks.push(Buffer.from(chunk))
    return
  }

  if (ArrayBuffer.isView(chunk)) {
    chunks.push(Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength))
    return
  }

  if (typeof chunk === 'string') {
    chunks.push(Buffer.from(chunk, encoding || 'utf8'))
    return
  }

  // Ignore unsupported chunk types to avoid corrupting captured request payload.
  // (e.g. String(object) would produce inaccurate body text)
}

function bufferToUtf8(value) {
  if (Buffer.isBuffer(value)) {
    return value.toString('utf8')
  }
  return typeof value === 'string' ? value : String(value)
}

function writeJsonl(filename, payload) {
  const filePath = path.join(config.captureDir, filename)
  const line = `${safeJsonStringify(payload, config.maxRecordBytes, payload.type || 'capture_event')}\n`

  queueFileWrite(filePath, line)
}

function queueFileWrite(filePath, line) {
  const previous = writeQueues.get(filePath) || Promise.resolve()

  const current = previous
    .then(async () => {
      await ensureInitialized()
      await appendWithRotate(filePath, line)
    })
    .catch((error) => {
      logError('Failed to write capture line', {
        filePath,
        error: error && error.message ? error.message : String(error)
      })
    })

  writeQueues.set(filePath, current)
}

async function ensureInitialized() {
  if (!initPromise) {
    initPromise = fs.mkdir(config.captureDir, { recursive: true })
  }
  return initPromise
}

async function appendWithRotate(filePath, line) {
  const maxFileBytes = config.maxFileBytes
  const nextSize = Buffer.byteLength(line, 'utf8')

  let currentSize = 0
  try {
    const stat = await fs.stat(filePath)
    currentSize = stat.size
  } catch (_) {
    currentSize = 0
  }

  if (currentSize + nextSize > maxFileBytes) {
    await rotateFile(filePath)
  }

  await fs.appendFile(filePath, line, { encoding: 'utf8' })
}

async function rotateFile(filePath) {
  if (config.backupFiles <= 0) {
    await fs.unlink(filePath).catch(() => {})
    return
  }

  const backupLimit = config.backupFiles

  // Shift old backups: .bak.(n-1) -> .bak.n
  for (let i = backupLimit - 1; i >= 1; i -= 1) {
    const src = i === 1 ? `${filePath}.bak` : `${filePath}.bak.${i - 1}`
    const dest = `${filePath}.bak.${i}`
    await fs.rename(src, dest).catch(() => {})
  }

  await fs.rename(filePath, `${filePath}.bak`).catch(() => {})
}

function logDebug(message, meta) {
  if (!config.debug) {
    return
  }
  if (meta) {
    // eslint-disable-next-line no-console
    console.log(`[anthropic-capture] ${message}`, meta)
    return
  }
  // eslint-disable-next-line no-console
  console.log(`[anthropic-capture] ${message}`)
}

function logError(message, meta) {
  if (meta) {
    // eslint-disable-next-line no-console
    console.error(`[anthropic-capture] ${message}`, meta)
    return
  }
  // eslint-disable-next-line no-console
  console.error(`[anthropic-capture] ${message}`)
}
