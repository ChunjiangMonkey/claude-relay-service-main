'use strict'

const https = require('https')
const fs = require('fs/promises')
const path = require('path')
const crypto = require('crypto')
const zlib = require('zlib')
const { IncrementalSSEParser } = require('../../../src/utils/sseParser')

const PATCH_SENTINEL = Symbol.for('claude_relay.openai_capture_hook.installed')

if (global[PATCH_SENTINEL]) {
  return
}

global[PATCH_SENTINEL] = true

const REQUESTS_FILE = 'openai-upstream-requests.jsonl'
const RESPONSES_FILE = 'openai-upstream-responses.jsonl'
const STREAM_FINAL_FILE = 'openai-upstream-stream-final.jsonl'

const DEFAULT_CAPTURE_DIR = '/data/relay-capture'
const DEFAULT_MAX_RECORD_BYTES = 16 * 1024 * 1024
const DEFAULT_MAX_FILE_BYTES = 256 * 1024 * 1024
const DEFAULT_BACKUP_FILES = 3

const config = {
  enabled: isEnabled(process.env.OPENAI_CAPTURE_ENABLED, false),
  captureDir: process.env.OPENAI_CAPTURE_DIR || DEFAULT_CAPTURE_DIR,
  hosts: parseHosts(process.env.OPENAI_CAPTURE_HOSTS || 'api.openai.com,chatgpt.com'),
  captureMethods: parseCaptureMethods(process.env.OPENAI_CAPTURE_METHODS || 'POST'),
  capturePathPrefixes: parseCapturePathPrefixes(
    process.env.OPENAI_CAPTURE_PATH_PREFIXES ||
      '/v1/responses,/responses,/backend-api/codex/responses'
  ),
  maxRecordBytes: parsePositiveInt(
    process.env.OPENAI_CAPTURE_MAX_RECORD_BYTES,
    DEFAULT_MAX_RECORD_BYTES
  ),
  maxFileBytes: parsePositiveInt(process.env.OPENAI_CAPTURE_MAX_FILE_BYTES, DEFAULT_MAX_FILE_BYTES),
  backupFiles: parsePositiveInt(process.env.OPENAI_CAPTURE_BACKUP_FILES, DEFAULT_BACKUP_FILES),
  includeReasoning: isEnabled(process.env.OPENAI_CAPTURE_INCLUDE_REASONING, false),
  debug: isEnabled(process.env.OPENAI_CAPTURE_DEBUG, false)
}

const writeQueues = new Map()
let initPromise = null

if (!config.enabled) {
  logDebug('OpenAI capture hook loaded but disabled')
  return
}

patchHttpsRequest()
logDebug('OpenAI capture hook installed', {
  captureDir: config.captureDir,
  hosts: Array.from(config.hosts),
  captureMethods: config.captureMethods ? Array.from(config.captureMethods) : ['*'],
  capturePathPrefixes: config.capturePathPrefixes || ['*'],
  maxRecordBytes: config.maxRecordBytes,
  maxFileBytes: config.maxFileBytes,
  includeReasoning: config.includeReasoning
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

function parseCsvList(rawValue, fallbackRaw) {
  const source =
    rawValue !== undefined && rawValue !== null && rawValue !== '' ? rawValue : fallbackRaw
  return String(source || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean)
}

function parseCaptureMethods(rawValue) {
  const methods = parseCsvList(rawValue, 'POST')
  if (methods.includes('*')) {
    return null
  }
  return new Set(methods.map((item) => item.toUpperCase()))
}

function parseCapturePathPrefixes(rawValue) {
  const prefixes = parseCsvList(rawValue, '/v1/responses,/responses,/backend-api/codex/responses')
  if (prefixes.includes('*')) {
    return null
  }
  const normalized = prefixes.map((item) => (item.startsWith('/') ? item : `/${item}`))
  return normalized.length > 0
    ? normalized
    : ['/v1/responses', '/responses', '/backend-api/codex/responses']
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
  if (size > maxBytes) {
    logDebug(
      `⚠️ Large record: ${eventType} is ${(size / 1024 / 1024).toFixed(2)}MB (limit was ${(maxBytes / 1024 / 1024).toFixed(0)}MB), keeping full record`
    )
  }
  return json
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
  if (!config.hosts.has(meta.hostname)) {
    return false
  }
  if (meta.protocol !== 'https:') {
    return false
  }
  if (
    config.captureMethods &&
    !config.captureMethods.has(String(meta.method || '').toUpperCase())
  ) {
    return false
  }
  if (!config.capturePathPrefixes) {
    return true
  }
  return config.capturePathPrefixes.some((prefix) => String(meta.path || '').startsWith(prefix))
}

function determineProviderKind(meta) {
  if (!meta) {
    return 'openai-responses'
  }
  if (
    meta.hostname === 'chatgpt.com' &&
    String(meta.path || '').startsWith('/backend-api/codex/responses')
  ) {
    return 'chatgpt-codex'
  }
  return 'openai-responses'
}

function createTraceId() {
  return `trc_${crypto.randomUUID()}`
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

function extractAuthInfo(headers) {
  const authorization = getHeaderCaseInsensitive(headers, 'authorization')
  if (authorization) {
    return {
      authorizationHeaderName: 'authorization',
      authorizationRaw: String(authorization),
      authorizationSha256: sha256String(String(authorization))
    }
  }

  const apiKey = getHeaderCaseInsensitive(headers, 'x-api-key')
  if (apiKey) {
    return {
      authorizationHeaderName: 'x-api-key',
      authorizationRaw: String(apiKey),
      authorizationSha256: sha256String(String(apiKey))
    }
  }

  return {
    authorizationHeaderName: null,
    authorizationRaw: null,
    authorizationSha256: null
  }
}

function sha256String(value) {
  return crypto.createHash('sha256').update(value).digest('hex')
}

function captureChunk(chunks, chunk, encoding) {
  if (chunk === undefined || chunk === null) {
    return
  }
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
  }
}

function getContentEncoding(headers) {
  const raw =
    getHeaderCaseInsensitive(headers, 'content-encoding') ||
    getHeaderCaseInsensitive(headers, 'Content-Encoding') ||
    ''
  const first = String(raw || '')
    .split(',')
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean)[0]
  return first || ''
}

function isGzipMagic(buffer) {
  return Boolean(buffer && buffer.length >= 2 && buffer[0] === 0x1f && buffer[1] === 0x8b)
}

function decodeCompressedBody(buffer, headers) {
  const encoding = getContentEncoding(headers)

  const identityResult = {
    buffer,
    contentEncoding: encoding || 'identity',
    decompressed: false,
    decodeError: null,
    decodeSource: 'identity'
  }

  if (!Buffer.isBuffer(buffer) || buffer.length === 0) {
    return identityResult
  }

  try {
    if (encoding === 'gzip' || encoding === 'x-gzip') {
      return {
        buffer: zlib.gunzipSync(buffer),
        contentEncoding: 'gzip',
        decompressed: true,
        decodeError: null,
        decodeSource: 'content-encoding'
      }
    }

    if (encoding === 'br') {
      return {
        buffer: zlib.brotliDecompressSync(buffer),
        contentEncoding: 'br',
        decompressed: true,
        decodeError: null,
        decodeSource: 'content-encoding'
      }
    }

    if (encoding === 'deflate') {
      try {
        return {
          buffer: zlib.inflateSync(buffer),
          contentEncoding: 'deflate',
          decompressed: true,
          decodeError: null,
          decodeSource: 'content-encoding'
        }
      } catch (_) {
        return {
          buffer: zlib.inflateRawSync(buffer),
          contentEncoding: 'deflate',
          decompressed: true,
          decodeError: null,
          decodeSource: 'content-encoding-inflateRaw'
        }
      }
    }
  } catch (error) {
    return {
      ...identityResult,
      decodeError: `${encoding || 'unknown'}_decode_failed: ${error.message || String(error)}`,
      decodeSource: 'content-encoding'
    }
  }

  if (!encoding && isGzipMagic(buffer)) {
    try {
      return {
        buffer: zlib.gunzipSync(buffer),
        contentEncoding: 'gzip',
        decompressed: true,
        decodeError: null,
        decodeSource: 'magic'
      }
    } catch (error) {
      return {
        ...identityResult,
        contentEncoding: 'gzip',
        decodeError: `gzip_magic_decode_failed: ${error.message || String(error)}`,
        decodeSource: 'magic'
      }
    }
  }

  return identityResult
}

function createStreamState() {
  return {
    eventCount: 0,
    eventTypes: new Set(),
    responseId: null,
    responseModel: null,
    status: null,
    usage: null,
    assistantTextChunks: [],
    reasoningTextChunks: [],
    toolCalls: new Map(),
    completedResponse: null,
    parseErrors: []
  }
}

function appendText(chunks, value) {
  if (typeof value !== 'string' || !value) {
    return
  }
  chunks.push(value)
}

function mergeToolCall(state, key, patch) {
  if (!key) {
    return
  }

  const existing = state.toolCalls.get(key) || {
    key,
    type: patch.type || 'function_call',
    id: patch.id || null,
    call_id: patch.call_id || null,
    name: null,
    arguments: '',
    status: null,
    output_index: patch.output_index ?? null
  }

  if (patch.id) {
    existing.id = patch.id
  }
  if (patch.call_id) {
    existing.call_id = patch.call_id
  }
  if (patch.name) {
    existing.name = patch.name
  }
  if (patch.status) {
    existing.status = patch.status
  }
  if (patch.output_index !== undefined && patch.output_index !== null) {
    existing.output_index = patch.output_index
  }
  if (patch.type) {
    existing.type = patch.type
  }
  if (patch.arguments) {
    existing.arguments += patch.arguments
  }

  state.toolCalls.set(key, existing)
}

function toolCallKeyFromPayload(payload) {
  return (
    payload.call_id ||
    payload.item_id ||
    payload.response_id ||
    payload.output_index ||
    payload.id ||
    null
  )
}

function collectAssistantTextFromResponse(response) {
  const chunks = []
  const output = Array.isArray(response && response.output) ? response.output : []

  for (const item of output) {
    if (!item || typeof item !== 'object') {
      continue
    }

    if (item.type === 'message' && Array.isArray(item.content)) {
      for (const content of item.content) {
        if (!content || typeof content !== 'object') {
          continue
        }
        if (
          (content.type === 'output_text' || content.type === 'text') &&
          typeof content.text === 'string'
        ) {
          chunks.push(content.text)
        }
      }
    }

    if ((item.type === 'output_text' || item.type === 'text') && typeof item.text === 'string') {
      chunks.push(item.text)
    }
  }

  return chunks.join('')
}

function collectReasoningTextFromResponse(response) {
  const chunks = []
  const output = Array.isArray(response && response.output) ? response.output : []

  for (const item of output) {
    if (!item || typeof item !== 'object' || item.type !== 'reasoning') {
      continue
    }

    if (typeof item.text === 'string') {
      chunks.push(item.text)
    }

    if (typeof item.summary === 'string') {
      chunks.push(item.summary)
    }

    if (Array.isArray(item.summary)) {
      for (const part of item.summary) {
        if (part && typeof part.text === 'string') {
          chunks.push(part.text)
        }
      }
    }

    if (Array.isArray(item.content)) {
      for (const content of item.content) {
        if (content && typeof content.text === 'string') {
          chunks.push(content.text)
        }
      }
    }
  }

  return chunks.join('')
}

function collectToolCallsFromResponse(response) {
  const output = Array.isArray(response && response.output) ? response.output : []
  const toolCalls = []

  for (const item of output) {
    if (!item || typeof item !== 'object') {
      continue
    }

    if (item.type === 'function_call' || item.type === 'custom_tool_call') {
      toolCalls.push({
        type: item.type,
        id: item.id || null,
        call_id: item.call_id || null,
        name: item.name || null,
        arguments: item.arguments || '',
        status: item.status || null,
        output_index: item.output_index ?? null
      })
    }
  }

  return toolCalls
}

function extractStructuredResponseFields(response) {
  if (!response || typeof response !== 'object') {
    return {
      responseId: null,
      responseModel: null,
      status: null,
      usage: null,
      assistantTextFull: '',
      reasoningTextFull: '',
      toolCalls: []
    }
  }

  return {
    responseId: response.id || null,
    responseModel: response.model || null,
    status: response.status || null,
    usage: response.usage || null,
    assistantTextFull: collectAssistantTextFromResponse(response),
    reasoningTextFull: config.includeReasoning ? collectReasoningTextFromResponse(response) : '',
    toolCalls: collectToolCallsFromResponse(response)
  }
}

function applyStreamPayload(state, payload) {
  state.eventCount += 1
  if (payload && typeof payload.type === 'string') {
    state.eventTypes.add(payload.type)
  }

  if (!payload || typeof payload !== 'object') {
    return
  }

  if (payload.response && typeof payload.response === 'object') {
    if (payload.response.id) {
      state.responseId = payload.response.id
    }
    if (payload.response.model) {
      state.responseModel = payload.response.model
    }
    if (payload.response.status) {
      state.status = payload.response.status
    }
    if (payload.response.usage) {
      state.usage = payload.response.usage
    }
  }

  if (payload.type === 'response.output_text.delta' && typeof payload.delta === 'string') {
    appendText(state.assistantTextChunks, payload.delta)
    return
  }

  if (config.includeReasoning) {
    if (
      (payload.type === 'response.reasoning.delta' ||
        payload.type === 'response.reasoning_summary_text.delta') &&
      typeof payload.delta === 'string'
    ) {
      appendText(state.reasoningTextChunks, payload.delta)
      return
    }

    if (
      payload.type === 'response.output_item.added' ||
      payload.type === 'response.output_item.done'
    ) {
      const item = payload.item
      if (item && item.type === 'reasoning') {
        appendText(state.reasoningTextChunks, collectReasoningTextFromResponse({ output: [item] }))
      }
    }
  }

  if (
    payload.type === 'response.function_call_arguments.delta' &&
    typeof payload.delta === 'string'
  ) {
    mergeToolCall(state, toolCallKeyFromPayload(payload), {
      id: payload.item_id || payload.id || null,
      call_id: payload.call_id || null,
      name: payload.name || null,
      arguments: payload.delta,
      output_index: payload.output_index ?? null
    })
    return
  }

  if (payload.type === 'response.function_call_arguments.done') {
    mergeToolCall(state, toolCallKeyFromPayload(payload), {
      id: payload.item_id || payload.id || null,
      call_id: payload.call_id || null,
      name: payload.name || null,
      arguments: typeof payload.arguments === 'string' ? payload.arguments : '',
      output_index: payload.output_index ?? null
    })
    return
  }

  if (
    payload.type === 'response.output_item.added' ||
    payload.type === 'response.output_item.done'
  ) {
    const item = payload.item
    if (item && (item.type === 'function_call' || item.type === 'custom_tool_call')) {
      mergeToolCall(state, item.call_id || item.id || payload.output_index || null, {
        type: item.type,
        id: item.id || null,
        call_id: item.call_id || null,
        name: item.name || null,
        arguments: typeof item.arguments === 'string' ? item.arguments : '',
        status: item.status || null,
        output_index: item.output_index ?? payload.output_index ?? null
      })
    }
    return
  }

  if (
    payload.type === 'response.completed' &&
    payload.response &&
    typeof payload.response === 'object'
  ) {
    state.completedResponse = payload.response
    const structured = extractStructuredResponseFields(payload.response)
    if (structured.responseId) {
      state.responseId = structured.responseId
    }
    if (structured.responseModel) {
      state.responseModel = structured.responseModel
    }
    if (structured.status) {
      state.status = structured.status
    }
    if (structured.usage) {
      state.usage = structured.usage
    }
    if (structured.assistantTextFull) {
      state.assistantTextChunks = [structured.assistantTextFull]
    }
    if (config.includeReasoning && structured.reasoningTextFull) {
      state.reasoningTextChunks = [structured.reasoningTextFull]
    }
    if (structured.toolCalls.length > 0) {
      state.toolCalls.clear()
      for (const toolCall of structured.toolCalls) {
        const key = toolCall.call_id || toolCall.id || toolCall.output_index || crypto.randomUUID()
        state.toolCalls.set(key, toolCall)
      }
    }
  }
}

function applySseText(streamState, text) {
  const parser = new IncrementalSSEParser()
  const events = parser.feed(text)
  if (parser.getRemaining().trim()) {
    events.push(...parser.feed('\n\n'))
  }

  for (const event of events) {
    if (!event) {
      continue
    }

    if (event.type === 'invalid') {
      streamState.parseErrors.push(event.error ? event.error.message : 'invalid_sse_event')
      continue
    }

    if (event.type === 'event' && event.name) {
      streamState.eventTypes.add(`event:${event.name}`)
      continue
    }

    if (event.type === 'done') {
      streamState.eventTypes.add('[DONE]')
      continue
    }

    if (event.type === 'data') {
      applyStreamPayload(streamState, event.data)
    }
  }
}

function finalizeToolCalls(streamState) {
  return Array.from(streamState.toolCalls.values())
    .map((toolCall) => ({
      type: toolCall.type || 'function_call',
      id: toolCall.id || null,
      call_id: toolCall.call_id || null,
      name: toolCall.name || null,
      arguments: toolCall.arguments || '',
      status: toolCall.status || null,
      output_index: toolCall.output_index ?? null
    }))
    .sort((left, right) => {
      const leftIndex = left.output_index ?? Number.MAX_SAFE_INTEGER
      const rightIndex = right.output_index ?? Number.MAX_SAFE_INTEGER
      return leftIndex - rightIndex
    })
}

function buildStreamRecord(
  traceId,
  providerKind,
  requestRecord,
  requestMeta,
  responseMeta,
  streamState,
  timing,
  error
) {
  const requestJson = requestRecord.requestBodyJson || null
  return {
    ts: new Date().toISOString(),
    type: 'openai_upstream_stream_final',
    trace_id: traceId,
    capture_version: 1,
    provider_kind: providerKind,
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
      request_model:
        requestRecord.requestModel ||
        (requestJson && typeof requestJson.model === 'string' ? requestJson.model : null),
      request_stream: true,
      authorization_sha256: requestRecord.authorizationSha256,
      authorization_header_name: requestRecord.authorizationHeaderName
    },
    stream: {
      event_count: streamState.eventCount,
      event_types: Array.from(streamState.eventTypes),
      response_id: streamState.responseId,
      response_model: streamState.responseModel,
      status: streamState.status,
      assistant_text_full: streamState.assistantTextChunks.join(''),
      reasoning_text_full: config.includeReasoning
        ? streamState.reasoningTextChunks.join('')
        : undefined,
      tool_calls: finalizeToolCalls(streamState),
      usage: streamState.usage,
      response_json: streamState.completedResponse,
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

function buildNonStreamRecord(
  traceId,
  providerKind,
  requestRecord,
  requestMeta,
  responseMeta,
  responseBodyRaw,
  timing,
  error,
  decodeMeta
) {
  const responseJson = parseMaybeJson(responseBodyRaw)
  const requestJson = requestRecord.requestBodyJson || null
  const structured = extractStructuredResponseFields(responseJson)

  return {
    ts: new Date().toISOString(),
    type: 'openai_upstream_response_non_stream',
    trace_id: traceId,
    capture_version: 1,
    provider_kind: providerKind,
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
      request_model:
        requestRecord.requestModel ||
        (requestJson && typeof requestJson.model === 'string' ? requestJson.model : null),
      request_stream: Boolean(requestRecord.requestStream),
      authorization_sha256: requestRecord.authorizationSha256,
      authorization_header_name: requestRecord.authorizationHeaderName
    },
    response: {
      body_raw: responseBodyRaw,
      body_json: responseJson,
      content_encoding:
        decodeMeta && decodeMeta.contentEncoding ? decodeMeta.contentEncoding : 'identity',
      decompressed: Boolean(decodeMeta && decodeMeta.decompressed),
      decode_source: decodeMeta && decodeMeta.decodeSource ? decodeMeta.decodeSource : 'identity',
      decode_error: decodeMeta && decodeMeta.decodeError ? decodeMeta.decodeError : null,
      usage: structured.usage,
      response_id: structured.responseId,
      model: structured.responseModel,
      status: structured.status,
      assistant_text_full: structured.assistantTextFull,
      reasoning_text_full: config.includeReasoning ? structured.reasoningTextFull : undefined,
      tool_calls: structured.toolCalls
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

    const providerKind = determineProviderKind(requestMeta)
    const traceId = createTraceId()
    const startedAt = new Date().toISOString()

    const requestChunks = []
    const requestRecord = {
      requestBodyRaw: '',
      requestBodyJson: null,
      requestModel: null,
      requestStream: false,
      authorizationRaw: null,
      authorizationSha256: null,
      authorizationHeaderName: null
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
        persistRequestRecord(traceId, providerKind, requestMeta, requestChunks, requestRecord)
      }
      return originalEnd(chunk, encoding, callback)
    }

    const attachResponseCapture = (res) => {
      if (res.__openaiCaptureAttached) {
        return
      }
      res.__openaiCaptureAttached = true

      const responseHeaders = sanitizeHeaders(res.headers || {})
      const contentTypeRaw =
        responseHeaders['content-type'] || responseHeaders['Content-Type'] || ''
      const contentType = String(contentTypeRaw || '').toLowerCase()
      const isStreamResponse = contentType.includes('text/event-stream')

      const streamState = createStreamState()
      const responseChunks = []

      res.on('data', (chunk) => {
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
          const responseBodyBuffer = Buffer.concat(responseChunks)
          const decodeMeta = decodeCompressedBody(responseBodyBuffer, responseHeaders)
          if (decodeMeta.decodeError) {
            streamState.parseErrors.push(decodeMeta.decodeError)
          }
          applySseText(streamState, decodeMeta.buffer.toString('utf8'))

          const streamRecord = buildStreamRecord(
            traceId,
            providerKind,
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
            type: 'openai_upstream_response_stream_summary',
            trace_id: traceId,
            capture_version: 1,
            provider_kind: providerKind,
            statusCode: responseMeta.statusCode,
            response_id: streamRecord.stream.response_id,
            response_model: streamRecord.stream.response_model,
            status: streamRecord.stream.status,
            usage: streamRecord.stream.usage,
            event_count: streamRecord.stream.event_count,
            content_encoding: decodeMeta.contentEncoding,
            decompressed: decodeMeta.decompressed,
            decode_source: decodeMeta.decodeSource,
            decode_error: decodeMeta.decodeError,
            latency_ms: latencyMs,
            error: streamRecord.error
          })
          return
        }

        const responseBodyBuffer = Buffer.concat(responseChunks)
        const decodeMeta = decodeCompressedBody(responseBodyBuffer, responseHeaders)
        const responseBodyRaw = decodeMeta.buffer.toString('utf8')
        const nonStreamRecord = buildNonStreamRecord(
          traceId,
          providerKind,
          requestRecord,
          requestMeta,
          responseMeta,
          responseBodyRaw,
          { startedAt, endedAt, latencyMs },
          error,
          decodeMeta
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
        persistRequestRecord(traceId, providerKind, requestMeta, requestChunks, requestRecord)
      }

      if (responseFinished) {
        return
      }

      const endedAt = new Date().toISOString()
      const latencyMs = Date.now() - Date.parse(startedAt)
      writeJsonl(RESPONSES_FILE, {
        ts: new Date().toISOString(),
        type: 'openai_upstream_response_transport_error',
        trace_id: traceId,
        capture_version: 1,
        provider_kind: providerKind,
        upstream: {
          protocol: requestMeta.protocol,
          host: requestMeta.host,
          hostname: requestMeta.hostname,
          path: requestMeta.path,
          method: requestMeta.method
        },
        request: {
          request_model: requestRecord.requestModel,
          request_stream: Boolean(requestRecord.requestStream),
          authorization_sha256: requestRecord.authorizationSha256,
          authorization_header_name: requestRecord.authorizationHeaderName
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

function persistRequestRecord(traceId, providerKind, requestMeta, requestChunks, requestRecord) {
  const requestBodyRaw = Buffer.concat(requestChunks).toString('utf8')
  const requestBodyJson = parseMaybeJson(requestBodyRaw)
  const authInfo = extractAuthInfo(requestMeta.headers)

  requestRecord.requestBodyRaw = requestBodyRaw
  requestRecord.requestBodyJson = requestBodyJson
  requestRecord.requestModel =
    requestBodyJson && typeof requestBodyJson.model === 'string' ? requestBodyJson.model : null
  requestRecord.requestStream = Boolean(requestBodyJson && requestBodyJson.stream === true)
  requestRecord.authorizationRaw = authInfo.authorizationRaw
  requestRecord.authorizationSha256 = authInfo.authorizationSha256
  requestRecord.authorizationHeaderName = authInfo.authorizationHeaderName

  writeJsonl(REQUESTS_FILE, {
    ts: new Date().toISOString(),
    type: 'openai_upstream_request',
    trace_id: traceId,
    capture_version: 1,
    provider_kind: providerKind,
    upstream: {
      protocol: requestMeta.protocol,
      host: requestMeta.host,
      hostname: requestMeta.hostname,
      path: requestMeta.path,
      method: requestMeta.method
    },
    headers: sanitizeHeaders(requestMeta.headers),
    authorization_header_name: authInfo.authorizationHeaderName,
    authorization_raw: authInfo.authorizationRaw,
    authorization_sha256: authInfo.authorizationSha256,
    request_body_raw: requestBodyRaw,
    request_body_json: requestBodyJson,
    request_model: requestRecord.requestModel,
    request_stream: requestRecord.requestStream
  })
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
    console.log(`[openai-capture] ${message}`, meta)
    return
  }
  // eslint-disable-next-line no-console
  console.log(`[openai-capture] ${message}`)
}

function logError(message, meta) {
  if (meta) {
    // eslint-disable-next-line no-console
    console.error(`[openai-capture] ${message}`, meta)
    return
  }
  // eslint-disable-next-line no-console
  console.error(`[openai-capture] ${message}`)
}
