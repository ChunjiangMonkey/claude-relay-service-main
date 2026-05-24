'use strict'

const https = require('https')
const fs = require('fs/promises')
const path = require('path')
const crypto = require('crypto')
const zlib = require('zlib')
const { StringDecoder } = require('string_decoder')
const { IncrementalSSEParser } = require('../../../src/utils/sseParser')

const PATCH_SENTINEL = Symbol.for('claude_relay.openai_capture_hook.installed')
const ACTIVE_SENTINEL = Symbol.for('claude_relay.openai_capture_hook.active')

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
const DEFAULT_AUDIT_MAX_FILE_BYTES = 16 * 1024 * 1024
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
  includeRaw: isEnabled(process.env.OPENAI_CAPTURE_INCLUDE_RAW, false),
  auditEnabled:
    isEnabled(process.env.CAPTURE_AUDIT_ENABLED, false) ||
    isEnabled(process.env.OPENAI_CAPTURE_AUDIT_ENABLED, false),
  auditFile: resolveOutputPath(
    process.env.CAPTURE_AUDIT_FILE ||
      process.env.OPENAI_CAPTURE_AUDIT_FILE ||
      'capture-audit.jsonl',
    process.env.OPENAI_CAPTURE_DIR || DEFAULT_CAPTURE_DIR
  ),
  auditMaxFileBytes: parsePositiveInt(
    process.env.CAPTURE_AUDIT_MAX_FILE_BYTES || process.env.OPENAI_CAPTURE_AUDIT_MAX_FILE_BYTES,
    DEFAULT_AUDIT_MAX_FILE_BYTES
  ),
  auditBackupFiles: parsePositiveInt(
    process.env.CAPTURE_AUDIT_BACKUP_FILES || process.env.OPENAI_CAPTURE_AUDIT_BACKUP_FILES,
    DEFAULT_BACKUP_FILES
  ),
  debug: isEnabled(process.env.OPENAI_CAPTURE_DEBUG, false)
}

const writeQueues = new Map()
let initPromise = null

if (!config.enabled) {
  logDebug('OpenAI capture hook loaded but disabled')
  return
}

patchHttpsRequest()
global[ACTIVE_SENTINEL] = true
logDebug('OpenAI capture hook installed', {
  captureDir: config.captureDir,
  hosts: Array.from(config.hosts),
  captureMethods: config.captureMethods ? Array.from(config.captureMethods) : ['*'],
  capturePathPrefixes: config.capturePathPrefixes || ['*'],
  maxRecordBytes: config.maxRecordBytes,
  maxFileBytes: config.maxFileBytes,
  includeReasoning: config.includeReasoning,
  includeRaw: config.includeRaw,
  auditEnabled: config.auditEnabled,
  auditFile: config.auditFile
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

function resolveOutputPath(rawPath, captureDir) {
  const outputPath = String(rawPath || '').trim()
  if (!outputPath) {
    return path.join(captureDir, 'capture-audit.jsonl')
  }
  return path.isAbsolute(outputPath) ? outputPath : path.join(captureDir, outputPath)
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

  const compacted = dropOptionalRawFields(payload)
  if (compacted.changed) {
    try {
      const compactJson = JSON.stringify(compacted.payload)
      const compactSize = Buffer.byteLength(compactJson, 'utf8')
      if (compactSize <= maxBytes) {
        logDebug(`Large record ${eventType}: omitted optional raw fields to stay under limit`)
        return compactJson
      }

      logDebug(
        `⚠️ Large semantic record: ${eventType} is ${(compactSize / 1024 / 1024).toFixed(2)}MB after raw omission (limit ${(maxBytes / 1024 / 1024).toFixed(0)}MB), keeping semantic fields`
      )
      return compactJson
    } catch (_) {
      // fall through to original JSON; the original stringify already succeeded.
    }
  }

  logDebug(
    `⚠️ Large semantic record: ${eventType} is ${(size / 1024 / 1024).toFixed(2)}MB (limit ${(maxBytes / 1024 / 1024).toFixed(0)}MB), keeping semantic fields`
  )
  return json
}

function dropOptionalRawFields(payload) {
  let changed = false
  const output = { ...payload }

  if (Object.prototype.hasOwnProperty.call(output, 'request_body_raw')) {
    output.request_body_raw_omitted = true
    output.request_body_raw_omit_reason = 'record_size_limit'
    delete output.request_body_raw
    changed = true
  }

  if (Object.prototype.hasOwnProperty.call(output, 'authorization_raw')) {
    output.authorization_raw_omitted = true
    output.authorization_raw_omit_reason = 'record_size_limit'
    delete output.authorization_raw
    changed = true
  }

  if (output.response && Object.prototype.hasOwnProperty.call(output.response, 'body_raw')) {
    output.response = {
      ...output.response,
      body_raw_omitted: true,
      body_raw_omit_reason: 'record_size_limit'
    }
    delete output.response.body_raw
    changed = true
  }

  return { payload: output, changed }
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

function stripInternalHeaders(requestArgs) {
  const targetKey = 'x-relay-key-id'
  const deleteHeaderCaseInsensitive = (headers) => {
    if (!headers) {
      return
    }
    if (typeof headers.delete === 'function') {
      headers.delete(targetKey)
      return
    }
    if (Array.isArray(headers)) {
      for (let index = headers.length - 2; index >= 0; index -= 2) {
        if (String(headers[index]).toLowerCase() === targetKey) {
          headers.splice(index, 2)
        }
      }
      return
    }
    if (typeof headers === 'object') {
      for (const key of Object.keys(headers)) {
        if (String(key).toLowerCase() === targetKey) {
          delete headers[key]
        }
      }
    }
  }

  const opts = requestArgs[0]
  if (opts && typeof opts === 'object' && !(opts instanceof URL)) {
    deleteHeaderCaseInsensitive(opts.headers)
  }
  if (requestArgs[1] && typeof requestArgs[1] === 'object') {
    deleteHeaderCaseInsensitive(requestArgs[1].headers)
  }
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

function toBuffer(chunk, encoding) {
  if (Buffer.isBuffer(chunk)) {
    return chunk
  }
  if (chunk instanceof ArrayBuffer) {
    return Buffer.from(chunk)
  }
  if (ArrayBuffer.isView(chunk)) {
    return Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength)
  }
  if (typeof chunk === 'string') {
    return Buffer.from(chunk, encoding || 'utf8')
  }
  return null
}

function rawBodyMeta(rawText) {
  const value = typeof rawText === 'string' ? rawText : ''
  return {
    bytes: Buffer.byteLength(value, 'utf8'),
    sha256: value ? crypto.createHash('sha256').update(value).digest('hex') : null
  }
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

function buildAuditWarnings({ httpStatus, decodeError, responseSemanticPresent, eventType }) {
  const warnings = []
  if (decodeError) {
    warnings.push('decode_error')
  }
  if (
    httpStatus >= 200 &&
    httpStatus < 300 &&
    responseSemanticPresent === false &&
    eventType !== 'transport_error'
  ) {
    warnings.push('empty_success_response_semantics')
  }
  return warnings
}

function writeAudit(event, payload) {
  if (!config.auditEnabled) {
    return
  }

  const line = `${JSON.stringify({
    ts: new Date().toISOString(),
    source: 'hook',
    provider: 'openai',
    event,
    ...payload
  })}\n`

  queueFileWrite(config.auditFile, line, {
    maxFileBytes: config.auditMaxFileBytes,
    backupFiles: config.auditBackupFiles
  })
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

function createStreamingBodyDecoder(headers, onText) {
  const encoding = getContentEncoding(headers)
  const stringDecoder = new StringDecoder('utf8')
  let decodeError = null
  let closed = false
  let activeEncoding = encoding || ''
  let pendingSniffBuffer = null

  const emitBuffer = (buffer) => {
    const text = stringDecoder.write(buffer)
    if (text) {
      onText(text)
    }
  }

  const finishText = () => {
    const text = stringDecoder.end()
    if (text) {
      onText(text)
    }
  }

  const baseMeta = {
    contentEncoding: encoding || 'identity',
    decompressed: false,
    decodeSource: 'identity'
  }

  let decoder = null
  let done = Promise.resolve()

  const installDecoder = (kind, source) => {
    if (kind === 'gzip' || kind === 'x-gzip') {
      decoder = zlib.createGunzip()
      baseMeta.contentEncoding = 'gzip'
    } else if (kind === 'br') {
      decoder = zlib.createBrotliDecompress()
      baseMeta.contentEncoding = 'br'
    } else if (kind === 'deflate') {
      decoder = zlib.createInflate()
      baseMeta.contentEncoding = 'deflate'
    }

    if (!decoder) {
      return
    }

    activeEncoding = baseMeta.contentEncoding
    baseMeta.decompressed = true
    baseMeta.decodeSource = source
    done = new Promise((resolve) => {
      decoder.on('data', emitBuffer)
      decoder.on('error', (error) => {
        decodeError = `${activeEncoding || 'unknown'}_decode_failed: ${error.message || String(error)}`
        closed = true
        resolve()
      })
      decoder.on('end', () => {
        closed = true
        finishText()
        resolve()
      })
    })
  }

  if (encoding === 'gzip' || encoding === 'x-gzip') {
    installDecoder('gzip', 'content-encoding')
  } else if (encoding === 'br') {
    installDecoder('br', 'content-encoding')
  } else if (encoding === 'deflate') {
    installDecoder('deflate', 'content-encoding')
  }

  const writeToDecoder = (buffer) => {
    try {
      decoder.write(buffer)
    } catch (error) {
      decodeError = `${activeEncoding || 'unknown'}_decode_failed: ${error.message || String(error)}`
    }
  }

  const feedIdentityOrSniff = (buffer) => {
    if (encoding) {
      emitBuffer(buffer)
      return
    }

    const candidate = pendingSniffBuffer ? Buffer.concat([pendingSniffBuffer, buffer]) : buffer
    if (candidate.length < 2) {
      pendingSniffBuffer = candidate
      return
    }

    pendingSniffBuffer = null
    if (isGzipMagic(candidate)) {
      installDecoder('gzip', 'magic')
      writeToDecoder(candidate)
      return
    }

    emitBuffer(candidate)
  }

  return {
    feed(chunk) {
      if (closed || decodeError) {
        return
      }
      const buffer = toBuffer(chunk)
      if (!buffer || buffer.length === 0) {
        return
      }

      if (!decoder) {
        feedIdentityOrSniff(buffer)
        return
      }

      writeToDecoder(buffer)
    },
    async finish() {
      if (!decoder && pendingSniffBuffer) {
        emitBuffer(pendingSniffBuffer)
        pendingSniffBuffer = null
      }

      if (decoder && !closed) {
        try {
          decoder.end()
          await done
        } catch (error) {
          decodeError = `${activeEncoding || 'unknown'}_decode_failed: ${error.message || String(error)}`
        }
      } else if (!decoder) {
        finishText()
      }

      return {
        ...baseMeta,
        decodeError
      }
    }
  }
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

function createOpenaiSseTextParser(streamState) {
  const parser = new IncrementalSSEParser()

  const processEvents = (events) => {
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

  return {
    feedText(text) {
      if (!text) {
        return
      }
      processEvents(parser.feed(text))
    },
    finish() {
      if (parser.getRemaining().trim()) {
        processEvents(parser.feed('\n\n'))
      }
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
      request_model: requestRecord.requestModel,
      request_stream: true,
      authorization_sha256: requestRecord.authorizationSha256,
      authorization_header_name: requestRecord.authorizationHeaderName
    },
    relay_key_id: requestRecord.relayKeyId,
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
  const structured = extractStructuredResponseFields(responseJson)
  const rawMeta = rawBodyMeta(responseBodyRaw)
  const response = {
    body_json: responseJson,
    body_raw_bytes: rawMeta.bytes,
    body_raw_sha256: rawMeta.sha256,
    body_raw_omitted: !config.includeRaw,
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
  }

  if (config.includeRaw) {
    response.body_raw = responseBodyRaw
    response.body_raw_omitted = false
  }

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
      request_model: requestRecord.requestModel,
      request_stream: Boolean(requestRecord.requestStream),
      authorization_sha256: requestRecord.authorizationSha256,
      authorization_header_name: requestRecord.authorizationHeaderName
    },
    relay_key_id: requestRecord.relayKeyId,
    response,
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

    // 提取 relay key id 用于溯源（在发往上游前删除）
    const relayKeyId = getHeaderCaseInsensitive(requestMeta.headers, 'x-relay-key-id') || null
    stripInternalHeaders(args)

    const requestChunks = []
    const requestRecord = {
      requestModel: null,
      requestStream: false,
      authorizationSha256: null,
      authorizationHeaderName: null,
      relayKeyId
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
      const statusCode = Number(res.statusCode || 0)
      const requestDeclaredStream =
        requestRecord.requestStream === true && statusCode >= 200 && statusCode < 400
      const isStreamResponse = contentType.includes('text/event-stream') || requestDeclaredStream

      const streamState = createStreamState()
      const sseTextParser = isStreamResponse ? createOpenaiSseTextParser(streamState) : null
      const streamDecoder = isStreamResponse
        ? createStreamingBodyDecoder(responseHeaders, (text) => sseTextParser.feedText(text))
        : null
      const responseChunks = isStreamResponse ? null : []

      res.on('data', (chunk) => {
        if (isStreamResponse) {
          streamDecoder.feed(chunk)
          return
        }

        const buffer = toBuffer(chunk)
        if (buffer) {
          responseChunks.push(buffer)
        }
      })

      const finalize = async (error) => {
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
          const decodeMeta = await streamDecoder.finish()
          if (decodeMeta.decodeError) {
            streamState.parseErrors.push(decodeMeta.decodeError)
          }
          sseTextParser.finish()

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
            relay_key_id: requestRecord.relayKeyId,
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
          const responseSemanticPresent = Boolean(
            streamRecord.stream.assistant_text_full ||
              streamRecord.stream.reasoning_text_full ||
              (Array.isArray(streamRecord.stream.tool_calls) &&
                streamRecord.stream.tool_calls.length > 0) ||
              streamRecord.stream.usage ||
              streamRecord.stream.response_json
          )
          writeAudit('response_written', {
            trace_id: traceId,
            provider_kind: providerKind,
            relay_key_id: requestRecord.relayKeyId,
            upstream_host: requestMeta.hostname,
            upstream_path: requestMeta.path,
            response_kind: 'stream',
            http_status: responseMeta.statusCode,
            request_model: requestRecord.requestModel,
            response_model: streamRecord.stream.response_model,
            request_stream: true,
            response_semantic_present: responseSemanticPresent,
            assistant_text_len: String(streamRecord.stream.assistant_text_full || '').length,
            reasoning_text_len: String(streamRecord.stream.reasoning_text_full || '').length,
            tool_call_count: Array.isArray(streamRecord.stream.tool_calls)
              ? streamRecord.stream.tool_calls.length
              : 0,
            usage_present: Boolean(streamRecord.stream.usage),
            response_id: streamRecord.stream.response_id,
            decode_source: decodeMeta.decodeSource,
            decode_error: decodeMeta.decodeError,
            warnings: buildAuditWarnings({
              httpStatus: responseMeta.statusCode,
              decodeError: decodeMeta.decodeError,
              responseSemanticPresent,
              eventType: 'stream'
            })
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
        const responseSemanticPresent = Boolean(
          nonStreamRecord.response.body_json ||
            nonStreamRecord.response.assistant_text_full ||
            nonStreamRecord.response.reasoning_text_full ||
            (Array.isArray(nonStreamRecord.response.tool_calls) &&
              nonStreamRecord.response.tool_calls.length > 0) ||
            nonStreamRecord.response.usage
        )
        writeAudit('response_written', {
          trace_id: traceId,
          provider_kind: providerKind,
          relay_key_id: requestRecord.relayKeyId,
          upstream_host: requestMeta.hostname,
          upstream_path: requestMeta.path,
          response_kind: 'non_stream',
          http_status: responseMeta.statusCode,
          request_model: requestRecord.requestModel,
          response_model: nonStreamRecord.response.model,
          request_stream: Boolean(requestRecord.requestStream),
          response_semantic_present: responseSemanticPresent,
          assistant_text_len: String(nonStreamRecord.response.assistant_text_full || '').length,
          reasoning_text_len: String(nonStreamRecord.response.reasoning_text_full || '').length,
          tool_call_count: Array.isArray(nonStreamRecord.response.tool_calls)
            ? nonStreamRecord.response.tool_calls.length
            : 0,
          usage_present: Boolean(nonStreamRecord.response.usage),
          response_id: nonStreamRecord.response.response_id,
          decode_source: decodeMeta.decodeSource,
          decode_error: decodeMeta.decodeError,
          warnings: buildAuditWarnings({
            httpStatus: responseMeta.statusCode,
            decodeError: decodeMeta.decodeError,
            responseSemanticPresent,
            eventType: 'non_stream'
          })
        })
      }

      res.on('end', () => {
        finalize(null).catch((error) => {
          logError('Failed to finalize stream capture', {
            error: error && error.message ? error.message : String(error)
          })
        })
      })
      res.on('error', (error) => {
        finalize(error).catch((finalizeError) => {
          logError('Failed to finalize errored stream capture', {
            error:
              finalizeError && finalizeError.message ? finalizeError.message : String(finalizeError)
          })
        })
      })
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
        relay_key_id: requestRecord.relayKeyId,
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
      writeAudit('response_written', {
        trace_id: traceId,
        provider_kind: providerKind,
        relay_key_id: requestRecord.relayKeyId,
        upstream_host: requestMeta.hostname,
        upstream_path: requestMeta.path,
        response_kind: 'transport_error',
        http_status: null,
        request_model: requestRecord.requestModel,
        request_stream: Boolean(requestRecord.requestStream),
        response_semantic_present: false,
        assistant_text_len: 0,
        reasoning_text_len: 0,
        tool_call_count: 0,
        usage_present: false,
        error_code: error && error.code ? error.code : null,
        warnings: ['transport_error']
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
  const rawMeta = rawBodyMeta(requestBodyRaw)

  requestRecord.requestModel =
    requestBodyJson && typeof requestBodyJson.model === 'string' ? requestBodyJson.model : null
  requestRecord.requestStream = Boolean(requestBodyJson && requestBodyJson.stream === true)
  requestRecord.authorizationSha256 = authInfo.authorizationSha256
  requestRecord.authorizationHeaderName = authInfo.authorizationHeaderName

  const payload = {
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
    authorization_sha256: authInfo.authorizationSha256,
    authorization_raw_omitted: !config.includeRaw || !authInfo.authorizationRaw,
    relay_key_id: requestRecord.relayKeyId,
    request_body_raw_bytes: rawMeta.bytes,
    request_body_raw_sha256: rawMeta.sha256,
    request_body_raw_omitted: !config.includeRaw,
    request_body_json: requestBodyJson,
    request_model: requestRecord.requestModel,
    request_stream: requestRecord.requestStream
  }

  if (config.includeRaw) {
    payload.request_body_raw = requestBodyRaw
    payload.request_body_raw_omitted = false
    if (authInfo.authorizationRaw) {
      payload.authorization_raw = authInfo.authorizationRaw
      payload.authorization_raw_omitted = false
    }
  }

  writeJsonl(REQUESTS_FILE, payload)
  writeAudit('request_written', {
    trace_id: traceId,
    provider_kind: providerKind,
    relay_key_id: requestRecord.relayKeyId,
    upstream_host: requestMeta.hostname,
    upstream_path: requestMeta.path,
    model: requestRecord.requestModel,
    stream: Boolean(requestRecord.requestStream),
    request_json_present: Boolean(requestBodyJson),
    request_json_bytes: jsonByteLength(requestBodyJson),
    request_body_raw_bytes: rawMeta.bytes,
    request_body_raw_omitted: payload.request_body_raw_omitted,
    authorization_header_name: authInfo.authorizationHeaderName,
    authorization_sha256_present: Boolean(authInfo.authorizationSha256)
  })
  requestChunks.length = 0
}

function writeJsonl(filename, payload) {
  const filePath = path.join(config.captureDir, filename)
  const line = `${safeJsonStringify(payload, config.maxRecordBytes, payload.type || 'capture_event')}\n`
  queueFileWrite(filePath, line)
}

function queueFileWrite(filePath, line, options = {}) {
  const previous = writeQueues.get(filePath) || Promise.resolve()

  const current = previous
    .then(async () => {
      await ensureInitialized()
      await appendWithRotate(filePath, line, options)
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

async function appendWithRotate(filePath, line, options = {}) {
  const maxFileBytes = options.maxFileBytes || config.maxFileBytes
  const backupFiles =
    options.backupFiles === undefined || options.backupFiles === null
      ? config.backupFiles
      : options.backupFiles
  const nextSize = Buffer.byteLength(line, 'utf8')

  let currentSize = 0
  try {
    await fs.mkdir(path.dirname(filePath), { recursive: true })
    const stat = await fs.stat(filePath)
    currentSize = stat.size
  } catch (_) {
    currentSize = 0
  }

  if (currentSize + nextSize > maxFileBytes) {
    await rotateFile(filePath, backupFiles)
  }

  await fs.appendFile(filePath, line, { encoding: 'utf8' })
}

async function rotateFile(filePath, backupFiles = config.backupFiles) {
  if (backupFiles <= 0) {
    await fs.unlink(filePath).catch(() => {})
    return
  }

  const backupLimit = backupFiles
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
