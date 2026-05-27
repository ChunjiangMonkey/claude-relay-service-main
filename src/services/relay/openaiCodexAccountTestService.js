const axios = require('axios')
const { StringDecoder } = require('string_decoder')
const openaiAccountService = require('../account/openaiAccountService')
const ProxyHelper = require('../../utils/proxyHelper')
const logger = require('../../utils/logger')
const { buildCodexUpstreamHeaders } = require('../../utils/openaiCodexUpstreamHeaders')
const { createCodexTestPayload, extractErrorMessage } = require('../../utils/testPayloadHelper')
const { getSafeMessage, mapToErrorCode } = require('../../utils/errorSanitizer')

const DEFAULT_CODEX_TEST_MODEL = 'gpt-5.5'
const CODEX_TEST_ENDPOINT = 'https://chatgpt.com/backend-api/codex/responses'
const CAPTURE_SKIP_HEADER = 'x-relay-capture-skip'
const CAPTURE_SKIP_REASON = 'admin-openai-account-test'
const TEST_TIMEOUT_MS = 30000
const MAX_ERROR_BODY_BYTES = 64 * 1024
const MAX_RESPONSE_PREVIEW_CHARS = 2000
const MAX_SSE_LINE_CHARS = 64 * 1024

function normalizeModel(model) {
  if (typeof model !== 'string' || !model.trim() || model.length > 256) {
    return DEFAULT_CODEX_TEST_MODEL
  }
  return model.trim()
}

function sanitizeCodexTestError(message, statusCode = null) {
  const mapped = mapToErrorCode(
    {
      message,
      statusCode
    },
    { logOriginal: false }
  )
  return `[${mapped.code}] ${mapped.message}`
}

function createHttpError(message, statusCode) {
  const error = new Error(message)
  error.statusCode = statusCode
  return error
}

function parseProxy(proxy) {
  if (!proxy) {
    return null
  }
  if (typeof proxy === 'string') {
    try {
      return JSON.parse(proxy)
    } catch (error) {
      logger.warn('Failed to parse OpenAI account proxy for test:', error)
      return proxy
    }
  }
  return proxy
}

async function prepareAccount(accountId) {
  let account = await openaiAccountService.getAccount(accountId)
  if (!account) {
    throw createHttpError(`OpenAI account ${accountId} not found`, 404)
  }

  if (openaiAccountService.isTokenExpired(account)) {
    if (!account.refreshToken) {
      throw createHttpError(`Token expired and no refresh token available for ${account.name}`, 401)
    }
    await openaiAccountService.refreshAccountToken(accountId)
    account = await openaiAccountService.getAccount(accountId)
    if (!account) {
      throw createHttpError(`OpenAI account ${accountId} not found after token refresh`, 404)
    }
  }

  if (!account.accessToken) {
    throw createHttpError('OpenAI account has no access token', 401)
  }

  const accessToken = openaiAccountService.decrypt(account.accessToken)
  if (!accessToken) {
    throw createHttpError('Failed to decrypt OpenAI access token', 401)
  }

  return {
    account,
    accessToken,
    proxy: parseProxy(account.proxy)
  }
}

function safeWriteSse(responseStream, payload) {
  if (!responseStream || responseStream.destroyed || responseStream.writableEnded) {
    return false
  }
  responseStream.write(`data: ${JSON.stringify(payload)}\n\n`)
  return true
}

function extractOutputText(response) {
  const output = Array.isArray(response?.output) ? response.output : []
  let text = ''

  for (const item of output) {
    const content = Array.isArray(item?.content) ? item.content : []
    for (const block of content) {
      const part = block?.text || block?.delta?.text || ''
      if (part) {
        text += part
      }
      if (text.length >= MAX_RESPONSE_PREVIEW_CHARS) {
        return text.slice(0, MAX_RESPONSE_PREVIEW_CHARS)
      }
    }
  }

  return text
}

function appendPreview(state, text, onContent) {
  if (!text) {
    return
  }

  const remaining = MAX_RESPONSE_PREVIEW_CHARS - state.responseText.length
  if (remaining <= 0) {
    return
  }

  const chunk = String(text).slice(0, remaining)
  state.responseText += chunk
  if (onContent) {
    onContent(chunk)
  }
}

function applyResponseSnapshot(state, response) {
  if (!response || typeof response !== 'object') {
    return
  }

  state.responseId = response.id || state.responseId
  state.responseModel = response.model || state.responseModel
  state.status = response.status || state.status
  state.usage = response.usage || state.usage
}

function processSsePayload(state, payload, onContent) {
  if (!payload || typeof payload !== 'object') {
    return
  }

  if (payload.response) {
    applyResponseSnapshot(state, payload.response)
  }

  if (payload.error) {
    state.error = extractErrorMessage(payload, 'Codex test stream returned an error')
    return
  }

  if (payload.type === 'response.output_text.delta' && payload.delta) {
    appendPreview(state, payload.delta, onContent)
    return
  }

  if (payload.type === 'response.content_part.delta' && payload.delta?.text) {
    appendPreview(state, payload.delta.text, onContent)
    return
  }

  if (
    (payload.type === 'response.completed' || payload.type === 'response.done') &&
    payload.response &&
    !state.responseText
  ) {
    appendPreview(state, extractOutputText(payload.response), onContent)
  }
}

async function readLimitedErrorBody(stream) {
  const decoder = new StringDecoder('utf8')
  let body = ''
  let bytes = 0
  let truncated = false
  let settled = false

  return await new Promise((resolve) => {
    const finish = () => {
      if (settled) {
        return
      }
      settled = true
      body += decoder.end()
      resolve({ body, truncated })
    }

    stream.on('data', (chunk) => {
      if (settled) {
        return
      }
      const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
      bytes += buffer.length

      if (bytes <= MAX_ERROR_BODY_BYTES) {
        body += decoder.write(buffer)
        return
      }

      const allowed = Math.max(0, buffer.length - (bytes - MAX_ERROR_BODY_BYTES))
      if (allowed > 0) {
        body += decoder.write(buffer.subarray(0, allowed))
      }
      truncated = true
      stream.destroy()
      finish()
    })
    stream.on('end', finish)
    stream.on('error', finish)
    stream.on('close', finish)
  })
}

function extractErrorFromBody(body, fallback) {
  if (!body) {
    return fallback
  }
  try {
    return extractErrorMessage(JSON.parse(body), fallback)
  } catch {
    return body.length <= 500 ? body : fallback
  }
}

async function processUpstreamStream(stream, state, onContent) {
  const decoder = new StringDecoder('utf8')
  let buffer = ''

  const processLine = (rawLine) => {
    const line = rawLine.endsWith('\r') ? rawLine.slice(0, -1) : rawLine
    if (!line.startsWith('data:')) {
      return
    }

    const jsonStr = line.slice(5).trim()
    if (!jsonStr || jsonStr === '[DONE]') {
      return
    }

    try {
      processSsePayload(state, JSON.parse(jsonStr), onContent)
    } catch {
      state.parseErrors += 1
    }
  }

  const processText = (text) => {
    if (!text) {
      return
    }

    buffer += text

    if (buffer.length > MAX_SSE_LINE_CHARS && !buffer.includes('\n')) {
      state.parseErrors += 1
      buffer = ''
      return
    }

    const lines = buffer.split('\n')
    buffer = lines.pop() || ''

    for (const rawLine of lines) {
      if (rawLine.length > MAX_SSE_LINE_CHARS) {
        state.parseErrors += 1
        continue
      }
      processLine(rawLine)
    }

    if (buffer.length > MAX_SSE_LINE_CHARS) {
      state.parseErrors += 1
      buffer = ''
    }
  }

  await new Promise((resolve, reject) => {
    stream.on('data', (chunk) => {
      const bufferChunk = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
      processText(decoder.write(bufferChunk))
    })
    stream.on('end', () => {
      processText(decoder.end())
      if (buffer.trim()) {
        processLine(buffer)
        buffer = ''
      }
      resolve()
    })
    stream.on('error', reject)
  })
}

async function runPreparedTest({
  accountId,
  account,
  accessToken,
  proxy,
  model,
  onContent,
  responseStream
}) {
  const testModel = normalizeModel(model)
  const startedAt = Date.now()
  const controller = new AbortController()
  let upstreamStream = null
  let finished = false

  const cleanup = () => {
    if (finished) {
      return
    }
    controller.abort()
    if (upstreamStream?.destroy) {
      upstreamStream.destroy()
    }
  }

  if (responseStream?.on) {
    responseStream.on('close', cleanup)
  }

  try {
    const headers = buildCodexUpstreamHeaders({
      incomingHeaders: {},
      accessToken,
      account,
      accountId,
      isStream: true,
      apiKeyId: null
    })
    headers[CAPTURE_SKIP_HEADER] = CAPTURE_SKIP_REASON

    const requestConfig = {
      headers,
      timeout: TEST_TIMEOUT_MS,
      responseType: 'stream',
      signal: controller.signal,
      validateStatus: () => true
    }

    const proxyAgent = ProxyHelper.createProxyAgent(proxy)
    if (proxyAgent) {
      requestConfig.httpAgent = proxyAgent
      requestConfig.httpsAgent = proxyAgent
      requestConfig.proxy = false
    }

    const payload = createCodexTestPayload(testModel)

    const upstream = await axios.post(CODEX_TEST_ENDPOINT, payload, requestConfig)
    upstreamStream = upstream.data

    if (upstream.status !== 200) {
      const { body, truncated } = await readLimitedErrorBody(upstreamStream)
      const fallback = `Codex test request returned HTTP ${upstream.status}`
      const error = extractErrorFromBody(body, fallback)
      return {
        success: false,
        error: sanitizeCodexTestError(truncated ? `${error} (truncated)` : error, upstream.status),
        httpStatus: upstream.status,
        latencyMs: Date.now() - startedAt,
        model: testModel,
        timestamp: new Date().toISOString()
      }
    }

    const state = {
      responseText: '',
      responseId: null,
      responseModel: testModel,
      status: null,
      usage: null,
      error: null,
      parseErrors: 0
    }

    await processUpstreamStream(upstreamStream, state, onContent)

    if (state.error || state.status === 'failed' || state.status === 'cancelled') {
      return {
        success: false,
        error: sanitizeCodexTestError(state.error || `Codex response status: ${state.status}`),
        httpStatus: upstream.status,
        latencyMs: Date.now() - startedAt,
        model: testModel,
        responseId: state.responseId,
        responseModel: state.responseModel,
        status: state.status,
        timestamp: new Date().toISOString()
      }
    }

    return {
      success: true,
      latencyMs: Date.now() - startedAt,
      model: testModel,
      responseText: state.responseText,
      responseId: state.responseId,
      responseModel: state.responseModel,
      status: state.status || 'completed',
      usage: state.usage,
      parseErrors: state.parseErrors,
      timestamp: new Date().toISOString()
    }
  } finally {
    finished = true
    if (responseStream?.off) {
      responseStream.off('close', cleanup)
    }
  }
}

async function persistStreamingResult(options, result) {
  if (typeof options?.onResult !== 'function') {
    return
  }

  try {
    await options.onResult(result)
  } catch (error) {
    logger.error('Failed to persist OpenAI/Codex account test result:', error)
  }
}

async function testAccountConnection(
  accountId,
  responseStream,
  model = DEFAULT_CODEX_TEST_MODEL,
  options = {}
) {
  if (!responseStream.headersSent) {
    responseStream.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: responseStream.getHeader?.('Connection') || 'keep-alive',
      'X-Accel-Buffering': 'no'
    })
  }

  safeWriteSse(responseStream, { type: 'test_start', message: 'Test started' })

  try {
    const prepared = await prepareAccount(accountId)
    const result = await runPreparedTest({
      ...prepared,
      accountId,
      model,
      responseStream,
      onContent: (text) => safeWriteSse(responseStream, { type: 'content', text })
    })

    await persistStreamingResult(options, result)

    safeWriteSse(responseStream, {
      type: 'test_complete',
      success: result.success,
      error: result.error,
      latencyMs: result.latencyMs,
      responseId: result.responseId,
      model: result.responseModel || result.model
    })
    return result
  } catch (error) {
    const result = {
      success: false,
      error: getSafeMessage(error),
      model: normalizeModel(model),
      timestamp: new Date().toISOString()
    }
    await persistStreamingResult(options, result)
    safeWriteSse(responseStream, {
      type: 'test_complete',
      success: false,
      error: result.error
    })
    return result
  } finally {
    if (!responseStream.destroyed && !responseStream.writableEnded) {
      responseStream.end()
    }
  }
}

async function testAccountConnectionSync(accountId, model = DEFAULT_CODEX_TEST_MODEL) {
  const startedAt = Date.now()

  try {
    const prepared = await prepareAccount(accountId)
    return await runPreparedTest({
      ...prepared,
      accountId,
      model
    })
  } catch (error) {
    return {
      success: false,
      error: getSafeMessage(error),
      statusCode: error.statusCode,
      latencyMs: Date.now() - startedAt,
      model: normalizeModel(model),
      timestamp: new Date().toISOString()
    }
  }
}

module.exports = {
  DEFAULT_CODEX_TEST_MODEL,
  CAPTURE_SKIP_HEADER,
  CAPTURE_SKIP_REASON,
  testAccountConnection,
  testAccountConnectionSync
}
