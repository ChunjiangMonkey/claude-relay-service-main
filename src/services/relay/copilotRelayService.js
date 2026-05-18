const axios = require('axios')
const crypto = require('crypto')
const config = require('../../../config/config')
const logger = require('../../utils/logger')
const ProxyHelper = require('../../utils/proxyHelper')
const { filterForClaude, filterForOpenAI } = require('../../utils/headerFilter')
const { IncrementalSSEParser } = require('../../utils/sseParser')
const upstreamErrorHelper = require('../../utils/upstreamErrorHelper')
const {
  ANTHROPIC_CAPTURE_HOOK_ACTIVE,
  OPENAI_CAPTURE_HOOK_ACTIVE,
  attachRelayKeyIdHeader
} = require('../../utils/relayKeyCapture')
const {
  createRequestDetailMeta,
  extractOpenAICacheReadTokens
} = require('../../utils/requestDetailHelper')
const apiKeyService = require('../apiKeyService')
const copilotAccountService = require('../account/copilotAccountService')
const copilotScheduler = require('../scheduler/copilotScheduler')

function normalizeBaseApi(baseApi = '') {
  const value = String(baseApi || '').trim()
  return value.endsWith('/') ? value.slice(0, -1) : value
}

function buildTargetUrl(baseApi, targetPath) {
  const normalizedBaseApi = normalizeBaseApi(baseApi)
  let path = targetPath || '/v1/messages'
  if (!path.startsWith('/')) {
    path = `/${path}`
  }
  if (normalizedBaseApi.endsWith('/v1') && path.startsWith('/v1/')) {
    path = path.slice(3)
  }
  return `${normalizedBaseApi}${path}`
}

function cloneBodyWithResolvedModel(body, resolvedModel) {
  return {
    ...(body || {}),
    model: resolvedModel
  }
}

function buildSessionHash(req) {
  const sessionId =
    req?.headers?.session_id ||
    req?.headers?.['x-session-id'] ||
    req?.body?.session_id ||
    req?.body?.conversation_id ||
    req?.body?.prompt_cache_key ||
    null
  return sessionId ? crypto.createHash('sha256').update(sessionId).digest('hex') : null
}

function safeJsonParse(raw) {
  try {
    return JSON.parse(raw)
  } catch (_) {
    return null
  }
}

function extractCacheCreationTokens(usageData = {}) {
  const details = usageData.input_tokens_details || usageData.prompt_tokens_details || {}
  const candidates = [
    details.cache_creation_input_tokens,
    details.cache_creation_tokens,
    usageData.cache_creation_input_tokens,
    usageData.cache_creation_tokens
  ]

  for (const value of candidates) {
    const parsed = Number(value)
    if (Number.isFinite(parsed)) {
      return Math.max(0, parsed)
    }
  }
  return 0
}

function normalizeOpenAIUsage(usageData = {}) {
  const totalInputTokens = usageData.input_tokens || usageData.prompt_tokens || 0
  const outputTokens = usageData.output_tokens || usageData.completion_tokens || 0
  const cacheReadTokens = extractOpenAICacheReadTokens(usageData)
  const cacheCreateTokens = extractCacheCreationTokens(usageData)
  return {
    totalInputTokens,
    actualInputTokens: Math.max(0, totalInputTokens - cacheReadTokens),
    outputTokens,
    cacheReadTokens,
    cacheCreateTokens,
    totalTokens: usageData.total_tokens || totalInputTokens + outputTokens + cacheCreateTokens
  }
}

async function readStreamBody(stream) {
  const chunks = []
  await new Promise((resolve) => {
    stream.on('data', (chunk) => chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)))
    stream.on('end', resolve)
    stream.on('error', resolve)
    setTimeout(resolve, 5000)
  })
  return Buffer.concat(chunks).toString('utf8')
}

class CopilotRelayService {
  constructor() {
    this.defaultTimeout = config.requestTimeout || 600000
  }

  async relayAnthropicRequest(requestBody, apiKeyData, req, res, headers, selection, options = {}) {
    const account = await copilotAccountService.getAccount(selection.accountId || selection)
    if (!account) {
      throw new Error('Copilot account not found')
    }

    const resolvedModel = options.resolvedModel || selection.resolvedModel
    const body = cloneBodyWithResolvedModel(requestBody, resolvedModel)
    const targetUrl = buildTargetUrl(account.baseApi, options.path || '/v1/messages')
    const abortController = new AbortController()
    const handleClientDisconnect = () => abortController.abort()

    req?.once?.('close', handleClientDisconnect)
    res?.once?.('close', handleClientDisconnect)

    try {
      const requestHeaders = this._buildAnthropicHeaders(headers, account, apiKeyData, selection)
      const requestOptions = this._buildAxiosOptions(account, requestHeaders, body, false)
      requestOptions.method = 'POST'
      requestOptions.url = targetUrl
      requestOptions.signal = abortController.signal

      logger.info('Forwarding Copilot Anthropic request', {
        accountId: account.id,
        targetUrl,
        requestedModel: selection.requestedModel,
        resolvedModel,
        stream: body.stream === true
      })

      const response = await axios(requestOptions)
      await this._handleAnthropicStatus(account, response.status)
      await copilotAccountService.updateAccountUsage(account.id, 0).catch(() => {})

      return {
        statusCode: response.status,
        headers: response.headers || {},
        body: typeof response.data === 'string' ? response.data : JSON.stringify(response.data),
        accountId: account.id
      }
    } finally {
      req?.removeListener?.('close', handleClientDisconnect)
      res?.removeListener?.('close', handleClientDisconnect)
    }
  }

  async relayAnthropicStreamRequestWithUsageCapture(
    requestBody,
    apiKeyData,
    res,
    headers,
    usageCallback,
    selection,
    req = null
  ) {
    const account = await copilotAccountService.getAccount(selection.accountId || selection)
    if (!account) {
      throw new Error('Copilot account not found')
    }

    const resolvedModel = selection.resolvedModel || requestBody?.model
    const body = cloneBodyWithResolvedModel(requestBody, resolvedModel)
    const targetUrl = buildTargetUrl(account.baseApi, '/v1/messages')
    const abortController = new AbortController()
    const handleClientDisconnect = () => abortController.abort()

    req?.once?.('close', handleClientDisconnect)
    res.once('close', handleClientDisconnect)

    const requestHeaders = this._buildAnthropicHeaders(headers, account, apiKeyData, selection)
    const requestOptions = this._buildAxiosOptions(account, requestHeaders, body, true)
    requestOptions.method = 'POST'
    requestOptions.url = targetUrl
    requestOptions.signal = abortController.signal

    logger.info('Forwarding Copilot Anthropic stream request', {
      accountId: account.id,
      targetUrl,
      requestedModel: selection.requestedModel,
      resolvedModel
    })

    const response = await axios(requestOptions)
    if (response.status >= 400) {
      const errorText = response.data?.pipe ? await readStreamBody(response.data) : response.data
      await this._handleAnthropicStatus(account, response.status)
      if (!res.headersSent) {
        res
          .status(response.status)
          .json(
            upstreamErrorHelper.sanitizeErrorForClient(
              typeof errorText === 'string'
                ? safeJsonParse(errorText) || { error: { message: errorText } }
                : errorText
            )
          )
      }
      return
    }

    res.status(response.status)
    res.setHeader('Content-Type', 'text/event-stream')
    res.setHeader('Cache-Control', 'no-cache')
    res.setHeader('Connection', 'keep-alive')
    res.setHeader('X-Accel-Buffering', 'no')
    if (typeof res.flushHeaders === 'function') {
      res.flushHeaders()
    }

    const usageState = {
      accountId: account.id,
      model: resolvedModel,
      input_tokens: undefined,
      output_tokens: undefined,
      cache_creation_input_tokens: 0,
      cache_read_input_tokens: 0,
      cache_creation: null,
      requested_model: selection.requestedModel,
      resolved_model: resolvedModel
    }

    response.data.on('data', (chunk) => {
      if (!res.destroyed) {
        res.write(chunk)
      }
      this._parseAnthropicUsageChunk(chunk, usageState)
    })

    response.data.on('end', async () => {
      req?.removeListener?.('close', handleClientDisconnect)
      res.removeListener('close', handleClientDisconnect)
      await this._handleAnthropicStatus(account, response.status)
      await copilotAccountService.updateAccountUsage(account.id, 0).catch(() => {})
      if (
        usageState.input_tokens !== undefined ||
        usageState.output_tokens !== undefined ||
        Object.keys(usageState.cache_creation || {}).length > 0
      ) {
        usageCallback?.(usageState)
      }
      if (!res.destroyed) {
        res.end()
      }
    })

    response.data.on('error', (error) => {
      logger.error('Copilot Anthropic stream error:', error)
      req?.removeListener?.('close', handleClientDisconnect)
      res.removeListener('close', handleClientDisconnect)
      if (!res.headersSent) {
        res.status(502).json({ error: { message: 'Upstream stream error' } })
      } else if (!res.destroyed) {
        res.end()
      }
    })
  }

  async handleResponsesRequest(req, res, selection, apiKeyData) {
    const account = await copilotAccountService.getAccount(selection.accountId)
    if (!account) {
      return res.status(404).json({ error: { message: 'Copilot account not found' } })
    }

    const originalBody = req.body || {}
    const body = cloneBodyWithResolvedModel(originalBody, selection.resolvedModel)
    const targetUrl = buildTargetUrl(account.baseApi, req.path || '/v1/responses')
    const isStream = body.stream !== false
    const sessionHash = buildSessionHash(req)
    const abortController = new AbortController()
    const handleClientDisconnect = () => abortController.abort()

    req.once('close', handleClientDisconnect)
    res.once('close', handleClientDisconnect)

    try {
      const requestHeaders = this._buildOpenAIHeaders(req.headers, account, apiKeyData, selection)
      const requestOptions = this._buildAxiosOptions(account, requestHeaders, body, isStream)
      requestOptions.method = req.method || 'POST'
      requestOptions.url = targetUrl
      requestOptions.signal = abortController.signal

      logger.info('Forwarding Copilot Responses request', {
        accountId: account.id,
        targetUrl,
        requestedModel: selection.requestedModel,
        resolvedModel: selection.resolvedModel,
        stream: isStream
      })

      const response = await axios(requestOptions)

      if (response.status >= 400) {
        return await this._handleOpenAIError(response, res, account, sessionHash, isStream)
      }

      await copilotAccountService.updateAccountUsage(account.id, 0).catch(() => {})

      if (isStream && response.data && typeof response.data.pipe === 'function') {
        return this._handleOpenAIStream(response, res, account, apiKeyData, req, selection)
      }

      return await this._handleOpenAINonStream(response, res, account, apiKeyData, req, selection)
    } catch (error) {
      if (abortController && !abortController.signal.aborted) {
        abortController.abort()
      }
      logger.error('Copilot Responses relay error:', {
        message: error.message,
        code: error.code,
        status: error.response?.status
      })
      if ((error.code === 'ECONNREFUSED' || error.code === 'ETIMEDOUT') && account?.id) {
        await upstreamErrorHelper.markTempUnavailable(account.id, 'copilot', 503).catch(() => {})
      }
      if (!res.headersSent) {
        return res.status(error.response?.status || 500).json({
          error: { message: error.message || 'Copilot upstream request failed' }
        })
      }
      return res.end()
    } finally {
      req.removeListener('close', handleClientDisconnect)
      res.removeListener('close', handleClientDisconnect)
    }
  }

  _buildAnthropicHeaders(incomingHeaders, account, apiKeyData, selection) {
    const headers = {
      ...filterForClaude(incomingHeaders || {}),
      'Content-Type': 'application/json',
      'User-Agent':
        account.userAgent || incomingHeaders?.['user-agent'] || 'claude-relay-service/1.0.0',
      'x-copilot-requested-model': selection.requestedModel || '',
      'x-copilot-resolved-model': selection.resolvedModel || ''
    }

    if (account.apiKey) {
      if (account.apiKey.startsWith('sk-ant-')) {
        headers['x-api-key'] = account.apiKey
      } else {
        headers.Authorization = `Bearer ${account.apiKey}`
      }
    }
    attachRelayKeyIdHeader(headers, apiKeyData?.id, ANTHROPIC_CAPTURE_HOOK_ACTIVE)
    return headers
  }

  _buildOpenAIHeaders(incomingHeaders, account, apiKeyData, selection) {
    const headers = {
      ...filterForOpenAI(incomingHeaders || {}),
      'Content-Type': 'application/json',
      'x-copilot-requested-model': selection.requestedModel || '',
      'x-copilot-resolved-model': selection.resolvedModel || ''
    }
    if (account.apiKey) {
      headers.Authorization = `Bearer ${account.apiKey}`
    }
    if (account.userAgent) {
      headers['User-Agent'] = account.userAgent
    } else if (incomingHeaders?.['user-agent']) {
      headers['User-Agent'] = incomingHeaders['user-agent']
    }
    attachRelayKeyIdHeader(headers, apiKeyData?.id, OPENAI_CAPTURE_HOOK_ACTIVE)
    return headers
  }

  _buildAxiosOptions(account, headers, body, isStream) {
    const requestOptions = {
      headers,
      data: body,
      timeout: this.defaultTimeout,
      responseType: isStream ? 'stream' : 'json',
      validateStatus: () => true
    }

    const proxyAgent = copilotAccountService._createProxyAgent(account.proxy)
    if (proxyAgent) {
      requestOptions.httpAgent = proxyAgent
      requestOptions.httpsAgent = proxyAgent
      requestOptions.proxy = false
      logger.info(
        `Using proxy for Copilot request: ${ProxyHelper.getProxyDescription(account.proxy)}`
      )
    }
    return requestOptions
  }

  _parseAnthropicUsageChunk(chunk, usageState) {
    const lines = chunk.toString('utf8').split('\n')
    for (const line of lines) {
      if (!line.startsWith('data: ')) {
        continue
      }
      const json = safeJsonParse(line.slice(6).trim())
      if (!json || typeof json !== 'object') {
        continue
      }
      if (json.type === 'message_start' && json.message) {
        usageState.model = json.message.model || usageState.model
        this._mergeAnthropicUsage(usageState, json.message.usage)
      } else if (json.type === 'message_delta') {
        this._mergeAnthropicUsage(usageState, json.usage || json.delta?.usage)
      } else if (json.usage) {
        this._mergeAnthropicUsage(usageState, json.usage)
      }
    }
  }

  _mergeAnthropicUsage(target, usage) {
    if (!usage || typeof usage !== 'object') {
      return
    }
    const numericFields = [
      'input_tokens',
      'output_tokens',
      'cache_creation_input_tokens',
      'cache_read_input_tokens'
    ]
    for (const field of numericFields) {
      if (Number.isFinite(usage[field])) {
        target[field] = usage[field]
      }
    }
    if (usage.cache_creation && typeof usage.cache_creation === 'object') {
      target.cache_creation = {
        ...(target.cache_creation || {}),
        ...usage.cache_creation
      }
    }
  }

  async _handleAnthropicStatus(account, status) {
    if (status === 401 || status === 403) {
      await copilotAccountService.markAccountUnauthorized(account.id).catch(() => {})
      await upstreamErrorHelper.markTempUnavailable(account.id, 'copilot', status).catch(() => {})
      return
    }
    if (status === 429) {
      await copilotAccountService.markAccountRateLimited(account.id).catch(() => {})
      await upstreamErrorHelper.markTempUnavailable(account.id, 'copilot', 429).catch(() => {})
      return
    }
    if (status === 529) {
      await copilotAccountService.markAccountOverloaded(account.id).catch(() => {})
      await upstreamErrorHelper.markTempUnavailable(account.id, 'copilot', 529).catch(() => {})
      return
    }
    if (status >= 500) {
      await upstreamErrorHelper.markTempUnavailable(account.id, 'copilot', status).catch(() => {})
      return
    }
    if (status >= 200 && status < 300) {
      if (await copilotAccountService.isAccountRateLimited(account.id)) {
        await copilotAccountService.removeAccountRateLimit(account.id).catch(() => {})
      }
      if (await copilotAccountService.isAccountOverloaded(account.id)) {
        await copilotAccountService.removeAccountOverload(account.id).catch(() => {})
      }
    }
  }

  async _handleOpenAIError(response, res, account, sessionHash, isStream) {
    let errorData = response.data
    if (response.data && typeof response.data.pipe === 'function') {
      const raw = await readStreamBody(response.data)
      errorData = safeJsonParse(raw) || { error: { message: raw || 'Upstream error' } }
    }

    if (response.status === 401 || response.status === 403) {
      await copilotScheduler.markAccountUnauthorized(account.id, sessionHash).catch(() => {})
      await upstreamErrorHelper
        .markTempUnavailable(account.id, 'copilot', response.status)
        .catch(() => {})
    } else if (response.status === 429) {
      const retryAfter = upstreamErrorHelper.parseRetryAfter(response.headers)
      await copilotScheduler
        .markAccountRateLimited(account.id, sessionHash, retryAfter)
        .catch(() => {})
      await upstreamErrorHelper
        .markTempUnavailable(account.id, 'copilot', 429, retryAfter)
        .catch(() => {})
    } else if (response.status >= 500) {
      await upstreamErrorHelper
        .markTempUnavailable(account.id, 'copilot', response.status)
        .catch(() => {})
      if (sessionHash) {
        await copilotScheduler._deleteSessionMapping(sessionHash).catch(() => {})
      }
    }

    if (isStream && !res.headersSent) {
      return res.status(response.status).json(upstreamErrorHelper.sanitizeErrorForClient(errorData))
    }
    return res.status(response.status).json(upstreamErrorHelper.sanitizeErrorForClient(errorData))
  }

  async _handleOpenAINonStream(response, res, account, apiKeyData, req, selection) {
    const responseData = response.data
    const usageData = responseData?.usage || responseData?.response?.usage
    const actualModel =
      responseData?.model || responseData?.response?.model || selection.resolvedModel

    if (usageData) {
      await this._recordOpenAIUsage(
        usageData,
        actualModel,
        response.status,
        false,
        account,
        apiKeyData,
        req,
        selection
      )
    }

    res.status(response.status).json(responseData)
  }

  _handleOpenAIStream(response, res, account, apiKeyData, req, selection) {
    res.status(response.status)
    res.setHeader('Content-Type', 'text/event-stream')
    res.setHeader('Cache-Control', 'no-cache')
    res.setHeader('Connection', 'keep-alive')
    res.setHeader('X-Accel-Buffering', 'no')
    if (typeof res.flushHeaders === 'function') {
      res.flushHeaders()
    }

    const parser = new IncrementalSSEParser()
    let usageData = null
    let actualModel = null
    let usageReported = false
    let rateLimitDetected = false

    const cleanupUpstream = () => {
      try {
        if (
          response.data &&
          !response.data.destroyed &&
          typeof response.data.destroy === 'function'
        ) {
          response.data.destroy()
        }
      } catch (_) {}
    }
    const removeClientDisconnectListeners = () => {
      req?.removeListener?.('close', cleanupUpstream)
      req?.removeListener?.('aborted', cleanupUpstream)
      res.removeListener('close', cleanupUpstream)
    }

    req?.once?.('close', cleanupUpstream)
    req?.once?.('aborted', cleanupUpstream)
    res.once('close', cleanupUpstream)

    const processEvent = (eventData) => {
      if (eventData?.type === 'response.completed' && eventData.response) {
        actualModel = eventData.response.model || actualModel
        usageData = eventData.response.usage || usageData
      }
      if (eventData?.error?.type === 'usage_limit_reached') {
        rateLimitDetected = true
      }
    }

    response.data.on('data', (chunk) => {
      if (!res.destroyed) {
        res.write(chunk)
      }
      const events = parser.feed(chunk.toString('utf8'))
      for (const event of events) {
        if (event.type === 'data') {
          processEvent(event.data)
        }
      }
    })

    response.data.on('end', async () => {
      removeClientDisconnectListeners()
      const remaining = parser.getRemaining()
      if (remaining.trim()) {
        const events = parser.feed('\n\n')
        for (const event of events) {
          if (event.type === 'data') {
            processEvent(event.data)
          }
        }
      }

      if (!usageReported && usageData) {
        try {
          await this._recordOpenAIUsage(
            usageData,
            actualModel || selection.resolvedModel,
            response.status,
            true,
            account,
            apiKeyData,
            req,
            selection
          )
          usageReported = true
        } catch (error) {
          logger.error('Failed to record Copilot Responses stream usage:', error)
        }
      }

      if (rateLimitDetected) {
        await copilotScheduler
          .markAccountRateLimited(account.id, buildSessionHash(req))
          .catch(() => {})
      } else if (response.status >= 200 && response.status < 300) {
        if (await copilotAccountService.isAccountRateLimited(account.id)) {
          await copilotAccountService.removeAccountRateLimit(account.id).catch(() => {})
        }
      }

      if (!res.destroyed) {
        res.end()
      }
    })

    response.data.on('error', (error) => {
      removeClientDisconnectListeners()
      logger.error('Copilot Responses stream error:', error)
      if (!res.headersSent) {
        res.status(502).json({ error: { message: 'Upstream stream error' } })
      } else if (!res.destroyed) {
        res.end()
      }
    })
  }

  async _recordOpenAIUsage(
    usageData,
    actualModel,
    statusCode,
    stream,
    account,
    apiKeyData,
    req,
    selection
  ) {
    const usage = normalizeOpenAIUsage(usageData)
    const serviceTier = req._serviceTier || null
    const requestBodyForDetail = {
      ...(req.body || {}),
      model: selection.requestedModelWithPrefix,
      resolved_model: selection.resolvedModel
    }

    await apiKeyService.recordUsage(
      apiKeyData.id,
      usage.actualInputTokens,
      usage.outputTokens,
      usage.cacheCreateTokens,
      usage.cacheReadTokens,
      actualModel || selection.resolvedModel,
      account.id,
      'copilot',
      serviceTier,
      createRequestDetailMeta(req, {
        requestBody: requestBodyForDetail,
        stream,
        statusCode
      })
    )

    await copilotAccountService.updateAccountUsage(account.id, usage.totalTokens)
    if (Number(account.dailyQuota || 0) > 0) {
      const CostCalculator = require('../../utils/costCalculator')
      const costInfo = CostCalculator.calculateCost(
        {
          input_tokens: usage.actualInputTokens,
          output_tokens: usage.outputTokens,
          cache_creation_input_tokens: usage.cacheCreateTokens,
          cache_read_input_tokens: usage.cacheReadTokens
        },
        actualModel || selection.resolvedModel,
        serviceTier
      )
      await copilotAccountService.updateUsageQuota(account.id, costInfo.costs.total)
    }
  }
}

module.exports = new CopilotRelayService()
