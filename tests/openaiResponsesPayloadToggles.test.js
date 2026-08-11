const crypto = require('crypto')
const { PassThrough } = require('stream')

const mockRouter = {
  get: jest.fn(),
  post: jest.fn()
}

jest.mock(
  'express',
  () => ({
    Router: () => mockRouter
  }),
  { virtual: true }
)

jest.mock(
  '../config/config',
  () => ({
    requestTimeout: 1000
  }),
  { virtual: true }
)

jest.mock('../src/middleware/auth', () => ({
  authenticateApiKey: jest.fn((_req, _res, next) => next())
}))

jest.mock('axios', () => ({
  post: jest.fn()
}))

jest.mock('../src/services/scheduler/unifiedOpenAIScheduler', () => ({
  selectAccountForApiKey: jest.fn(),
  markAccountTemporarilyUnavailable: jest.fn(),
  markAccountRateLimited: jest.fn(),
  isAccountRateLimited: jest.fn().mockResolvedValue(false),
  removeAccountRateLimit: jest.fn(),
  markAccountUnauthorized: jest.fn()
}))

jest.mock('../src/services/account/openaiAccountService', () => ({
  getAccount: jest.fn(),
  decrypt: jest.fn(),
  isTokenExpired: jest.fn(() => false),
  refreshAccountToken: jest.fn(),
  updateCodexUsageSnapshot: jest.fn()
}))

jest.mock('../src/services/account/openaiResponsesAccountService', () => ({
  getAccount: jest.fn()
}))

jest.mock('../src/services/relay/openaiResponsesRelayService', () => ({
  handleRequest: jest.fn()
}))

jest.mock('../src/services/apiKeyService', () => ({
  hasPermission: jest.fn(() => true),
  recordUsage: jest.fn()
}))

jest.mock('../src/models/redis', () => ({
  getUsageStats: jest.fn()
}))

jest.mock('../src/utils/logger', () => ({
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
  debug: jest.fn(),
  api: jest.fn(),
  security: jest.fn()
}))

jest.mock('../src/utils/proxyHelper', () => ({
  createProxyAgent: jest.fn(() => null),
  getProxyDescription: jest.fn(() => 'none')
}))

jest.mock('../src/utils/rateLimitHelper', () => ({
  updateRateLimitCounters: jest.fn()
}))

jest.mock('../src/utils/sseParser', () => ({
  IncrementalSSEParser: jest.fn().mockImplementation(() => ({
    feed: jest.fn(() => []),
    getRemaining: jest.fn(() => '')
  }))
}))

jest.mock('../src/utils/errorSanitizer', () => ({
  getSafeMessage: jest.fn((error) => error?.message || 'error')
}))

jest.mock('../src/utils/requestDetailHelper', () => ({
  createRequestDetailMeta: jest.fn(() => null),
  extractOpenAICacheReadTokens: jest.fn(() => 0)
}))

const unifiedOpenAIScheduler = require('../src/services/scheduler/unifiedOpenAIScheduler')
const axios = require('axios')
const apiKeyService = require('../src/services/apiKeyService')
const openaiAccountService = require('../src/services/account/openaiAccountService')
const openaiResponsesAccountService = require('../src/services/account/openaiResponsesAccountService')
const openaiResponsesRelayService = require('../src/services/relay/openaiResponsesRelayService')
const openaiRoutes = require('../src/routes/openaiRoutes')
const { applyCodexResponsesLitePayload } = require('../src/utils/testPayloadHelper')

function createHash(value) {
  return crypto.createHash('sha256').update(value).digest('hex')
}

function createReq({
  path = '/v1/responses',
  body = {},
  userAgent = 'my-client/1.0',
  apiKeyOverrides = {},
  fromUnifiedEndpoint = false
} = {}) {
  return {
    method: 'POST',
    path,
    originalUrl: `/openai${path}`,
    headers: {
      'user-agent': userAgent
    },
    body: JSON.parse(JSON.stringify(body)),
    apiKey: {
      id: 'key_1',
      permissions: ['openai'],
      enableOpenAIResponsesCodexAdaptation: true,
      enableOpenAIResponsesPayloadRules: false,
      openaiResponsesPayloadRules: [],
      ...apiKeyOverrides
    },
    _fromUnifiedEndpoint: fromUnifiedEndpoint,
    on: jest.fn()
  }
}

function createRes() {
  const res = {
    statusCode: 200,
    headers: {},
    destroyed: false,
    writableEnded: false,
    headersSent: false,
    status: jest.fn((code) => {
      res.statusCode = code
      return res
    }),
    json: jest.fn((payload) => {
      res.payload = payload
      return res
    }),
    setHeader: jest.fn((key, value) => {
      res.headers[key] = value
    }),
    set: jest.fn((key, value) => {
      res.headers[key] = value
      return res
    }),
    flushHeaders: jest.fn(() => {
      res.headersSent = true
    }),
    write: jest.fn((chunk) => {
      res.headersSent = true
      res.chunks = res.chunks || []
      res.chunks.push(chunk)
      return true
    }),
    end: jest.fn(() => {
      res.writableEnded = true
    })
  }
  return res
}

describe('openai responses payload toggles', () => {
  beforeEach(() => {
    jest.clearAllMocks()

    unifiedOpenAIScheduler.selectAccountForApiKey.mockResolvedValue({
      accountId: 'resp-1',
      accountType: 'openai-responses'
    })

    openaiResponsesAccountService.getAccount.mockResolvedValue({
      id: 'resp-1',
      name: 'Responses Account',
      apiKey: 'sk-responses'
    })

    openaiResponsesRelayService.handleRequest.mockResolvedValue({ ok: true })
    openaiAccountService.decrypt.mockReturnValue('decrypted-token')
    unifiedOpenAIScheduler.markAccountTemporarilyUnavailable.mockResolvedValue({ success: true })
  })

  test('temporarily cools down an OAuth account after an HTTP 5xx response', async () => {
    unifiedOpenAIScheduler.selectAccountForApiKey.mockResolvedValue({
      accountId: 'openai-1',
      accountType: 'openai'
    })
    openaiAccountService.getAccount.mockResolvedValue({
      id: 'openai-1',
      name: 'OpenAI Account',
      accessToken: 'encrypted-token',
      accountId: 'chatgpt-account-1'
    })
    axios.post.mockResolvedValue({
      status: 503,
      data: {
        error: {
          type: 'server_error',
          message: 'Model is at capacity'
        }
      },
      headers: {
        'retry-after': '12',
        'x-request-id': 'req-http-503'
      }
    })

    const req = createReq({
      body: {
        model: 'gpt-5.6-sol',
        prompt_cache_key: 'failed-http-session',
        stream: false
      },
      userAgent: 'codex_cli_rs/0.146.0'
    })
    const res = createRes()

    await openaiRoutes.handleResponses(req, res)

    expect(unifiedOpenAIScheduler.markAccountTemporarilyUnavailable).toHaveBeenCalledWith(
      'openai-1',
      'openai',
      createHash('failed-http-session'),
      12,
      503,
      expect.objectContaining({
        source: 'codex_http',
        model: 'gpt-5.6-sol',
        requestId: 'req-http-503'
      })
    )
    expect(res.statusCode).toBe(503)
    expect(res.payload.error.message).toBe('Model is at capacity')
  })

  test('uses Retry-After for a transient Codex TPM 429 instead of the one-hour fallback', async () => {
    unifiedOpenAIScheduler.selectAccountForApiKey.mockResolvedValue({
      accountId: 'openai-1',
      accountType: 'openai'
    })
    openaiAccountService.getAccount.mockResolvedValue({
      id: 'openai-1',
      name: 'OpenAI Account',
      accessToken: 'encrypted-token',
      accountId: 'chatgpt-account-1'
    })
    axios.post.mockResolvedValue({
      status: 429,
      data: {
        error: {
          type: 'rate_limit_exceeded',
          message: 'Rate limit reached for gpt-5.6-sol. Please try again in 10.632s.'
        }
      },
      headers: {
        'retry-after': '10.632',
        'x-request-id': 'req-tpm-429'
      }
    })

    const req = createReq({
      body: {
        model: 'gpt-5.6-sol',
        prompt_cache_key: 'large-session',
        stream: false
      },
      userAgent: 'codex_cli_rs/0.146.0'
    })
    const res = createRes()

    await openaiRoutes.handleResponses(req, res)

    expect(unifiedOpenAIScheduler.markAccountRateLimited).toHaveBeenCalledWith(
      'openai-1',
      'openai',
      createHash('large-session'),
      11
    )
    expect(res.statusCode).toBe(429)
    expect(res.headers['Retry-After']).toBe('10.632')
  })

  test('prefers an authoritative usage reset in the 429 body over Retry-After', async () => {
    unifiedOpenAIScheduler.selectAccountForApiKey.mockResolvedValue({
      accountId: 'openai-1',
      accountType: 'openai'
    })
    openaiAccountService.getAccount.mockResolvedValue({
      id: 'openai-1',
      name: 'OpenAI Account',
      accessToken: 'encrypted-token',
      accountId: 'chatgpt-account-1'
    })
    axios.post.mockResolvedValue({
      status: 429,
      data: {
        error: {
          type: 'usage_limit_reached',
          message: 'The usage limit has been reached',
          resets_in_seconds: 7200
        }
      },
      headers: { 'retry-after': '12' }
    })

    const req = createReq({
      body: {
        model: 'gpt-5.6-sol',
        prompt_cache_key: 'quota-session',
        stream: false
      },
      userAgent: 'codex_cli_rs/0.146.0'
    })

    await openaiRoutes.handleResponses(req, createRes())

    expect(unifiedOpenAIScheduler.markAccountRateLimited).toHaveBeenCalledWith(
      'openai-1',
      'openai',
      createHash('quota-session'),
      7200
    )
  })

  test('cools down the OAuth account before closing a capacity-failed SSE stream', async () => {
    const ActualSSEParser = jest.requireActual('../src/utils/sseParser').IncrementalSSEParser
    const { IncrementalSSEParser } = require('../src/utils/sseParser')
    IncrementalSSEParser.mockImplementationOnce(() => new ActualSSEParser())

    unifiedOpenAIScheduler.selectAccountForApiKey.mockResolvedValue({
      accountId: 'openai-1',
      accountType: 'openai'
    })
    openaiAccountService.getAccount.mockResolvedValue({
      id: 'openai-1',
      name: 'OpenAI Account',
      accessToken: 'encrypted-token',
      accountId: 'chatgpt-account-1'
    })

    let resolveCooldown
    unifiedOpenAIScheduler.markAccountTemporarilyUnavailable.mockReturnValue(
      new Promise((resolve) => {
        resolveCooldown = resolve
      })
    )

    const upstreamStream = new PassThrough()
    axios.post.mockResolvedValue({
      status: 200,
      data: upstreamStream,
      headers: { 'x-request-id': 'req-sse-capacity' }
    })

    const req = createReq({
      body: {
        model: 'gpt-5.6-sol',
        prompt_cache_key: 'failed-sse-session',
        stream: true
      },
      userAgent: 'codex_cli_rs/0.146.0'
    })
    const res = createRes()

    await openaiRoutes.handleResponses(req, res)
    upstreamStream.end(
      'data: {"type":"response.failed","response":{"status":"failed","error":{"message":"Model is at capacity"}}}\n\n'
    )
    await new Promise((resolve) => setImmediate(resolve))

    expect(unifiedOpenAIScheduler.markAccountTemporarilyUnavailable).toHaveBeenCalledWith(
      'openai-1',
      'openai',
      createHash('failed-sse-session'),
      null,
      503,
      expect.objectContaining({
        source: 'codex_sse',
        requestId: 'req-sse-capacity'
      })
    )
    expect(res.end).not.toHaveBeenCalled()
    expect(res.write).not.toHaveBeenCalled()

    resolveCooldown({ success: true })
    await new Promise((resolve) => setImmediate(resolve))

    expect(res.write).toHaveBeenCalledTimes(1)
    expect(res.end).toHaveBeenCalledTimes(1)
  })

  test('keeps standard responses payload unchanged for openai-responses when both toggles are off', async () => {
    const req = createReq({
      body: {
        model: 'gpt-5-2025-08-07',
        temperature: 0.2,
        service_tier: 'priority',
        prompt_cache_key: 'session-a'
      },
      apiKeyOverrides: {
        enableOpenAIResponsesCodexAdaptation: false,
        enableOpenAIResponsesPayloadRules: false
      }
    })

    await openaiRoutes.handleResponses(req, createRes())

    expect(req.body).toEqual({
      model: 'gpt-5-2025-08-07',
      temperature: 0.2,
      service_tier: 'priority',
      prompt_cache_key: 'session-a'
    })
    expect(unifiedOpenAIScheduler.selectAccountForApiKey).toHaveBeenCalledWith(
      req.apiKey,
      createHash('session-a'),
      'gpt-5'
    )
  })

  test('preserves an explicit GPT-5.6 reasoning configuration', async () => {
    const req = createReq({
      body: {
        model: 'gpt-5.6-sol',
        reasoning: { effort: 'high', summary: 'auto' }
      },
      apiKeyOverrides: {
        enableOpenAIResponsesCodexAdaptation: false,
        enableOpenAIResponsesPayloadRules: false
      }
    })

    await openaiRoutes.handleResponses(req, createRes())

    expect(req.body.reasoning).toEqual({ effort: 'high', summary: 'auto' })
  })

  test('does not apply the internal Lite payload contract to openai-responses accounts', async () => {
    const req = createReq({
      body: {
        model: 'gpt-5.6-luna',
        input: 'hello'
      },
      apiKeyOverrides: {
        enableOpenAIResponsesCodexAdaptation: false,
        enableOpenAIResponsesPayloadRules: false
      }
    })

    await openaiRoutes.handleResponses(req, createRes())

    expect(openaiResponsesRelayService.handleRequest).toHaveBeenCalled()
    expect(req.body).toEqual({
      model: 'gpt-5.6-luna',
      input: 'hello'
    })
  })

  test('does not duplicate Lite input items from an official Codex CLI payload', () => {
    const payload = {
      model: 'gpt-5.6-terra',
      input: [
        {
          type: 'additional_tools',
          role: 'developer',
          tools: [{ type: 'function', name: 'shell_command' }]
        },
        {
          type: 'message',
          role: 'user',
          content: [{ type: 'input_text', text: 'hello' }]
        }
      ],
      tool_choice: 'auto',
      parallel_tool_calls: false,
      reasoning: { effort: 'high', context: 'all_turns' },
      include: ['reasoning.encrypted_content']
    }

    const originalInput = JSON.parse(JSON.stringify(payload.input))
    applyCodexResponsesLitePayload(payload)

    expect(payload.input).toEqual(originalInput)
    expect(payload.input.filter((item) => item.type === 'additional_tools')).toHaveLength(1)
    expect(payload.reasoning).toEqual({ effort: 'high', context: 'all_turns' })
  })

  test('applies Codex adaptation only when adaptation toggle is on', async () => {
    const req = createReq({
      body: {
        model: 'gpt-5-2025-08-07',
        temperature: 0.2,
        service_tier: 'priority',
        prompt_cache_key: 'session-b'
      }
    })

    await openaiRoutes.handleResponses(req, createRes())

    expect(req.body.model).toBe('gpt-5')
    expect(req.body.instructions).toBe(openaiRoutes.CODEX_CLI_INSTRUCTIONS)
    expect(req.body.temperature).toBeUndefined()
    expect(req.body.service_tier).toBeUndefined()
    expect(unifiedOpenAIScheduler.selectAccountForApiKey).toHaveBeenCalledWith(
      req.apiKey,
      createHash('session-b'),
      'gpt-5'
    )
  })

  test('applies payload rules directly on the original payload when adaptation is off', async () => {
    const req = createReq({
      body: {
        model: 'gpt-4.1',
        temperature: 0.5,
        prompt_cache_key: 'old-key',
        text: { format: {} }
      },
      apiKeyOverrides: {
        enableOpenAIResponsesCodexAdaptation: false,
        enableOpenAIResponsesPayloadRules: true,
        openaiResponsesPayloadRules: [
          { path: 'model', valueType: 'string', value: 'gpt-5' },
          { path: 'prompt_cache_key', valueType: 'string', value: 'new-key' },
          { path: 'text.format.type', valueType: 'string', value: 'json_schema' }
        ]
      }
    })

    await openaiRoutes.handleResponses(req, createRes())

    expect(req.body).toEqual({
      model: 'gpt-5',
      temperature: 0.5,
      prompt_cache_key: 'new-key',
      text: {
        format: {
          type: 'json_schema'
        }
      }
    })
    expect(req.body.instructions).toBeUndefined()
    expect(unifiedOpenAIScheduler.selectAccountForApiKey).toHaveBeenCalledWith(
      req.apiKey,
      createHash('new-key'),
      'gpt-5'
    )
  })

  test('applies payload rules after Codex adaptation when both toggles are on', async () => {
    const req = createReq({
      body: {
        model: 'gpt-5-2025-08-07',
        prompt_cache_key: 'legacy-key',
        temperature: 0.2,
        instructions: 'raw'
      },
      apiKeyOverrides: {
        enableOpenAIResponsesCodexAdaptation: true,
        enableOpenAIResponsesPayloadRules: true,
        openaiResponsesPayloadRules: [
          { path: 'model', valueType: 'string', value: 'gpt-5.5' },
          { path: 'instructions', valueType: 'string', value: 'custom instructions' },
          { path: 'prompt_cache_key', valueType: 'string', value: 'rule-key' }
        ]
      }
    })

    await openaiRoutes.handleResponses(req, createRes())

    expect(req.body.model).toBe('gpt-5.5')
    expect(req.body.instructions).toBe('custom instructions')
    expect(req.body.temperature).toBeUndefined()
    expect(unifiedOpenAIScheduler.selectAccountForApiKey).toHaveBeenCalledWith(
      req.apiKey,
      createHash('rule-key'),
      'gpt-5.5'
    )
  })

  test('normalizes dated gpt-5 models only for scheduling and upstream openai requests when adaptation is off', async () => {
    unifiedOpenAIScheduler.selectAccountForApiKey.mockResolvedValue({
      accountId: 'openai-1',
      accountType: 'openai'
    })
    openaiAccountService.getAccount.mockResolvedValue({
      id: 'openai-1',
      name: 'OpenAI Account',
      accessToken: 'encrypted-token',
      accountId: 'chatgpt-account-1'
    })
    axios.post.mockResolvedValue({
      status: 200,
      data: {
        model: 'gpt-5',
        usage: {
          input_tokens: 10,
          output_tokens: 4,
          total_tokens: 14
        }
      },
      headers: {}
    })

    const req = createReq({
      body: {
        model: 'gpt-5-2025-08-07',
        service_tier: 'priority',
        prompt_cache_key: 'compat-key',
        stream: false
      },
      apiKeyOverrides: {
        enableOpenAIResponsesCodexAdaptation: false,
        enableOpenAIResponsesPayloadRules: false
      }
    })

    await openaiRoutes.handleResponses(req, createRes())

    expect(unifiedOpenAIScheduler.selectAccountForApiKey).toHaveBeenCalledWith(
      req.apiKey,
      createHash('compat-key'),
      'gpt-5'
    )
    expect(req.body.model).toBe('gpt-5')
    expect(req.body.service_tier).toBe('priority')
    expect(axios.post).toHaveBeenCalled()
    expect(axios.post.mock.calls[0][1]).toMatchObject({
      model: 'gpt-5',
      service_tier: 'priority',
      store: false
    })
  })

  test('selects the Responses Lite route when forwarding GPT-5.6 through an openai account', async () => {
    unifiedOpenAIScheduler.selectAccountForApiKey.mockResolvedValue({
      accountId: 'openai-1',
      accountType: 'openai'
    })
    openaiAccountService.getAccount.mockResolvedValue({
      id: 'openai-1',
      name: 'OpenAI Account',
      accessToken: 'encrypted-token',
      accountId: 'chatgpt-account-1'
    })
    axios.post.mockResolvedValue({
      status: 200,
      data: {
        model: 'gpt-5.6-luna',
        usage: {
          input_tokens: 8,
          output_tokens: 3,
          total_tokens: 11
        }
      },
      headers: {
        'x-codex-turn-state': 'turn-state-1',
        'x-codex-primary-used-percent': '12',
        'x-ratelimit-remaining-requests': '99',
        'retry-after': '3',
        'set-cookie': 'must-not-pass'
      }
    })

    const req = createReq({
      body: {
        model: 'gpt-5.6-luna',
        input: 'hello',
        tool_choice: 'required',
        stream: false
      }
    })

    const res = createRes()
    await openaiRoutes.handleResponses(req, res)

    expect(axios.post.mock.calls[0][2].headers).toMatchObject({
      'x-openai-internal-codex-responses-lite': 'true'
    })
    expect(axios.post.mock.calls[0][1].reasoning).toEqual({
      effort: 'medium',
      context: 'all_turns'
    })
    expect(axios.post.mock.calls[0][1]).toMatchObject({
      tool_choice: 'auto',
      parallel_tool_calls: false,
      text: { verbosity: 'low' },
      include: ['reasoning.encrypted_content']
    })
    expect(axios.post.mock.calls[0][1].instructions).toBeUndefined()
    expect(axios.post.mock.calls[0][1].input[0]).toEqual({
      type: 'additional_tools',
      role: 'developer',
      tools: []
    })
    expect(res.headers).toMatchObject({
      'x-codex-turn-state': 'turn-state-1',
      'x-codex-primary-used-percent': '12',
      'x-ratelimit-remaining-requests': '99',
      'retry-after': '3'
    })
    expect(res.headers['set-cookie']).toBeUndefined()
  })

  test('normalizes payload-rule gpt-5 aliases for openai scheduling without applying full Codex adaptation', async () => {
    unifiedOpenAIScheduler.selectAccountForApiKey.mockResolvedValue({
      accountId: 'openai-1',
      accountType: 'openai'
    })
    openaiAccountService.getAccount.mockResolvedValue({
      id: 'openai-1',
      name: 'OpenAI Account',
      accessToken: 'encrypted-token',
      accountId: 'chatgpt-account-1'
    })
    axios.post.mockResolvedValue({
      status: 200,
      data: {
        model: 'gpt-5',
        usage: {
          input_tokens: 8,
          output_tokens: 3,
          total_tokens: 11
        }
      },
      headers: {}
    })

    const req = createReq({
      body: {
        model: 'gpt-4.1',
        text: { format: {} },
        prompt_cache_key: 'rule-model-key',
        stream: false
      },
      apiKeyOverrides: {
        enableOpenAIResponsesCodexAdaptation: false,
        enableOpenAIResponsesPayloadRules: true,
        openaiResponsesPayloadRules: [
          { path: 'model', valueType: 'string', value: 'gpt-5-2025-08-07' }
        ]
      }
    })

    await openaiRoutes.handleResponses(req, createRes())

    expect(unifiedOpenAIScheduler.selectAccountForApiKey).toHaveBeenCalledWith(
      req.apiKey,
      createHash('rule-model-key'),
      'gpt-5'
    )
    expect(req.body.model).toBe('gpt-5')
    expect(req.body.text).toEqual({ format: {} })
    expect(req.body.instructions).toBeUndefined()
    expect(axios.post.mock.calls[0][1]).toMatchObject({
      model: 'gpt-5',
      text: { format: {} },
      store: false
    })
  })

  test('records the mutated service_tier for standard responses sent through openai accounts', async () => {
    unifiedOpenAIScheduler.selectAccountForApiKey.mockResolvedValue({
      accountId: 'openai-1',
      accountType: 'openai'
    })
    openaiAccountService.getAccount.mockResolvedValue({
      id: 'openai-1',
      name: 'OpenAI Account',
      accessToken: 'encrypted-token',
      accountId: 'chatgpt-account-1'
    })
    axios.post.mockResolvedValue({
      status: 200,
      data: {
        model: 'gpt-4.1',
        usage: {
          input_tokens: 12,
          output_tokens: 6,
          total_tokens: 18
        }
      },
      headers: {}
    })

    const req = createReq({
      body: {
        model: 'gpt-4.1',
        prompt_cache_key: 'tier-rule-key',
        stream: false
      },
      apiKeyOverrides: {
        enableOpenAIResponsesCodexAdaptation: false,
        enableOpenAIResponsesPayloadRules: true,
        openaiResponsesPayloadRules: [
          { path: 'service_tier', valueType: 'string', value: 'priority' }
        ]
      }
    })

    await openaiRoutes.handleResponses(req, createRes())

    expect(req._serviceTier).toBe('priority')
    expect(apiKeyService.recordUsage).toHaveBeenCalled()
    expect(apiKeyService.recordUsage.mock.calls[0][8]).toBe('priority')
  })

  test('records null service_tier after Codex adaptation removes it for openai accounts', async () => {
    unifiedOpenAIScheduler.selectAccountForApiKey.mockResolvedValue({
      accountId: 'openai-1',
      accountType: 'openai'
    })
    openaiAccountService.getAccount.mockResolvedValue({
      id: 'openai-1',
      name: 'OpenAI Account',
      accessToken: 'encrypted-token',
      accountId: 'chatgpt-account-1'
    })
    axios.post.mockResolvedValue({
      status: 200,
      data: {
        model: 'gpt-5',
        usage: {
          input_tokens: 10,
          output_tokens: 4,
          total_tokens: 14
        }
      },
      headers: {}
    })

    const req = createReq({
      body: {
        model: 'gpt-5-2025-08-07',
        temperature: 0.2,
        service_tier: 'priority',
        prompt_cache_key: 'adapt-tier-key',
        stream: false
      }
    })

    await openaiRoutes.handleResponses(req, createRes())

    expect(req.body.service_tier).toBeUndefined()
    expect(req._serviceTier).toBeNull()
    expect(apiKeyService.recordUsage).toHaveBeenCalled()
    expect(apiKeyService.recordUsage.mock.calls[0][8]).toBeNull()
  })

  test('captures the post-rule service_tier before relaying openai-responses requests', async () => {
    const req = createReq({
      body: {
        model: 'gpt-4.1',
        prompt_cache_key: 'relay-tier-key'
      },
      apiKeyOverrides: {
        enableOpenAIResponsesCodexAdaptation: false,
        enableOpenAIResponsesPayloadRules: true,
        openaiResponsesPayloadRules: [
          { path: 'service_tier', valueType: 'string', value: 'priority' }
        ]
      }
    })

    await openaiRoutes.handleResponses(req, createRes())

    expect(req._serviceTier).toBe('priority')
    expect(openaiResponsesRelayService.handleRequest).toHaveBeenCalled()
    expect(openaiResponsesRelayService.handleRequest.mock.calls[0][0]._serviceTier).toBe('priority')
  })

  test('does not apply the new rule flow to compact responses routes', async () => {
    const req = createReq({
      path: '/v1/responses/compact',
      body: {
        model: 'o1-mini',
        prompt_cache_key: 'compact-key',
        temperature: 0.1
      },
      apiKeyOverrides: {
        enableOpenAIResponsesCodexAdaptation: false,
        enableOpenAIResponsesPayloadRules: true,
        openaiResponsesPayloadRules: [
          { path: 'model', valueType: 'string', value: 'gpt-5' },
          { path: 'prompt_cache_key', valueType: 'string', value: 'rule-key' }
        ]
      }
    })

    await openaiRoutes.handleResponses(req, createRes())

    expect(req.body.model).toBe('o1-mini')
    expect(req.body.prompt_cache_key).toBe('compact-key')
    expect(req.body.instructions).toBe(openaiRoutes.CODEX_CLI_INSTRUCTIONS)
  })

  test('forwards GPT-5.6 compact requests as unary JSON without Responses-only fields', async () => {
    unifiedOpenAIScheduler.selectAccountForApiKey.mockResolvedValue({
      accountId: 'openai-1',
      accountType: 'openai'
    })
    openaiAccountService.getAccount.mockResolvedValue({
      id: 'openai-1',
      name: 'OpenAI Account',
      accessToken: 'encrypted-token',
      accountId: 'chatgpt-account-1'
    })
    axios.post.mockResolvedValue({
      status: 200,
      data: {
        output: [
          {
            type: 'message',
            role: 'assistant',
            content: [{ type: 'output_text', text: 'compacted state' }]
          }
        ]
      },
      headers: { 'x-request-id': 'req-compact-1' }
    })

    const compactBody = {
      model: 'gpt-5.6-sol',
      input: [
        {
          type: 'message',
          role: 'user',
          content: [{ type: 'input_text', text: 'hello' }]
        }
      ],
      instructions: 'Compact the conversation.',
      parallel_tool_calls: false,
      reasoning: { effort: 'medium', summary: 'auto' },
      prompt_cache_key: 'compact-gpt-5.6-session',
      text: { verbosity: 'low' }
    }
    const req = createReq({
      path: '/v1/responses/compact',
      body: compactBody,
      userAgent: 'codex_cli_rs/0.146.0'
    })
    const res = createRes()

    await openaiRoutes.handleResponses(req, res)

    expect(axios.post.mock.calls[0][0]).toBe(
      'https://chatgpt.com/backend-api/codex/responses/compact'
    )
    expect(axios.post.mock.calls[0][1]).toEqual(compactBody)
    expect(axios.post.mock.calls[0][1].tool_choice).toBeUndefined()
    expect(axios.post.mock.calls[0][1].include).toBeUndefined()
    expect(axios.post.mock.calls[0][2].responseType).toBeUndefined()
    expect(res.headers['Content-Type']).toBe('application/json')
    expect(res.payload.output[0].content[0].text).toBe('compacted state')
  })
})
