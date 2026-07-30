jest.mock('../src/utils/proxyHelper', () => ({
  createProxyAgent: jest.fn(() => null),
  getProxyDescription: jest.fn(() => 'none')
}))

jest.mock('../src/utils/headerFilter', () => ({
  filterForClaude: jest.fn((headers) => headers || {})
}))

jest.mock('../src/services/account/claudeAccountService', () => ({
  getAccount: jest.fn(),
  getValidAccessToken: jest.fn(),
  clearExpiredOpusRateLimit: jest.fn(),
  isAccountOpusRateLimited: jest.fn(),
  markAccountOpusRateLimited: jest.fn(),
  markAccountModelRateLimited: jest.fn(),
  updateSessionWindowStatus: jest.fn(),
  clearInternalErrors: jest.fn(),
  isAccountOverloaded: jest.fn(),
  removeAccountOverload: jest.fn()
}))

jest.mock('../src/services/scheduler/unifiedClaudeScheduler', () => ({
  selectAccountForApiKey: jest.fn(),
  markAccountRateLimited: jest.fn(),
  isAccountRateLimited: jest.fn(),
  removeAccountRateLimit: jest.fn(),
  clearSessionMapping: jest.fn(),
  markAccountBlocked: jest.fn()
}))

jest.mock('../src/utils/sessionHelper', () => ({
  generateSessionHash: jest.fn(() => 'session-1')
}))

jest.mock('../src/utils/logger', () => ({
  debug: jest.fn(),
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
  success: jest.fn(),
  performance: jest.fn()
}))

jest.mock(
  '../config/config',
  () => ({
    claude: {
      apiVersion: '2023-06-01',
      betaHeader: 'claude-code-20250219',
      systemPrompt: "You are Claude Code, Anthropic's official CLI for Claude.",
      overloadHandling: {
        enabled: 0
      }
    }
  }),
  { virtual: true }
)

jest.mock('../src/services/claudeCodeHeadersService', () => ({
  storeAccountHeaders: jest.fn(),
  getAccountHeaders: jest.fn()
}))

jest.mock('../src/models/redis', () => ({}))

jest.mock('../src/validators/clients/claudeCodeValidator', () => ({
  includesClaudeCodeSystemPrompt: jest.fn(() => false)
}))

jest.mock('../src/utils/dateHelper', () => ({
  formatDateWithTimezone: jest.fn((value) => String(value))
}))

jest.mock('../src/services/requestIdentityService', () => ({
  getOrCreateIdentity: jest.fn()
}))

jest.mock('../src/utils/testPayloadHelper', () => ({
  createClaudeTestPayload: jest.fn(),
  sendStreamTestRequest: jest.fn()
}))

jest.mock('../src/services/userMessageQueueService', () => ({
  isUserMessageRequest: jest.fn(() => false),
  acquireQueueLock: jest.fn(),
  releaseQueueLock: jest.fn()
}))

jest.mock('../src/utils/streamHelper', () => ({
  isStreamWritable: jest.fn(() => true)
}))

jest.mock('../src/utils/upstreamErrorHelper', () => ({
  markTempUnavailable: jest.fn(),
  parseRetryAfter: jest.fn(() => 30)
}))

jest.mock('../src/utils/metadataUserIdHelper', () => ({
  extractSessionId: jest.fn()
}))

jest.mock('../src/utils/performanceOptimizer', () => ({
  getHttpsAgentForStream: jest.fn(() => null),
  getHttpsAgentForNonStream: jest.fn(() => null),
  getPricingData: jest.fn(() => null)
}))

jest.mock('../src/utils/relayKeyCapture', () => ({
  ANTHROPIC_CAPTURE_HOOK_ACTIVE: false,
  getRelayKeyIdForActiveCapture: jest.fn(() => null),
  attachRelayKeyIdHeader: jest.fn()
}))

const { EventEmitter } = require('events')
const claudeRelayService = require('../src/services/relay/claudeRelayService')
const claudeAccountService = require('../src/services/account/claudeAccountService')
const unifiedClaudeScheduler = require('../src/services/scheduler/unifiedClaudeScheduler')
const upstreamErrorHelper = require('../src/utils/upstreamErrorHelper')

function createRequestBody(overrides = {}) {
  return {
    model: 'claude-sonnet-4-6',
    stream: false,
    system: '',
    messages: [
      {
        role: 'user',
        content: [{ type: 'text', text: 'hello' }]
      }
    ],
    ...overrides
  }
}

function createRateLimitResponse() {
  return {
    statusCode: 429,
    headers: {
      'anthropic-ratelimit-unified-reset': '1767225600'
    },
    body: JSON.stringify({
      error: {
        message: "exceed your account's rate limit"
      }
    })
  }
}

describe('Claude Relay Agent View auxiliary request handling', () => {
  beforeEach(() => {
    jest.clearAllMocks()

    unifiedClaudeScheduler.selectAccountForApiKey.mockResolvedValue({
      accountId: 'claude-account-1',
      accountType: 'claude-official'
    })
    unifiedClaudeScheduler.isAccountRateLimited.mockResolvedValue(false)

    claudeAccountService.getAccount.mockResolvedValue({
      id: 'claude-account-1',
      name: 'Claude Account 1',
      rateLimitEndAt: null,
      proxy: null
    })
    claudeAccountService.getValidAccessToken.mockResolvedValue('access-token')
    claudeAccountService.isAccountOpusRateLimited.mockResolvedValue(false)
    claudeAccountService.markAccountModelRateLimited.mockResolvedValue(undefined)
    upstreamErrorHelper.markTempUnavailable.mockResolvedValue(undefined)

    jest.spyOn(claudeRelayService, '_getProxyAgent').mockResolvedValue(null)
    jest.spyOn(claudeRelayService, '_validateAndLimitMaxTokens').mockImplementation(() => {})
    jest
      .spyOn(claudeRelayService, '_makeClaudeRequest')
      .mockResolvedValue(createRateLimitResponse())
  })

  afterEach(() => {
    jest.restoreAllMocks()
  })

  it('detects Claude Code Agent View auxiliary label requests', () => {
    expect(
      claudeRelayService._isAgentViewAuxiliaryRequest(
        createRequestBody({
          messages: [
            {
              role: 'user',
              content: [{ type: 'text', text: 'Return a 2-4 word lowercase label for this job.' }]
            }
          ]
        }),
        { 'X-App': 'cli-bg' }
      )
    ).toBe(true)
  })

  it('does not treat streamed or non-cli-bg requests as Agent View auxiliary requests', () => {
    const body = createRequestBody({
      system: [{ type: 'text', text: 'A user kicked off a Claude Code agent' }]
    })

    expect(
      claudeRelayService._isAgentViewAuxiliaryRequest(
        { ...body, stream: true },
        { 'x-app': 'cli-bg' }
      )
    ).toBe(false)
    expect(claudeRelayService._isAgentViewAuxiliaryRequest(body, { 'x-app': 'cli' })).toBe(false)
  })

  it('skips account-level rate-limit marking for Agent View auxiliary 429 responses', async () => {
    await claudeRelayService.relayRequest(
      createRequestBody({
        system: [{ type: 'text', text: 'A user kicked off a Claude Code agent' }]
      }),
      { id: 'key-1', name: 'API Key 1' },
      new EventEmitter(),
      new EventEmitter(),
      { 'x-app': 'cli-bg' }
    )

    expect(unifiedClaudeScheduler.markAccountRateLimited).not.toHaveBeenCalled()
    expect(claudeAccountService.markAccountModelRateLimited).not.toHaveBeenCalled()
    expect(upstreamErrorHelper.markTempUnavailable).not.toHaveBeenCalledWith(
      'claude-account-1',
      'claude-official',
      429,
      expect.anything()
    )
  })

  it('keeps normal 429 model protection unchanged', async () => {
    await claudeRelayService.relayRequest(
      createRequestBody(),
      { id: 'key-1', name: 'API Key 1' },
      new EventEmitter(),
      new EventEmitter(),
      { 'x-app': 'cli-bg' }
    )

    expect(claudeAccountService.markAccountModelRateLimited).toHaveBeenCalledWith(
      'claude-account-1',
      'sonnet',
      1767225600
    )
    expect(unifiedClaudeScheduler.markAccountRateLimited).not.toHaveBeenCalled()
    expect(upstreamErrorHelper.markTempUnavailable).not.toHaveBeenCalled()
  })
})
