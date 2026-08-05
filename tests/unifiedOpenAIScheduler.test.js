const mockRedisDel = jest.fn()

jest.mock('../src/services/account/openaiAccountService', () => ({
  getAccount: jest.fn(),
  setAccountRateLimited: jest.fn()
}))

jest.mock('../src/services/account/openaiResponsesAccountService', () => ({
  getAccount: jest.fn(),
  markAccountRateLimited: jest.fn(),
  updateAccount: jest.fn()
}))

jest.mock('../src/services/accountGroupService', () => ({}))
jest.mock('../src/models/redis', () => ({
  getClientSafe: jest.fn(() => ({
    del: mockRedisDel
  }))
}))
jest.mock('../src/utils/logger', () => ({
  debug: jest.fn(),
  error: jest.fn(),
  info: jest.fn(),
  warn: jest.fn()
}))
jest.mock('../src/utils/commonHelper', () => ({
  isSchedulable: jest.fn((value) => value !== false && value !== 'false'),
  sortAccountsByPriority: jest.fn((accounts) => accounts)
}))
jest.mock('../src/utils/upstreamErrorHelper', () => ({
  classifyError: jest.fn((statusCode) => (statusCode >= 500 ? 'server_error' : null)),
  markTempUnavailable: jest.fn(),
  recordErrorHistory: jest.fn(),
  isTempUnavailable: jest.fn()
}))

const openaiAccountService = require('../src/services/account/openaiAccountService')
const openaiResponsesAccountService = require('../src/services/account/openaiResponsesAccountService')
const upstreamErrorHelper = require('../src/utils/upstreamErrorHelper')
const unifiedOpenAIScheduler = require('../src/services/scheduler/unifiedOpenAIScheduler')

describe('UnifiedOpenAIScheduler', () => {
  beforeEach(() => {
    jest.clearAllMocks()
    mockRedisDel.mockResolvedValue(1)
    openaiAccountService.getAccount.mockResolvedValue({
      id: 'account-1',
      disableAutoProtection: false
    })
    upstreamErrorHelper.markTempUnavailable.mockResolvedValue({ success: true, ttlSeconds: 60 })
    upstreamErrorHelper.recordErrorHistory.mockResolvedValue(undefined)
  })

  describe('markAccountTemporarilyUnavailable', () => {
    it('starts a cooldown and clears the failed sticky session', async () => {
      const context = { source: 'codex_sse', requestId: 'req-1' }

      await unifiedOpenAIScheduler.markAccountTemporarilyUnavailable(
        'account-1',
        'openai',
        'session-hash',
        45,
        503,
        context
      )

      expect(upstreamErrorHelper.markTempUnavailable).toHaveBeenCalledWith(
        'account-1',
        'openai',
        503,
        45,
        context
      )
      expect(mockRedisDel).toHaveBeenCalledWith('unified_openai_session_mapping:session-hash')
    })

    it('still clears the sticky session when auto protection is disabled', async () => {
      openaiAccountService.getAccount.mockResolvedValue({
        id: 'account-1',
        disableAutoProtection: 'true'
      })

      const result = await unifiedOpenAIScheduler.markAccountTemporarilyUnavailable(
        'account-1',
        'openai',
        'session-hash',
        null,
        500
      )

      expect(result).toMatchObject({ success: true, skipped: true })
      expect(upstreamErrorHelper.markTempUnavailable).not.toHaveBeenCalled()
      expect(upstreamErrorHelper.recordErrorHistory).toHaveBeenCalledWith(
        'account-1',
        'openai',
        500,
        'server_error',
        null
      )
      expect(mockRedisDel).toHaveBeenCalledWith('unified_openai_session_mapping:session-hash')
    })
  })

  describe('markAccountRateLimited', () => {
    it('does not disable scheduling again when OpenAI-Responses auto protection is disabled', async () => {
      openaiResponsesAccountService.getAccount.mockResolvedValue({
        id: 'account-1',
        disableAutoProtection: 'true'
      })

      await unifiedOpenAIScheduler.markAccountRateLimited(
        'account-1',
        'openai-responses',
        null,
        120
      )

      expect(openaiResponsesAccountService.markAccountRateLimited).toHaveBeenCalledWith(
        'account-1',
        2
      )
      expect(openaiResponsesAccountService.updateAccount).not.toHaveBeenCalled()
    })

    it('keeps disabling scheduling for protected OpenAI-Responses accounts', async () => {
      openaiResponsesAccountService.getAccount.mockResolvedValue({
        id: 'account-1',
        disableAutoProtection: 'false'
      })

      await unifiedOpenAIScheduler.markAccountRateLimited(
        'account-1',
        'openai-responses',
        null,
        120
      )

      expect(openaiResponsesAccountService.updateAccount).toHaveBeenCalledWith(
        'account-1',
        expect.objectContaining({
          schedulable: 'false'
        })
      )
    })
  })
})
