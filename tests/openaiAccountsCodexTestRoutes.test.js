const express = require('express')
const request = require('supertest')

jest.mock('../src/middleware/auth', () => ({
  authenticateAdmin: (req, res, next) => next()
}))

jest.mock('../src/services/relay/openaiCodexAccountTestService', () => ({
  DEFAULT_CODEX_TEST_MODEL: 'gpt-5.5',
  testAccountConnection: jest.fn(async (_accountId, res, model, options) => {
    if (options?.onResult) {
      await options.onResult({
        success: true,
        model,
        latencyMs: 10,
        timestamp: '2026-05-26T00:00:00.000Z'
      })
    }
    res.writeHead(200, { 'Content-Type': 'text/event-stream' })
    res.write('data: {"type":"test_complete","success":true}\\n\\n')
    res.end()
  }),
  testAccountConnectionSync: jest.fn(async (_accountId, model) => ({
    success: true,
    model,
    latencyMs: 10,
    timestamp: '2026-05-26T00:00:00.000Z'
  }))
}))

jest.mock('../src/services/account/openaiAccountService', () => ({
  getAccount: jest.fn(),
  createAccount: jest.fn(),
  updateAccount: jest.fn(),
  deleteAccount: jest.fn(),
  refreshAccountToken: jest.fn(),
  resetAccountStatus: jest.fn(),
  toggleSchedulable: jest.fn()
}))

jest.mock('../src/services/accountGroupService', () => ({}))
jest.mock('../src/services/apiKeyService', () => ({
  unbindAccountFromAllKeys: jest.fn()
}))
jest.mock('../src/models/redis', () => ({
  getAccountTestHistory: jest.fn(),
  getAccountsTestHistory: jest.fn(),
  saveAccountTestResult: jest.fn(),
  setAccountLastTestTime: jest.fn()
}))
jest.mock('../src/utils/logger', () => ({
  debug: jest.fn(),
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
  success: jest.fn()
}))
jest.mock('../src/utils/proxyHelper', () => ({
  createProxyAgent: jest.fn()
}))
jest.mock('../src/utils/webhookNotifier', () => ({
  sendAccountAnomalyNotification: jest.fn()
}))
jest.mock('../src/routes/admin/utils', () => ({
  formatAccountExpiry: jest.fn((account) => account),
  mapExpiryField: jest.fn((updates) => updates)
}))

const redis = require('../src/models/redis')
const openaiCodexAccountTestService = require('../src/services/relay/openaiCodexAccountTestService')
const openaiAccountsRouter = require('../src/routes/admin/openaiAccounts')

describe('OpenAI/Codex account test admin routes', () => {
  const buildApp = () => {
    const app = express()
    app.use(express.json())
    app.use('/admin/openai-accounts', openaiAccountsRouter)
    return app
  }

  beforeEach(() => {
    jest.clearAllMocks()
  })

  it('streams manual account tests with the selected model', async () => {
    const response = await request(buildApp())
      .post('/admin/openai-accounts/account-1/test')
      .send({ model: 'gpt-5.5' })

    expect(response.status).toBe(200)
    expect(openaiCodexAccountTestService.testAccountConnection).toHaveBeenCalledWith(
      'account-1',
      expect.any(Object),
      'gpt-5.5',
      expect.objectContaining({ onResult: expect.any(Function) })
    )
    expect(redis.saveAccountTestResult).toHaveBeenCalledWith(
      'account-1',
      'openai',
      expect.objectContaining({ success: true, model: 'gpt-5.5' })
    )
    expect(redis.setAccountLastTestTime).toHaveBeenCalledWith('account-1', 'openai')
  })

  it('runs sync tests with gpt-5.5 by default and saves history', async () => {
    const response = await request(buildApp())
      .post('/admin/openai-accounts/account-1/test-sync')
      .send({})

    expect(response.status).toBe(200)
    expect(openaiCodexAccountTestService.testAccountConnectionSync).toHaveBeenCalledWith(
      'account-1',
      'gpt-5.5'
    )
    expect(redis.saveAccountTestResult).toHaveBeenCalledWith(
      'account-1',
      'openai',
      expect.objectContaining({ success: true, model: 'gpt-5.5' })
    )
    expect(redis.setAccountLastTestTime).toHaveBeenCalledWith('account-1', 'openai')
  })

  it('saves failed sync test results as history entries', async () => {
    openaiCodexAccountTestService.testAccountConnectionSync.mockResolvedValueOnce({
      success: false,
      error: 'Token expired',
      model: 'gpt-5.5',
      latencyMs: 5,
      timestamp: '2026-05-26T00:00:00.000Z'
    })

    const response = await request(buildApp())
      .post('/admin/openai-accounts/account-1/test-sync')
      .send({})

    expect(response.status).toBe(200)
    expect(redis.saveAccountTestResult).toHaveBeenCalledWith(
      'account-1',
      'openai',
      expect.objectContaining({ success: false, error: 'Token expired', model: 'gpt-5.5' })
    )
    expect(redis.setAccountLastTestTime).toHaveBeenCalledWith('account-1', 'openai')
  })

  it('returns single-account test history', async () => {
    redis.getAccountTestHistory.mockResolvedValue([{ success: true }])

    const response = await request(buildApp()).get('/admin/openai-accounts/account-1/test-history')

    expect(response.status).toBe(200)
    expect(redis.getAccountTestHistory).toHaveBeenCalledWith('account-1', 'openai')
    expect(response.body.data).toEqual({
      accountId: 'account-1',
      platform: 'openai',
      history: [{ success: true }]
    })
  })

  it('returns batch test history', async () => {
    redis.getAccountsTestHistory.mockResolvedValue({
      'account-1': [{ success: true }]
    })

    const response = await request(buildApp())
      .post('/admin/openai-accounts/batch-test-history')
      .send({ accountIds: ['account-1'] })

    expect(response.status).toBe(200)
    expect(redis.getAccountsTestHistory).toHaveBeenCalledWith([
      { accountId: 'account-1', platform: 'openai' }
    ])
    expect(response.body.data).toEqual({
      'account-1': [{ success: true }]
    })
  })
})
