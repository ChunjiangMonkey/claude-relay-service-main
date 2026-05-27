const { PassThrough } = require('stream')

jest.mock('axios', () => ({
  post: jest.fn()
}))

jest.mock('../src/services/account/openaiAccountService', () => ({
  getAccount: jest.fn(),
  isTokenExpired: jest.fn(() => false),
  refreshAccountToken: jest.fn(),
  decrypt: jest.fn()
}))

jest.mock('../src/utils/proxyHelper', () => ({
  createProxyAgent: jest.fn(() => null)
}))

jest.mock('../src/utils/logger', () => ({
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
  success: jest.fn(),
  debug: jest.fn()
}))

const axios = require('axios')
const openaiAccountService = require('../src/services/account/openaiAccountService')
const ProxyHelper = require('../src/utils/proxyHelper')
const openaiCodexAccountTestService = require('../src/services/relay/openaiCodexAccountTestService')

function createAccount(overrides = {}) {
  return {
    id: 'acct-1',
    name: 'Codex Account',
    accountId: 'chatgpt-account-1',
    accessToken: 'encrypted-access-token',
    refreshToken: 'refresh-token',
    proxy: null,
    ...overrides
  }
}

function createCompletedSse(model = 'gpt-5.5') {
  return [
    'event: response.output_text.delta',
    'data: {"type":"response.output_text.delta","delta":"hello codex"}',
    '',
    'event: response.completed',
    `data: ${JSON.stringify({
      type: 'response.completed',
      response: {
        id: 'resp_test',
        model,
        status: 'completed',
        usage: { input_tokens: 1, output_tokens: 2, total_tokens: 3 }
      }
    })}`,
    ''
  ].join('\n')
}

describe('openaiCodexAccountTestService', () => {
  beforeEach(() => {
    jest.clearAllMocks()
    openaiAccountService.getAccount.mockResolvedValue(createAccount())
    openaiAccountService.decrypt.mockReturnValue('plain-access-token')
    openaiAccountService.isTokenExpired.mockReturnValue(false)
  })

  it('tests a specific Codex account without scheduler or usage side effects', async () => {
    const upstream = new PassThrough()
    axios.post.mockResolvedValue({
      status: 200,
      data: upstream
    })

    const resultPromise = openaiCodexAccountTestService.testAccountConnectionSync(
      'acct-1',
      'gpt-5.5'
    )
    await Promise.resolve()
    upstream.end(createCompletedSse())

    const result = await resultPromise

    expect(result).toMatchObject({
      success: true,
      model: 'gpt-5.5',
      responseText: 'hello codex',
      responseId: 'resp_test',
      responseModel: 'gpt-5.5',
      status: 'completed',
      usage: { input_tokens: 1, output_tokens: 2, total_tokens: 3 }
    })
    expect(axios.post).toHaveBeenCalledTimes(1)
    const [url, payload, requestConfig] = axios.post.mock.calls[0]
    expect(url).toBe('https://chatgpt.com/backend-api/codex/responses')
    expect(payload).toMatchObject({
      model: 'gpt-5.5',
      stream: true,
      store: false,
      instructions: expect.stringContaining('You are Codex')
    })
    expect(payload.max_output_tokens).toBeUndefined()
    expect(payload.input).toEqual([
      {
        type: 'message',
        role: 'user',
        content: [{ type: 'input_text', text: 'hi' }]
      }
    ])
    expect(requestConfig).toMatchObject({
      timeout: 30000,
      responseType: 'stream',
      validateStatus: expect.any(Function)
    })
    expect(requestConfig.headers).toMatchObject({
      authorization: 'Bearer plain-access-token',
      'chatgpt-account-id': 'chatgpt-account-1',
      accept: 'text/event-stream',
      'content-type': 'application/json',
      'x-relay-capture-skip': 'admin-openai-account-test'
    })
  })

  it('normalizes dated gpt-5 model names for the Codex backend only', async () => {
    const upstream = new PassThrough()
    axios.post.mockResolvedValue({
      status: 200,
      data: upstream
    })

    const resultPromise = openaiCodexAccountTestService.testAccountConnectionSync(
      'acct-1',
      'gpt-5-2025-08-07'
    )
    await Promise.resolve()
    upstream.end(createCompletedSse('gpt-5'))

    const result = await resultPromise

    expect(result).toMatchObject({
      success: true,
      model: 'gpt-5-2025-08-07',
      responseModel: 'gpt-5'
    })
    expect(axios.post.mock.calls[0][1]).toMatchObject({
      model: 'gpt-5',
      stream: true,
      store: false
    })
  })

  it('refreshes an expired token before testing', async () => {
    const expiredAccount = createAccount({ expiresAt: '2020-01-01T00:00:00.000Z' })
    const refreshedAccount = createAccount({
      accessToken: 'encrypted-refreshed-token',
      expiresAt: '2099-01-01T00:00:00.000Z'
    })
    openaiAccountService.getAccount
      .mockResolvedValueOnce(expiredAccount)
      .mockResolvedValueOnce(refreshedAccount)
    openaiAccountService.isTokenExpired.mockReturnValueOnce(true)
    openaiAccountService.decrypt.mockReturnValue('refreshed-access-token')

    const upstream = new PassThrough()
    axios.post.mockResolvedValue({
      status: 200,
      data: upstream
    })

    const resultPromise = openaiCodexAccountTestService.testAccountConnectionSync('acct-1')
    await Promise.resolve()
    upstream.end(createCompletedSse())

    await expect(resultPromise).resolves.toMatchObject({ success: true })
    expect(openaiAccountService.refreshAccountToken).toHaveBeenCalledWith('acct-1')
    expect(axios.post.mock.calls[0][2].headers.authorization).toBe('Bearer refreshed-access-token')
  })

  it('uses account proxy when present', async () => {
    const proxy = { type: 'http', host: '127.0.0.1', port: 1080 }
    const agent = { proxyAgent: true }
    openaiAccountService.getAccount.mockResolvedValue(createAccount({ proxy }))
    ProxyHelper.createProxyAgent.mockReturnValue(agent)

    const upstream = new PassThrough()
    axios.post.mockResolvedValue({
      status: 200,
      data: upstream
    })

    const resultPromise = openaiCodexAccountTestService.testAccountConnectionSync('acct-1')
    await Promise.resolve()
    upstream.end(createCompletedSse())

    await expect(resultPromise).resolves.toMatchObject({ success: true })
    expect(ProxyHelper.createProxyAgent).toHaveBeenCalledWith(proxy)
    expect(axios.post.mock.calls[0][2]).toMatchObject({
      httpAgent: agent,
      httpsAgent: agent,
      proxy: false
    })
  })

  it('returns a bounded error result for non-200 upstream responses', async () => {
    const upstream = new PassThrough()
    axios.post.mockResolvedValue({
      status: 429,
      data: upstream
    })

    const resultPromise = openaiCodexAccountTestService.testAccountConnectionSync('acct-1')
    await Promise.resolve()
    upstream.end(JSON.stringify({ error: { message: 'rate limited' } }))

    await expect(resultPromise).resolves.toMatchObject({
      success: false,
      error: '[E004] Rate limit exceeded',
      httpStatus: 429,
      model: 'gpt-5.5'
    })
  })

  it('keeps HTTP status-specific error codes when the upstream body is empty', async () => {
    const upstream = new PassThrough()
    axios.post.mockResolvedValue({
      status: 401,
      data: upstream
    })

    const resultPromise = openaiCodexAccountTestService.testAccountConnectionSync('acct-1')
    await Promise.resolve()
    upstream.end('')

    await expect(resultPromise).resolves.toMatchObject({
      success: false,
      error: '[E003] Authentication failed',
      httpStatus: 401,
      model: 'gpt-5.5'
    })
  })

  it('returns a failed result instead of throwing when account preparation fails', async () => {
    openaiAccountService.getAccount.mockResolvedValue(null)

    const result = await openaiCodexAccountTestService.testAccountConnectionSync('missing-account')

    expect(result).toMatchObject({
      success: false,
      error: 'Resource not found',
      statusCode: 404,
      model: 'gpt-5.5'
    })
    expect(axios.post).not.toHaveBeenCalled()
  })

  it('aborts the upstream request when the client closes the SSE response', async () => {
    const responseStream = new PassThrough()
    responseStream.headersSent = false
    responseStream.writeHead = jest.fn(() => {
      responseStream.headersSent = true
    })
    responseStream.getHeader = jest.fn(() => undefined)

    let requestSignal = null
    axios.post.mockImplementation((_url, _payload, requestConfig) => {
      requestSignal = requestConfig.signal
      return new Promise((resolve, reject) => {
        requestSignal.addEventListener('abort', () => {
          reject(Object.assign(new Error('canceled'), { name: 'CanceledError' }))
        })
      })
    })

    const resultPromise = openaiCodexAccountTestService.testAccountConnection(
      'acct-1',
      responseStream,
      'gpt-5.5'
    )
    await new Promise((resolve) => setImmediate(resolve))
    expect(requestSignal).toBeTruthy()

    responseStream.emit('close')
    await resultPromise

    expect(requestSignal.aborted).toBe(true)
  })

  it('streams and persists a failed result when manual account preparation fails', async () => {
    openaiAccountService.getAccount.mockResolvedValue(null)
    const responseStream = new PassThrough()
    const chunks = []
    responseStream.headersSent = false
    responseStream.writeHead = jest.fn(() => {
      responseStream.headersSent = true
    })
    responseStream.getHeader = jest.fn(() => undefined)
    responseStream.write = jest.fn((chunk) => {
      chunks.push(String(chunk))
      return true
    })
    responseStream.end = jest.fn()
    const onResult = jest.fn()

    const result = await openaiCodexAccountTestService.testAccountConnection(
      'missing-account',
      responseStream,
      'gpt-5.5',
      { onResult }
    )

    expect(result).toMatchObject({
      success: false,
      error: 'Resource not found',
      model: 'gpt-5.5'
    })
    expect(onResult).toHaveBeenCalledWith(expect.objectContaining({ success: false }))
    expect(chunks.join('')).toContain('"type":"test_complete"')
    expect(chunks.join('')).toContain('"success":false')
    expect(axios.post).not.toHaveBeenCalled()
  })
})
