const express = require('express')
const http = require('http')
const request = require('supertest')
const { PassThrough } = require('stream')

jest.mock(
  '../config/config',
  () => ({
    server: { port: 3456 }
  }),
  { virtual: true }
)

jest.mock('axios', () => ({
  post: jest.fn()
}))

jest.mock('../src/models/redis', () => ({}))
jest.mock('../src/services/account/claudeAccountService', () => ({}))
jest.mock('../src/services/account/openaiAccountService', () => ({}))
jest.mock('../src/services/serviceRatesService', () => ({}))
jest.mock('../src/services/apiKeyService', () => ({
  validateApiKeyForStats: jest.fn(),
  hasPermission: jest.fn()
}))
jest.mock('../src/utils/logger', () => ({
  api: jest.fn(),
  security: jest.fn(),
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
  debug: jest.fn(),
  success: jest.fn()
}))

const axios = require('axios')
const apiKeyService = require('../src/services/apiKeyService')
const apiStatsRouter = require('../src/routes/apiStats')
const CodexCliValidator = require('../src/validators/clients/codexCliValidator')

function buildApp() {
  const app = express()
  app.use(express.json())
  app.use('/apiStats', apiStatsRouter)
  return app
}

function createValidKey(overrides = {}) {
  return {
    id: 'key-1',
    name: 'Codex Key',
    permissions: ['openai'],
    ...overrides
  }
}

async function waitForAxiosPost() {
  for (let i = 0; i < 10; i++) {
    if (axios.post.mock.calls.length > 0) {
      return
    }
    await new Promise((resolve) => setImmediate(resolve))
  }
}

async function waitForCondition(predicate) {
  for (let i = 0; i < 20; i++) {
    if (predicate()) {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
  throw new Error('condition was not met')
}

function startOpenAITestRequest(payload) {
  return request(buildApp())
    .post('/apiStats/api-key/test-openai')
    .send(payload)
    .then((res) => res)
}

async function listen(app) {
  return await new Promise((resolve) => {
    const server = app.listen(0, () => resolve(server))
  })
}

async function closeServer(server) {
  return await new Promise((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error)
        return
      }
      resolve()
    })
  })
}

describe('OpenAI/Codex API Key test route', () => {
  beforeEach(() => {
    jest.clearAllMocks()
    apiKeyService.validateApiKeyForStats.mockResolvedValue({
      valid: true,
      keyData: createValidKey()
    })
    apiKeyService.hasPermission.mockReturnValue(true)
  })

  it('sends a Codex-compatible payload and headers through the real relay route', async () => {
    const upstream = new PassThrough()
    axios.post.mockResolvedValue({
      status: 200,
      data: upstream
    })

    const responsePromise = startOpenAITestRequest({
      apiKey: 'cr_test_key_12345',
      model: 'gpt-5.5',
      prompt: 'hello codex',
      maxTokens: 4096
    })

    await waitForAxiosPost()
    expect(axios.post).toHaveBeenCalledTimes(1)

    const [url, payload, config] = axios.post.mock.calls[0]
    expect(url).toBe('http://127.0.0.1:3456/openai/responses')
    expect(payload).toMatchObject({
      model: 'gpt-5.5',
      stream: true,
      store: false,
      instructions: expect.stringContaining(
        'You are Codex, based on GPT-5. You are running as a coding agent in the Codex CLI'
      )
    })
    expect(payload.max_output_tokens).toBeUndefined()
    expect(payload.input).toEqual([
      {
        type: 'message',
        role: 'user',
        content: [{ type: 'input_text', text: 'hello codex' }]
      }
    ])
    expect(config.headers).toMatchObject({
      'Content-Type': 'application/json',
      Accept: 'text/event-stream',
      'x-api-key': 'cr_test_key_12345',
      'User-Agent': 'codex_cli_rs/1.0.0',
      originator: 'codex_cli_rs'
    })
    expect(config.headers.session_id).toMatch(/^codex_test_/)
    expect(
      CodexCliValidator.validate({
        path: '/openai/responses',
        headers: {
          'user-agent': config.headers['User-Agent'],
          originator: config.headers.originator,
          session_id: config.headers.session_id
        },
        body: payload
      })
    ).toBe(true)

    upstream.end(
      [
        'data: {"type":"response.output_text.delta","delta":"ok"}',
        '',
        'data: {"type":"response.completed","response":{"status":"completed"}}',
        ''
      ].join('\n')
    )

    const response = await responsePromise
    expect(response.status).toBe(200)
    expect(response.text).toContain('"type":"content","text":"ok"')
    expect(response.text).toContain('"type":"test_complete","success":true')
  })

  it('aborts the internal relay request when the test client disconnects', async () => {
    axios.post.mockImplementation(
      (_url, _payload, config) =>
        new Promise((_resolve, reject) => {
          config.signal.addEventListener('abort', () => {
            const error = new Error('canceled')
            error.code = 'ERR_CANCELED'
            reject(error)
          })
        })
    )

    const server = await listen(buildApp())
    try {
      const body = JSON.stringify({ apiKey: 'cr_test_key_12345' })
      const clientRequest = http.request(
        {
          host: '127.0.0.1',
          port: server.address().port,
          path: '/apiStats/api-key/test-openai',
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(body)
          }
        },
        (clientResponse) => {
          clientResponse.resume()
        }
      )
      clientRequest.on('error', () => {})
      const clientClosed = new Promise((resolve) => clientRequest.on('close', resolve))
      clientRequest.end(body)

      await waitForAxiosPost()
      const config = axios.post.mock.calls[0][2]
      expect(config.signal).toBeDefined()

      clientRequest.destroy()

      await waitForCondition(() => config.signal.aborted)
      await clientClosed
    } finally {
      await closeServer(server)
    }
  })

  it('returns a failed SSE result for upstream SSE errors', async () => {
    const upstream = new PassThrough()
    axios.post.mockResolvedValue({
      status: 200,
      data: upstream
    })

    const responsePromise = startOpenAITestRequest({ apiKey: 'cr_test_key_12345' })

    await waitForAxiosPost()
    upstream.end('data: {"type":"error","error":{"message":"rate limited"}}\n\n')

    const response = await responsePromise
    expect(response.status).toBe(200)
    expect(response.text).toContain('"success":false')
    expect(response.text).toContain('[E004] Rate limit exceeded')
  })

  it('keeps HTTP status-specific error codes for non-200 relay responses', async () => {
    const upstream = new PassThrough()
    axios.post.mockResolvedValue({
      status: 401,
      data: upstream
    })

    const responsePromise = startOpenAITestRequest({ apiKey: 'cr_test_key_12345' })

    await waitForAxiosPost()
    upstream.end('')

    const response = await responsePromise
    expect(response.status).toBe(200)
    expect(response.text).toContain('"success":false')
    expect(response.text).toContain('[E003] Authentication failed')
  })

  it('handles oversized non-200 error bodies without exposing the raw body', async () => {
    const upstream = new PassThrough()
    axios.post.mockResolvedValue({
      status: 500,
      data: upstream
    })

    const responsePromise = startOpenAITestRequest({ apiKey: 'cr_test_key_12345' })

    await waitForAxiosPost()
    upstream.end('x'.repeat(128 * 1024))

    const response = await responsePromise
    expect(response.status).toBe(200)
    expect(response.text).toContain('"success":false')
    expect(response.text).not.toContain('x'.repeat(1024))
  })

  it('rejects API keys without OpenAI permission before calling the relay', async () => {
    apiKeyService.hasPermission.mockReturnValue(false)

    const response = await request(buildApp())
      .post('/apiStats/api-key/test-openai')
      .send({ apiKey: 'cr_test_key_12345' })

    expect(response.status).toBe(403)
    expect(response.body).toMatchObject({
      error: 'Permission denied'
    })
    expect(axios.post).not.toHaveBeenCalled()
  })

  it('rejects invalid API keys before calling the relay', async () => {
    apiKeyService.validateApiKeyForStats.mockResolvedValue({
      valid: false,
      error: 'invalid key'
    })

    const response = await request(buildApp())
      .post('/apiStats/api-key/test-openai')
      .send({ apiKey: 'cr_test_key_12345' })

    expect(response.status).toBe(401)
    expect(response.body).toMatchObject({
      error: 'Invalid API key',
      message: 'invalid key'
    })
    expect(axios.post).not.toHaveBeenCalled()
  })
})
