jest.mock(
  '../config/config',
  () => ({
    system: { timezoneOffset: 8 },
    logging: {
      dirname: require('os').tmpdir(),
      maxSize: '1m',
      maxFiles: '1d',
      level: 'error'
    }
  }),
  { virtual: true }
)

describe('logger sanitizer', () => {
  let logger

  beforeEach(() => {
    jest.resetModules()
    logger = require('../src/utils/logger')
    logger.silent = true
  })

  afterEach(() => {
    if (logger && typeof logger.close === 'function') {
      logger.close()
    }
  })

  test('summarizes AxiosError payloads and redacts sensitive headers before serialization', () => {
    const rawInstruction = `VERY_SECRET_INSTRUCTIONS_${'x'.repeat(20000)}`
    const responseSecret = `VERY_SECRET_RESPONSE_${'y'.repeat(20000)}`
    const axiosError = new Error('Request failed with status code 500')
    axiosError.name = 'AxiosError'
    axiosError.isAxiosError = true
    axiosError.code = 'ERR_BAD_RESPONSE'
    axiosError.config = {
      method: 'post',
      url: '/v1/responses',
      baseURL: 'https://chatgpt.com',
      timeout: 600000,
      headers: {
        authorization: 'Bearer secret-token',
        'x-api-key': 'secret-api-key',
        'content-type': 'application/json'
      },
      data: JSON.stringify({ model: 'gpt-5.5', instructions: rawInstruction })
    }
    axiosError.response = {
      status: 500,
      statusText: 'Internal Server Error',
      headers: {
        'set-cookie': 'session=secret',
        'content-type': 'application/json'
      },
      data: { error: { message: responseSecret, code: 'upstream_error' } }
    }
    axiosError.request = { socket: { huge: rawInstruction } }

    const output = logger._test.safeStringify({ error: axiosError })
    const parsed = JSON.parse(output)

    expect(output.length).toBeLessThan(15000)
    expect(output).toContain('AxiosError')
    expect(output).toContain('ERR_BAD_RESPONSE')
    expect(output).toContain('/v1/responses')
    expect(output).toContain('[Redacted]')
    expect(output).not.toContain('Bearer secret-token')
    expect(output).not.toContain('secret-api-key')
    expect(output).not.toContain(rawInstruction)
    expect(output).not.toContain(responseSecret)
    expect(parsed.error.config.data).toMatchObject({ type: 'string', omitted: true })
    expect(parsed.error.response.data).toMatchObject({ type: 'object', omitted: true })
    expect(parsed.error.request).toBeUndefined()
  })

  test('keeps useful fields from ordinary Error objects', () => {
    const error = new Error('plain failure')
    error.code = 'E_PLAIN'
    error.statusCode = 502

    const parsed = JSON.parse(logger._test.safeStringify({ error }))

    expect(parsed.error.name).toBe('Error')
    expect(parsed.error.message).toBe('plain failure')
    expect(parsed.error.code).toBe('E_PLAIN')
    expect(parsed.error.statusCode).toBe(502)
    expect(parsed.error.stack).toContain('plain failure')
  })
})
