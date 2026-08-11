jest.mock('../src/models/redis', () => ({}))
jest.mock('../src/utils/logger', () => ({
  debug: jest.fn(),
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn()
}))
jest.mock('../config/config', () => ({ upstreamError: {} }), { virtual: true })

const {
  parseDurationSeconds,
  parseRetryAfter,
  parseRetryAfterMessage
} = require('../src/utils/upstreamErrorHelper')

describe('upstream Retry-After parsing', () => {
  test.each([
    ['10.632', 11],
    ['750ms', 1],
    ['1m30s', 90],
    ['2h', 7200]
  ])('parses %s as %i seconds', (value, expected) => {
    expect(parseDurationSeconds(value)).toBe(expected)
  })

  test('handles case-insensitive Retry-After headers', () => {
    expect(parseRetryAfter({ 'Retry-After': '10.632' })).toBe(11)
  })

  test('handles OpenAI token reset duration headers', () => {
    expect(parseRetryAfter({ 'x-ratelimit-reset-tokens': '1m30s' })).toBe(90)
  })

  test('extracts a fractional retry delay from a TPM error message', () => {
    expect(
      parseRetryAfterMessage({
        error: {
          message: 'Rate limit reached for gpt-5.6-sol. Please try again in 10.632s.'
        }
      })
    ).toBe(11)
  })
})
