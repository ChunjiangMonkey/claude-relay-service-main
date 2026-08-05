jest.mock('../src/utils/logger', () => ({
  warn: jest.fn()
}))

const { isUnstableUpstreamError } = require('../src/utils/unstableUpstreamHelper')

describe('unstableUpstreamHelper', () => {
  test('recognizes nested Codex response.failed server errors', () => {
    expect(
      isUnstableUpstreamError(200, {
        type: 'response.failed',
        response: {
          status: 'failed',
          error: { type: 'server_error', message: 'Upstream failed' }
        }
      })
    ).toBe(true)
  })

  test('recognizes the Codex capacity message without an explicit error type', () => {
    expect(isUnstableUpstreamError(200, { message: 'Model is at capacity' })).toBe(true)
  })

  test('does not classify request validation failures as unstable upstream errors', () => {
    expect(
      isUnstableUpstreamError(400, {
        error: { type: 'invalid_request_error', message: 'Missing input' }
      })
    ).toBe(false)
  })
})
