jest.mock(
  '../config/config',
  () => ({
    concurrency: {}
  }),
  { virtual: true }
)

jest.mock('../src/utils/logger', () => ({
  api: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
  info: jest.fn(),
  debug: jest.fn(),
  database: jest.fn(),
  security: jest.fn()
}))

jest.mock('../src/models/redis', () => ({}))
jest.mock('../src/services/apiKeyService', () => ({}))
jest.mock('../src/services/userService', () => ({}))
jest.mock('../src/services/claudeRelayConfigService', () => ({}))
jest.mock('../src/validators/clientValidator', () => ({ validateRequest: jest.fn() }))
jest.mock('../src/validators/clients/claudeCodeValidator', () => ({ validate: jest.fn() }))
jest.mock('../src/utils/modelHelper', () => ({ isClaudeFamilyModel: jest.fn(() => false) }))

describe('request logger summaries', () => {
  let helpers

  beforeEach(() => {
    jest.resetModules()
    helpers = require('../src/middleware/auth')._test
  })

  test('summarizes large request bodies without retaining prompt text', () => {
    const rawInstruction = `VERY_SECRET_INSTRUCTIONS_${'x'.repeat(10000)}`
    const rawMessage = `VERY_SECRET_MESSAGE_${'y'.repeat(10000)}`
    const summary = helpers.summarizeRequestBodyForLog({
      model: 'gpt-5.5',
      stream: true,
      instructions: rawInstruction,
      input: [
        { role: 'user', content: rawMessage },
        { role: 'assistant', content: 'ok' }
      ],
      messages: [{ role: 'user', content: rawMessage }],
      tools: [{ name: 'tool_one', input_schema: { type: 'object' } }]
    })
    const output = JSON.stringify(summary)

    expect(summary.model).toBe('gpt-5.5')
    expect(summary.stream).toBe(true)
    expect(summary.keys).toEqual(['model', 'stream', 'instructions', 'input', 'messages', 'tools'])
    expect(summary.instructionsChars).toBe(rawInstruction.length)
    expect(summary.inputItems).toBe(2)
    expect(summary.messagesCount).toBe(1)
    expect(summary.toolsCount).toBe(1)
    expect(summary.approxChars).toBeGreaterThan(rawInstruction.length)
    expect(output).not.toContain(rawInstruction)
    expect(output).not.toContain(rawMessage)
  })

  test('summarizes response bodies while preserving small 404 diagnostics', () => {
    const summary = helpers.summarizeResponseBodyForLog({
      error: 'Not Found',
      message: 'Route /favicon.ico not found',
      timestamp: '2026-05-24T06:30:00.406Z'
    })

    expect(summary.type).toBe('object')
    expect(summary.keys).toEqual(['error', 'message', 'timestamp'])
    expect(summary.error).toBe('Not Found')
    expect(summary.message).toBe('Route /favicon.ico not found')
    expect(summary.approxChars).toBeGreaterThan(0)
  })
})
