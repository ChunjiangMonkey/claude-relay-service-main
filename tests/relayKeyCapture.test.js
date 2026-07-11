const {
  OPENAI_CAPTURE_HOOK_ACTIVE,
  attachRelayKeyIdHeader,
  getRelayKeyIdForActiveCapture
} = require('../src/utils/relayKeyCapture')
const { buildCodexUpstreamHeaders } = require('../src/utils/openaiCodexUpstreamHeaders')

describe('relayKeyCapture', () => {
  afterEach(() => {
    delete global[OPENAI_CAPTURE_HOOK_ACTIVE]
  })

  it('does not attach relay key header when capture hook is inactive', () => {
    const headers = {}

    const attached = attachRelayKeyIdHeader(headers, 'key-123', OPENAI_CAPTURE_HOOK_ACTIVE)

    expect(attached).toBe(false)
    expect(headers['x-relay-key-id']).toBeUndefined()
    expect(getRelayKeyIdForActiveCapture('key-123', OPENAI_CAPTURE_HOOK_ACTIVE)).toBe('')
  })

  it('attaches relay key header when capture hook is active', () => {
    global[OPENAI_CAPTURE_HOOK_ACTIVE] = true
    const headers = {}

    const attached = attachRelayKeyIdHeader(headers, 'key-123', OPENAI_CAPTURE_HOOK_ACTIVE)

    expect(attached).toBe(true)
    expect(headers['x-relay-key-id']).toBe('key-123')
    expect(getRelayKeyIdForActiveCapture('key-123', OPENAI_CAPTURE_HOOK_ACTIVE)).toBe('key-123')
  })
})

describe('buildCodexUpstreamHeaders', () => {
  afterEach(() => {
    delete global[OPENAI_CAPTURE_HOOK_ACTIVE]
  })

  it('builds chatgpt codex headers without relay key when hook is inactive', () => {
    const headers = buildCodexUpstreamHeaders({
      incomingHeaders: {
        version: '2025-03-01',
        'openai-beta': 'beta-a',
        session_id: 'sess-1',
        'session-id': 'sess-modern',
        'thread-id': 'thread-1',
        'x-codex-installation-id': 'installation-1',
        'x-codex-turn-state': 'turn-state-1',
        'x-codex-beta-features': 'feature-a',
        'user-agent': 'codex_cli_rs/1.2.3',
        authorization: 'should-not-pass-through'
      },
      accessToken: 'access-token',
      account: { accountId: 'acct-123' },
      accountId: 'fallback-acct',
      isStream: true,
      apiKeyId: 'relay-key-1'
    })

    expect(headers).toEqual({
      version: '2025-03-01',
      'openai-beta': 'beta-a',
      session_id: 'sess-1',
      'session-id': 'sess-modern',
      'thread-id': 'thread-1',
      'x-codex-installation-id': 'installation-1',
      'x-codex-turn-state': 'turn-state-1',
      'x-codex-beta-features': 'feature-a',
      'user-agent': 'codex_cli_rs/1.2.3',
      authorization: 'Bearer access-token',
      'chatgpt-account-id': 'acct-123',
      host: 'chatgpt.com',
      accept: 'text/event-stream',
      'content-type': 'application/json'
    })
  })

  it('adds relay key for chatgpt codex headers when hook is active', () => {
    global[OPENAI_CAPTURE_HOOK_ACTIVE] = true

    const headers = buildCodexUpstreamHeaders({
      incomingHeaders: {},
      accessToken: 'access-token',
      account: { chatgptUserId: 'user-123' },
      accountId: 'fallback-acct',
      isStream: false,
      apiKeyId: 'relay-key-1'
    })

    expect(headers).toEqual({
      authorization: 'Bearer access-token',
      'chatgpt-account-id': 'user-123',
      host: 'chatgpt.com',
      accept: 'application/json',
      'content-type': 'application/json',
      'x-relay-key-id': 'relay-key-1'
    })
  })

  it('selects the Responses Lite route for GPT-5.6 models', () => {
    const headers = buildCodexUpstreamHeaders({
      incomingHeaders: {
        'x-openai-internal-codex-responses-lite': 'false'
      },
      accessToken: 'access-token',
      account: { accountId: 'acct-123' },
      accountId: 'fallback-acct',
      isStream: true,
      apiKeyId: null,
      model: 'gpt-5.6-luna'
    })

    expect(headers['x-openai-internal-codex-responses-lite']).toBe('true')
  })

  it('does not select the Responses Lite route for GPT-5.5', () => {
    const headers = buildCodexUpstreamHeaders({
      incomingHeaders: {
        'x-openai-internal-codex-responses-lite': 'true'
      },
      accessToken: 'access-token',
      account: { accountId: 'acct-123' },
      accountId: 'fallback-acct',
      isStream: true,
      apiKeyId: null,
      model: 'gpt-5.5'
    })

    expect(headers['x-openai-internal-codex-responses-lite']).toBeUndefined()
  })
})
