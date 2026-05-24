describe('capture collector postgres adapter sanitization', () => {
  let queries
  let adapter

  beforeEach(() => {
    jest.resetModules()
    queries = []
    jest.doMock(
      'pg',
      () => ({
        Pool: jest.fn().mockImplementation(() => ({
          query: jest.fn((sql, params) => {
            queries.push({ sql, params })
            return Promise.resolve({ rows: [], rowCount: 1 })
          }),
          end: jest.fn()
        }))
      }),
      { virtual: true }
    )

    const {
      createPostgresAdapter
    } = require('../extensions/anthropic-capture/collector/src/db/postgres')
    adapter = createPostgresAdapter({
      databaseUrl: 'postgres://example',
      dbPoolMax: 1
    })
  })

  test('sanitizes postgres-incompatible characters in JSONB request payloads', async () => {
    await adapter.upsertOpenaiRequest({
      traceId: 'trc_pg_json',
      providerKind: 'openai-responses',
      model: 'gpt-test',
      isStream: true,
      requestJson: {
        input: [{ role: 'user', content: 'before\u0000after' }],
        badSurrogate: 'x\ud800y',
        emoji: 'ok 🙂'
      },
      relayKeyId: 'relay-key'
    })

    const jsonParam = queries[0].params[4]
    const parsed = JSON.parse(jsonParam)

    expect(parsed.input[0].content).toBe('before\\u0000after')
    expect(parsed.badSurrogate).toBe('x�y')
    expect(parsed.emoji).toBe('ok 🙂')
    expect(jsonParam).not.toContain('\u0000')
  })

  test('sanitizes postgres-incompatible characters in text response fields', async () => {
    await adapter.upsertOpenaiResponse({
      traceId: 'trc_pg_text',
      providerKind: 'openai-responses',
      model: 'gpt-test',
      isStream: false,
      responseId: 'resp_1',
      assistantTextFull: 'answer\u0000tail',
      reasoningTextFull: 'reason\ud800tail',
      toolCalls: null,
      usageJson: null,
      inputTokens: null,
      outputTokens: null,
      totalTokens: null,
      cachedTokens: null,
      reasoningTokens: null,
      httpStatus: 200,
      latencyMs: 10,
      relayKeyId: 'relay-key',
      status: 'completed'
    })

    expect(queries[0].params[5]).toBe('answer\\u0000tail')
    expect(queries[0].params[6]).toBe('reason�tail')
  })
})
