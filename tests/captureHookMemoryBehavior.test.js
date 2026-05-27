const fs = require('fs/promises')
const fsSync = require('fs')
const Module = require('module')
const os = require('os')
const path = require('path')
const zlib = require('zlib')
const EventEmitter = require('events')
const https = require('https')

const originalHttpsRequest = https.request

function resetHook(modulePath, symbols) {
  for (const symbol of symbols) {
    delete global[Symbol.for(symbol)]
  }
  const resolved = path.resolve(__dirname, '..', modulePath)
  delete require.cache[resolved]
}

function loadCommonJsHook(modulePath) {
  const resolved = path.resolve(__dirname, '..', modulePath)
  const mod = new Module(resolved, module)
  mod.filename = resolved
  mod.paths = Module._nodeModulePaths(path.dirname(resolved))
  require.cache[resolved] = mod
  const wrapper = new Function(
    'exports',
    'require',
    'module',
    '__filename',
    '__dirname',
    'process',
    fsSync.readFileSync(resolved, 'utf8')
  )
  wrapper(
    mod.exports,
    Module.createRequire(resolved),
    mod,
    resolved,
    path.dirname(resolved),
    process
  )
  return mod.exports
}

function installFakeHttpsRequest() {
  let lastRequest = null

  https.request = jest.fn(() => {
    const req = new EventEmitter()
    req.write = jest.fn()
    req.end = jest.fn()
    lastRequest = req
    return req
  })

  return () => lastRequest
}

function restoreEnv(snapshot) {
  for (const key of Object.keys(process.env)) {
    if (!Object.prototype.hasOwnProperty.call(snapshot, key)) {
      delete process.env[key]
    }
  }
  for (const [key, value] of Object.entries(snapshot)) {
    process.env[key] = value
  }
}

async function waitForJsonl(dir, filename, minRows = 1) {
  const filePath = path.join(dir, filename)
  for (let i = 0; i < 100; i += 1) {
    try {
      const content = await fs.readFile(filePath, 'utf8')
      const rows = content
        .trim()
        .split('\n')
        .filter(Boolean)
        .map((line) => JSON.parse(line))
      if (rows.length >= minRows) {
        return rows
      }
    } catch (_) {
      // file not written yet
    }
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
  throw new Error(`Timed out waiting for ${filename}`)
}

describe('capture hooks memory-safe semantic output', () => {
  let tempDir
  let previousEnv

  beforeEach(async () => {
    previousEnv = { ...process.env }
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'capture-hook-'))
    https.request = originalHttpsRequest
    resetHook('extensions/anthropic-capture/hook/anthropic-hook.js', [
      'claude_relay.anthropic_capture_hook.installed',
      'claude_relay.anthropic_capture_hook.active'
    ])
    resetHook('extensions/openai-capture/hook/openai-hook.js', [
      'claude_relay.openai_capture_hook.installed',
      'claude_relay.openai_capture_hook.active'
    ])
  })

  afterEach(async () => {
    restoreEnv(previousEnv)
    https.request = originalHttpsRequest
    resetHook('extensions/anthropic-capture/hook/anthropic-hook.js', [
      'claude_relay.anthropic_capture_hook.installed',
      'claude_relay.anthropic_capture_hook.active'
    ])
    resetHook('extensions/openai-capture/hook/openai-hook.js', [
      'claude_relay.openai_capture_hook.installed',
      'claude_relay.openai_capture_hook.active'
    ])
    await fs.rm(tempDir, { recursive: true, force: true })
  })

  test('anthropic gzip stream captures semantic fields without raw request body by default', async () => {
    process.env.ANTHROPIC_CAPTURE_ENABLED = 'true'
    process.env.ANTHROPIC_CAPTURE_DIR = tempDir
    process.env.ANTHROPIC_CAPTURE_INCLUDE_THINKING = 'true'
    process.env.ANTHROPIC_CAPTURE_INCLUDE_RAW = 'false'

    const getLastRequest = installFakeHttpsRequest()
    loadCommonJsHook('extensions/anthropic-capture/hook/anthropic-hook.js')

    const req = https.request({
      protocol: 'https:',
      hostname: 'api.anthropic.com',
      host: 'api.anthropic.com',
      path: '/v1/messages',
      method: 'POST',
      headers: {
        'x-request-id': 'req_123',
        'x-relay-key-id': 'relay-key-1'
      }
    })

    const requestBody = {
      model: 'claude-test',
      stream: true,
      system: 'system prompt',
      tools: [{ name: 'Bash', input_schema: { type: 'object' } }],
      messages: [{ role: 'user', content: 'hello' }]
    }
    req.write(JSON.stringify(requestBody))
    req.end()
    expect(getLastRequest()).toBe(req)

    const res = new EventEmitter()
    res.statusCode = 200
    res.headers = {
      'content-type': 'text/event-stream',
      'content-encoding': 'gzip'
    }

    req.emit('response', res)

    const sse = [
      'event: message_start',
      'data: {"type":"message_start","message":{"id":"msg_1","model":"claude-test","usage":{"input_tokens":10}}}',
      '',
      'event: content_block_start',
      'data: {"type":"content_block_start","index":0,"content_block":{"type":"thinking","text":"think "}}',
      '',
      'event: content_block_delta',
      'data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","text":"more"}}',
      '',
      'event: content_block_start',
      'data: {"type":"content_block_start","index":1,"content_block":{"type":"text","text":"hel"}}',
      '',
      'event: content_block_delta',
      'data: {"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"lo"}}',
      '',
      'event: content_block_start',
      'data: {"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"toolu_1","name":"Bash"}}',
      '',
      'event: content_block_delta',
      'data: {"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\\"command\\":\\"ls\\"}"}}',
      '',
      'event: content_block_stop',
      'data: {"type":"content_block_stop","index":2}',
      '',
      'event: message_delta',
      'data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}',
      ''
    ].join('\n')

    const gzipped = zlib.gzipSync(sse)
    res.emit('data', gzipped.subarray(0, 7))
    res.emit('data', gzipped.subarray(7))
    res.emit('end')

    const requestRows = await waitForJsonl(tempDir, 'anthropic-upstream-requests.jsonl')
    expect(requestRows[0].request_body_json).toEqual(requestBody)
    expect(requestRows[0]).not.toHaveProperty('request_body_raw')
    expect(requestRows[0].request_body_raw_omitted).toBe(true)

    const streamRows = await waitForJsonl(tempDir, 'anthropic-upstream-stream-final.jsonl')
    expect(streamRows[0].request).toEqual({
      relay_request_id: 'req_123',
      model: 'claude-test',
      stream: true
    })
    expect(streamRows[0].stream.assistant_text_full).toBe('hello')
    expect(streamRows[0].stream.thought_text_full).toBe('think more')
    expect(streamRows[0].stream.usage).toEqual({ input_tokens: 10, output_tokens: 2 })
    expect(streamRows[0].stream.stop_reason).toBe('end_turn')
    expect(streamRows[0].stream.message_id).toBe('msg_1')
    expect(streamRows[0].stream.message_model).toBe('claude-test')
    expect(streamRows[0].stream.tool_calls).toEqual([
      {
        index: 2,
        id: 'toolu_1',
        name: 'Bash',
        input: { command: 'ls' }
      }
    ])
  })

  test('anthropic gzip stream uses magic decode when content-encoding is missing', async () => {
    process.env.ANTHROPIC_CAPTURE_ENABLED = 'true'
    process.env.ANTHROPIC_CAPTURE_DIR = tempDir
    process.env.ANTHROPIC_CAPTURE_INCLUDE_THINKING = 'true'
    process.env.ANTHROPIC_CAPTURE_INCLUDE_RAW = 'false'

    installFakeHttpsRequest()
    loadCommonJsHook('extensions/anthropic-capture/hook/anthropic-hook.js')

    const req = https.request({
      protocol: 'https:',
      hostname: 'api.anthropic.com',
      host: 'api.anthropic.com',
      path: '/v1/messages',
      method: 'POST',
      headers: {
        'x-request-id': 'req_magic',
        'x-relay-key-id': 'relay-key-magic'
      }
    })

    req.write(
      JSON.stringify({
        model: 'claude-test',
        stream: true,
        messages: [{ role: 'user', content: 'hello' }]
      })
    )
    req.end()

    const res = new EventEmitter()
    res.statusCode = 200
    res.headers = {
      'content-type': 'text/event-stream'
    }
    req.emit('response', res)

    const sse = [
      'event: message_start',
      'data: {"type":"message_start","message":{"id":"msg_magic","model":"claude-test","usage":{"input_tokens":1}}}',
      '',
      'event: content_block_start',
      'data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":"magic"}}',
      '',
      'event: message_delta',
      'data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}',
      ''
    ].join('\n')

    const gzipped = zlib.gzipSync(sse)
    res.emit('data', gzipped.subarray(0, 1))
    res.emit('data', gzipped.subarray(1))
    res.emit('end')

    const streamRows = await waitForJsonl(tempDir, 'anthropic-upstream-stream-final.jsonl')
    expect(streamRows[0].stream.assistant_text_full).toBe('magic')
    expect(streamRows[0].stream.message_id).toBe('msg_magic')

    const summaryRows = await waitForJsonl(tempDir, 'anthropic-upstream-responses.jsonl')
    expect(summaryRows[0].decode_source).toBe('magic')
    expect(summaryRows[0].decompressed).toBe(true)
    expect(summaryRows[0].decode_error).toBeNull()
  })

  test('anthropic stream request captures SSE when content-type is missing', async () => {
    process.env.ANTHROPIC_CAPTURE_ENABLED = 'true'
    process.env.ANTHROPIC_CAPTURE_DIR = tempDir
    process.env.ANTHROPIC_CAPTURE_INCLUDE_THINKING = 'true'
    process.env.ANTHROPIC_CAPTURE_INCLUDE_RAW = 'false'

    installFakeHttpsRequest()
    loadCommonJsHook('extensions/anthropic-capture/hook/anthropic-hook.js')

    const req = https.request({
      protocol: 'https:',
      hostname: 'api.anthropic.com',
      host: 'api.anthropic.com',
      path: '/v1/messages',
      method: 'POST',
      headers: {
        'x-request-id': 'req_missing_content_type',
        'x-relay-key-id': 'relay-key-missing-content-type'
      }
    })

    req.write(
      JSON.stringify({
        model: 'claude-test',
        stream: true,
        messages: [{ role: 'user', content: 'hello' }]
      })
    )
    req.end()

    const res = new EventEmitter()
    res.statusCode = 200
    res.headers = {}
    req.emit('response', res)

    const sse = [
      'event: message_start',
      'data: {"type":"message_start","message":{"id":"msg_missing_content_type","model":"claude-test","usage":{"input_tokens":3}}}',
      '',
      'event: content_block_start',
      'data: {"type":"content_block_start","index":0,"content_block":{"type":"thinking","text":"hidden thought"}}',
      '',
      'event: content_block_start',
      'data: {"type":"content_block_start","index":1,"content_block":{"type":"text","text":"visible answer"}}',
      '',
      'event: message_delta',
      'data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":4}}',
      ''
    ].join('\n')

    res.emit('data', Buffer.from(sse))
    res.emit('end')

    const streamRows = await waitForJsonl(tempDir, 'anthropic-upstream-stream-final.jsonl')
    expect(streamRows[0].stream.assistant_text_full).toBe('visible answer')
    expect(streamRows[0].stream.thought_text_full).toBe('hidden thought')
    expect(streamRows[0].stream.message_id).toBe('msg_missing_content_type')
    expect(streamRows[0].stream.usage).toEqual({ input_tokens: 3, output_tokens: 4 })

    const summaryRows = await waitForJsonl(tempDir, 'anthropic-upstream-responses.jsonl')
    expect(summaryRows[0].type).toBe('anthropic_upstream_response_stream_summary')
    expect(summaryRows[0].decode_source).toBe('identity')
    expect(summaryRows[0].decompressed).toBe(false)
    expect(summaryRows[0].decode_error).toBeNull()
  })

  test('anthropic stream request with json content-type stays non-stream', async () => {
    process.env.ANTHROPIC_CAPTURE_ENABLED = 'true'
    process.env.ANTHROPIC_CAPTURE_DIR = tempDir
    process.env.ANTHROPIC_CAPTURE_INCLUDE_THINKING = 'true'
    process.env.ANTHROPIC_CAPTURE_INCLUDE_RAW = 'false'

    installFakeHttpsRequest()
    loadCommonJsHook('extensions/anthropic-capture/hook/anthropic-hook.js')

    const req = https.request({
      protocol: 'https:',
      hostname: 'api.anthropic.com',
      host: 'api.anthropic.com',
      path: '/v1/messages',
      method: 'POST',
      headers: {
        'x-request-id': 'req_json_content_type',
        'x-relay-key-id': 'relay-key-json-content-type'
      }
    })

    req.write(
      JSON.stringify({
        model: 'claude-test',
        stream: true,
        messages: [{ role: 'user', content: 'hello' }]
      })
    )
    req.end()

    const res = new EventEmitter()
    res.statusCode = 200
    res.headers = {
      'content-type': 'application/json'
    }
    req.emit('response', res)

    const responseBody = {
      id: 'msg_json_content_type',
      type: 'message',
      role: 'assistant',
      model: 'claude-test',
      content: [{ type: 'text', text: 'json answer' }],
      stop_reason: 'end_turn',
      usage: { input_tokens: 3, output_tokens: 2 }
    }

    res.emit('data', Buffer.from(JSON.stringify(responseBody)))
    res.emit('end')

    const responseRows = await waitForJsonl(tempDir, 'anthropic-upstream-responses.jsonl')
    expect(responseRows[0].type).toBe('anthropic_upstream_response_non_stream')
    expect(responseRows[0].response.body_json).toEqual(responseBody)
    expect(responseRows[0].response.message_id).toBe('msg_json_content_type')
    expect(fsSync.existsSync(path.join(tempDir, 'anthropic-upstream-stream-final.jsonl'))).toBe(
      false
    )
  })

  test('openai non-stream captures structured response fields without body_raw by default', async () => {
    process.env.OPENAI_CAPTURE_ENABLED = 'true'
    process.env.OPENAI_CAPTURE_DIR = tempDir
    process.env.OPENAI_CAPTURE_INCLUDE_REASONING = 'true'
    process.env.OPENAI_CAPTURE_INCLUDE_RAW = 'false'

    installFakeHttpsRequest()
    loadCommonJsHook('extensions/openai-capture/hook/openai-hook.js')

    const req = https.request({
      protocol: 'https:',
      hostname: 'api.openai.com',
      host: 'api.openai.com',
      path: '/v1/responses',
      method: 'POST',
      headers: {
        authorization: 'Bearer test-token',
        'x-relay-key-id': 'relay-key-2'
      }
    })

    const requestBody = {
      model: 'gpt-test',
      stream: false,
      input: [{ role: 'user', content: 'hello' }]
    }
    req.write(JSON.stringify(requestBody))
    req.end()

    const res = new EventEmitter()
    res.statusCode = 200
    res.headers = {
      'content-type': 'application/json'
    }
    req.emit('response', res)

    const responseBody = {
      id: 'resp_1',
      model: 'gpt-test',
      status: 'completed',
      output: [
        {
          type: 'message',
          content: [{ type: 'output_text', text: 'answer' }]
        },
        {
          type: 'reasoning',
          summary: [{ text: 'reasoning' }]
        },
        {
          type: 'function_call',
          id: 'fc_1',
          call_id: 'call_1',
          name: 'tool',
          arguments: '{"ok":true}',
          status: 'completed'
        }
      ],
      usage: {
        input_tokens: 1,
        output_tokens: 2,
        total_tokens: 3,
        input_tokens_details: { cached_tokens: 0 },
        output_tokens_details: { reasoning_tokens: 1 }
      }
    }

    res.emit('data', Buffer.from(JSON.stringify(responseBody)))
    res.emit('end')

    const requestRows = await waitForJsonl(tempDir, 'openai-upstream-requests.jsonl')
    expect(requestRows[0].request_body_json).toEqual(requestBody)
    expect(requestRows[0]).not.toHaveProperty('request_body_raw')
    expect(requestRows[0]).not.toHaveProperty('authorization_raw')
    expect(requestRows[0].authorization_sha256).toHaveLength(64)

    const responseRows = await waitForJsonl(tempDir, 'openai-upstream-responses.jsonl')
    expect(responseRows[0].response).not.toHaveProperty('body_raw')
    expect(responseRows[0].response.body_raw_omitted).toBe(true)
    expect(responseRows[0].response.body_json).toEqual(responseBody)
    expect(responseRows[0].response.assistant_text_full).toBe('answer')
    expect(responseRows[0].response.reasoning_text_full).toBe('reasoning')
    expect(responseRows[0].response.tool_calls).toEqual([
      {
        type: 'function_call',
        id: 'fc_1',
        call_id: 'call_1',
        name: 'tool',
        arguments: '{"ok":true}',
        status: 'completed',
        output_index: null
      }
    ])
    expect(responseRows[0].response.usage).toEqual(responseBody.usage)
    expect(responseRows[0].response.response_id).toBe('resp_1')
    expect(responseRows[0].response.model).toBe('gpt-test')
    expect(responseRows[0].response.status).toBe('completed')
  })

  test('openai capture skip header bypasses capture and is stripped before upstream', async () => {
    process.env.OPENAI_CAPTURE_ENABLED = 'true'
    process.env.OPENAI_CAPTURE_DIR = tempDir
    process.env.OPENAI_CAPTURE_HOSTS = 'api.openai.com,chatgpt.com'
    process.env.OPENAI_CAPTURE_PATH_PREFIXES =
      '/v1/responses,/responses,/backend-api/codex/responses'

    let capturedOptions = null
    https.request = jest.fn((options) => {
      capturedOptions = options
      const req = new EventEmitter()
      req.write = jest.fn()
      req.end = jest.fn()
      return req
    })

    loadCommonJsHook('extensions/openai-capture/hook/openai-hook.js')

    const req = https.request({
      protocol: 'https:',
      hostname: 'chatgpt.com',
      host: 'chatgpt.com',
      path: '/backend-api/codex/responses',
      method: 'POST',
      headers: {
        authorization: 'Bearer test-token',
        'x-relay-capture-skip': 'admin-openai-account-test'
      }
    })

    req.write(JSON.stringify({ model: 'gpt-5.5', stream: true }))
    req.end()

    expect(capturedOptions.headers.authorization).toBe('Bearer test-token')
    expect(capturedOptions.headers['x-relay-capture-skip']).toBeUndefined()
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(fsSync.existsSync(path.join(tempDir, 'openai-upstream-requests.jsonl'))).toBe(false)
    expect(fsSync.existsSync(path.join(tempDir, 'openai-upstream-responses.jsonl'))).toBe(false)
    expect(fsSync.existsSync(path.join(tempDir, 'openai-upstream-stream-final.jsonl'))).toBe(false)
  })

  test('openai gzip stream captures semantics and audit when content-encoding is missing', async () => {
    process.env.OPENAI_CAPTURE_ENABLED = 'true'
    process.env.OPENAI_CAPTURE_DIR = tempDir
    process.env.OPENAI_CAPTURE_INCLUDE_REASONING = 'true'
    process.env.OPENAI_CAPTURE_INCLUDE_RAW = 'false'
    process.env.CAPTURE_AUDIT_ENABLED = 'true'
    process.env.CAPTURE_AUDIT_FILE = 'capture-audit.jsonl'

    installFakeHttpsRequest()
    loadCommonJsHook('extensions/openai-capture/hook/openai-hook.js')

    const req = https.request({
      protocol: 'https:',
      hostname: 'api.openai.com',
      host: 'api.openai.com',
      path: '/v1/responses',
      method: 'POST',
      headers: {
        authorization: 'Bearer stream-token',
        'x-relay-key-id': 'relay-key-openai-stream'
      }
    })

    const requestBody = {
      model: 'gpt-stream',
      stream: true,
      input: [{ role: 'user', content: 'hello stream' }]
    }
    req.write(JSON.stringify(requestBody))
    req.end()

    const res = new EventEmitter()
    res.statusCode = 200
    res.headers = {
      'content-type': 'text/event-stream'
    }
    req.emit('response', res)

    const completedResponse = {
      id: 'resp_stream',
      model: 'gpt-stream',
      status: 'completed',
      output: [
        {
          type: 'message',
          content: [{ type: 'output_text', text: 'hello stream answer' }]
        },
        {
          type: 'reasoning',
          summary: [{ text: 'stream reasoning' }]
        },
        {
          type: 'function_call',
          id: 'fc_stream',
          call_id: 'call_stream',
          name: 'tool',
          arguments: '{"ok":true}',
          status: 'completed'
        }
      ],
      usage: {
        input_tokens: 4,
        output_tokens: 5,
        total_tokens: 9,
        input_tokens_details: { cached_tokens: 1 },
        output_tokens_details: { reasoning_tokens: 2 }
      }
    }

    const sse = [
      'event: response.created',
      'data: {"type":"response.created","response":{"id":"resp_stream","model":"gpt-stream","status":"in_progress"}}',
      '',
      'event: response.output_text.delta',
      'data: {"type":"response.output_text.delta","delta":"hello "}',
      '',
      'event: response.reasoning_summary_text.delta',
      'data: {"type":"response.reasoning_summary_text.delta","delta":"stream reasoning"}',
      '',
      'event: response.function_call_arguments.done',
      'data: {"type":"response.function_call_arguments.done","item_id":"fc_stream","call_id":"call_stream","name":"tool","arguments":"{\\"ok\\":true}","output_index":1}',
      '',
      'event: response.completed',
      `data: ${JSON.stringify({ type: 'response.completed', response: completedResponse })}`,
      ''
    ].join('\n')

    const gzipped = zlib.gzipSync(sse)
    res.emit('data', gzipped.subarray(0, 1))
    res.emit('data', gzipped.subarray(1))
    res.emit('end')

    const requestRows = await waitForJsonl(tempDir, 'openai-upstream-requests.jsonl')
    expect(requestRows[0].request_body_json).toEqual(requestBody)

    const streamRows = await waitForJsonl(tempDir, 'openai-upstream-stream-final.jsonl')
    expect(streamRows[0].stream.response_id).toBe('resp_stream')
    expect(streamRows[0].stream.response_model).toBe('gpt-stream')
    expect(streamRows[0].stream.status).toBe('completed')
    expect(streamRows[0].stream.assistant_text_full).toBe('hello stream answer')
    expect(streamRows[0].stream.reasoning_text_full).toBe('stream reasoning')
    expect(streamRows[0].stream.tool_calls).toEqual([
      {
        type: 'function_call',
        id: 'fc_stream',
        call_id: 'call_stream',
        name: 'tool',
        arguments: '{"ok":true}',
        status: 'completed',
        output_index: null
      }
    ])
    expect(streamRows[0].stream.usage).toEqual(completedResponse.usage)

    const summaryRows = await waitForJsonl(tempDir, 'openai-upstream-responses.jsonl')
    expect(summaryRows[0].decode_source).toBe('magic')
    expect(summaryRows[0].decompressed).toBe(true)
    expect(summaryRows[0].decode_error).toBeNull()

    const auditRows = await waitForJsonl(tempDir, 'capture-audit.jsonl', 2)
    expect(auditRows.map((row) => row.event)).toEqual(['request_written', 'response_written'])
    expect(auditRows[0]).toMatchObject({
      source: 'hook',
      provider: 'openai',
      event: 'request_written',
      relay_key_id: 'relay-key-openai-stream',
      request_json_present: true
    })
    expect(auditRows[1]).toMatchObject({
      source: 'hook',
      provider: 'openai',
      event: 'response_written',
      relay_key_id: 'relay-key-openai-stream',
      response_semantic_present: true,
      assistant_text_len: 'hello stream answer'.length,
      reasoning_text_len: 'stream reasoning'.length,
      tool_call_count: 1,
      usage_present: true,
      decode_source: 'magic',
      decode_error: null,
      warnings: []
    })
  })

  test('openai stream request captures SSE when content-type is missing', async () => {
    process.env.OPENAI_CAPTURE_ENABLED = 'true'
    process.env.OPENAI_CAPTURE_DIR = tempDir
    process.env.OPENAI_CAPTURE_HOSTS = 'api.openai.com,chatgpt.com'
    process.env.OPENAI_CAPTURE_PATH_PREFIXES =
      '/v1/responses,/responses,/backend-api/codex/responses'
    process.env.OPENAI_CAPTURE_INCLUDE_REASONING = 'true'
    process.env.OPENAI_CAPTURE_INCLUDE_RAW = 'false'

    installFakeHttpsRequest()
    loadCommonJsHook('extensions/openai-capture/hook/openai-hook.js')

    const req = https.request({
      protocol: 'https:',
      hostname: 'chatgpt.com',
      host: 'chatgpt.com',
      path: '/backend-api/codex/responses',
      method: 'POST',
      headers: {
        authorization: 'Bearer chatgpt-token',
        'x-relay-key-id': 'relay-key-chatgpt-codex'
      }
    })

    const requestBody = {
      model: 'gpt-5.5',
      stream: true,
      input: [{ role: 'user', content: 'hello codex' }]
    }
    req.write(JSON.stringify(requestBody))
    req.end()

    const res = new EventEmitter()
    res.statusCode = 200
    res.headers = {}
    req.emit('response', res)

    const completedResponse = {
      id: 'resp_codex',
      model: 'gpt-5.5',
      status: 'completed',
      output: [
        {
          type: 'message',
          content: [{ type: 'output_text', text: 'hello codex answer' }]
        },
        {
          type: 'reasoning',
          summary: [{ text: 'codex reasoning' }]
        }
      ],
      usage: {
        input_tokens: 11,
        output_tokens: 13,
        total_tokens: 24,
        output_tokens_details: { reasoning_tokens: 7 }
      }
    }

    const sse = [
      'event: response.output_text.delta',
      'data: {"type":"response.output_text.delta","delta":"hello codex answer"}',
      '',
      'event: response.reasoning_summary_text.delta',
      'data: {"type":"response.reasoning_summary_text.delta","delta":"codex reasoning"}',
      '',
      'event: response.completed',
      `data: ${JSON.stringify({ type: 'response.completed', response: completedResponse })}`,
      ''
    ].join('\n')

    res.emit('data', Buffer.from(sse))
    res.emit('end')

    const streamRows = await waitForJsonl(tempDir, 'openai-upstream-stream-final.jsonl')
    expect(streamRows[0].provider_kind).toBe('chatgpt-codex')
    expect(streamRows[0].stream.response_id).toBe('resp_codex')
    expect(streamRows[0].stream.response_model).toBe('gpt-5.5')
    expect(streamRows[0].stream.status).toBe('completed')
    expect(streamRows[0].stream.assistant_text_full).toBe('hello codex answer')
    expect(streamRows[0].stream.reasoning_text_full).toBe('codex reasoning')
    expect(streamRows[0].stream.usage).toEqual(completedResponse.usage)

    const summaryRows = await waitForJsonl(tempDir, 'openai-upstream-responses.jsonl')
    expect(summaryRows[0].type).toBe('openai_upstream_response_stream_summary')
    expect(summaryRows[0].decode_source).toBe('identity')
    expect(summaryRows[0].decompressed).toBe(false)
    expect(summaryRows[0].decode_error).toBeNull()
    expect(summaryRows[0].usage).toEqual(completedResponse.usage)
  })
})
