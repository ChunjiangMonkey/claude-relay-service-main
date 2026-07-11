'use strict'

const { OPENAI_CAPTURE_HOOK_ACTIVE, attachRelayKeyIdHeader } = require('./relayKeyCapture')

const RESPONSES_LITE_HEADER = 'x-openai-internal-codex-responses-lite'
const CODEX_PASSTHROUGH_REQUEST_HEADERS = [
  'version',
  'openai-beta',
  'user-agent',
  'originator',
  'session-id',
  'session_id',
  'thread-id',
  'thread_id',
  'x-client-request-id',
  'x-codex-installation-id',
  'x-codex-beta-features',
  'x-codex-turn-state',
  'x-codex-turn-metadata',
  'x-codex-parent-thread-id',
  'x-codex-window-id',
  'x-openai-subagent',
  'x-responsesapi-include-timing-metrics'
]

function usesCodexResponsesLite(model) {
  return typeof model === 'string' && model.toLowerCase().startsWith('gpt-5.6-')
}

function buildCodexUpstreamHeaders({
  incomingHeaders,
  accessToken,
  account,
  accountId,
  isStream,
  apiKeyId,
  model
}) {
  const headers = {}

  for (const key of CODEX_PASSTHROUGH_REQUEST_HEADERS) {
    if (incomingHeaders[key] !== undefined) {
      headers[key] = incomingHeaders[key]
    }
  }

  headers['authorization'] = `Bearer ${accessToken}`
  headers['chatgpt-account-id'] = account.accountId || account.chatgptUserId || accountId
  headers['host'] = 'chatgpt.com'
  headers['accept'] = isStream ? 'text/event-stream' : 'application/json'
  headers['content-type'] = 'application/json'
  if (usesCodexResponsesLite(model)) {
    headers[RESPONSES_LITE_HEADER] = 'true'
  }
  attachRelayKeyIdHeader(headers, apiKeyId, OPENAI_CAPTURE_HOOK_ACTIVE)

  return headers
}

module.exports = {
  RESPONSES_LITE_HEADER,
  CODEX_PASSTHROUGH_REQUEST_HEADERS,
  usesCodexResponsesLite,
  buildCodexUpstreamHeaders
}
