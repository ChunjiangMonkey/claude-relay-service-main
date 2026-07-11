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

function resolveChatGptAccountId(account = {}) {
  if (typeof account.accountId === 'string' && account.accountId.trim()) {
    return account.accountId.trim()
  }

  if (typeof account.idToken !== 'string') {
    return null
  }

  try {
    const parts = account.idToken.split('.')
    if (parts.length !== 3) {
      return null
    }
    const payload = JSON.parse(Buffer.from(parts[1], 'base64url').toString('utf8'))
    const accountId =
      payload?.['https://api.openai.com/auth']?.chatgpt_account_id || payload?.chatgpt_account_id
    return typeof accountId === 'string' && accountId.trim() ? accountId.trim() : null
  } catch {
    return null
  }
}

function buildCodexUpstreamHeaders({
  incomingHeaders,
  accessToken,
  account,
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
  const chatGptAccountId = resolveChatGptAccountId(account)
  if (chatGptAccountId) {
    headers['chatgpt-account-id'] = chatGptAccountId
  }
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
  resolveChatGptAccountId,
  buildCodexUpstreamHeaders
}
