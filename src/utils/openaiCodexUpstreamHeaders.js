'use strict'

const { OPENAI_CAPTURE_HOOK_ACTIVE, attachRelayKeyIdHeader } = require('./relayKeyCapture')

function buildCodexUpstreamHeaders({
  incomingHeaders,
  accessToken,
  account,
  accountId,
  isStream,
  apiKeyId
}) {
  const allowedKeys = ['version', 'openai-beta', 'session_id']
  const headers = {}

  for (const key of allowedKeys) {
    if (incomingHeaders[key] !== undefined) {
      headers[key] = incomingHeaders[key]
    }
  }

  headers['authorization'] = `Bearer ${accessToken}`
  headers['chatgpt-account-id'] = account.accountId || account.chatgptUserId || accountId
  headers['host'] = 'chatgpt.com'
  headers['accept'] = isStream ? 'text/event-stream' : 'application/json'
  headers['content-type'] = 'application/json'
  attachRelayKeyIdHeader(headers, apiKeyId, OPENAI_CAPTURE_HOOK_ACTIVE)

  return headers
}

module.exports = {
  buildCodexUpstreamHeaders
}
