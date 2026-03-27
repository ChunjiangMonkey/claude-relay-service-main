'use strict'

const OPENAI_CAPTURE_HOOK_ACTIVE = Symbol.for('claude_relay.openai_capture_hook.active')
const ANTHROPIC_CAPTURE_HOOK_ACTIVE = Symbol.for('claude_relay.anthropic_capture_hook.active')

function isCaptureHookActive(activeSentinel) {
  return Boolean(global[activeSentinel])
}

function getRelayKeyIdForActiveCapture(apiKeyId, activeSentinel) {
  if (!apiKeyId || !isCaptureHookActive(activeSentinel)) {
    return ''
  }
  return apiKeyId
}

function attachRelayKeyIdHeader(headers, apiKeyId, activeSentinel) {
  if (!headers || typeof headers !== 'object') {
    return false
  }

  const relayKeyId = getRelayKeyIdForActiveCapture(apiKeyId, activeSentinel)
  if (!relayKeyId) {
    return false
  }

  headers['x-relay-key-id'] = relayKeyId
  return true
}

module.exports = {
  OPENAI_CAPTURE_HOOK_ACTIVE,
  ANTHROPIC_CAPTURE_HOOK_ACTIVE,
  isCaptureHookActive,
  getRelayKeyIdForActiveCapture,
  attachRelayKeyIdHeader
}
