'use strict'

/**
 * Parse OpenAI SSE text and extract the last `response.completed` event.
 *
 * @param {string} bodyRaw - Raw SSE text from body_raw
 * @returns {{ model, status, responseId, usage, output, error } | null}
 */
function parseOpenaiSse(bodyRaw) {
  if (!bodyRaw || typeof bodyRaw !== 'string') {
    return null
  }

  const events = bodyRaw.split('\n\n')
  let lastCompleted = null

  for (const event of events) {
    const lines = event.split('\n')
    for (const line of lines) {
      if (!line.startsWith('data: ')) {
        continue
      }
      const dataStr = line.slice(6)
      if (dataStr === '[DONE]') {
        continue
      }
      let parsed
      try {
        parsed = JSON.parse(dataStr)
      } catch (_) {
        continue
      }
      if (parsed && parsed.type === 'response.completed' && parsed.response) {
        lastCompleted = parsed.response
      }
    }
  }

  if (!lastCompleted) {
    return null
  }

  return {
    model: lastCompleted.model || null,
    status: lastCompleted.status || null,
    responseId: lastCompleted.id || null,
    usage: lastCompleted.usage || null,
    output: Array.isArray(lastCompleted.output) ? lastCompleted.output : [],
    error: lastCompleted.error || null
  }
}

/**
 * Extract assistant text and tool_calls from an OpenAI output array.
 * Reasoning content is encrypted (encrypted_content) and cannot be decrypted,
 * so reasoningTextFull is always empty.
 *
 * @param {Array} output - The output array from a response.completed event
 * @returns {{ assistantTextFull: string, reasoningTextFull: string, toolCalls: Array }}
 */
function extractOutputContent(output) {
  if (!Array.isArray(output)) {
    return { assistantTextFull: '', reasoningTextFull: '', toolCalls: [] }
  }

  const textChunks = []
  const toolCalls = []

  for (const item of output) {
    if (!item || typeof item !== 'object') {
      continue
    }

    if (item.type === 'message' && Array.isArray(item.content)) {
      for (const content of item.content) {
        if (!content || typeof content !== 'object') {
          continue
        }
        if (
          (content.type === 'output_text' || content.type === 'text') &&
          typeof content.text === 'string'
        ) {
          textChunks.push(content.text)
        }
      }
      continue
    }

    if ((item.type === 'output_text' || item.type === 'text') && typeof item.text === 'string') {
      textChunks.push(item.text)
    }

    if (item.type === 'function_call' || item.type === 'custom_tool_call') {
      toolCalls.push({
        type: item.type,
        name: item.name || null,
        call_id: item.call_id || null,
        arguments: item.arguments || ''
      })
    }
  }

  return {
    assistantTextFull: textChunks.join(''),
    reasoningTextFull: '',
    toolCalls
  }
}

module.exports = {
  parseOpenaiSse,
  extractOutputContent
}
