const { CLAUDE_MODELS, GEMINI_MODELS, OPENAI_MODELS } = require('../config/models')

describe('models config', () => {
  it('places Claude Sonnet 4.6 as the second Claude model option', () => {
    expect(CLAUDE_MODELS[1]).toEqual({
      value: 'claude-sonnet-4-6',
      label: 'Claude Sonnet 4.6'
    })
  })

  it('exposes the latest Claude, OpenAI, and Gemini test models', () => {
    expect(CLAUDE_MODELS.map(({ value }) => value)).toEqual(
      expect.arrayContaining(['claude-opus-4-6', 'claude-sonnet-4-6'])
    )
    expect(OPENAI_MODELS.map(({ value }) => value)).toEqual(
      expect.arrayContaining(['gpt-5.6-sol', 'gpt-5.6-terra', 'gpt-5.6-luna'])
    )
    expect(GEMINI_MODELS.map(({ value }) => value)).toEqual(
      expect.arrayContaining(['gemini-3-flash-preview', 'gemini-3.1-pro-preview'])
    )
  })
})
