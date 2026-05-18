describe('copilotModelResolver', () => {
  let resolver
  let accountService

  beforeEach(() => {
    jest.resetModules()
    jest.doMock('../src/services/account/copilotAccountService', () => ({
      getMappedModel: jest.fn((mapping, requestedModel) => {
        if (!mapping || typeof mapping !== 'object' || Array.isArray(mapping)) return null
        if (mapping[requestedModel]) return mapping[requestedModel]
        const lower = requestedModel.toLowerCase()
        const entry = Object.entries(mapping).find(([key]) => key.toLowerCase() === lower)
        return entry ? entry[1] : null
      }),
      getModelsForResolvedAccount: jest.fn()
    }))
    jest.doMock('../src/utils/logger', () => ({
      warn: jest.fn(),
      info: jest.fn(),
      debug: jest.fn(),
      error: jest.fn()
    }))
    resolver = require('../src/services/copilotModelResolver')
    accountService = require('../src/services/account/copilotAccountService')
  })

  afterEach(() => {
    jest.clearAllMocks()
  })

  test('manual exact mapping has highest priority', async () => {
    accountService.getModelsForResolvedAccount.mockResolvedValue({ models: ['claude-opus-x'] })
    const result = await resolver.resolve(
      { supportedModels: { opus: 'claude-opus-4.6-1m[1m]' } },
      'copilot,opus'
    )

    expect(result).toMatchObject({
      requestedModel: 'opus',
      resolvedModel: 'claude-opus-4.6-1m[1m]',
      source: 'manual_mapping'
    })
  })

  test('dynamic exact match resolves full upstream model', async () => {
    accountService.getModelsForResolvedAccount.mockResolvedValue({
      models: ['gpt-5.5', 'claude-sonnet-4.6']
    })

    const result = await resolver.resolve({ supportedModels: [] }, 'copilot,gpt-5.5')

    expect(result).toMatchObject({
      requestedModel: 'gpt-5.5',
      resolvedModel: 'gpt-5.5',
      source: 'dynamic_exact'
    })
  })

  test('builtin alias is selected from dynamic model list', async () => {
    accountService.getModelsForResolvedAccount.mockResolvedValue({
      models: ['claude-haiku-4.5', 'claude-opus-4.6-1m[1m]', 'claude-opus-4.5']
    })

    const result = await resolver.resolve({ supportedModels: [] }, 'copilot,opus')

    expect(result.resolvedModel).toBe('claude-opus-4.6-1m[1m]')
    expect(result.source).toBe('builtin_alias')
  })

  test('full model name passes through when model list is unavailable', async () => {
    accountService.getModelsForResolvedAccount.mockRejectedValue(new Error('network down'))

    const result = await resolver.resolve({ supportedModels: [] }, 'copilot,gpt-5.5')

    expect(result).toMatchObject({
      requestedModel: 'gpt-5.5',
      resolvedModel: 'gpt-5.5',
      source: 'passthrough'
    })
    expect(result.warning).toContain('network down')
  })

  test('alias fails when dynamic model list is unavailable', async () => {
    accountService.getModelsForResolvedAccount.mockRejectedValue(new Error('network down'))

    await expect(resolver.resolve({ supportedModels: [] }, 'copilot,opus')).rejects.toMatchObject({
      code: 'COPILOT_MODEL_RESOLUTION_FAILED'
    })
  })

  test('unknown nickname fails instead of silently passing through', async () => {
    accountService.getModelsForResolvedAccount.mockResolvedValue({ models: ['gpt-5.5'] })

    await expect(
      resolver.resolve({ supportedModels: [] }, 'copilot,unknown-alias')
    ).rejects.toMatchObject({
      code: 'COPILOT_MODEL_RESOLUTION_FAILED'
    })
  })
})
