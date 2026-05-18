const logger = require('../utils/logger')
const copilotAccountService = require('./account/copilotAccountService')
const { parseVendorPrefixedModel } = require('../utils/modelHelper')

const BUILTIN_ALIASES = new Set(['default', 'opus', 'sonnet', 'haiku', 'gpt', 'codex'])

function normalizeModelName(value) {
  return String(value || '').trim()
}

function caseInsensitiveFind(items, target) {
  const targetLower = normalizeModelName(target).toLowerCase()
  return items.find((item) => normalizeModelName(item).toLowerCase() === targetLower) || null
}

function extractVersionScore(model) {
  const matches = String(model || '').match(/\d+(?:\.\d+)?/g) || []
  return matches.reduce((score, raw, index) => {
    const value = Number(raw)
    return score + (Number.isFinite(value) ? value / 10 ** index : 0)
  }, 0)
}

function rankModels(models) {
  return [...models].sort((a, b) => {
    const aText = String(a).toLowerCase()
    const bText = String(b).toLowerCase()
    const aOneMillion = aText.includes('[1m]') || aText.includes('1m')
    const bOneMillion = bText.includes('[1m]') || bText.includes('1m')
    if (aOneMillion !== bOneMillion) {
      return aOneMillion ? -1 : 1
    }
    const scoreDiff = extractVersionScore(b) - extractVersionScore(a)
    if (scoreDiff !== 0) {
      return scoreDiff
    }
    return a.localeCompare(b)
  })
}

function isBuiltinAlias(model) {
  return BUILTIN_ALIASES.has(normalizeModelName(model).toLowerCase())
}

function looksLikeFullModelName(model) {
  const value = normalizeModelName(model).toLowerCase()
  if (!value) {
    return false
  }
  if (
    /^(claude|gpt|o\d|o-|codex|chatgpt|computer-use|text-|dall-e|tts|whisper)[\w.-]*/.test(value)
  ) {
    return true
  }
  if (value.includes('/') || value.includes(':') || value.includes('[')) {
    return true
  }
  return /\d/.test(value) && value.includes('-')
}

function pickAliasModel(alias, dynamicModels) {
  const models = Array.isArray(dynamicModels) ? dynamicModels.filter(Boolean) : []
  if (models.length === 0) {
    return null
  }

  const ranked = rankModels(models)
  const lowerAlias = normalizeModelName(alias).toLowerCase()
  const familyPredicates = {
    opus: (model) => model.includes('opus'),
    sonnet: (model) => model.includes('sonnet'),
    haiku: (model) => model.includes('haiku'),
    gpt: (model) => model.includes('gpt'),
    codex: (model) => model.includes('codex')
  }

  if (lowerAlias === 'default') {
    const defaultOrder = ['sonnet', 'opus', 'gpt', 'codex', 'haiku']
    for (const family of defaultOrder) {
      const found = ranked.find((model) => familyPredicates[family](model.toLowerCase()))
      if (found) {
        return found
      }
    }
    return ranked[0]
  }

  const predicate = familyPredicates[lowerAlias]
  return predicate ? ranked.find((model) => predicate(model.toLowerCase())) || null : null
}

class CopilotModelResolver {
  isAlias(model) {
    return isBuiltinAlias(model)
  }

  stripCopilotPrefix(model) {
    const { vendor, baseModel } = parseVendorPrefixedModel(model)
    return vendor === 'copilot' ? baseModel : model
  }

  async resolve(account, requestedModel, options = {}) {
    const baseRequestedModel = normalizeModelName(this.stripCopilotPrefix(requestedModel))
    const manualMapping = account?.supportedModels

    if (!baseRequestedModel) {
      throw this._createResolutionError('Copilot model is required', requestedModel)
    }

    const mapped = copilotAccountService.getMappedModel(manualMapping, baseRequestedModel)
    if (mapped) {
      return this._buildResult(baseRequestedModel, mapped, 'manual_mapping')
    }

    let modelCache = null
    let cacheError = null
    try {
      modelCache = await copilotAccountService.getModelsForResolvedAccount(account, {
        force: options.forceRefresh === true,
        allowStale: true
      })
    } catch (error) {
      cacheError = error
    }

    const dynamicModels = modelCache?.models || []
    const exactDynamic = caseInsensitiveFind(dynamicModels, baseRequestedModel)
    if (exactDynamic) {
      return this._buildResult(baseRequestedModel, exactDynamic, 'dynamic_exact', {
        stale: modelCache?.stale === true
      })
    }

    if (isBuiltinAlias(baseRequestedModel)) {
      const aliasModel = pickAliasModel(baseRequestedModel, dynamicModels)
      if (aliasModel) {
        return this._buildResult(baseRequestedModel, aliasModel, 'builtin_alias', {
          stale: modelCache?.stale === true
        })
      }
      throw this._createResolutionError(
        `Copilot alias "${baseRequestedModel}" cannot be resolved without a model list or manual mapping`,
        requestedModel,
        cacheError
      )
    }

    if (looksLikeFullModelName(baseRequestedModel)) {
      const warning = cacheError
        ? `Copilot model list unavailable, passing through full model name: ${cacheError.message}`
        : dynamicModels.length === 0
          ? 'Copilot model list is empty, passing through full model name'
          : null
      if (warning) {
        logger.warn(warning)
      }
      return this._buildResult(baseRequestedModel, baseRequestedModel, 'passthrough', {
        warning,
        stale: modelCache?.stale === true
      })
    }

    throw this._createResolutionError(
      `Copilot model "${baseRequestedModel}" is not a known alias and does not look like a full upstream model name`,
      requestedModel,
      cacheError
    )
  }

  _buildResult(requestedModel, resolvedModel, source, extra = {}) {
    return {
      requestedModel,
      requestedModelWithPrefix: `copilot,${requestedModel}`,
      resolvedModel,
      accountType: 'copilot',
      source,
      warning: extra.warning || null,
      stale: extra.stale === true
    }
  }

  _createResolutionError(message, requestedModel, cause = null) {
    const error = new Error(message)
    error.code = 'COPILOT_MODEL_RESOLUTION_FAILED'
    error.requestedModel = requestedModel
    if (cause) {
      error.cause = cause
    }
    return error
  }
}

module.exports = new CopilotModelResolver()
module.exports.BUILTIN_ALIASES = BUILTIN_ALIASES
module.exports._test = {
  pickAliasModel,
  looksLikeFullModelName,
  rankModels
}
