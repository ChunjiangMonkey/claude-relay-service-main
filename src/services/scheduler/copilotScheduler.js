const redis = require('../../models/redis')
const logger = require('../../utils/logger')
const { isSchedulable, sortAccountsByPriority } = require('../../utils/commonHelper')
const upstreamErrorHelper = require('../../utils/upstreamErrorHelper')
const copilotAccountService = require('../account/copilotAccountService')
const copilotModelResolver = require('../copilotModelResolver')

class CopilotScheduler {
  constructor() {
    this.SESSION_MAPPING_PREFIX = 'copilot_session_mapping:'
  }

  async selectAccountForApiKey(apiKeyData, sessionHash = null, requestedModel = null) {
    const baseRequestedModel = copilotModelResolver.stripCopilotPrefix(requestedModel)
    const mapped = sessionHash ? await this._getSessionMapping(sessionHash) : null

    if (mapped?.accountId && mapped?.requestedModel === baseRequestedModel) {
      const selection = await this._tryBuildSelection(mapped.accountId, baseRequestedModel)
      if (selection) {
        logger.info(`Using sticky Copilot account ${selection.accountId} for ${baseRequestedModel}`)
        return selection
      }
      await this._deleteSessionMapping(sessionHash)
    }

    const available = await this._getAvailableAccounts(baseRequestedModel)
    if (available.length === 0) {
      const error = new Error(`No available Copilot account for model: ${baseRequestedModel}`)
      error.statusCode = 402
      throw error
    }

    const selected = sortAccountsByPriority(available)[0]
    if (sessionHash) {
      await this._setSessionMapping(sessionHash, selected)
    }

    logger.info(
      `Selected Copilot account ${selected.name} (${selected.accountId}) for ${baseRequestedModel} -> ${selected.resolvedModel}`
    )
    return selected
  }

  async _getAvailableAccounts(requestedModel) {
    const accounts = await copilotAccountService.getAllAccounts()
    const available = []

    for (const account of accounts) {
      if (!this._isAccountSchedulingCandidate(account)) {
        continue
      }

      const selection = await this._tryBuildSelection(account.id, requestedModel, account)
      if (selection) {
        available.push(selection)
      }
    }

    return available
  }

  async _tryBuildSelection(accountId, requestedModel, accountOverride = null) {
    const account =
      accountOverride && accountOverride.apiKey !== '***'
        ? accountOverride
        : await copilotAccountService.getAccount(accountId)
    if (!account || !this._isAccountSchedulingCandidate(account)) {
      return null
    }

    if (await upstreamErrorHelper.isTempUnavailable(account.id, 'copilot')) {
      return null
    }
    if (await copilotAccountService.isAccountRateLimited(account.id)) {
      return null
    }
    if (await copilotAccountService.isAccountQuotaExceeded(account.id)) {
      return null
    }
    if (await copilotAccountService.isAccountOverloaded(account.id)) {
      return null
    }

    let resolution
    try {
      resolution = await copilotModelResolver.resolve(account, requestedModel)
    } catch (error) {
      logger.warn(
        `Copilot model ${requestedModel} not supported by ${account.name || account.id}: ${error.message}`
      )
      return null
    }

    if (
      !copilotAccountService.isModelAllowed(
        account.supportedModels,
        resolution.requestedModel,
        resolution.resolvedModel
      )
    ) {
      logger.info(
        `Copilot account ${account.name || account.id} blocked model ${resolution.requestedModel} -> ${resolution.resolvedModel}`
      )
      return null
    }

    return {
      accountId: account.id,
      accountType: 'copilot',
      name: account.name,
      priority: account.priority || 50,
      lastUsedAt: account.lastUsedAt || '',
      requestedModel: resolution.requestedModel,
      requestedModelWithPrefix: resolution.requestedModelWithPrefix,
      resolvedModel: resolution.resolvedModel,
      resolution,
      modelResolutionSource: resolution.source
    }
  }

  _isAccountSchedulingCandidate(account) {
    if (!account) {
      return false
    }
    if (account.isActive !== true) {
      return false
    }
    if (account.status && account.status !== 'active') {
      return false
    }
    if ((account.accountType || 'shared') !== 'shared') {
      return false
    }
    if (!isSchedulable(account.schedulable)) {
      return false
    }
    if (copilotAccountService.isSubscriptionExpired(account)) {
      return false
    }
    return true
  }

  async markAccountRateLimited(accountId, sessionHash = null, duration = null) {
    if (sessionHash) {
      await this._deleteSessionMapping(sessionHash)
    }
    return copilotAccountService.markAccountRateLimited(accountId, duration)
  }

  async markAccountUnauthorized(accountId, sessionHash = null, reason = null) {
    if (sessionHash) {
      await this._deleteSessionMapping(sessionHash)
    }
    return copilotAccountService.markAccountUnauthorized(accountId, reason)
  }

  async removeAccountRateLimit(accountId) {
    return copilotAccountService.removeAccountRateLimit(accountId)
  }

  async isAccountRateLimited(accountId) {
    return copilotAccountService.isAccountRateLimited(accountId)
  }

  async _getSessionMapping(sessionHash) {
    if (!sessionHash) {
      return null
    }
    try {
      const client = redis.getClientSafe()
      const raw = await client.get(`${this.SESSION_MAPPING_PREFIX}${sessionHash}`)
      return raw ? JSON.parse(raw) : null
    } catch (error) {
      logger.warn(`Failed to read Copilot session mapping: ${error.message}`)
      return null
    }
  }

  async _setSessionMapping(sessionHash, selection) {
    if (!sessionHash || !selection?.accountId) {
      return
    }
    const client = redis.getClientSafe()
    await client.setex(
      `${this.SESSION_MAPPING_PREFIX}${sessionHash}`,
      24 * 60 * 60,
      JSON.stringify({
        accountId: selection.accountId,
        accountType: 'copilot',
        requestedModel: selection.requestedModel,
        resolvedModel: selection.resolvedModel,
        createdAt: new Date().toISOString()
      })
    )
  }

  async _deleteSessionMapping(sessionHash) {
    if (!sessionHash) {
      return
    }
    const client = redis.getClientSafe()
    await client.del(`${this.SESSION_MAPPING_PREFIX}${sessionHash}`)
  }
}

module.exports = new CopilotScheduler()
