const { v4: uuidv4 } = require('uuid')
const axios = require('axios')
const ProxyHelper = require('../../utils/proxyHelper')
const redis = require('../../models/redis')
const logger = require('../../utils/logger')
const { createEncryptor } = require('../../utils/commonHelper')
const upstreamErrorHelper = require('../../utils/upstreamErrorHelper')

const MODEL_CACHE_TTL_MS = 60 * 60 * 1000

function safeJsonParse(rawValue, fallback) {
  if (rawValue === undefined || rawValue === null || rawValue === '') {
    return fallback
  }
  if (typeof rawValue !== 'string') {
    return rawValue
  }
  try {
    return JSON.parse(rawValue)
  } catch (_) {
    return fallback
  }
}

function normalizeBaseApi(baseApi = '') {
  const value = String(baseApi || '').trim()
  return value.endsWith('/') ? value.slice(0, -1) : value
}

function normalizeSupportedModels(value) {
  if (!value) {
    return []
  }
  if (typeof value === 'string') {
    return safeJsonParse(value, [])
  }
  if (Array.isArray(value)) {
    return value.filter((item) => typeof item === 'string' && item.trim())
  }
  if (typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value).filter(([key, target]) => key && typeof target === 'string' && target)
    )
  }
  return []
}

function normalizeModelId(model) {
  if (typeof model === 'string') {
    return model.trim()
  }
  if (model && typeof model === 'object' && typeof model.id === 'string') {
    return model.id.trim()
  }
  return ''
}

class CopilotAccountService {
  constructor() {
    this.ACCOUNT_KEY_PREFIX = 'copilot_account:'
    this.ACCOUNT_INDEX_KEY = 'copilot_account:index'
    this.SHARED_ACCOUNTS_KEY = 'shared_copilot_accounts'
    this.MODEL_CACHE_TTL_MS = MODEL_CACHE_TTL_MS
    this._encryptor = createEncryptor('copilot-account-salt')

    setInterval(
      () => {
        this._encryptor.clearCache()
        logger.info('Copilot account decrypt cache cleanup completed', this._encryptor.getStats())
      },
      10 * 60 * 1000
    )
  }

  async createAccount(options = {}) {
    const {
      name = 'Copilot Account',
      description = '',
      baseApi = '',
      apiKey = '',
      priority = 50,
      supportedModels = [],
      userAgent = 'claude-relay-service/1.0.0',
      proxy = null,
      isActive = true,
      accountType = 'shared',
      schedulable = true,
      rateLimitDuration = 60,
      dailyQuota = 0,
      quotaResetTime = '00:00',
      disableAutoProtection = false
    } = options

    if (!baseApi) {
      throw new Error('Base API URL is required for Copilot account')
    }

    const accountId = uuidv4()
    const now = new Date().toISOString()
    const accountData = {
      id: accountId,
      platform: 'copilot',
      name,
      description,
      baseApi: normalizeBaseApi(baseApi),
      apiKey: apiKey ? this._encryptSensitiveData(apiKey) : '',
      priority: String(priority || 50),
      supportedModels: JSON.stringify(normalizeSupportedModels(supportedModels)),
      userAgent,
      proxy: proxy ? JSON.stringify(proxy) : '',
      isActive: String(isActive),
      accountType,
      schedulable: String(schedulable),
      rateLimitDuration: String(rateLimitDuration),
      dailyQuota: String(dailyQuota || 0),
      dailyUsage: '0',
      lastResetDate: redis.getDateStringInTimezone(),
      quotaResetTime,
      quotaStoppedAt: '',
      disableAutoProtection: String(disableAutoProtection),
      modelCache: '',
      modelCacheUpdatedAt: '',
      createdAt: now,
      lastUsedAt: '',
      status: 'active',
      errorMessage: ''
    }

    const client = redis.getClientSafe()
    await client.hset(`${this.ACCOUNT_KEY_PREFIX}${accountId}`, accountData)
    await redis.addToIndex(this.ACCOUNT_INDEX_KEY, accountId)
    if (accountType === 'shared') {
      await client.sadd(this.SHARED_ACCOUNTS_KEY, accountId)
    }

    logger.success(`Created Copilot account: ${name} (${accountId})`)
    return this._sanitizeAccountData(accountData)
  }

  async getAllAccounts() {
    const accountIds = await redis.getAllIdsByIndex(
      this.ACCOUNT_INDEX_KEY,
      `${this.ACCOUNT_KEY_PREFIX}*`,
      /^copilot_account:(.+)$/
    )
    const keys = accountIds.map((id) => `${this.ACCOUNT_KEY_PREFIX}${id}`)
    const dataList = await redis.batchHgetallChunked(keys)

    return dataList
      .filter((data) => data && Object.keys(data).length > 0)
      .map((data) => this._sanitizeAccountData(data))
  }

  async getAccount(accountId) {
    const client = redis.getClientSafe()
    const accountData = await client.hgetall(`${this.ACCOUNT_KEY_PREFIX}${accountId}`)
    if (!accountData || Object.keys(accountData).length === 0) {
      return null
    }

    const account = this._sanitizeAccountData(accountData)
    account.apiKey = accountData.apiKey ? this._decryptSensitiveData(accountData.apiKey) : ''
    return account
  }

  async updateAccount(accountId, updates = {}) {
    const existing = await this.getAccount(accountId)
    if (!existing) {
      throw new Error('Copilot account not found')
    }

    const data = {}
    const simpleFields = [
      'name',
      'description',
      'userAgent',
      'quotaResetTime',
      'status',
      'errorMessage',
      'lastUsedAt',
      'rateLimitedAt',
      'rateLimitStatus',
      'rateLimitResetAt',
      'quotaStoppedAt',
      'modelCacheUpdatedAt'
    ]

    for (const field of simpleFields) {
      if (updates[field] !== undefined) {
        data[field] = updates[field] || ''
      }
    }

    if (updates.baseApi !== undefined) {
      data.baseApi = normalizeBaseApi(updates.baseApi)
    }
    if (updates.apiKey !== undefined) {
      data.apiKey = updates.apiKey ? this._encryptSensitiveData(updates.apiKey) : ''
    }
    if (updates.priority !== undefined) {
      data.priority = String(updates.priority)
    }
    if (updates.supportedModels !== undefined) {
      data.supportedModels = JSON.stringify(normalizeSupportedModels(updates.supportedModels))
    }
    if (updates.proxy !== undefined) {
      data.proxy = updates.proxy ? JSON.stringify(updates.proxy) : ''
    }
    if (updates.isActive !== undefined) {
      data.isActive = String(updates.isActive)
    }
    if (updates.schedulable !== undefined) {
      data.schedulable = String(updates.schedulable)
    }
    if (updates.rateLimitDuration !== undefined) {
      data.rateLimitDuration = String(updates.rateLimitDuration)
    }
    if (updates.dailyQuota !== undefined) {
      data.dailyQuota = String(updates.dailyQuota)
    }
    if (updates.dailyUsage !== undefined) {
      data.dailyUsage = String(updates.dailyUsage)
    }
    if (updates.disableAutoProtection !== undefined) {
      data.disableAutoProtection = String(updates.disableAutoProtection)
    }
    if (updates.modelCache !== undefined) {
      data.modelCache =
        typeof updates.modelCache === 'string'
          ? updates.modelCache
          : JSON.stringify(updates.modelCache)
    }

    const client = redis.getClientSafe()
    if (Object.keys(data).length > 0) {
      await client.hset(`${this.ACCOUNT_KEY_PREFIX}${accountId}`, data)
    }

    if (updates.accountType !== undefined) {
      await client.hset(
        `${this.ACCOUNT_KEY_PREFIX}${accountId}`,
        'accountType',
        updates.accountType
      )
      if (updates.accountType === 'shared') {
        await client.sadd(this.SHARED_ACCOUNTS_KEY, accountId)
      } else {
        await client.srem(this.SHARED_ACCOUNTS_KEY, accountId)
      }
    }

    logger.success(`Updated Copilot account: ${accountId}`)
    return this.getAccount(accountId)
  }

  async deleteAccount(accountId) {
    const client = redis.getClientSafe()
    await client.srem(this.SHARED_ACCOUNTS_KEY, accountId)
    await redis.removeFromIndex(this.ACCOUNT_INDEX_KEY, accountId)
    const result = await client.del(`${this.ACCOUNT_KEY_PREFIX}${accountId}`)
    if (result === 0) {
      throw new Error('Copilot account not found or already deleted')
    }
    logger.success(`Deleted Copilot account: ${accountId}`)
    return { success: true }
  }

  async getDecryptedCredentials(accountId) {
    const account = await this.getAccount(accountId)
    if (!account) {
      return null
    }
    return {
      baseApi: account.baseApi,
      apiKey: account.apiKey || ''
    }
  }

  getMappedModel(modelMapping, requestedModel) {
    if (!modelMapping || typeof modelMapping !== 'object' || Array.isArray(modelMapping)) {
      return null
    }
    if (Object.prototype.hasOwnProperty.call(modelMapping, requestedModel)) {
      return modelMapping[requestedModel]
    }
    const lower = String(requestedModel || '').toLowerCase()
    for (const [key, value] of Object.entries(modelMapping)) {
      if (String(key).toLowerCase() === lower) {
        return value
      }
    }
    return null
  }

  isModelAllowed(supportedModels, requestedModel, resolvedModel = null) {
    if (!supportedModels) {
      return true
    }
    if (Array.isArray(supportedModels)) {
      if (supportedModels.length === 0) {
        return true
      }
      const allowed = new Set(supportedModels.map((item) => String(item).toLowerCase()))
      return (
        allowed.has(String(requestedModel || '').toLowerCase()) ||
        allowed.has(String(resolvedModel || '').toLowerCase())
      )
    }
    if (typeof supportedModels === 'object') {
      const keys = Object.keys(supportedModels)
      if (keys.length === 0) {
        return true
      }
      const requestedLower = String(requestedModel || '').toLowerCase()
      const resolvedLower = String(resolvedModel || '').toLowerCase()
      return keys.some((key) => {
        const target = supportedModels[key]
        return (
          String(key).toLowerCase() === requestedLower ||
          String(key).toLowerCase() === resolvedLower ||
          String(target).toLowerCase() === resolvedLower ||
          String(target).toLowerCase() === requestedLower
        )
      })
    }
    return true
  }

  async getModelsForAccount(accountId, options = {}) {
    const account = await this.getAccount(accountId)
    if (!account) {
      throw new Error('Copilot account not found')
    }
    return this.getModelsForResolvedAccount(account, options)
  }

  async getModelsForResolvedAccount(account, options = {}) {
    const { force = false, allowStale = true } = options
    const cached = this._getParsedModelCache(account)
    if (!force && cached && !this._isModelCacheStale(cached)) {
      return { ...cached, stale: false, source: 'cache' }
    }

    try {
      return await this.refreshModelCache(account.id, account)
    } catch (error) {
      if (allowStale && cached) {
        logger.warn(`Using stale Copilot model cache for ${account.name}: ${error.message}`)
        return { ...cached, stale: true, source: 'stale-cache', error: error.message }
      }
      throw error
    }
  }

  async refreshModelCache(accountId, accountOverride = null) {
    const account = accountOverride || (await this.getAccount(accountId))
    if (!account) {
      throw new Error('Copilot account not found')
    }

    const modelsUrl = `${normalizeBaseApi(account.baseApi)}/v1/models`
    const headers = { Accept: 'application/json' }
    if (account.apiKey) {
      headers.Authorization = `Bearer ${account.apiKey}`
    }
    if (account.userAgent) {
      headers['User-Agent'] = account.userAgent
    }

    const requestOptions = {
      method: 'GET',
      url: modelsUrl,
      headers,
      timeout: 30000,
      validateStatus: () => true
    }

    const proxyAgent = this._createProxyAgent(account.proxy)
    if (proxyAgent) {
      requestOptions.httpAgent = proxyAgent
      requestOptions.httpsAgent = proxyAgent
      requestOptions.proxy = false
    }

    const response = await axios(requestOptions)
    if (response.status < 200 || response.status >= 300) {
      throw new Error(`Copilot model refresh failed with HTTP ${response.status}`)
    }

    const rawModels = Array.isArray(response.data?.data) ? response.data.data : []
    const models = rawModels.map(normalizeModelId).filter(Boolean)
    const cache = {
      fetchedAt: new Date().toISOString(),
      models,
      raw: rawModels
    }

    await this.updateAccount(account.id, {
      modelCache: cache,
      modelCacheUpdatedAt: cache.fetchedAt
    })

    return { ...cache, stale: false, source: 'refresh' }
  }

  async getPlatformTestModels() {
    const accounts = await this.getAllAccounts()
    const seen = new Set()
    const output = []

    for (const account of accounts) {
      const cache = this._getParsedModelCache(account)
      for (const model of cache?.models || []) {
        if (seen.has(model)) {
          continue
        }
        seen.add(model)
        output.push({ value: `copilot,${model}`, label: model })
      }
    }

    const aliases = ['default', 'opus', 'sonnet', 'haiku', 'gpt', 'codex']
    for (const alias of aliases) {
      const value = `copilot,${alias}`
      if (!seen.has(value)) {
        output.unshift({ value, label: `Copilot ${alias}` })
      }
    }

    return output
  }

  async markAccountRateLimited(accountId, duration = null) {
    const account = await this.getAccount(accountId)
    if (!account) {
      return { success: false, reason: 'not_found' }
    }
    if (account.disableAutoProtection === true || account.disableAutoProtection === 'true') {
      upstreamErrorHelper
        .recordErrorHistory(accountId, 'copilot', 429, 'rate_limit')
        .catch(() => {})
      return { success: true, skipped: true }
    }
    const minutes =
      Number(duration) > 0 ? Number(duration) : Number(account.rateLimitDuration) || 60
    const now = new Date()
    const resetAt = new Date(now.getTime() + minutes * 60 * 1000)
    await this.updateAccount(accountId, {
      status: 'rate_limited',
      rateLimitedAt: now.toISOString(),
      rateLimitStatus: 'active',
      rateLimitResetAt: resetAt.toISOString(),
      errorMessage: `Rate limited until ${resetAt.toISOString()}`
    })
    return { success: true, resetAt: resetAt.toISOString() }
  }

  async removeAccountRateLimit(accountId) {
    const client = redis.getClientSafe()
    const key = `${this.ACCOUNT_KEY_PREFIX}${accountId}`
    const quotaStoppedAt = await client.hget(key, 'quotaStoppedAt')
    await client.hdel(key, 'rateLimitedAt', 'rateLimitStatus', 'rateLimitResetAt')
    await client.hset(key, {
      status: quotaStoppedAt ? 'quota_exceeded' : 'active',
      errorMessage: quotaStoppedAt ? 'Account stopped due to quota exceeded' : ''
    })
    return { success: true }
  }

  async isAccountRateLimited(accountId) {
    const client = redis.getClientSafe()
    const key = `${this.ACCOUNT_KEY_PREFIX}${accountId}`
    const [rateLimitedAt, rateLimitDuration] = await client.hmget(
      key,
      'rateLimitedAt',
      'rateLimitDuration'
    )
    if (!rateLimitedAt) {
      return false
    }
    const duration = Number(rateLimitDuration) || 60
    const expiresAt = new Date(rateLimitedAt).getTime() + duration * 60 * 1000
    if (Date.now() < expiresAt) {
      return true
    }
    await this.removeAccountRateLimit(accountId)
    return false
  }

  async markAccountUnauthorized(accountId, reason = 'Copilot account unauthorized') {
    const account = await this.getAccount(accountId)
    if (!account) {
      return { success: false, reason: 'not_found' }
    }
    if (account.disableAutoProtection === true || account.disableAutoProtection === 'true') {
      upstreamErrorHelper
        .recordErrorHistory(accountId, 'copilot', 401, 'auth_error')
        .catch(() => {})
      return { success: true, skipped: true }
    }
    await this.updateAccount(accountId, {
      status: 'unauthorized',
      errorMessage: reason,
      unauthorizedAt: new Date().toISOString()
    })
    return { success: true }
  }

  async markAccountOverloaded(accountId) {
    const account = await this.getAccount(accountId)
    if (!account) {
      return { success: false, reason: 'not_found' }
    }
    if (account.disableAutoProtection === true || account.disableAutoProtection === 'true') {
      upstreamErrorHelper.recordErrorHistory(accountId, 'copilot', 529, 'overload').catch(() => {})
      return { success: true, skipped: true }
    }
    await this.updateAccount(accountId, {
      status: 'overloaded',
      overloadedAt: new Date().toISOString(),
      errorMessage: 'Account overloaded'
    })
    return { success: true }
  }

  async removeAccountOverload(accountId) {
    const client = redis.getClientSafe()
    const key = `${this.ACCOUNT_KEY_PREFIX}${accountId}`
    await client.hdel(key, 'overloadedAt')
    await client.hset(key, { status: 'active', errorMessage: '' })
    return { success: true }
  }

  async isAccountOverloaded(accountId) {
    const client = redis.getClientSafe()
    const status = await client.hget(`${this.ACCOUNT_KEY_PREFIX}${accountId}`, 'status')
    return status === 'overloaded'
  }

  async checkQuotaUsage(accountId) {
    const account = await this.getAccount(accountId)
    if (!account) {
      return false
    }
    const dailyQuota = Number(account.dailyQuota || 0)
    if (dailyQuota <= 0) {
      return false
    }
    if (account.lastResetDate !== redis.getDateStringInTimezone()) {
      await this.resetDailyUsage(accountId)
      return false
    }
    const stats = await this.getAccountUsageStats(accountId)
    const dailyUsage = stats?.dailyUsage || 0
    if (dailyUsage < dailyQuota) {
      return false
    }
    await this.updateAccount(accountId, {
      status: 'quota_exceeded',
      errorMessage: `Daily quota exceeded: $${dailyUsage.toFixed(2)} / $${dailyQuota.toFixed(2)}`,
      quotaStoppedAt: new Date().toISOString()
    })
    return true
  }

  async isAccountQuotaExceeded(accountId) {
    return this.checkQuotaUsage(accountId)
  }

  async updateAccountUsage(accountId, tokens = 0) {
    const client = redis.getClientSafe()
    await client.hset(
      `${this.ACCOUNT_KEY_PREFIX}${accountId}`,
      'lastUsedAt',
      new Date().toISOString()
    )
    return { success: true, tokens }
  }

  async updateUsageQuota(accountId, cost = 0) {
    const client = redis.getClientSafe()
    const key = `${this.ACCOUNT_KEY_PREFIX}${accountId}`
    const current = Number((await client.hget(key, 'dailyUsage')) || 0)
    await client.hset(key, 'dailyUsage', String(current + Number(cost || 0)))
    return { success: true }
  }

  async resetDailyUsage(accountId) {
    const client = redis.getClientSafe()
    await client.hset(`${this.ACCOUNT_KEY_PREFIX}${accountId}`, {
      dailyUsage: '0',
      lastResetDate: redis.getDateStringInTimezone(),
      quotaStoppedAt: ''
    })
    return { success: true }
  }

  async resetAllDailyUsage() {
    const accounts = await this.getAllAccounts()
    for (const account of accounts) {
      await this.resetDailyUsage(account.id)
    }
    return { success: true, resetCount: accounts.length }
  }

  async getAccountUsageStats(accountId) {
    const usageStats = await redis.getAccountUsageStats(accountId)
    const account = await this.getAccount(accountId)
    if (!account) {
      return null
    }
    const dailyQuota = Number(account.dailyQuota || 0)
    const currentDailyCost = usageStats?.daily?.cost || Number(account.dailyUsage || 0)
    return {
      dailyQuota,
      dailyUsage: currentDailyCost,
      remainingQuota: dailyQuota > 0 ? Math.max(0, dailyQuota - currentDailyCost) : null,
      usagePercentage: dailyQuota > 0 ? (currentDailyCost / dailyQuota) * 100 : 0,
      lastResetDate: account.lastResetDate,
      quotaResetTime: account.quotaResetTime,
      quotaStoppedAt: account.quotaStoppedAt,
      isQuotaExceeded: dailyQuota > 0 && currentDailyCost >= dailyQuota,
      fullUsageStats: usageStats
    }
  }

  async resetAccountStatus(accountId) {
    const client = redis.getClientSafe()
    const key = `${this.ACCOUNT_KEY_PREFIX}${accountId}`
    await client.hset(key, {
      status: 'active',
      errorMessage: '',
      schedulable: 'true',
      isActive: 'true'
    })
    await client.hdel(
      key,
      'rateLimitedAt',
      'rateLimitStatus',
      'rateLimitResetAt',
      'unauthorizedAt',
      'overloadedAt',
      'quotaStoppedAt'
    )
    await upstreamErrorHelper.clearTempUnavailable(accountId, 'copilot').catch(() => {})
    return { success: true, accountId }
  }

  isSubscriptionExpired(account) {
    if (!account?.subscriptionExpiresAt) {
      return false
    }
    return new Date(account.subscriptionExpiresAt) <= new Date()
  }

  _sanitizeAccountData(accountData) {
    const supportedModels = normalizeSupportedModels(accountData.supportedModels)
    const modelCache = this._parseModelCache(accountData.modelCache)
    return {
      id: accountData.id,
      platform: accountData.platform || 'copilot',
      name: accountData.name,
      description: accountData.description || '',
      baseApi: accountData.baseApi || '',
      apiKey: accountData.apiKey ? '***' : '',
      priority: Number(accountData.priority) || 50,
      supportedModels,
      userAgent: accountData.userAgent || '',
      proxy: accountData.proxy ? safeJsonParse(accountData.proxy, null) : null,
      isActive: accountData.isActive !== 'false',
      accountType: accountData.accountType || 'shared',
      schedulable: accountData.schedulable !== 'false',
      rateLimitDuration: Number(accountData.rateLimitDuration) || 60,
      dailyQuota: Number(accountData.dailyQuota || 0),
      dailyUsage: Number(accountData.dailyUsage || 0),
      lastResetDate: accountData.lastResetDate || '',
      quotaResetTime: accountData.quotaResetTime || '00:00',
      quotaStoppedAt: accountData.quotaStoppedAt || null,
      disableAutoProtection: accountData.disableAutoProtection === 'true',
      modelCache,
      modelCacheUpdatedAt: accountData.modelCacheUpdatedAt || modelCache?.fetchedAt || '',
      dynamicModels: modelCache?.models || [],
      createdAt: accountData.createdAt || '',
      lastUsedAt: accountData.lastUsedAt || '',
      status: accountData.status || 'active',
      errorMessage: accountData.errorMessage || '',
      expiresAt: accountData.subscriptionExpiresAt || null,
      subscriptionExpiresAt: accountData.subscriptionExpiresAt || null
    }
  }

  _parseModelCache(rawValue) {
    const parsed = safeJsonParse(rawValue, null)
    if (!parsed || typeof parsed !== 'object') {
      return null
    }
    const models = Array.isArray(parsed.models)
      ? parsed.models.map(normalizeModelId).filter(Boolean)
      : []
    return {
      fetchedAt: parsed.fetchedAt || parsed.updatedAt || '',
      models,
      raw: Array.isArray(parsed.raw) ? parsed.raw : []
    }
  }

  _getParsedModelCache(account) {
    if (!account) {
      return null
    }
    if (account.modelCache && typeof account.modelCache === 'object') {
      return this._parseModelCache(account.modelCache)
    }
    return this._parseModelCache(account.modelCache)
  }

  _isModelCacheStale(cache) {
    if (!cache?.fetchedAt) {
      return true
    }
    const fetchedAt = Date.parse(cache.fetchedAt)
    if (!Number.isFinite(fetchedAt)) {
      return true
    }
    return Date.now() - fetchedAt > this.MODEL_CACHE_TTL_MS
  }

  _encryptSensitiveData(data) {
    return this._encryptor.encrypt(data)
  }

  _decryptSensitiveData(encryptedData) {
    return this._encryptor.decrypt(encryptedData)
  }

  _createProxyAgent(proxy) {
    return ProxyHelper.createProxyAgent(proxy)
  }
}

module.exports = new CopilotAccountService()
