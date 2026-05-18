const express = require('express')
const axios = require('axios')
const copilotAccountService = require('../../services/account/copilotAccountService')
const copilotModelResolver = require('../../services/copilotModelResolver')
const accountGroupService = require('../../services/accountGroupService')
const apiKeyService = require('../../services/apiKeyService')
const redis = require('../../models/redis')
const { authenticateAdmin } = require('../../middleware/auth')
const logger = require('../../utils/logger')
const webhookNotifier = require('../../utils/webhookNotifier')
const { extractErrorMessage } = require('../../utils/testPayloadHelper')

const router = express.Router()

function normalizeBaseApi(baseApi = '') {
  const value = String(baseApi || '').trim()
  return value.endsWith('/') ? value.slice(0, -1) : value
}

function buildTargetUrl(baseApi, targetPath) {
  const normalizedBaseApi = normalizeBaseApi(baseApi)
  let path = targetPath || '/v1/messages'
  if (!path.startsWith('/')) {
    path = `/${path}`
  }
  if (normalizedBaseApi.endsWith('/v1') && path.startsWith('/v1/')) {
    path = path.slice(3)
  }
  return `${normalizedBaseApi}${path}`
}

router.get('/', authenticateAdmin, async (req, res) => {
  try {
    const { platform, groupId } = req.query
    let accounts = await copilotAccountService.getAllAccounts()

    if (platform && platform !== 'all' && platform !== 'copilot') {
      accounts = []
    }

    if (groupId && groupId !== 'all') {
      if (groupId === 'ungrouped') {
        const filtered = []
        for (const account of accounts) {
          const groups = await accountGroupService.getAccountGroups(account.id)
          if (!groups || groups.length === 0) {
            filtered.push(account)
          }
        }
        accounts = filtered
      } else {
        const groupMembers = await accountGroupService.getGroupMembers(groupId)
        accounts = accounts.filter((account) => groupMembers.includes(account.id))
      }
    }

    const accountsWithStats = await Promise.all(
      accounts.map(async (account) => {
        const usageStats = await redis.getAccountUsageStats(account.id).catch(() => null)
        const groupInfos = await accountGroupService.getAccountGroups(account.id).catch(() => [])
        return {
          ...account,
          groupInfos,
          usage: {
            daily: usageStats?.daily || { tokens: 0, requests: 0, allTokens: 0 },
            total: usageStats?.total || { tokens: 0, requests: 0, allTokens: 0 },
            averages: usageStats?.averages || { rpm: 0, tpm: 0 }
          }
        }
      })
    )

    return res.json({ success: true, data: accountsWithStats })
  } catch (error) {
    logger.error('Failed to get Copilot accounts:', error)
    return res.status(500).json({ error: 'Failed to get Copilot accounts', message: error.message })
  }
})

router.post('/', authenticateAdmin, async (req, res) => {
  try {
    const {
      name,
      description,
      baseApi,
      apiKey,
      priority,
      supportedModels,
      userAgent,
      proxy,
      accountType,
      groupId,
      rateLimitDuration,
      dailyQuota,
      quotaResetTime,
      disableAutoProtection
    } = req.body

    if (!name || !baseApi) {
      return res.status(400).json({ error: 'Name and Base API are required' })
    }
    if (priority !== undefined && (priority < 1 || priority > 100)) {
      return res.status(400).json({ error: 'Priority must be between 1 and 100' })
    }
    if (accountType && !['shared', 'dedicated', 'group'].includes(accountType)) {
      return res
        .status(400)
        .json({ error: 'Invalid account type. Must be "shared", "dedicated" or "group"' })
    }
    if (accountType === 'group' && !groupId) {
      return res.status(400).json({ error: 'Group ID is required for group type accounts' })
    }

    const account = await copilotAccountService.createAccount({
      name,
      description,
      baseApi,
      apiKey,
      priority: priority || 50,
      supportedModels: supportedModels || [],
      userAgent,
      proxy,
      accountType: accountType || 'shared',
      rateLimitDuration:
        rateLimitDuration !== undefined && rateLimitDuration !== null ? rateLimitDuration : 60,
      dailyQuota: dailyQuota || 0,
      quotaResetTime: quotaResetTime || '00:00',
      disableAutoProtection: disableAutoProtection === true
    })

    if (accountType === 'group' && groupId) {
      await accountGroupService.addAccountToGroup(account.id, groupId, 'copilot')
    }

    return res.json({ success: true, data: account })
  } catch (error) {
    logger.error('Failed to create Copilot account:', error)
    return res
      .status(500)
      .json({ error: 'Failed to create Copilot account', message: error.message })
  }
})

router.put('/:accountId', authenticateAdmin, async (req, res) => {
  try {
    const { accountId } = req.params
    const updates = req.body || {}

    if (updates.priority !== undefined && (updates.priority < 1 || updates.priority > 100)) {
      return res.status(400).json({ error: 'Priority must be between 1 and 100' })
    }
    if (updates.accountType && !['shared', 'dedicated', 'group'].includes(updates.accountType)) {
      return res
        .status(400)
        .json({ error: 'Invalid account type. Must be "shared", "dedicated" or "group"' })
    }

    const currentAccount = await copilotAccountService.getAccount(accountId)
    if (!currentAccount) {
      return res.status(404).json({ error: 'Account not found' })
    }

    if (updates.accountType !== undefined) {
      if (currentAccount.accountType === 'group') {
        const oldGroups = await accountGroupService.getAccountGroups(accountId)
        for (const oldGroup of oldGroups) {
          await accountGroupService.removeAccountFromGroup(accountId, oldGroup.id)
        }
      }
      if (updates.accountType === 'group') {
        if (Array.isArray(updates.groupIds)) {
          if (updates.groupIds.length > 0) {
            await accountGroupService.setAccountGroups(accountId, updates.groupIds, 'copilot')
          } else {
            await accountGroupService.removeAccountFromAllGroups(accountId)
          }
        } else if (updates.groupId) {
          await accountGroupService.addAccountToGroup(accountId, updates.groupId, 'copilot')
        }
      }
    }

    const account = await copilotAccountService.updateAccount(accountId, updates)
    return res.json({
      success: true,
      data: account,
      message: 'Copilot account updated successfully'
    })
  } catch (error) {
    logger.error('Failed to update Copilot account:', error)
    return res
      .status(500)
      .json({ error: 'Failed to update Copilot account', message: error.message })
  }
})

router.delete('/:accountId', authenticateAdmin, async (req, res) => {
  try {
    const { accountId } = req.params
    const unboundCount = await apiKeyService.unbindAccountFromAllKeys(accountId, 'copilot')
    const account = await copilotAccountService.getAccount(accountId)
    if (account && account.accountType === 'group') {
      const groups = await accountGroupService.getAccountGroups(accountId)
      for (const group of groups) {
        await accountGroupService.removeAccountFromGroup(accountId, group.id)
      }
    }
    await copilotAccountService.deleteAccount(accountId)
    return res.json({ success: true, message: 'Copilot账号已成功删除', unboundKeys: unboundCount })
  } catch (error) {
    logger.error('Failed to delete Copilot account:', error)
    return res
      .status(500)
      .json({ error: 'Failed to delete Copilot account', message: error.message })
  }
})

router.put('/:accountId/toggle', authenticateAdmin, async (req, res) => {
  try {
    const { accountId } = req.params
    const account = await copilotAccountService.getAccount(accountId)
    if (!account) {
      return res.status(404).json({ error: 'Account not found' })
    }
    const isActive = !account.isActive
    await copilotAccountService.updateAccount(accountId, { isActive })
    return res.json({ success: true, isActive })
  } catch (error) {
    return res
      .status(500)
      .json({ error: 'Failed to toggle account status', message: error.message })
  }
})

router.put('/:accountId/toggle-schedulable', authenticateAdmin, async (req, res) => {
  try {
    const { accountId } = req.params
    const account = await copilotAccountService.getAccount(accountId)
    if (!account) {
      return res.status(404).json({ error: 'Account not found' })
    }
    const schedulable = !account.schedulable
    await copilotAccountService.updateAccount(accountId, { schedulable })

    if (!schedulable) {
      await webhookNotifier.sendAccountAnomalyNotification({
        accountId: account.id,
        accountName: account.name || 'Copilot Account',
        platform: 'copilot',
        status: 'disabled',
        errorCode: 'COPILOT_MANUALLY_DISABLED',
        reason: '账号已被管理员手动禁用调度',
        timestamp: new Date().toISOString()
      })
    }

    return res.json({ success: true, schedulable })
  } catch (error) {
    return res
      .status(500)
      .json({ error: 'Failed to toggle schedulable status', message: error.message })
  }
})

router.get('/:accountId/usage', authenticateAdmin, async (req, res) => {
  try {
    const usageStats = await copilotAccountService.getAccountUsageStats(req.params.accountId)
    if (!usageStats) {
      return res.status(404).json({ error: 'Account not found' })
    }
    return res.json(usageStats)
  } catch (error) {
    return res.status(500).json({ error: 'Failed to get usage stats', message: error.message })
  }
})

router.post('/:accountId/reset-usage', authenticateAdmin, async (req, res) => {
  try {
    await copilotAccountService.resetDailyUsage(req.params.accountId)
    return res.json({ success: true, message: 'Daily usage reset successfully' })
  } catch (error) {
    return res.status(500).json({ error: 'Failed to reset daily usage', message: error.message })
  }
})

router.post('/:accountId/reset-status', authenticateAdmin, async (req, res) => {
  try {
    const result = await copilotAccountService.resetAccountStatus(req.params.accountId)
    return res.json({ success: true, data: result })
  } catch (error) {
    return res.status(500).json({ error: 'Failed to reset status', message: error.message })
  }
})

router.post('/reset-all-usage', authenticateAdmin, async (req, res) => {
  try {
    const result = await copilotAccountService.resetAllDailyUsage()
    return res.json({ success: true, ...result })
  } catch (error) {
    return res
      .status(500)
      .json({ error: 'Failed to reset all daily usage', message: error.message })
  }
})

router.post('/:accountId/refresh-models', authenticateAdmin, async (req, res) => {
  try {
    const result = await copilotAccountService.refreshModelCache(req.params.accountId)
    return res.json({ success: true, data: result })
  } catch (error) {
    logger.error('Failed to refresh Copilot models:', error)
    return res.status(500).json({ error: 'Failed to refresh models', message: error.message })
  }
})

router.post('/:accountId/test', authenticateAdmin, async (req, res) => {
  const { accountId } = req.params
  const { model = 'copilot,default', protocol = 'anthropic' } = req.body || {}
  const startTime = Date.now()

  try {
    const account = await copilotAccountService.getAccount(accountId)
    if (!account) {
      return res.status(404).json({ error: 'Account not found' })
    }

    const resolution = await copilotModelResolver.resolve(account, model)
    const headers = { 'Content-Type': 'application/json' }
    if (account.apiKey) {
      headers.Authorization = `Bearer ${account.apiKey}`
    }
    if (account.userAgent) {
      headers['User-Agent'] = account.userAgent
    }

    const targetPath = protocol === 'responses' ? '/v1/responses' : '/v1/messages'
    const targetUrl = buildTargetUrl(account.baseApi, targetPath)
    const payload =
      protocol === 'responses'
        ? {
            model: resolution.resolvedModel,
            input: 'Say "Hello" in one word.',
            stream: false
          }
        : {
            model: resolution.resolvedModel,
            max_tokens: 100,
            messages: [{ role: 'user', content: 'Say "Hello" in one word.' }]
          }

    const requestConfig = {
      headers,
      timeout: 30000,
      validateStatus: () => true
    }
    const proxyAgent = copilotAccountService._createProxyAgent(account.proxy)
    if (proxyAgent) {
      requestConfig.httpAgent = proxyAgent
      requestConfig.httpsAgent = proxyAgent
      requestConfig.proxy = false
    }

    const response = await axios.post(targetUrl, payload, requestConfig)
    const latency = Date.now() - startTime
    if (response.status < 200 || response.status >= 300) {
      return res.status(response.status).json({
        success: false,
        error: 'Test failed',
        message: extractErrorMessage(response.data, `HTTP ${response.status}`),
        latency
      })
    }

    const responseText =
      response.data?.content?.[0]?.text ||
      response.data?.output_text ||
      response.data?.output?.[0]?.content?.[0]?.text ||
      ''

    return res.json({
      success: true,
      data: {
        accountId,
        accountName: account.name,
        requestedModel: model,
        resolvedModel: resolution.resolvedModel,
        protocol,
        latency,
        responseText: String(responseText).slice(0, 200)
      }
    })
  } catch (error) {
    const latency = Date.now() - startTime
    logger.error(`Copilot account test failed: ${accountId}`, error)
    return res.status(500).json({
      success: false,
      error: 'Test failed',
      message: extractErrorMessage(error.response?.data, error.message),
      latency
    })
  }
})

module.exports = router
