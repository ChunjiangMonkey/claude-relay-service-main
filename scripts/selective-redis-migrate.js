#!/usr/bin/env node

const fs = require('fs').promises
const crypto = require('crypto')
const redis = require('../src/models/redis')
const config = require('../config/config')

const FORMAT = 'claude-relay-selective-migration'
const VERSION = 1
const OPENAI_ACCOUNT_PREFIX = 'openai:account:'
const OPENAI_ACCOUNT_INDEX = 'openai:account:index'
const SHARED_OPENAI_ACCOUNTS = 'shared_openai_accounts'
const API_KEY_HASH_MAP = 'apikey:hash_map'
const ACCOUNT_GROUPS_KEY = 'account_groups'
const ACCOUNT_GROUP_PREFIX = 'account_group:'
const ACCOUNT_GROUP_MEMBERS_PREFIX = 'account_group_members:'
const ACCOUNT_GROUPS_REVERSE_PREFIX = 'account_groups_reverse:'

const ACCOUNT_DEFINITIONS = [
  {
    kind: 'claude',
    label: 'Claude',
    prefix: 'claude:account:',
    index: 'claude:account:index',
    storage: 'hash',
    groupPlatform: 'claude'
  },
  {
    kind: 'claude-console',
    label: 'Claude Console',
    prefix: 'claude_console_account:',
    index: 'claude_console_account:index',
    storage: 'hash',
    sharedSet: 'shared_claude_console_accounts',
    groupPlatform: 'claude'
  },
  {
    kind: 'openai',
    label: 'OpenAI',
    prefix: OPENAI_ACCOUNT_PREFIX,
    index: OPENAI_ACCOUNT_INDEX,
    storage: 'hash',
    sharedSet: SHARED_OPENAI_ACCOUNTS,
    groupPlatform: 'openai'
  },
  {
    kind: 'openai-responses',
    label: 'OpenAI Responses',
    prefix: 'openai_responses_account:',
    index: 'openai_responses_account:index',
    storage: 'hash',
    sharedSet: 'shared_openai_responses_accounts',
    groupPlatform: 'openai'
  },
  {
    kind: 'azure-openai',
    label: 'Azure OpenAI',
    prefix: 'azure_openai:account:',
    index: 'azure_openai:account:index',
    storage: 'hash',
    sharedSet: 'shared_azure_openai_accounts',
    groupPlatform: 'openai'
  },
  {
    kind: 'gemini',
    label: 'Gemini',
    prefix: 'gemini_account:',
    index: 'gemini_account:index',
    storage: 'hash',
    sharedSet: 'shared_gemini_accounts',
    groupPlatform: 'gemini'
  },
  {
    kind: 'gemini-api',
    label: 'Gemini API',
    prefix: 'gemini_api_account:',
    index: 'gemini_api_account:index',
    storage: 'hash',
    sharedSet: 'shared_gemini_api_accounts',
    groupPlatform: 'gemini'
  },
  {
    kind: 'bedrock',
    label: 'Bedrock',
    prefix: 'bedrock_account:',
    index: 'bedrock_account:index',
    storage: 'string',
    groupPlatform: 'openai'
  },
  {
    kind: 'droid',
    label: 'Droid',
    prefix: 'droid:account:',
    index: 'droid:account:index',
    storage: 'hash',
    groupPlatform: 'droid'
  },
  {
    kind: 'ccr',
    label: 'CCR',
    prefix: 'ccr_account:',
    index: 'ccr_account:index',
    storage: 'hash',
    sharedSet: 'shared_ccr_accounts',
    groupPlatform: 'claude'
  }
]

const ACCOUNT_DEFINITION_BY_KIND = new Map(
  ACCOUNT_DEFINITIONS.map((definition) => [definition.kind, definition])
)

function parseArgs(argv) {
  const [command, ...rest] = argv
  const params = {}

  for (const arg of rest) {
    if (!arg.startsWith('--')) {
      continue
    }

    const eqIndex = arg.indexOf('=')
    if (eqIndex === -1) {
      params[arg.slice(2)] = true
      continue
    }

    params[arg.slice(2, eqIndex)] = arg.slice(eqIndex + 1)
  }

  return { command, params }
}

function normalizeText(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
}

function parseListParam(value) {
  if (!value) {
    return []
  }

  if (Array.isArray(value)) {
    return value.map((item) => String(item).trim()).filter(Boolean)
  }

  return String(value)
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean)
}

function safeParseJsonObject(value) {
  if (!value || typeof value !== 'string') {
    return null
  }

  try {
    const parsed = JSON.parse(value)
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : null
  } catch {
    return null
  }
}

function uniqueSorted(values) {
  return [...new Set(values.filter(Boolean))].sort()
}

function parseJsonArray(value) {
  if (Array.isArray(value)) {
    return value
  }

  if (!value || typeof value !== 'string') {
    return []
  }

  try {
    const parsed = JSON.parse(value)
    return Array.isArray(parsed) ? parsed : []
  } catch {
    return value
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean)
  }
}

function parseTags(value) {
  return parseJsonArray(value)
    .map((tag) => String(tag).trim())
    .filter(Boolean)
}

function hasTag(apiKeyData, tag) {
  const normalizedTag = normalizeText(tag)
  return parseTags(apiKeyData.tags).some((item) => normalizeText(item) === normalizedTag)
}

function isApiKeyDataKey(key) {
  return (
    key.startsWith('apikey:') &&
    key !== API_KEY_HASH_MAP &&
    !key.startsWith('apikey:idx:') &&
    !key.startsWith('apikey:set:') &&
    !key.startsWith('apikey:tags:') &&
    !key.startsWith('apikey:tag:') &&
    !key.startsWith('apikey:index:') &&
    key.split(':').length === 2
  )
}

function globToRegex(pattern) {
  const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*')
  return new RegExp(`^${escaped}$`)
}

async function scanKeys(client, pattern) {
  const keys = []
  let cursor = '0'

  do {
    const [nextCursor, batch] = await client.scan(cursor, 'MATCH', pattern, 'COUNT', 200)
    cursor = nextCursor
    keys.push(...batch)
  } while (cursor !== '0')

  return keys
}

async function readHashRecords(client, keys, prefix) {
  const records = []

  for (const key of keys.sort()) {
    if (typeof client.type === 'function') {
      const type = await client.type(key)
      if (type !== 'hash') {
        continue
      }
    }

    const data = await client.hgetall(key)
    if (!data || Object.keys(data).length === 0) {
      continue
    }

    const ttl = typeof client.ttl === 'function' ? await client.ttl(key) : -1
    records.push({
      id: key.slice(prefix.length),
      key,
      ttl,
      data: { ...data, id: key.slice(prefix.length) }
    })
  }

  return records
}

async function readStringRecords(client, keys, prefix) {
  const records = []

  for (const key of keys.sort()) {
    if (typeof client.type === 'function') {
      const type = await client.type(key)
      if (type !== 'string') {
        continue
      }
    }

    const value = await client.get(key)
    if (!value) {
      continue
    }

    const parsed = safeParseJsonObject(value)
    const id = key.slice(prefix.length)
    const ttl = typeof client.ttl === 'function' ? await client.ttl(key) : -1
    records.push({
      id,
      key,
      ttl,
      value,
      data: parsed ? { ...parsed, id: parsed.id || id } : { id }
    })
  }

  return records
}

async function loadApiKeys(client) {
  const keys = (await scanKeys(client, 'apikey:*')).filter(isApiKeyDataKey)
  return readHashRecords(client, keys, 'apikey:')
}

async function loadOpenAIAccounts(client) {
  const keys = await scanKeys(client, `${OPENAI_ACCOUNT_PREFIX}*`)
  return readHashRecords(client, keys, OPENAI_ACCOUNT_PREFIX)
}

async function loadAccountRecords(client, definition) {
  const keys = await scanKeys(client, `${definition.prefix}*`)
  const records =
    definition.storage === 'string'
      ? await readStringRecords(client, keys, definition.prefix)
      : await readHashRecords(client, keys, definition.prefix)

  return records.map((record) => ({
    ...record,
    kind: definition.kind,
    storage: definition.storage
  }))
}

async function loadAllAccountRecords(client) {
  const accountGroups = await Promise.all(
    ACCOUNT_DEFINITIONS.map((definition) => loadAccountRecords(client, definition))
  )
  return accountGroups.flat()
}

function encryptionKeyFingerprint(runtimeConfig = config) {
  return crypto
    .createHash('sha256')
    .update(runtimeConfig.security?.encryptionKey || '')
    .digest('hex')
    .slice(0, 16)
}

function apiKeyPrefix(runtimeConfig = config) {
  return runtimeConfig.security?.apiKeyPrefix || 'cr_'
}

function normalizeInspectInvocation(options = {}, runtimeConfig = config) {
  if (options?.security && runtimeConfig === config) {
    return { options: {}, runtimeConfig: options }
  }

  return { options: options || {}, runtimeConfig: runtimeConfig || config }
}

function accountDefinitionForKind(kind) {
  const definition = ACCOUNT_DEFINITION_BY_KIND.get(kind)
  if (!definition) {
    throw new Error(`Unsupported account kind in migration package: ${kind}`)
  }
  return definition
}

function accountRecordData(record) {
  return record.data || safeParseJsonObject(record.value) || {}
}

function accountRecordName(record) {
  const data = accountRecordData(record)
  return (
    data.name ||
    data.displayName ||
    data.email ||
    data.username ||
    data.description ||
    data.id ||
    record.id ||
    ''
  )
}

function accountRecordRedisKey(record) {
  const definition = accountDefinitionForKind(record.kind || 'openai')
  return `${definition.prefix}${record.id}`
}

function accountRecordIsShared(record) {
  const data = accountRecordData(record)
  return data.accountType === true || data.accountType === 'shared'
}

function accountIdentity(kind, id) {
  return `${kind}:${id}`
}

function decryptWithSalt(value, salt, runtimeConfig = config) {
  if (!value || typeof value !== 'string' || !value.includes(':')) {
    return value || ''
  }

  try {
    const [ivHex, encryptedHex] = value.split(':')
    const key = crypto.scryptSync(runtimeConfig.security.encryptionKey, salt, 32)
    const iv = Buffer.from(ivHex, 'hex')
    const decipher = crypto.createDecipheriv('aes-256-cbc', key, iv)
    let decrypted = decipher.update(encryptedHex, 'hex', 'utf8')
    decrypted += decipher.final('utf8')
    return decrypted
  } catch {
    return value
  }
}

function openAIAccountSearchText(accountRecord, runtimeConfig = config) {
  const data = accountRecord.data || {}
  const decryptedEmail = decryptWithSalt(data.email, 'openai-account-salt', runtimeConfig)
  return [
    data.id,
    data.name,
    data.description,
    data.email,
    decryptedEmail,
    data.accountId,
    data.chatgptUserId,
    data.organizationId,
    data.organizationTitle,
    data.planType
  ]
    .filter(Boolean)
    .join(' ')
}

function accountMatchesKeyword(accountRecord, keyword, runtimeConfig = config) {
  if (!keyword) {
    return false
  }

  return normalizeText(openAIAccountSearchText(accountRecord, runtimeConfig)).includes(
    normalizeText(keyword)
  )
}

function directOpenAIAccountId(binding) {
  if (!binding || typeof binding !== 'string') {
    return null
  }

  if (binding.startsWith('responses:') || binding.startsWith('group:')) {
    return null
  }

  return binding
}

function parseGroupBinding(binding) {
  if (!binding || typeof binding !== 'string' || !binding.startsWith('group:')) {
    return null
  }
  return binding.slice('group:'.length)
}

function apiKeyAccountBindings(apiKeyData = {}) {
  const bindings = []
  const pushDirect = (field, kind, rawId) => {
    if (!rawId || typeof rawId !== 'string') {
      return
    }

    const groupId = parseGroupBinding(rawId)
    if (groupId) {
      bindings.push({ field, groupId, raw: rawId })
      return
    }

    bindings.push({ field, kind, id: rawId, raw: rawId })
  }

  pushDirect('claudeAccountId', 'claude', apiKeyData.claudeAccountId)
  pushDirect('claudeConsoleAccountId', 'claude-console', apiKeyData.claudeConsoleAccountId)

  if (apiKeyData.geminiAccountId) {
    if (apiKeyData.geminiAccountId.startsWith('api:')) {
      pushDirect('geminiAccountId', 'gemini-api', apiKeyData.geminiAccountId.slice('api:'.length))
    } else {
      pushDirect('geminiAccountId', 'gemini', apiKeyData.geminiAccountId)
    }
  }

  if (apiKeyData.openaiAccountId) {
    if (apiKeyData.openaiAccountId.startsWith('responses:')) {
      pushDirect(
        'openaiAccountId',
        'openai-responses',
        apiKeyData.openaiAccountId.slice('responses:'.length)
      )
    } else {
      pushDirect('openaiAccountId', 'openai', apiKeyData.openaiAccountId)
    }
  }

  pushDirect('azureOpenaiAccountId', 'azure-openai', apiKeyData.azureOpenaiAccountId)
  pushDirect('bedrockAccountId', 'bedrock', apiKeyData.bedrockAccountId)
  pushDirect('droidAccountId', 'droid', apiKeyData.droidAccountId)
  pushDirect('ccrAccountId', 'ccr', apiKeyData.ccrAccountId)

  return bindings
}

function unsupportedBindingWarnings(apiKeyRecord) {
  const data = apiKeyRecord.data || {}
  const warnings = []
  const unsupportedFields = [
    'claudeAccountId',
    'claudeConsoleAccountId',
    'geminiAccountId',
    'azureOpenaiAccountId',
    'bedrockAccountId',
    'droidAccountId'
  ]

  for (const field of unsupportedFields) {
    if (data[field]) {
      warnings.push(
        `API Key ${data.name || data.id} (${data.id}) has unsupported binding ${field}=${data[field]}`
      )
    }
  }

  if (data.openaiAccountId && data.openaiAccountId.startsWith('responses:')) {
    warnings.push(
      `API Key ${data.name || data.id} (${data.id}) binds OpenAI Responses account ${data.openaiAccountId}; first version does not migrate it`
    )
  }

  if (data.openaiAccountId && data.openaiAccountId.startsWith('group:')) {
    warnings.push(
      `API Key ${data.name || data.id} (${data.id}) binds OpenAI group ${data.openaiAccountId}; first version does not migrate groups`
    )
  }

  return warnings
}

async function buildSourceSelection(client, options, runtimeConfig = config) {
  if (!options.tag) {
    throw new Error('Missing required --tag')
  }

  const [allApiKeys, allOpenAIAccounts] = await Promise.all([
    loadApiKeys(client),
    loadOpenAIAccounts(client)
  ])
  const apiKeys = allApiKeys.filter((record) => hasTag(record.data, options.tag))
  const accountById = new Map(allOpenAIAccounts.map((record) => [record.id, record]))
  const selectedAccountIds = new Set()
  const accountReasons = new Map()
  const warnings = []

  for (const account of allOpenAIAccounts) {
    if (accountMatchesKeyword(account, options.openaiName, runtimeConfig)) {
      selectedAccountIds.add(account.id)
      accountReasons.set(account.id, 'keyword')
    }
  }

  for (const apiKey of apiKeys) {
    for (const warning of unsupportedBindingWarnings(apiKey)) {
      warnings.push(warning)
    }

    const boundAccountId = directOpenAIAccountId(apiKey.data.openaiAccountId)
    if (!boundAccountId) {
      continue
    }

    if (!accountById.has(boundAccountId)) {
      warnings.push(
        `API Key ${apiKey.data.name || apiKey.id} (${apiKey.id}) binds missing OpenAI account ${boundAccountId}`
      )
      continue
    }

    selectedAccountIds.add(boundAccountId)
    if (!accountReasons.has(boundAccountId)) {
      accountReasons.set(boundAccountId, 'bound_by_api_key')
    }
  }

  const openAIAccounts = [...selectedAccountIds]
    .sort()
    .map((accountId) => ({
      ...accountById.get(accountId),
      reason: accountReasons.get(accountId) || 'selected'
    }))
    .filter(Boolean)

  return {
    format: FORMAT,
    version: VERSION,
    createdAt: new Date().toISOString(),
    source: {
      tag: options.tag,
      openaiName: options.openaiName || '',
      encryptionKeyFingerprint: encryptionKeyFingerprint(runtimeConfig),
      apiKeyPrefix: apiKeyPrefix(runtimeConfig)
    },
    records: {
      apiKeys,
      openAIAccounts
    },
    warnings
  }
}

function isApiKeyExcludedForRemaining(apiKeyRecord, options) {
  const excludeTags = parseListParam(options.excludeTag)
  return excludeTags.some((tag) => hasTag(apiKeyRecord.data, tag))
}

function isAccountExcludedForRemaining(accountRecord, options, runtimeConfig = config) {
  const excludeOpenAINames = parseListParam(options.excludeOpenAIName)
  if (accountRecord.kind !== 'openai' || excludeOpenAINames.length === 0) {
    return false
  }

  return excludeOpenAINames.some((keyword) =>
    accountMatchesKeyword(accountRecord, keyword, runtimeConfig)
  )
}

async function loadSelectedAccountGroups(client, selectedGroupIds, selectedAccountIds, warnings) {
  const groups = []
  const groupIds =
    typeof client.smembers === 'function' ? await client.smembers(ACCOUNT_GROUPS_KEY) : []

  for (const groupId of groupIds.sort()) {
    const key = `${ACCOUNT_GROUP_PREFIX}${groupId}`
    if (typeof client.type === 'function') {
      const type = await client.type(key)
      if (type !== 'hash') {
        continue
      }
    }

    const data = await client.hgetall(key)
    if (!data || Object.keys(data).length === 0) {
      if (selectedGroupIds.has(groupId)) {
        warnings.push(`API Key binds missing account group ${groupId}`)
      }
      continue
    }

    const members =
      typeof client.smembers === 'function'
        ? await client.smembers(`${ACCOUNT_GROUP_MEMBERS_PREFIX}${groupId}`)
        : []
    const includeGroup =
      selectedGroupIds.has(groupId) || members.some((memberId) => selectedAccountIds.has(memberId))

    if (!includeGroup) {
      continue
    }

    const ttl = typeof client.ttl === 'function' ? await client.ttl(key) : -1
    groups.push({
      id: groupId,
      key,
      ttl,
      data: { ...data, id: data.id || groupId },
      members: uniqueSorted(members),
      reason: selectedGroupIds.has(groupId) ? 'bound_by_api_key' : 'contains_selected_account'
    })
  }

  for (const groupId of selectedGroupIds) {
    if (!groups.some((group) => group.id === groupId)) {
      warnings.push(`API Key binds missing account group ${groupId}`)
    }
  }

  return groups
}

async function loadAllAccountGroupRecords(client) {
  const groups = []
  const groupIds =
    typeof client.smembers === 'function' ? await client.smembers(ACCOUNT_GROUPS_KEY) : []

  for (const groupId of groupIds.sort()) {
    const key = `${ACCOUNT_GROUP_PREFIX}${groupId}`
    if (typeof client.type === 'function') {
      const type = await client.type(key)
      if (type !== 'hash') {
        continue
      }
    }

    const data = await client.hgetall(key)
    if (!data || Object.keys(data).length === 0) {
      continue
    }

    const members =
      typeof client.smembers === 'function'
        ? await client.smembers(`${ACCOUNT_GROUP_MEMBERS_PREFIX}${groupId}`)
        : []
    const ttl = typeof client.ttl === 'function' ? await client.ttl(key) : -1
    groups.push({
      id: groupId,
      key,
      ttl,
      data: { ...data, id: data.id || groupId },
      members: uniqueSorted(members)
    })
  }

  return groups
}

async function buildRemainingSourceSelection(client, options = {}, runtimeConfig = config) {
  const [allApiKeys, allAccounts] = await Promise.all([
    loadApiKeys(client),
    loadAllAccountRecords(client)
  ])
  const apiKeys = allApiKeys.filter((record) => !isApiKeyExcludedForRemaining(record, options))
  const excludedApiKeys = allApiKeys.filter((record) =>
    isApiKeyExcludedForRemaining(record, options)
  )
  const accounts = allAccounts.filter(
    (record) => !isAccountExcludedForRemaining(record, options, runtimeConfig)
  )
  const excludedAccounts = allAccounts.filter((record) =>
    isAccountExcludedForRemaining(record, options, runtimeConfig)
  )
  const allAccountByIdentity = new Map(
    allAccounts.map((record) => [accountIdentity(record.kind, record.id), record])
  )
  const selectedAccountByIdentity = new Map(
    accounts.map((record) => [accountIdentity(record.kind, record.id), record])
  )
  const selectedAccountIds = new Set(accounts.map((record) => record.id))
  const selectedGroupIds = new Set()
  const warnings = []

  for (const apiKey of apiKeys) {
    for (const binding of apiKeyAccountBindings(apiKey.data)) {
      if (binding.groupId) {
        selectedGroupIds.add(binding.groupId)
        continue
      }

      const identity = accountIdentity(binding.kind, binding.id)
      if (!allAccountByIdentity.has(identity)) {
        warnings.push(
          `API Key ${apiKey.data.name || apiKey.id} (${apiKey.id}) binds missing ${binding.kind} account ${binding.id}`
        )
      } else if (!selectedAccountByIdentity.has(identity)) {
        warnings.push(
          `API Key ${apiKey.data.name || apiKey.id} (${apiKey.id}) binds excluded ${binding.kind} account ${binding.id}; target must already contain it`
        )
      }
    }
  }

  const accountGroups = await loadSelectedAccountGroups(
    client,
    selectedGroupIds,
    selectedAccountIds,
    warnings
  )

  return {
    format: FORMAT,
    version: VERSION,
    createdAt: new Date().toISOString(),
    source: {
      mode: 'remaining',
      excludeTags: parseListParam(options.excludeTag),
      excludeOpenAINames: parseListParam(options.excludeOpenAIName),
      excludedApiKeys: excludedApiKeys.length,
      excludedAccounts: excludedAccounts.length,
      encryptionKeyFingerprint: encryptionKeyFingerprint(runtimeConfig),
      apiKeyPrefix: apiKeyPrefix(runtimeConfig)
    },
    records: {
      apiKeys,
      openAIAccounts: [],
      accounts,
      accountGroups
    },
    warnings
  }
}

function migrationRecords(payload) {
  const records = payload.records || {}
  const apiKeys = Array.isArray(records.apiKeys) ? records.apiKeys : []
  const openAIAccounts = Array.isArray(records.openAIAccounts) ? records.openAIAccounts : []
  const accounts = Array.isArray(records.accounts) ? [...records.accounts] : []
  const accountGroups = Array.isArray(records.accountGroups) ? records.accountGroups : []
  const accountMap = new Map()

  for (const record of openAIAccounts) {
    accountMap.set(accountIdentity('openai', record.id), {
      ...record,
      kind: 'openai',
      storage: 'hash'
    })
  }

  for (const record of accounts) {
    accountMap.set(accountIdentity(record.kind, record.id), record)
  }

  return {
    apiKeys,
    openAIAccounts,
    accounts: [...accountMap.values()],
    explicitAccounts: accounts,
    accountGroups
  }
}

function summarizeApiKey(record) {
  return {
    id: record.id,
    name: record.data.name || '',
    tags: parseTags(record.data.tags),
    isActive: record.data.isActive,
    openaiAccountId: record.data.openaiAccountId || '',
    hashPrefix: record.data.apiKey ? `${record.data.apiKey.slice(0, 10)}...` : ''
  }
}

function summarizeOpenAIAccount(record) {
  return {
    id: record.id,
    name: record.data.name || '',
    accountType: record.data.accountType || '',
    isActive: record.data.isActive,
    schedulable: record.data.schedulable,
    reason: record.reason || ''
  }
}

function summarizeAccount(record) {
  const data = accountRecordData(record)
  const definition = ACCOUNT_DEFINITION_BY_KIND.get(record.kind) || {}
  return {
    kind: record.kind,
    id: record.id,
    name: accountRecordName(record),
    accountType: data.accountType || '',
    isActive: data.isActive,
    schedulable: data.schedulable,
    storage: record.storage || definition.storage || 'hash'
  }
}

function summarizeAccountGroup(record) {
  return {
    id: record.id,
    name: record.data?.name || '',
    platform: record.data?.platform || '',
    members: record.members || [],
    reason: record.reason || ''
  }
}

function countAccountsByKind(records) {
  return records.reduce((counts, record) => {
    counts[record.kind] = (counts[record.kind] || 0) + 1
    return counts
  }, {})
}

function summarizePayload(payload) {
  const records = migrationRecords(payload)

  return {
    format: payload.format,
    version: payload.version,
    createdAt: payload.createdAt,
    source: payload.source,
    counts: {
      apiKeys: records.apiKeys.length,
      openAIAccounts: records.openAIAccounts.length,
      accounts: records.explicitAccounts.length,
      accountGroups: records.accountGroups.length,
      accountKinds: countAccountsByKind(records.accounts),
      warnings: (payload.warnings || []).length
    },
    apiKeys: records.apiKeys.map(summarizeApiKey),
    openAIAccounts: records.openAIAccounts.map(summarizeOpenAIAccount),
    accounts: records.explicitAccounts.map(summarizeAccount),
    accountGroups: records.accountGroups.map(summarizeAccountGroup),
    warnings: payload.warnings || []
  }
}

function requirePassphrase(env = process.env) {
  const passphrase = env.MIGRATION_PASSPHRASE
  if (!passphrase || passphrase.length < 16) {
    throw new Error('MIGRATION_PASSPHRASE is required and must be at least 16 characters')
  }
  return passphrase
}

function encryptPayload(payload, passphrase) {
  const salt = crypto.randomBytes(16)
  const iv = crypto.randomBytes(12)
  const key = crypto.scryptSync(passphrase, salt, 32)
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv)
  const plaintext = Buffer.from(JSON.stringify(payload), 'utf8')
  const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()])
  const authTag = cipher.getAuthTag()

  return {
    format: `${FORMAT}.encrypted`,
    version: VERSION,
    crypto: {
      cipher: 'aes-256-gcm',
      kdf: 'scrypt',
      salt: salt.toString('base64'),
      iv: iv.toString('base64'),
      authTag: authTag.toString('base64')
    },
    payload: ciphertext.toString('base64')
  }
}

function decryptPackage(packageData, passphrase) {
  if (packageData.format !== `${FORMAT}.encrypted`) {
    if (packageData.format === FORMAT) {
      return packageData
    }
    throw new Error('Invalid migration package format')
  }

  const salt = Buffer.from(packageData.crypto.salt, 'base64')
  const iv = Buffer.from(packageData.crypto.iv, 'base64')
  const authTag = Buffer.from(packageData.crypto.authTag, 'base64')
  const ciphertext = Buffer.from(packageData.payload, 'base64')
  const key = crypto.scryptSync(passphrase, salt, 32)
  const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv)
  decipher.setAuthTag(authTag)
  const plaintext = Buffer.concat([decipher.update(ciphertext), decipher.final()])
  return JSON.parse(plaintext.toString('utf8'))
}

async function writeEncryptedPayload(filePath, payload, passphrase) {
  await fs.writeFile(filePath, `${JSON.stringify(encryptPayload(payload, passphrase), null, 2)}\n`)
}

async function readMigrationPayload(filePath, passphrase) {
  const raw = await fs.readFile(filePath, 'utf8')
  const packageData = JSON.parse(raw)
  const payload = decryptPackage(packageData, passphrase)
  validatePayload(payload)
  return payload
}

function validatePayload(payload) {
  if (!payload || payload.format !== FORMAT || payload.version !== VERSION) {
    throw new Error('Unsupported migration payload')
  }

  if (!payload.records || !Array.isArray(payload.records.apiKeys)) {
    throw new Error('Migration payload is missing records')
  }

  if (payload.records.openAIAccounts && !Array.isArray(payload.records.openAIAccounts)) {
    throw new Error('Migration payload openAIAccounts must be an array')
  }

  if (payload.records.accounts && !Array.isArray(payload.records.accounts)) {
    throw new Error('Migration payload accounts must be an array')
  }

  if (payload.records.accountGroups && !Array.isArray(payload.records.accountGroups)) {
    throw new Error('Migration payload accountGroups must be an array')
  }

  for (const record of payload.records.accounts || []) {
    accountDefinitionForKind(record.kind)
  }
}

function ensurePayloadHasRecords(payload) {
  const records = migrationRecords(payload)
  if (
    records.apiKeys.length === 0 &&
    records.accounts.length === 0 &&
    records.accountGroups.length === 0
  ) {
    throw new Error('No matching API keys, accounts, or groups found; run inspect-source first')
  }
}

async function inspectTargetPayload(client, payload, options = {}, runtimeConfig = config) {
  const { options: normalizedOptions, runtimeConfig: normalizedRuntimeConfig } =
    normalizeInspectInvocation(options, runtimeConfig)
  options = normalizedOptions
  runtimeConfig = normalizedRuntimeConfig

  validatePayload(payload)

  const records = migrationRecords(payload)
  const skipExisting = options.skipExisting !== false
  const errors = []
  const warnings = [...(payload.warnings || [])]
  const skipped = {
    apiKeys: [],
    accounts: [],
    accountGroups: []
  }
  const selectedAccountIdentities = new Set(
    records.accounts.map((record) => accountIdentity(record.kind, record.id))
  )
  const selectedGroupIds = new Set(records.accountGroups.map((record) => record.id))
  const targetApiKeys = await loadApiKeys(client)
  const targetAccounts = await loadAllAccountRecords(client)
  const targetGroups = await loadAllAccountGroupRecords(client)
  const targetApiKeyNames = new Map()
  const targetAccountNames = new Map()
  const targetGroupNames = new Map()

  for (const record of targetApiKeys) {
    const name = normalizeText(record.data.name)
    if (!name) {
      continue
    }
    if (!targetApiKeyNames.has(name)) {
      targetApiKeyNames.set(name, [])
    }
    targetApiKeyNames.get(name).push(record)
  }

  for (const record of targetAccounts) {
    const name = normalizeText(accountRecordName(record))
    if (!name) {
      continue
    }
    const nameKey = `${record.kind}:${name}`
    if (!targetAccountNames.has(nameKey)) {
      targetAccountNames.set(nameKey, [])
    }
    targetAccountNames.get(nameKey).push(record)
  }

  for (const record of targetGroups) {
    const name = normalizeText(record.data?.name)
    if (!name) {
      continue
    }
    const nameKey = `${record.data?.platform || ''}:${name}`
    if (!targetGroupNames.has(nameKey)) {
      targetGroupNames.set(nameKey, [])
    }
    targetGroupNames.get(nameKey).push(record)
  }

  if (payload.source.encryptionKeyFingerprint !== encryptionKeyFingerprint(runtimeConfig)) {
    errors.push('Target ENCRYPTION_KEY fingerprint does not match source package')
  }

  if (payload.source.apiKeyPrefix !== apiKeyPrefix(runtimeConfig)) {
    errors.push(
      `Target API_KEY_PREFIX is ${apiKeyPrefix(runtimeConfig)}, source package uses ${payload.source.apiKeyPrefix}`
    )
  }

  for (const record of records.apiKeys) {
    const key = `apikey:${record.id}`
    const targetHasApiKey = Boolean(await client.exists(key))
    if (targetHasApiKey && skipExisting) {
      skipped.apiKeys.push({
        id: record.id,
        name: record.data.name || '',
        reason: 'existing_id'
      })
    } else if (targetHasApiKey) {
      errors.push(`Target already has API Key id ${record.id}`)
    }

    const sameNameKeys = targetApiKeyNames.get(normalizeText(record.data.name)) || []
    for (const sameNameKey of sameNameKeys) {
      if (sameNameKey.id !== record.id) {
        warnings.push(
          `Target already has API Key name "${record.data.name}" with different id ${sameNameKey.id}`
        )
      }
    }

    if (record.data.apiKey) {
      const mappedId = await client.hget(API_KEY_HASH_MAP, record.data.apiKey)
      if (mappedId && mappedId !== record.id) {
        errors.push(`Target API Key hash already maps to ${mappedId}, cannot import ${record.id}`)
      } else if (mappedId === record.id && !skipExisting) {
        errors.push(`Target hash map already contains API Key id ${record.id}`)
      } else if (mappedId === record.id && !targetHasApiKey) {
        warnings.push(
          `Target hash map already contains API Key id ${record.id}, but API Key hash is missing; import will recreate it`
        )
      }
    }

    for (const binding of apiKeyAccountBindings(record.data)) {
      if (binding.groupId) {
        if (!selectedGroupIds.has(binding.groupId)) {
          const targetHasGroup = await client.exists(`${ACCOUNT_GROUP_PREFIX}${binding.groupId}`)
          if (!targetHasGroup) {
            warnings.push(
              `API Key ${record.data.name || record.id} binds account group ${binding.groupId}, but it is not in package or target`
            )
          }
        }
        continue
      }

      const identity = accountIdentity(binding.kind, binding.id)
      if (!selectedAccountIdentities.has(identity)) {
        const definition = accountDefinitionForKind(binding.kind)
        const targetHasAccount = await client.exists(`${definition.prefix}${binding.id}`)
        if (!targetHasAccount) {
          warnings.push(
            `API Key ${record.data.name || record.id} binds ${binding.kind} account ${binding.id}, but it is not in package or target`
          )
        }
      }
    }
  }

  for (const record of records.accounts) {
    const definition = accountDefinitionForKind(record.kind)
    const targetHasAccount = Boolean(await client.exists(`${definition.prefix}${record.id}`))
    if (targetHasAccount && skipExisting) {
      skipped.accounts.push({
        kind: record.kind,
        id: record.id,
        name: accountRecordName(record),
        reason: 'existing_id'
      })
    } else if (targetHasAccount) {
      errors.push(`Target already has ${record.kind} account id ${record.id}`)
    }

    const name = accountRecordName(record)
    const sameNameAccounts = targetAccountNames.get(`${record.kind}:${normalizeText(name)}`) || []
    for (const sameNameAccount of sameNameAccounts) {
      if (sameNameAccount.id !== record.id) {
        warnings.push(
          `Target already has ${record.kind} account name "${name}" with different id ${sameNameAccount.id}`
        )
      }
    }
  }

  for (const record of records.accountGroups) {
    const key = `${ACCOUNT_GROUP_PREFIX}${record.id}`
    const targetHasGroup = Boolean(await client.exists(key))
    if (targetHasGroup && skipExisting) {
      skipped.accountGroups.push({
        id: record.id,
        name: record.data?.name || '',
        platform: record.data?.platform || '',
        reason: 'existing_id'
      })
      const existingGroup = await client.hgetall(key)
      if (
        existingGroup?.platform &&
        record.data?.platform &&
        existingGroup.platform !== record.data.platform
      ) {
        errors.push(
          `Target account group id ${record.id} has platform ${existingGroup.platform}, package uses ${record.data.platform}`
        )
      } else if (
        existingGroup?.name &&
        record.data?.name &&
        existingGroup.name !== record.data.name
      ) {
        warnings.push(
          `Target account group id ${record.id} already exists with name "${existingGroup.name}", package name is "${record.data.name}"; metadata will not be overwritten`
        )
      }
    } else if (targetHasGroup) {
      errors.push(`Target already has account group id ${record.id}`)
    }

    const name = record.data?.name || ''
    const nameKey = `${record.data?.platform || ''}:${normalizeText(name)}`
    const sameNameGroups = targetGroupNames.get(nameKey) || []
    for (const sameNameGroup of sameNameGroups) {
      if (sameNameGroup.id !== record.id) {
        warnings.push(
          `Target already has ${record.data?.platform || ''} account group name "${name}" with different id ${sameNameGroup.id}`
        )
      }
    }
  }

  return {
    ok: errors.length === 0,
    errors,
    warnings,
    skipped,
    counts: {
      apiKeys: records.apiKeys.length,
      openAIAccounts: records.openAIAccounts.length,
      accounts: records.explicitAccounts.length,
      accountGroups: records.accountGroups.length,
      accountKinds: countAccountsByKind(records.accounts)
    }
  }
}

function addApiKeyIndexWrites(pipeline, record) {
  const { data, id: keyId } = record
  const createdAt = data.createdAt ? new Date(data.createdAt).getTime() : Date.now()
  const lastUsedAt = data.lastUsedAt ? new Date(data.lastUsedAt).getTime() : 0
  const name = normalizeText(data.name)
  const isActive = data.isActive === true || data.isActive === 'true'
  const isDeleted = data.isDeleted === true || data.isDeleted === 'true'
  const tags = parseTags(data.tags)

  pipeline.zadd('apikey:idx:createdAt', createdAt, keyId)
  pipeline.zadd('apikey:idx:lastUsedAt', lastUsedAt, keyId)
  pipeline.zadd('apikey:idx:name', 0, `${name}\x00${keyId}`)
  pipeline.sadd('apikey:idx:all', keyId)

  if (isDeleted) {
    pipeline.sadd('apikey:set:deleted', keyId)
  } else if (isActive) {
    pipeline.sadd('apikey:set:active', keyId)
  }

  for (const tag of tags) {
    pipeline.sadd(`apikey:tag:${tag}`, keyId)
    pipeline.sadd('apikey:tags:all', tag)
  }
}

function addAccountWrites(pipeline, record) {
  const definition = accountDefinitionForKind(record.kind)
  const key = `${definition.prefix}${record.id}`

  if (definition.storage === 'string') {
    const value = record.value || JSON.stringify(accountRecordData(record))
    pipeline.set(key, value)
  } else {
    pipeline.hset(key, accountRecordData(record))
  }

  pipeline.sadd(definition.index, record.id)
  pipeline.del(`${definition.index}:empty`)

  if (definition.sharedSet && accountRecordIsShared(record)) {
    pipeline.sadd(definition.sharedSet, record.id)
  }

  if (record.ttl > 0) {
    pipeline.expire(key, record.ttl)
  }
}

function addAccountGroupWrites(pipeline, record) {
  pipeline.hset(`${ACCOUNT_GROUP_PREFIX}${record.id}`, record.data)
  pipeline.sadd(ACCOUNT_GROUPS_KEY, record.id)

  if (record.ttl > 0) {
    pipeline.expire(`${ACCOUNT_GROUP_PREFIX}${record.id}`, record.ttl)
  }

  addAccountGroupMemberWrites(pipeline, record)
}

function addAccountGroupMemberWrites(pipeline, record) {
  for (const memberId of record.members || []) {
    pipeline.sadd(`${ACCOUNT_GROUP_MEMBERS_PREFIX}${record.id}`, memberId)
    if (record.data?.platform) {
      pipeline.sadd(
        `${ACCOUNT_GROUPS_REVERSE_PREFIX}${record.data.platform}:${memberId}`,
        record.id
      )
    }
  }
}

async function importTargetPayload(client, payload, options = {}, runtimeConfig = config) {
  const assessment = await inspectTargetPayload(client, payload, options, runtimeConfig)
  if (!assessment.ok) {
    return { applied: false, assessment }
  }

  if (!options.apply) {
    return { applied: false, assessment }
  }

  const records = migrationRecords(payload)
  const skippedApiKeyIds = new Set((assessment.skipped?.apiKeys || []).map((record) => record.id))
  const skippedAccountIds = new Set(
    (assessment.skipped?.accounts || []).map((record) => accountIdentity(record.kind, record.id))
  )
  const skippedGroupIds = new Set(
    (assessment.skipped?.accountGroups || []).map((record) => record.id)
  )
  const pipeline = client.pipeline()
  let importedApiKeys = 0
  let importedAccounts = 0
  let importedOpenAIAccounts = 0
  let importedAccountGroups = 0
  let mergedAccountGroups = 0

  for (const record of records.accounts) {
    if (skippedAccountIds.has(accountIdentity(record.kind, record.id))) {
      continue
    }
    addAccountWrites(pipeline, record)
    importedAccounts += 1
    if (record.kind === 'openai' && records.openAIAccounts.some((item) => item.id === record.id)) {
      importedOpenAIAccounts += 1
    }
  }

  for (const record of records.accountGroups) {
    if (skippedGroupIds.has(record.id)) {
      addAccountGroupMemberWrites(pipeline, record)
      mergedAccountGroups += 1
    } else {
      addAccountGroupWrites(pipeline, record)
      importedAccountGroups += 1
    }
  }

  for (const record of records.apiKeys) {
    if (skippedApiKeyIds.has(record.id)) {
      continue
    }
    pipeline.hset(`apikey:${record.id}`, record.data)
    if (record.data.apiKey) {
      pipeline.hset(API_KEY_HASH_MAP, record.data.apiKey, record.id)
    }
    if (record.ttl > 0) {
      pipeline.expire(`apikey:${record.id}`, record.ttl)
    }
    addApiKeyIndexWrites(pipeline, record)
    importedApiKeys += 1
  }

  await pipeline.exec()

  return {
    applied: true,
    assessment,
    imported: {
      apiKeys: importedApiKeys,
      openAIAccounts: importedOpenAIAccounts,
      accounts: importedAccounts,
      accountGroups: importedAccountGroups,
      mergedAccountGroups
    },
    skipped: assessment.skipped
  }
}

async function disableSourcePayload(client, payload, options = {}) {
  validatePayload(payload)

  const records = migrationRecords(payload)
  const result = {
    applied: Boolean(options.apply),
    apiKeys: [],
    openAIAccounts: [],
    accounts: [],
    warnings: []
  }

  const now = new Date().toISOString()

  for (const record of records.apiKeys) {
    const key = `apikey:${record.id}`
    if (!(await client.exists(key))) {
      result.warnings.push(`Source missing API Key ${record.id}`)
      continue
    }
    result.apiKeys.push({ id: record.id, name: record.data.name || '' })
  }

  for (const record of records.accounts) {
    const key = accountRecordRedisKey(record)
    if (!(await client.exists(key))) {
      result.warnings.push(`Source missing ${record.kind} account ${record.id}`)
      continue
    }
    const summary = { kind: record.kind, id: record.id, name: accountRecordName(record) }
    result.accounts.push(summary)
    if (record.kind === 'openai' && records.openAIAccounts.some((item) => item.id === record.id)) {
      result.openAIAccounts.push({ id: record.id, name: accountRecordName(record) })
    }
  }

  if (!options.apply) {
    return result
  }

  const pipeline = client.pipeline()
  for (const record of result.apiKeys) {
    pipeline.hset(`apikey:${record.id}`, {
      isActive: 'false',
      migrationDisabledAt: now
    })
  }
  for (const record of result.accounts) {
    const definition = accountDefinitionForKind(record.kind)
    const key = `${definition.prefix}${record.id}`
    if (definition.storage === 'string') {
      const currentValue = await client.get(key)
      const currentData = safeParseJsonObject(currentValue) || {}
      pipeline.set(
        key,
        JSON.stringify({
          ...currentData,
          id: currentData.id || record.id,
          isActive: false,
          schedulable: false,
          status: 'disabled',
          updatedAt: now,
          migrationDisabledAt: now
        })
      )
      continue
    }

    pipeline.hset(key, {
      isActive: 'false',
      schedulable: 'false',
      status: 'disabled',
      updatedAt: now,
      migrationDisabledAt: now
    })
  }
  await pipeline.exec()

  return result
}

function printJson(value) {
  console.log(JSON.stringify(value, null, 2))
}

function showHelp() {
  console.log(`
Selective Redis migration for Claude Relay Service

Usage:
  node scripts/selective-redis-migrate.js <command> [options]

Commands:
  inspect-source --tag=neu --openai-name=<keyword>
  export-source --tag=neu --openai-name=<keyword> --out=/tmp/neu-migration.json.enc
  inspect-remaining-source --exclude-tag=neu --exclude-openai-name=<keyword>
  export-remaining-source --exclude-tag=neu --exclude-openai-name=<keyword> --out=/tmp/b-remaining-migration.json.enc
  inspect-target --in=/tmp/neu-migration.json.enc [--strict-conflicts]
  import-target --in=/tmp/neu-migration.json.enc [--apply] [--strict-conflicts]
  disable-source --in=/tmp/neu-migration.json.enc [--apply]

Default import behavior:
  Existing same-ID API keys, accounts, and groups are skipped instead of overwritten.
  Real conflicts, such as an API Key hash mapping to another ID, still fail the import.

Environment:
  REDIS_HOST / REDIS_PORT / REDIS_PASSWORD / REDIS_DB
  ENCRYPTION_KEY
  API_KEY_PREFIX
  MIGRATION_PASSPHRASE for encrypted migration packages
`)
}

async function withRedis(task) {
  await redis.connect()
  try {
    return await task(redis.client)
  } finally {
    await redis.disconnect()
  }
}

async function main(argv = process.argv.slice(2)) {
  const { command, params } = parseArgs(argv)

  if (!command || command === 'help' || command === '--help') {
    showHelp()
    return
  }

  if (command === 'inspect-source') {
    const payload = await withRedis((client) =>
      buildSourceSelection(client, {
        tag: params.tag,
        openaiName: params['openai-name']
      })
    )
    printJson(summarizePayload(payload))
    return
  }

  if (command === 'export-source') {
    if (!params.out) {
      throw new Error('Missing required --out')
    }
    const passphrase = requirePassphrase()
    const payload = await withRedis((client) =>
      buildSourceSelection(client, {
        tag: params.tag,
        openaiName: params['openai-name']
      })
    )
    ensurePayloadHasRecords(payload)
    await writeEncryptedPayload(params.out, payload, passphrase)
    printJson({
      exported: true,
      out: params.out,
      summary: summarizePayload(payload)
    })
    return
  }

  if (command === 'inspect-remaining-source') {
    const payload = await withRedis((client) =>
      buildRemainingSourceSelection(client, {
        excludeTag: params['exclude-tag'],
        excludeOpenAIName: params['exclude-openai-name']
      })
    )
    printJson(summarizePayload(payload))
    return
  }

  if (command === 'export-remaining-source') {
    if (!params.out) {
      throw new Error('Missing required --out')
    }
    const passphrase = requirePassphrase()
    const payload = await withRedis((client) =>
      buildRemainingSourceSelection(client, {
        excludeTag: params['exclude-tag'],
        excludeOpenAIName: params['exclude-openai-name']
      })
    )
    ensurePayloadHasRecords(payload)
    await writeEncryptedPayload(params.out, payload, passphrase)
    printJson({
      exported: true,
      out: params.out,
      summary: summarizePayload(payload)
    })
    return
  }

  if (command === 'inspect-target') {
    if (!params.in) {
      throw new Error('Missing required --in')
    }
    const payload = await readMigrationPayload(params.in, requirePassphrase())
    const assessment = await withRedis((client) =>
      inspectTargetPayload(client, payload, { skipExisting: params['strict-conflicts'] !== true })
    )
    printJson({ summary: summarizePayload(payload), assessment })
    return
  }

  if (command === 'import-target') {
    if (!params.in) {
      throw new Error('Missing required --in')
    }
    const payload = await readMigrationPayload(params.in, requirePassphrase())
    const result = await withRedis((client) =>
      importTargetPayload(client, payload, {
        apply: params.apply === true,
        skipExisting: params['strict-conflicts'] !== true
      })
    )
    printJson(result)
    if (!result.assessment.ok) {
      process.exitCode = 1
    }
    return
  }

  if (command === 'disable-source') {
    if (!params.in) {
      throw new Error('Missing required --in')
    }
    const payload = await readMigrationPayload(params.in, requirePassphrase())
    const result = await withRedis((client) =>
      disableSourcePayload(client, payload, { apply: params.apply === true })
    )
    printJson(result)
    return
  }

  throw new Error(`Unknown command: ${command}`)
}

if (require.main === module) {
  main().catch((error) => {
    console.error(`Migration failed: ${error.message}`)
    process.exit(1)
  })
}

module.exports = {
  FORMAT,
  VERSION,
  parseArgs,
  parseTags,
  hasTag,
  isApiKeyDataKey,
  globToRegex,
  scanKeys,
  buildSourceSelection,
  buildRemainingSourceSelection,
  summarizePayload,
  encryptPayload,
  decryptPackage,
  inspectTargetPayload,
  importTargetPayload,
  disableSourcePayload,
  encryptionKeyFingerprint,
  apiKeyPrefix
}
