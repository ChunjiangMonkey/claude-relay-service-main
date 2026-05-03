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

async function loadApiKeys(client) {
  const keys = (await scanKeys(client, 'apikey:*')).filter(isApiKeyDataKey)
  return readHashRecords(client, keys, 'apikey:')
}

async function loadOpenAIAccounts(client) {
  const keys = await scanKeys(client, `${OPENAI_ACCOUNT_PREFIX}*`)
  return readHashRecords(client, keys, OPENAI_ACCOUNT_PREFIX)
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

function summarizePayload(payload) {
  return {
    format: payload.format,
    version: payload.version,
    createdAt: payload.createdAt,
    source: payload.source,
    counts: {
      apiKeys: payload.records.apiKeys.length,
      openAIAccounts: payload.records.openAIAccounts.length,
      warnings: payload.warnings.length
    },
    apiKeys: payload.records.apiKeys.map(summarizeApiKey),
    openAIAccounts: payload.records.openAIAccounts.map(summarizeOpenAIAccount),
    warnings: payload.warnings
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

  if (
    !payload.records ||
    !Array.isArray(payload.records.apiKeys) ||
    !Array.isArray(payload.records.openAIAccounts)
  ) {
    throw new Error('Migration payload is missing records')
  }
}

function ensurePayloadHasRecords(payload) {
  if (payload.records.apiKeys.length === 0 && payload.records.openAIAccounts.length === 0) {
    throw new Error('No matching API keys or OpenAI accounts found; run inspect-source first')
  }
}

async function inspectTargetPayload(client, payload, runtimeConfig = config) {
  validatePayload(payload)

  const errors = []
  const warnings = [...(payload.warnings || [])]
  const selectedOpenAIAccountIds = new Set(
    payload.records.openAIAccounts.map((record) => record.id)
  )

  if (payload.source.encryptionKeyFingerprint !== encryptionKeyFingerprint(runtimeConfig)) {
    errors.push('Target ENCRYPTION_KEY fingerprint does not match source package')
  }

  if (payload.source.apiKeyPrefix !== apiKeyPrefix(runtimeConfig)) {
    errors.push(
      `Target API_KEY_PREFIX is ${apiKeyPrefix(runtimeConfig)}, source package uses ${payload.source.apiKeyPrefix}`
    )
  }

  for (const record of payload.records.apiKeys) {
    const key = `apikey:${record.id}`
    if (await client.exists(key)) {
      errors.push(`Target already has API Key id ${record.id}`)
    }

    if (record.data.apiKey) {
      const mappedId = await client.hget(API_KEY_HASH_MAP, record.data.apiKey)
      if (mappedId && mappedId !== record.id) {
        errors.push(`Target API Key hash already maps to ${mappedId}, cannot import ${record.id}`)
      } else if (mappedId === record.id) {
        errors.push(`Target hash map already contains API Key id ${record.id}`)
      }
    }

    const boundOpenAIAccountId = directOpenAIAccountId(record.data.openaiAccountId)
    if (boundOpenAIAccountId && !selectedOpenAIAccountIds.has(boundOpenAIAccountId)) {
      const targetHasAccount = await client.exists(
        `${OPENAI_ACCOUNT_PREFIX}${boundOpenAIAccountId}`
      )
      if (!targetHasAccount) {
        warnings.push(
          `API Key ${record.data.name || record.id} binds OpenAI account ${boundOpenAIAccountId}, but it is not in package or target`
        )
      }
    }
  }

  for (const record of payload.records.openAIAccounts) {
    if (await client.exists(`${OPENAI_ACCOUNT_PREFIX}${record.id}`)) {
      errors.push(`Target already has OpenAI account id ${record.id}`)
    }
  }

  return {
    ok: errors.length === 0,
    errors,
    warnings,
    counts: {
      apiKeys: payload.records.apiKeys.length,
      openAIAccounts: payload.records.openAIAccounts.length
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

async function importTargetPayload(client, payload, options = {}, runtimeConfig = config) {
  const assessment = await inspectTargetPayload(client, payload, runtimeConfig)
  if (!assessment.ok) {
    return { applied: false, assessment }
  }

  if (!options.apply) {
    return { applied: false, assessment }
  }

  const pipeline = client.pipeline()

  for (const record of payload.records.openAIAccounts) {
    pipeline.hset(`${OPENAI_ACCOUNT_PREFIX}${record.id}`, record.data)
    pipeline.sadd(OPENAI_ACCOUNT_INDEX, record.id)
    pipeline.del(`${OPENAI_ACCOUNT_INDEX}:empty`)
    if (record.data.accountType === 'shared') {
      pipeline.sadd(SHARED_OPENAI_ACCOUNTS, record.id)
    }
    if (record.ttl > 0) {
      pipeline.expire(`${OPENAI_ACCOUNT_PREFIX}${record.id}`, record.ttl)
    }
  }

  for (const record of payload.records.apiKeys) {
    pipeline.hset(`apikey:${record.id}`, record.data)
    if (record.data.apiKey) {
      pipeline.hset(API_KEY_HASH_MAP, record.data.apiKey, record.id)
    }
    if (record.ttl > 0) {
      pipeline.expire(`apikey:${record.id}`, record.ttl)
    }
    addApiKeyIndexWrites(pipeline, record)
  }

  await pipeline.exec()

  return {
    applied: true,
    assessment,
    imported: {
      apiKeys: payload.records.apiKeys.length,
      openAIAccounts: payload.records.openAIAccounts.length
    }
  }
}

async function disableSourcePayload(client, payload, options = {}) {
  validatePayload(payload)

  const result = {
    applied: Boolean(options.apply),
    apiKeys: [],
    openAIAccounts: [],
    warnings: []
  }

  const now = new Date().toISOString()

  for (const record of payload.records.apiKeys) {
    const key = `apikey:${record.id}`
    if (!(await client.exists(key))) {
      result.warnings.push(`Source missing API Key ${record.id}`)
      continue
    }
    result.apiKeys.push({ id: record.id, name: record.data.name || '' })
  }

  for (const record of payload.records.openAIAccounts) {
    const key = `${OPENAI_ACCOUNT_PREFIX}${record.id}`
    if (!(await client.exists(key))) {
      result.warnings.push(`Source missing OpenAI account ${record.id}`)
      continue
    }
    result.openAIAccounts.push({ id: record.id, name: record.data.name || '' })
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
  for (const record of result.openAIAccounts) {
    pipeline.hset(`${OPENAI_ACCOUNT_PREFIX}${record.id}`, {
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
  inspect-target --in=/tmp/neu-migration.json.enc
  import-target --in=/tmp/neu-migration.json.enc [--apply]
  disable-source --in=/tmp/neu-migration.json.enc [--apply]

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

  if (command === 'inspect-target') {
    if (!params.in) {
      throw new Error('Missing required --in')
    }
    const payload = await readMigrationPayload(params.in, requirePassphrase())
    const assessment = await withRedis((client) => inspectTargetPayload(client, payload))
    printJson({ summary: summarizePayload(payload), assessment })
    return
  }

  if (command === 'import-target') {
    if (!params.in) {
      throw new Error('Missing required --in')
    }
    const payload = await readMigrationPayload(params.in, requirePassphrase())
    const result = await withRedis((client) =>
      importTargetPayload(client, payload, { apply: params.apply === true })
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
  summarizePayload,
  encryptPayload,
  decryptPackage,
  inspectTargetPayload,
  importTargetPayload,
  disableSourcePayload,
  encryptionKeyFingerprint,
  apiKeyPrefix
}
