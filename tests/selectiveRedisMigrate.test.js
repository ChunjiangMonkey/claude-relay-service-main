jest.mock(
  '../config/config',
  () => ({
    security: {
      encryptionKey: 'same-encryption-key-for-test-32',
      apiKeyPrefix: 'cr_'
    }
  }),
  { virtual: true }
)

jest.mock('../src/models/redis', () => ({
  connect: jest.fn(),
  disconnect: jest.fn(),
  client: null
}))

const {
  buildSourceSelection,
  encryptPayload,
  decryptPackage,
  inspectTargetPayload,
  importTargetPayload,
  disableSourcePayload
} = require('../scripts/selective-redis-migrate')

const runtimeConfig = {
  security: {
    encryptionKey: 'same-encryption-key-for-test-32',
    apiKeyPrefix: 'cr_'
  }
}

function patternToRegex(pattern) {
  const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*')
  return new RegExp(`^${escaped}$`)
}

class FakeRedis {
  constructor(seed = {}) {
    this.hashes = new Map()
    this.strings = new Map()
    this.sets = new Map()
    this.zsets = new Map()
    this.ttls = new Map()

    for (const [key, value] of Object.entries(seed.hashes || {})) {
      this.hashes.set(key, { ...value })
    }
    for (const [key, value] of Object.entries(seed.strings || {})) {
      this.strings.set(key, value)
    }
    for (const [key, values] of Object.entries(seed.sets || {})) {
      this.sets.set(key, new Set(values))
    }
  }

  async scan(_cursor, _match, pattern) {
    const regex = patternToRegex(pattern)
    const keys = [...this.hashes.keys(), ...this.strings.keys(), ...this.sets.keys()]
      .filter((key) => regex.test(key))
      .sort()
    return ['0', keys]
  }

  async hgetall(key) {
    if (this.sets.has(key) || this.strings.has(key) || this.zsets.has(key)) {
      throw new Error('WRONGTYPE Operation against a key holding the wrong kind of value')
    }
    return { ...(this.hashes.get(key) || {}) }
  }

  async hget(key, field) {
    return this.hashes.get(key)?.[field] || null
  }

  async hset(key, field, value) {
    const existing = this.hashes.get(key) || {}
    if (typeof field === 'object' && field !== null) {
      this.hashes.set(key, { ...existing, ...field })
    } else {
      this.hashes.set(key, { ...existing, [field]: value })
    }
  }

  async exists(key) {
    return this.hashes.has(key) || this.strings.has(key) || this.sets.has(key) ? 1 : 0
  }

  async ttl(key) {
    return this.ttls.has(key) ? this.ttls.get(key) : -1
  }

  async type(key) {
    if (this.hashes.has(key)) {
      return 'hash'
    }
    if (this.strings.has(key)) {
      return 'string'
    }
    if (this.sets.has(key)) {
      return 'set'
    }
    if (this.zsets.has(key)) {
      return 'zset'
    }
    return 'none'
  }

  async expire(key, seconds) {
    this.ttls.set(key, seconds)
  }

  async sadd(key, ...values) {
    const set = this.sets.get(key) || new Set()
    for (const value of values) {
      set.add(value)
    }
    this.sets.set(key, set)
  }

  async srem(key, ...values) {
    const set = this.sets.get(key) || new Set()
    for (const value of values) {
      set.delete(value)
    }
    this.sets.set(key, set)
  }

  async smembers(key) {
    return [...(this.sets.get(key) || new Set())]
  }

  async del(key) {
    this.hashes.delete(key)
    this.strings.delete(key)
    this.sets.delete(key)
    this.zsets.delete(key)
  }

  async zadd(key, score, member) {
    const zset = this.zsets.get(key) || new Map()
    zset.set(member, score)
    this.zsets.set(key, zset)
  }

  pipeline() {
    const operations = []
    const client = this
    return {
      hset(key, field, value) {
        operations.push(() => client.hset(key, field, value))
        return this
      },
      sadd(key, ...values) {
        operations.push(() => client.sadd(key, ...values))
        return this
      },
      srem(key, ...values) {
        operations.push(() => client.srem(key, ...values))
        return this
      },
      del(key) {
        operations.push(() => client.del(key))
        return this
      },
      zadd(key, score, member) {
        operations.push(() => client.zadd(key, score, member))
        return this
      },
      expire(key, seconds) {
        operations.push(() => client.expire(key, seconds))
        return this
      },
      async exec() {
        const results = []
        for (const operation of operations) {
          results.push([null, await operation()])
        }
        return results
      }
    }
  }
}

describe('selective Redis migration', () => {
  test('selects API keys by neu tag and includes bound OpenAI accounts', async () => {
    const source = new FakeRedis({
      hashes: {
        'apikey:key-neu': {
          id: 'key-neu',
          name: 'NEU key',
          apiKey: 'hash-neu',
          tags: JSON.stringify(['NEU']),
          isActive: 'true',
          openaiAccountId: 'oa-bound'
        },
        'apikey:key-other': {
          id: 'key-other',
          name: 'Other key',
          apiKey: 'hash-other',
          tags: JSON.stringify(['other']),
          isActive: 'true'
        },
        'apikey:key-responses': {
          id: 'key-responses',
          name: 'Responses key',
          apiKey: 'hash-responses',
          tags: JSON.stringify(['neu']),
          isActive: 'true',
          openaiAccountId: 'responses:responses-account'
        },
        'openai:account:oa-bound': {
          id: 'oa-bound',
          name: 'Production ChatGPT account',
          accountType: 'shared',
          isActive: 'true'
        }
      },
      sets: {
        'openai:account:index': ['oa-bound'],
        'apikey:idx:all': ['key-neu']
      }
    })

    const payload = await buildSourceSelection(
      source,
      { tag: 'neu', openaiName: 'chatgpt' },
      runtimeConfig
    )

    expect(payload.records.apiKeys.map((record) => record.id).sort()).toEqual([
      'key-neu',
      'key-responses'
    ])
    expect(payload.records.openAIAccounts.map((record) => record.id)).toEqual(['oa-bound'])
    expect(payload.records.openAIAccounts[0].reason).toBe('keyword')
    expect(payload.warnings).toContain(
      'API Key Responses key (key-responses) binds OpenAI Responses account responses:responses-account; first version does not migrate it'
    )
  })

  test('encrypts and decrypts migration payloads', async () => {
    const payload = {
      format: 'claude-relay-selective-migration',
      version: 1,
      source: {},
      records: { apiKeys: [], openAIAccounts: [] },
      warnings: []
    }

    const encrypted = encryptPayload(payload, 'very-long-test-passphrase')
    const decrypted = decryptPackage(encrypted, 'very-long-test-passphrase')

    expect(encrypted.format).toBe('claude-relay-selective-migration.encrypted')
    expect(decrypted).toEqual(payload)
  })

  test('target inspection rejects existing API key hash conflicts', async () => {
    const target = new FakeRedis({
      hashes: {
        'apikey:hash_map': {
          'hash-neu': 'other-key'
        }
      }
    })
    const payload = {
      format: 'claude-relay-selective-migration',
      version: 1,
      source: {
        encryptionKeyFingerprint: '989f1d36fd45e090',
        apiKeyPrefix: 'cr_'
      },
      records: {
        apiKeys: [
          {
            id: 'key-neu',
            data: { id: 'key-neu', name: 'NEU key', apiKey: 'hash-neu', tags: '["neu"]' },
            ttl: -1
          }
        ],
        openAIAccounts: []
      },
      warnings: []
    }

    const assessment = await inspectTargetPayload(target, payload, runtimeConfig)

    expect(assessment.ok).toBe(false)
    expect(assessment.errors).toContain(
      'Target API Key hash already maps to other-key, cannot import key-neu'
    )
  })

  test('imports API keys, hash map, indexes, and shared OpenAI account', async () => {
    const target = new FakeRedis({
      hashes: {
        'apikey:existing': {
          id: 'existing',
          name: 'Existing key'
        }
      }
    })
    const payload = {
      format: 'claude-relay-selective-migration',
      version: 1,
      source: {
        encryptionKeyFingerprint: '989f1d36fd45e090',
        apiKeyPrefix: 'cr_'
      },
      records: {
        apiKeys: [
          {
            id: 'key-neu',
            data: {
              id: 'key-neu',
              name: 'NEU key',
              apiKey: 'hash-neu',
              tags: JSON.stringify(['neu']),
              isActive: 'true',
              createdAt: '2026-01-01T00:00:00.000Z',
              openaiAccountId: 'oa-bound'
            },
            ttl: 3600
          }
        ],
        openAIAccounts: [
          {
            id: 'oa-bound',
            data: {
              id: 'oa-bound',
              name: 'Production ChatGPT account',
              accountType: 'shared',
              isActive: 'true'
            },
            ttl: -1
          }
        ]
      },
      warnings: []
    }

    const result = await importTargetPayload(target, payload, { apply: true }, runtimeConfig)

    expect(result.applied).toBe(true)
    expect(await target.hget('apikey:hash_map', 'hash-neu')).toBe('key-neu')
    expect((await target.hgetall('apikey:key-neu')).name).toBe('NEU key')
    expect((await target.smembers('apikey:idx:all')).sort()).toEqual(['key-neu'])
    expect(await target.smembers('apikey:tag:neu')).toEqual(['key-neu'])
    expect(await target.smembers('openai:account:index')).toEqual(['oa-bound'])
    expect(await target.smembers('shared_openai_accounts')).toEqual(['oa-bound'])
    expect((await target.hgetall('apikey:existing')).name).toBe('Existing key')
    expect(await target.ttl('apikey:key-neu')).toBe(3600)
  })

  test('disable source is dry-run by default and disables only with apply', async () => {
    const source = new FakeRedis({
      hashes: {
        'apikey:key-neu': {
          id: 'key-neu',
          name: 'NEU key',
          isActive: 'true'
        },
        'openai:account:oa-bound': {
          id: 'oa-bound',
          name: 'Production ChatGPT account',
          isActive: 'true',
          schedulable: 'true',
          status: 'active'
        }
      }
    })
    const payload = {
      format: 'claude-relay-selective-migration',
      version: 1,
      records: {
        apiKeys: [{ id: 'key-neu', data: { name: 'NEU key' } }],
        openAIAccounts: [{ id: 'oa-bound', data: { name: 'Production ChatGPT account' } }]
      },
      warnings: []
    }

    const dryRun = await disableSourcePayload(source, payload)
    expect(dryRun.applied).toBe(false)
    expect((await source.hgetall('apikey:key-neu')).isActive).toBe('true')

    const applied = await disableSourcePayload(source, payload, { apply: true })
    expect(applied.applied).toBe(true)
    expect((await source.hgetall('apikey:key-neu')).isActive).toBe('false')
    expect((await source.hgetall('openai:account:oa-bound')).schedulable).toBe('false')
    expect((await source.hgetall('openai:account:oa-bound')).status).toBe('disabled')
  })
})
