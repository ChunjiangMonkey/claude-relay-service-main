'use strict'

const { createPostgresAdapter } = require('./postgres')
const { createMysqlAdapter } = require('./mysql')

function normalizeBackend(rawValue) {
  const normalized = String(rawValue || 'mysql').trim().toLowerCase()
  if (normalized === 'postgresql') {
    return 'postgres'
  }
  return normalized
}

function createDbAdapter(config) {
  const backend = normalizeBackend(config.dbBackend)

  if (backend === 'mysql') {
    return createMysqlAdapter(config)
  }

  if (backend === 'postgres') {
    return createPostgresAdapter(config)
  }

  throw new Error(`Unsupported COLLECTOR_DB_BACKEND: ${backend}`)
}

module.exports = {
  createDbAdapter,
  normalizeBackend
}
