const winston = require('winston')
const DailyRotateFile = require('winston-daily-rotate-file')
const config = require('../../config/config')
const { formatDateWithTimezone } = require('../utils/dateHelper')
const path = require('path')
const fs = require('fs')
const os = require('os')

const MAX_LOG_DEPTH = 8
const MAX_LOG_STRING_CHARS = 2048
const MAX_LOG_STACK_CHARS = 4000
const MAX_LOG_ARRAY_ITEMS = 20
const MAX_LOG_OBJECT_KEYS = 50
const MAX_PAYLOAD_PREVIEW_CHARS = 512
const MAX_STRING_OUTPUT_CHARS = 50000
const TRANSPORT_OBJECTS = new Set([
  'Socket',
  'TLSSocket',
  'HTTPParser',
  'IncomingMessage',
  'ServerResponse',
  'ClientRequest'
])
const PROMPT_FIELD_NAMES = new Set([
  'instructions',
  'input',
  'messages',
  'tools',
  'system',
  'content',
  'prompt',
  'requestbody',
  'requestbodyjson',
  'requestbodyraw',
  'requestjson',
  'bodyraw',
  'bodyjson'
])
const PAYLOAD_FIELD_NAMES = new Set([
  'body',
  'payload',
  'data',
  'request',
  'response',
  'requestbody',
  'responsebody'
])
const SENSITIVE_KEY_PATTERN =
  /^(authorization|proxy[-_]?authorization|cookie|set[-_]?cookie|password|passwd|secret|client[-_]?secret|private[-_]?key|credential|credentials|api[-_]?key|x[-_]?api[-_]?key|x[-_]?goog[-_]?api[-_]?key|access[-_]?token|refresh[-_]?token|id[-_]?token|session[-_]?token)$/i

const cleanLogString = (value) => {
  try {
    return (
      String(value)
        // eslint-disable-next-line no-control-regex
        .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, '')
        .replace(/[\uD800-\uDFFF]/g, '')
        // eslint-disable-next-line no-control-regex
        .replace(/\u0000/g, '')
    )
  } catch (error) {
    return '[Invalid String Data]'
  }
}

const truncateCleanString = (value, maxChars = MAX_LOG_STRING_CHARS) => {
  const cleanValue = cleanLogString(value)
  if (cleanValue.length <= maxChars) {
    return cleanValue
  }
  return `${cleanValue.slice(0, maxChars)}...[truncated ${cleanValue.length - maxChars} chars]`
}

const normalizeLogKey = (key) =>
  String(key || '')
    .replace(/[-_]/g, '')
    .toLowerCase()

const isSensitiveKey = (key) => SENSITIVE_KEY_PATTERN.test(String(key || ''))

const isPromptLikeKey = (key) => PROMPT_FIELD_NAMES.has(normalizeLogKey(key))

const isPayloadLikeKey = (key) => PAYLOAD_FIELD_NAMES.has(normalizeLogKey(key))

const safeObjectKeys = (value) => {
  try {
    return Object.keys(value || {})
  } catch (error) {
    return []
  }
}

const compactObject = (value) => {
  const output = {}
  for (const [key, childValue] of Object.entries(value)) {
    if (childValue !== undefined) {
      output[key] = childValue
    }
  }
  return output
}

const summarizeLogPayload = (value, options = {}) => {
  const { includePreview = false } = options

  if (value === undefined) {
    return undefined
  }
  if (value === null) {
    return null
  }
  if (Buffer.isBuffer(value)) {
    return { type: 'buffer', bytes: value.length, omitted: true }
  }
  if (typeof value === 'string') {
    const summary = { type: 'string', chars: cleanLogString(value).length, omitted: true }
    if (includePreview) {
      summary.preview = truncateCleanString(value, MAX_PAYLOAD_PREVIEW_CHARS)
    }
    return summary
  }
  if (Array.isArray(value)) {
    const firstObject = value.find((item) => item && typeof item === 'object')
    const summary = { type: 'array', length: value.length, omitted: true }
    if (firstObject) {
      summary.firstItemKeys = safeObjectKeys(firstObject).slice(0, MAX_LOG_OBJECT_KEYS)
    }
    return summary
  }
  if (value && typeof value === 'object') {
    const keys = safeObjectKeys(value)
    const summary = {
      type: value.constructor?.name === 'Object' ? 'object' : value.constructor?.name || 'object',
      keys: keys.slice(0, MAX_LOG_OBJECT_KEYS),
      omitted: true
    }
    for (const key of ['id', 'model', 'status', 'code', 'type']) {
      if (typeof value[key] === 'string' || typeof value[key] === 'number') {
        summary[key] = truncateCleanString(value[key], 256)
      }
    }
    if (typeof value.message === 'string') {
      summary.message = truncateCleanString(value.message, 512)
    }
    if (typeof value.error === 'string') {
      summary.error = truncateCleanString(value.error, 512)
    } else if (value.error && typeof value.error === 'object') {
      summary.error = summarizeLogPayload(value.error)
    }
    if (keys.length > MAX_LOG_OBJECT_KEYS) {
      summary.truncatedKeys = keys.length - MAX_LOG_OBJECT_KEYS
    }
    return summary
  }
  return value
}

const shouldSummarizePayload = (key, value) => {
  if (!isPayloadLikeKey(key)) {
    return false
  }
  if (typeof value === 'string') {
    return value.length > MAX_LOG_STRING_CHARS
  }
  if (Array.isArray(value)) {
    return value.length > MAX_LOG_ARRAY_ITEMS
  }
  if (value && typeof value === 'object') {
    const keys = safeObjectKeys(value)
    return keys.length > MAX_LOG_OBJECT_KEYS || keys.some((childKey) => isPromptLikeKey(childKey))
  }
  return false
}

const isAxiosError = (value) =>
  value && typeof value === 'object' && (value.isAxiosError === true || value.name === 'AxiosError')

const sanitizeErrorValue = (value) =>
  compactObject({
    name: value.name || 'Error',
    message: truncateCleanString(value.message || String(value), MAX_LOG_STRING_CHARS),
    code: value.code,
    status: value.status,
    statusCode: value.statusCode,
    stack: value.stack ? truncateCleanString(value.stack, MAX_LOG_STACK_CHARS) : undefined
  })

const sanitizeAxiosHeaders = (headers) => {
  if (!headers || typeof headers !== 'object') {
    return undefined
  }

  const sanitized = {}
  for (const [key, value] of Object.entries(headers).slice(0, MAX_LOG_OBJECT_KEYS)) {
    sanitized[cleanLogString(key)] = isSensitiveKey(key)
      ? '[Redacted]'
      : sanitizeLogValue(key, value, 0, new WeakSet())
  }
  const keyCount = safeObjectKeys(headers).length
  if (keyCount > MAX_LOG_OBJECT_KEYS) {
    sanitized._truncatedKeys = keyCount - MAX_LOG_OBJECT_KEYS
  }
  return sanitized
}

const sanitizeAxiosErrorValue = (value) => {
  const configValue = value.config || {}
  const response = value.response || null
  const method = response?.config?.method || configValue.method
  const url = response?.config?.url || configValue.url || value.url
  const responseData = response?.data === undefined ? undefined : summarizeLogPayload(response.data)
  const configData =
    configValue.data === undefined ? undefined : summarizeLogPayload(configValue.data)

  return compactObject({
    name: value.name || 'AxiosError',
    message: truncateCleanString(value.message || 'Axios request failed', MAX_LOG_STRING_CHARS),
    code: value.code,
    status: value.status || response?.status,
    method,
    url,
    stack: value.stack ? truncateCleanString(value.stack, MAX_LOG_STACK_CHARS) : undefined,
    config: compactObject({
      method: configValue.method,
      url: configValue.url,
      baseURL: configValue.baseURL,
      timeout: configValue.timeout,
      headers: sanitizeAxiosHeaders(configValue.headers),
      data: configData
    }),
    response: response
      ? compactObject({
          status: response.status,
          statusText: response.statusText,
          headers: sanitizeAxiosHeaders(response.headers),
          data: responseData
        })
      : undefined
  })
}

function sanitizeLogValue(key, value, depth = 0, seen = new WeakSet()) {
  if (isSensitiveKey(key)) {
    return '[Redacted]'
  }

  if (value === null || value === undefined) {
    return value
  }

  if (typeof value === 'string') {
    return isPromptLikeKey(key) ? summarizeLogPayload(value) : truncateCleanString(value)
  }

  if (typeof value === 'number' || typeof value === 'boolean') {
    return value
  }

  if (typeof value === 'bigint') {
    return value.toString()
  }

  if (typeof value === 'function') {
    return `[Function ${value.name || 'anonymous'}]`
  }

  if (Buffer.isBuffer(value)) {
    return { type: 'buffer', bytes: value.length, omitted: true }
  }

  if (value instanceof Date) {
    return value.toISOString()
  }

  if (isAxiosError(value)) {
    return sanitizeAxiosErrorValue(value)
  }

  if (value instanceof Error) {
    return sanitizeErrorValue(value)
  }

  if (isPromptLikeKey(key) || shouldSummarizePayload(key, value)) {
    return summarizeLogPayload(value)
  }

  if (depth >= MAX_LOG_DEPTH) {
    return summarizeLogPayload(value)
  }

  if (value && typeof value === 'object') {
    if (seen.has(value)) {
      return '[Circular Reference]'
    }
    seen.add(value)

    const constructorName = value.constructor?.name
    if (TRANSPORT_OBJECTS.has(constructorName)) {
      return `[${constructorName} Object]`
    }

    if (Array.isArray(value)) {
      const output = value
        .slice(0, MAX_LOG_ARRAY_ITEMS)
        .map((item, index) => sanitizeLogValue(index, item, depth + 1, seen))
      if (value.length > MAX_LOG_ARRAY_ITEMS) {
        output.push(`[${value.length - MAX_LOG_ARRAY_ITEMS} more items truncated]`)
      }
      return output
    }

    const output = {}
    const entries = Object.entries(value)
    for (const [childKey, childValue] of entries.slice(0, MAX_LOG_OBJECT_KEYS)) {
      const safeKey = cleanLogString(childKey)
      output[safeKey] = sanitizeLogValue(safeKey, childValue, depth + 1, seen)
    }
    if (entries.length > MAX_LOG_OBJECT_KEYS) {
      output._truncatedKeys = entries.length - MAX_LOG_OBJECT_KEYS
    }
    return output
  }

  return value
}

const sanitizeLogArgs = (args) => args.map((arg) => sanitizeLogValue('', arg, 0, new WeakSet()))

// 安全的 JSON 序列化函数，处理循环引用和特殊字符
const safeStringify = (obj, maxDepth = MAX_LOG_DEPTH) => {
  try {
    const processed = sanitizeLogValue('', obj, Math.max(0, MAX_LOG_DEPTH - maxDepth))
    const result = JSON.stringify(processed)
    if (typeof result === 'string' && result.length > MAX_STRING_OUTPUT_CHARS) {
      return JSON.stringify({
        _truncated: true,
        _totalChars: result.length,
        value: `${result.slice(0, MAX_STRING_OUTPUT_CHARS)}...[truncated]`
      })
    }
    return result
  } catch (error) {
    try {
      return JSON.stringify({
        error: 'Failed to serialize object',
        message: error.message,
        type: typeof obj,
        keys: obj && typeof obj === 'object' ? safeObjectKeys(obj) : undefined
      })
    } catch (finalError) {
      return '{"error":"Critical serialization failure","message":"Unable to serialize any data"}'
    }
  }
}

// 控制台不显示的 metadata 字段（已在 message 中或低价值）
const CONSOLE_SKIP_KEYS = new Set(['type', 'level', 'message', 'timestamp', 'stack'])

// 控制台格式: 树形展示 metadata
const createConsoleFormat = () =>
  winston.format.combine(
    winston.format.timestamp({ format: () => formatDateWithTimezone(new Date(), false) }),
    winston.format.errors({ stack: true }),
    winston.format.colorize(),
    winston.format.printf(({ level: _level, message, timestamp, stack, ...rest }) => {
      // 时间戳只取时分秒
      const shortTime = timestamp ? timestamp.split(' ').pop() : ''

      let logMessage = `${shortTime} ${message}`

      // 收集要显示的 metadata
      const entries = Object.entries(rest).filter(([k]) => !CONSOLE_SKIP_KEYS.has(k))

      if (entries.length > 0) {
        const indent = ' '.repeat(shortTime.length + 1)
        entries.forEach(([key, value], i) => {
          const isLast = i === entries.length - 1
          const branch = isLast ? '└─' : '├─'
          const displayValue =
            value !== null && typeof value === 'object' ? safeStringify(value) : String(value)
          logMessage += `\n${indent}${branch} ${key}: ${displayValue}`
        })
      }

      if (stack) {
        logMessage += `\n${stack}`
      }
      return logMessage
    })
  )

// 文件格式: NDJSON（完整结构化数据）
const createFileFormat = () =>
  winston.format.combine(
    winston.format.timestamp({ format: () => formatDateWithTimezone(new Date(), false) }),
    winston.format.errors({ stack: true }),
    winston.format.printf(({ level, message, timestamp, stack, ...rest }) => {
      const entry = { ts: timestamp, lvl: level, msg: message }
      // 合并所有 metadata
      for (const [k, v] of Object.entries(rest)) {
        if (k !== 'level' && k !== 'message' && k !== 'timestamp' && k !== 'stack') {
          entry[k] = v
        }
      }
      if (stack) {
        entry.stack = stack
      }
      return safeStringify(entry)
    })
  )

const fileFormat = createFileFormat()
const consoleFormat = createConsoleFormat()
const isTestEnv = process.env.NODE_ENV === 'test' || process.env.JEST_WORKER_ID

// 📁 确保日志目录存在并设置权限
if (!fs.existsSync(config.logging.dirname)) {
  fs.mkdirSync(config.logging.dirname, { recursive: true, mode: 0o755 })
}

// 🔄 增强的日志轮转配置
const createRotateTransport = (filename, level = null) => {
  const transport = new DailyRotateFile({
    filename: path.join(config.logging.dirname, filename),
    datePattern: 'YYYY-MM-DD',
    zippedArchive: true,
    maxSize: config.logging.maxSize,
    maxFiles: config.logging.maxFiles,
    auditFile: path.join(config.logging.dirname, `.${filename.replace('%DATE%', 'audit')}.json`),
    format: fileFormat
  })

  if (level) {
    transport.level = level
  }

  // 监听轮转事件（测试环境关闭以避免 Jest 退出后输出）
  if (!isTestEnv) {
    transport.on('rotate', (oldFilename, newFilename) => {
      console.log(`📦 Log rotated: ${oldFilename} -> ${newFilename}`)
    })

    transport.on('new', (newFilename) => {
      console.log(`📄 New log file created: ${newFilename}`)
    })

    transport.on('archive', (zipFilename) => {
      console.log(`🗜️ Log archived: ${zipFilename}`)
    })
  }

  return transport
}

const dailyRotateFileTransport = createRotateTransport('claude-relay-%DATE%.log')
const errorFileTransport = createRotateTransport('claude-relay-error-%DATE%.log', 'error')

// 🔒 创建专门的安全日志记录器
const securityLogger = winston.createLogger({
  level: 'warn',
  format: fileFormat,
  transports: [createRotateTransport('claude-relay-security-%DATE%.log', 'warn')],
  silent: false
})

// 🔐 创建专门的认证详细日志记录器（记录完整的认证响应）
const authDetailLogger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp({ format: () => formatDateWithTimezone(new Date(), false) }),
    winston.format.printf(({ level, message, timestamp, data }) => {
      // 使用更深的深度和格式化的JSON输出
      const jsonData = data ? JSON.stringify(data, null, 2) : '{}'
      return `[${timestamp}] ${level.toUpperCase()}: ${message}\n${jsonData}\n${'='.repeat(80)}`
    })
  ),
  transports: [createRotateTransport('claude-relay-auth-detail-%DATE%.log', 'info')],
  silent: false
})

// 🌟 增强的 Winston logger
const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || config.logging.level,
  format: fileFormat,
  transports: [
    // 📄 文件输出
    dailyRotateFileTransport,
    errorFileTransport,

    // 🖥️ 控制台输出
    new winston.transports.Console({
      format: consoleFormat,
      handleExceptions: false,
      handleRejections: false
    })
  ],

  // 🚨 异常处理
  exceptionHandlers: [
    new winston.transports.File({
      filename: path.join(config.logging.dirname, 'exceptions.log'),
      format: fileFormat,
      maxsize: 10485760, // 10MB
      maxFiles: 5
    }),
    new winston.transports.Console({
      format: consoleFormat
    })
  ],

  // 🔄 未捕获异常处理
  rejectionHandlers: [
    new winston.transports.File({
      filename: path.join(config.logging.dirname, 'rejections.log'),
      format: fileFormat,
      maxsize: 10485760, // 10MB
      maxFiles: 5
    }),
    new winston.transports.Console({
      format: consoleFormat
    })
  ],

  // 防止进程退出
  exitOnError: false
})

// 🎯 增强的自定义方法
logger.success = (message, metadata = {}) => {
  logger.info(`✅ ${message}`, { type: 'success', ...metadata })
}

logger.start = (message, metadata = {}) => {
  logger.info(`🚀 ${message}`, { type: 'startup', ...metadata })
}

logger.request = (method, url, status, duration, metadata = {}) => {
  const emoji = status >= 400 ? '🔴' : status >= 300 ? '🟡' : '🟢'
  const level = status >= 400 ? 'error' : status >= 300 ? 'warn' : 'info'

  logger[level](`${emoji} ${method} ${url} - ${status} (${duration}ms)`, {
    type: 'request',
    method,
    url,
    status,
    duration,
    ...metadata
  })
}

logger.api = (message, metadata = {}) => {
  logger.info(`🔗 ${message}`, { type: 'api', ...metadata })
}

logger.security = (message, metadata = {}) => {
  const securityData = {
    type: 'security',
    timestamp: new Date().toISOString(),
    pid: process.pid,
    hostname: os.hostname(),
    ...metadata
  }

  // 记录到主日志
  logger.warn(`🔒 ${message}`, securityData)

  // 记录到专门的安全日志文件
  try {
    securityLogger.warn(`🔒 ${message}`, securityData)
  } catch (error) {
    // 如果安全日志文件不可用，只记录到主日志
    console.warn('Security logger not available:', error.message)
  }
}

logger.database = (message, metadata = {}) => {
  logger.debug(`💾 ${message}`, { type: 'database', ...metadata })
}

logger.performance = (message, metadata = {}) => {
  logger.info(`⚡ ${message}`, { type: 'performance', ...metadata })
}

logger.audit = (message, metadata = {}) => {
  logger.info(`📋 ${message}`, {
    type: 'audit',
    timestamp: new Date().toISOString(),
    pid: process.pid,
    ...metadata
  })
}

// 🔧 性能监控方法
logger.timer = (label) => {
  const start = Date.now()
  return {
    end: (message = '', metadata = {}) => {
      const duration = Date.now() - start
      logger.performance(`${label} ${message}`, { duration, ...metadata })
      return duration
    }
  }
}

// 📊 日志统计
logger.stats = {
  requests: 0,
  errors: 0,
  warnings: 0
}

// 重写原始方法以统计
const originalError = logger.error
const originalWarn = logger.warn
const originalInfo = logger.info
const originalDebug = logger.debug

logger.error = function (message, ...args) {
  logger.stats.errors++
  return originalError.call(
    this,
    sanitizeLogValue('message', message, 0, new WeakSet()),
    ...sanitizeLogArgs(args)
  )
}

logger.warn = function (message, ...args) {
  logger.stats.warnings++
  return originalWarn.call(
    this,
    sanitizeLogValue('message', message, 0, new WeakSet()),
    ...sanitizeLogArgs(args)
  )
}

logger.info = function (message, ...args) {
  // 检查是否是请求类型的日志
  if (args.length > 0 && typeof args[0] === 'object' && args[0].type === 'request') {
    logger.stats.requests++
  }
  return originalInfo.call(
    this,
    sanitizeLogValue('message', message, 0, new WeakSet()),
    ...sanitizeLogArgs(args)
  )
}

logger.debug = function (message, ...args) {
  return originalDebug.call(
    this,
    sanitizeLogValue('message', message, 0, new WeakSet()),
    ...sanitizeLogArgs(args)
  )
}

// 📈 获取日志统计
logger.getStats = () => ({ ...logger.stats })

// 🧹 清理统计
logger.resetStats = () => {
  logger.stats.requests = 0
  logger.stats.errors = 0
  logger.stats.warnings = 0
}

// 📡 健康检查
logger.healthCheck = () => {
  try {
    const testMessage = 'Logger health check'
    logger.debug(testMessage)
    return { healthy: true, timestamp: new Date().toISOString() }
  } catch (error) {
    return { healthy: false, error: error.message, timestamp: new Date().toISOString() }
  }
}

// 🔐 记录认证详细信息的方法
logger.authDetail = (message, data = {}) => {
  try {
    // 记录到主日志（简化版）
    logger.info(`🔐 ${message}`, {
      type: 'auth-detail',
      summary: {
        hasAccessToken: !!data.access_token,
        hasRefreshToken: !!data.refresh_token,
        scopes: data.scope || data.scopes,
        organization: data.organization?.name,
        account: data.account?.email_address
      }
    })

    // 记录到专门的认证详细日志文件（完整数据）
    authDetailLogger.info(message, { data })
  } catch (error) {
    logger.error('Failed to log auth detail:', error)
  }
}

// 🎬 启动日志记录系统
logger.start('Logger initialized', {
  level: process.env.LOG_LEVEL || config.logging.level,
  directory: config.logging.dirname,
  maxSize: config.logging.maxSize,
  maxFiles: config.logging.maxFiles,
  envOverride: process.env.LOG_LEVEL ? true : false
})

if (isTestEnv) {
  logger._test = {
    safeStringify,
    sanitizeLogValue,
    sanitizeLogArgs,
    summarizeLogPayload
  }
}

module.exports = logger
