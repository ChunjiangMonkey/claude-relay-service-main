const CODEX_FIVE_HOUR_WINDOW_MINUTES = 5 * 60
const CODEX_WEEKLY_WINDOW_MINUTES = 7 * 24 * 60

function normalizeHeaders(headers = {}) {
  if (!headers || typeof headers !== 'object') {
    return {}
  }

  const normalized = {}
  for (const [key, value] of Object.entries(headers)) {
    if (!key) {
      continue
    }
    normalized[key.toLowerCase()] = Array.isArray(value) ? value[0] : value
  }
  return normalized
}

function toNumberSafe(value) {
  if (value === undefined || value === null || value === '') {
    return null
  }
  const num = Number(value)
  return Number.isFinite(num) ? num : null
}

function hasCodexUsageWindow(window) {
  if (!window || typeof window !== 'object') {
    return false
  }

  return [
    window.usedPercent,
    window.resetAfterSeconds,
    window.windowMinutes,
    window.resetAt,
    window.remainingSeconds
  ].some((value) => value !== null && value !== undefined && value !== '')
}

function computeResetAtFromDelay(resetAfterSeconds, capturedAtMs = Date.now()) {
  const resetAfter = toNumberSafe(resetAfterSeconds)
  const capturedMs = Number(capturedAtMs)

  if (resetAfter === null || resetAfter < 0 || !Number.isFinite(capturedMs)) {
    return null
  }

  return new Date(capturedMs + resetAfter * 1000).toISOString()
}

function computeResetMeta(
  updatedAt,
  resetAfterSeconds,
  persistedResetAt = null,
  nowMs = Date.now()
) {
  let resetMs = persistedResetAt ? Date.parse(persistedResetAt) : NaN

  if (!Number.isFinite(resetMs)) {
    const updatedMs = updatedAt ? Date.parse(updatedAt) : NaN
    const resetAfter = toNumberSafe(resetAfterSeconds)
    if (!Number.isFinite(updatedMs) || resetAfter === null) {
      return {
        resetAt: null,
        remainingSeconds: null
      }
    }
    resetMs = updatedMs + resetAfter * 1000
  }

  return {
    resetAt: new Date(resetMs).toISOString(),
    remainingSeconds: Math.max(0, Math.round((resetMs - nowMs) / 1000))
  }
}

function classifyCodexUsageWindows(primary, secondary) {
  const result = {
    fiveHour: null,
    weekly: null,
    otherWindows: []
  }

  for (const [slot, window] of [
    ['primary', primary],
    ['secondary', secondary]
  ]) {
    if (!hasCodexUsageWindow(window)) {
      continue
    }

    const windowMinutes = toNumberSafe(window.windowMinutes)
    if (windowMinutes === CODEX_FIVE_HOUR_WINDOW_MINUTES && !result.fiveHour) {
      result.fiveHour = window
    } else if (windowMinutes === CODEX_WEEKLY_WINDOW_MINUTES && !result.weekly) {
      result.weekly = window
    } else {
      result.otherWindows.push({ slot, ...window })
    }
  }

  return result
}

function buildCodexUsageSnapshot(accountData = {}, nowMs = Date.now()) {
  const updatedAt = accountData.codexUsageUpdatedAt || null

  const primaryResetAfterSeconds = toNumberSafe(accountData.codexPrimaryResetAfterSeconds)
  const secondaryResetAfterSeconds = toNumberSafe(accountData.codexSecondaryResetAfterSeconds)
  const primaryMeta = computeResetMeta(
    updatedAt,
    primaryResetAfterSeconds,
    accountData.codexPrimaryResetAt,
    nowMs
  )
  const secondaryMeta = computeResetMeta(
    updatedAt,
    secondaryResetAfterSeconds,
    accountData.codexSecondaryResetAt,
    nowMs
  )

  const primary = {
    usedPercent: toNumberSafe(accountData.codexPrimaryUsedPercent),
    resetAfterSeconds: primaryResetAfterSeconds,
    windowMinutes: toNumberSafe(accountData.codexPrimaryWindowMinutes),
    resetAt: primaryMeta.resetAt,
    remainingSeconds: primaryMeta.remainingSeconds
  }
  const secondary = {
    usedPercent: toNumberSafe(accountData.codexSecondaryUsedPercent),
    resetAfterSeconds: secondaryResetAfterSeconds,
    windowMinutes: toNumberSafe(accountData.codexSecondaryWindowMinutes),
    resetAt: secondaryMeta.resetAt,
    remainingSeconds: secondaryMeta.remainingSeconds
  }

  if (!updatedAt && !hasCodexUsageWindow(primary) && !hasCodexUsageWindow(secondary)) {
    return null
  }

  return {
    updatedAt,
    primary,
    secondary,
    ...classifyCodexUsageWindows(primary, secondary),
    primaryOverSecondaryPercent: toNumberSafe(accountData.codexPrimaryOverSecondaryLimitPercent)
  }
}

function extractCodexUsageHeaders(headers) {
  const normalized = normalizeHeaders(headers)
  if (Object.keys(normalized).length === 0) {
    return null
  }

  const snapshot = {
    primaryUsedPercent: toNumberSafe(normalized['x-codex-primary-used-percent']),
    primaryResetAfterSeconds: toNumberSafe(normalized['x-codex-primary-reset-after-seconds']),
    primaryWindowMinutes: toNumberSafe(normalized['x-codex-primary-window-minutes']),
    secondaryUsedPercent: toNumberSafe(normalized['x-codex-secondary-used-percent']),
    secondaryResetAfterSeconds: toNumberSafe(normalized['x-codex-secondary-reset-after-seconds']),
    secondaryWindowMinutes: toNumberSafe(normalized['x-codex-secondary-window-minutes']),
    primaryOverSecondaryPercent: toNumberSafe(
      normalized['x-codex-primary-over-secondary-limit-percent']
    )
  }

  return Object.values(snapshot).some((value) => value !== null) ? snapshot : null
}

module.exports = {
  CODEX_FIVE_HOUR_WINDOW_MINUTES,
  CODEX_WEEKLY_WINDOW_MINUTES,
  extractCodexUsageHeaders,
  buildCodexUsageSnapshot,
  classifyCodexUsageWindows,
  computeResetAtFromDelay,
  computeResetMeta,
  hasCodexUsageWindow
}
