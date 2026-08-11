export const CODEX_FIVE_HOUR_WINDOW_MINUTES = 5 * 60
export const CODEX_WEEKLY_WINDOW_MINUTES = 7 * 24 * 60

const toFiniteNumber = (value) => {
  if (value === null || value === undefined || value === '') return null
  const number = Number(value)
  return Number.isFinite(number) ? number : null
}

export const hasCodexUsageWindow = (usageItem) => {
  if (!usageItem || typeof usageItem !== 'object') return false
  return [
    usageItem.usedPercent,
    usageItem.resetAfterSeconds,
    usageItem.windowMinutes,
    usageItem.resetAt,
    usageItem.remainingSeconds
  ].some((value) => value !== null && value !== undefined && value !== '')
}

const formatUnknownWindowLabel = (windowMinutes, slot) => {
  if (windowMinutes !== null && windowMinutes > 0) {
    if (windowMinutes % 1440 === 0) return `${windowMinutes / 1440}天`
    if (windowMinutes % 60 === 0) return `${windowMinutes / 60}h`
    return `${windowMinutes}m`
  }
  return slot === 'secondary' ? '次窗口' : '主窗口'
}

export const getCodexUsageWindows = (codexUsage) => {
  if (!codexUsage || typeof codexUsage !== 'object') return []

  const rawWindows = [
    { slot: 'primary', usage: codexUsage.primary },
    { slot: 'secondary', usage: codexUsage.secondary }
  ].filter(({ usage }) => hasCodexUsageWindow(usage))

  const findRawWindow = (windowMinutes) =>
    rawWindows.find(({ usage }) => toFiniteNumber(usage.windowMinutes) === windowMinutes)?.usage ||
    null

  const fiveHour = hasCodexUsageWindow(codexUsage.fiveHour)
    ? codexUsage.fiveHour
    : findRawWindow(CODEX_FIVE_HOUR_WINDOW_MINUTES)
  const weekly = hasCodexUsageWindow(codexUsage.weekly)
    ? codexUsage.weekly
    : findRawWindow(CODEX_WEEKLY_WINDOW_MINUTES)

  const windows = []
  if (fiveHour) {
    windows.push({ key: 'fiveHour', type: 'fiveHour', label: '5h', usage: fiveHour })
  }
  if (weekly) {
    windows.push({ key: 'weekly', type: 'weekly', label: '周限', usage: weekly })
  }

  for (const { slot, usage } of rawWindows) {
    const windowMinutes = toFiniteNumber(usage.windowMinutes)
    if (
      windowMinutes === CODEX_FIVE_HOUR_WINDOW_MINUTES ||
      windowMinutes === CODEX_WEEKLY_WINDOW_MINUTES
    ) {
      continue
    }

    windows.push({
      key: slot,
      type: 'other',
      label: formatUnknownWindowLabel(windowMinutes, slot),
      usage
    })
  }

  return windows
}

export const normalizeCodexUsagePercent = (usageItem) => {
  if (!usageItem) return null

  const basePercent = toFiniteNumber(usageItem.usedPercent)
  const resetAfterSeconds = toFiniteNumber(usageItem.resetAfterSeconds)
  const remainingSeconds = toFiniteNumber(usageItem.remainingSeconds)
  const resetAtMs = usageItem.resetAt ? Date.parse(usageItem.resetAt) : null
  const resetElapsed =
    resetAfterSeconds !== null &&
    ((remainingSeconds !== null && remainingSeconds <= 0) ||
      (resetAtMs !== null && !Number.isNaN(resetAtMs) && Date.now() >= resetAtMs))

  if (resetElapsed) return 0
  if (basePercent === null) return null
  return Math.max(0, Math.min(100, basePercent))
}

export const formatCodexUsagePercent = (usageItem) => {
  const percent = normalizeCodexUsagePercent(usageItem)
  return percent === null ? '--' : `${percent.toFixed(1)}%`
}

export const getCodexUsageWidth = (usageItem) => {
  const percent = normalizeCodexUsagePercent(usageItem)
  return percent === null ? '0%' : `${percent}%`
}

export const formatCodexRemaining = (usageItem) => {
  if (!usageItem) return '--'

  let seconds = toFiniteNumber(usageItem.remainingSeconds)
  if (seconds === null) seconds = toFiniteNumber(usageItem.resetAfterSeconds)
  if (seconds === null) return '--'

  seconds = Math.max(0, Math.floor(seconds))
  const days = Math.floor(seconds / 86400)
  const hours = Math.floor((seconds % 86400) / 3600)
  const minutes = Math.floor((seconds % 3600) / 60)
  const secs = seconds % 60

  if (days > 0) return hours > 0 ? `${days}天${hours}小时` : `${days}天`
  if (hours > 0) return minutes > 0 ? `${hours}小时${minutes}分钟` : `${hours}小时`
  if (minutes > 0) return `${minutes}分钟`
  return `${secs}秒`
}
