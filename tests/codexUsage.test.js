const {
  CODEX_FIVE_HOUR_WINDOW_MINUTES,
  CODEX_WEEKLY_WINDOW_MINUTES,
  buildCodexUsageSnapshot,
  classifyCodexUsageWindows,
  computeResetAtFromDelay,
  computeResetMeta,
  extractCodexUsageHeaders
} = require('../src/utils/codexUsage')

describe('Codex usage windows', () => {
  test('extracts reset durations and window lengths from Codex response headers', () => {
    expect(
      extractCodexUsageHeaders({
        'X-Codex-Primary-Used-Percent': '12.5',
        'X-Codex-Primary-Reset-After-Seconds': '7200',
        'X-Codex-Primary-Window-Minutes': '10080',
        'X-Codex-Secondary-Window-Minutes': '300'
      })
    ).toMatchObject({
      primaryUsedPercent: 12.5,
      primaryResetAfterSeconds: 7200,
      primaryWindowMinutes: CODEX_WEEKLY_WINDOW_MINUTES,
      secondaryWindowMinutes: CODEX_FIVE_HOUR_WINDOW_MINUTES
    })
  })

  test('classifies windows by duration instead of primary/secondary position', () => {
    const primary = { usedPercent: 25, windowMinutes: CODEX_WEEKLY_WINDOW_MINUTES }
    const secondary = { usedPercent: 40, windowMinutes: CODEX_FIVE_HOUR_WINDOW_MINUTES }

    const result = classifyCodexUsageWindows(primary, secondary)

    expect(result.fiveHour).toBe(secondary)
    expect(result.weekly).toBe(primary)
    expect(result.otherWindows).toEqual([])
  })

  test('keeps only a weekly window when the account has no 5h bucket', () => {
    const nowMs = Date.parse('2026-08-05T00:00:00.000Z')
    const snapshot = buildCodexUsageSnapshot(
      {
        codexUsageUpdatedAt: '2026-08-05T00:00:00.000Z',
        codexPrimaryUsedPercent: '5',
        codexPrimaryResetAfterSeconds: '244800',
        codexPrimaryWindowMinutes: '10080'
      },
      nowMs
    )

    expect(snapshot.fiveHour).toBeNull()
    expect(snapshot.weekly).toBe(snapshot.primary)
    expect(snapshot.weekly.remainingSeconds).toBe(244800)
  })

  test('prefers a persisted absolute reset time over the shared snapshot timestamp', () => {
    const nowMs = Date.parse('2026-08-05T12:00:00.000Z')
    const persistedResetAt = '2026-08-06T00:00:00.000Z'

    expect(computeResetMeta('2026-08-05T11:59:00.000Z', 604800, persistedResetAt, nowMs)).toEqual({
      resetAt: persistedResetAt,
      remainingSeconds: 43200
    })
  })

  test('does not turn a missing reset delay into an immediate reset time', () => {
    const capturedAt = Date.parse('2026-08-05T00:00:00.000Z')

    expect(computeResetAtFromDelay(null, capturedAt)).toBeNull()
    expect(computeResetAtFromDelay(undefined, capturedAt)).toBeNull()
    expect(computeResetAtFromDelay('', capturedAt)).toBeNull()
    expect(computeResetAtFromDelay('18000', capturedAt)).toBe('2026-08-05T05:00:00.000Z')
  })

  test('supports legacy snapshots that only stored reset-after seconds', () => {
    const nowMs = Date.parse('2026-08-05T01:00:00.000Z')

    expect(computeResetMeta('2026-08-05T00:00:00.000Z', 18000, null, nowMs)).toEqual({
      resetAt: '2026-08-05T05:00:00.000Z',
      remainingSeconds: 14400
    })
  })
})
