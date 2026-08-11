import test from 'node:test'
import assert from 'node:assert/strict'

import { formatCodexRemaining, getCodexUsageWindows } from '../src/utils/codexUsage.js'

test('labels a primary seven-day bucket as weekly instead of 5h', () => {
  const windows = getCodexUsageWindows({
    primary: {
      usedPercent: 5,
      windowMinutes: 10080,
      remainingSeconds: 244800
    },
    secondary: null
  })

  assert.equal(windows.length, 1)
  assert.equal(windows[0].type, 'weekly')
  assert.equal(windows[0].label, '周限')
  assert.equal(formatCodexRemaining(windows[0].usage), '2天20小时')
})

test('identifies 5h and weekly buckets regardless of slot order', () => {
  const windows = getCodexUsageWindows({
    primary: { usedPercent: 30, windowMinutes: 10080, remainingSeconds: 86400 },
    secondary: { usedPercent: 20, windowMinutes: 300, remainingSeconds: 7200 }
  })

  assert.deepEqual(
    windows.map(({ type, label }) => ({ type, label })),
    [
      { type: 'fiveHour', label: '5h' },
      { type: 'weekly', label: '周限' }
    ]
  )
})

test('uses normalized backend fields and hides empty quota slots', () => {
  const weekly = { usedPercent: 10, windowMinutes: 10080, remainingSeconds: 3600 }
  const windows = getCodexUsageWindows({
    primary: { usedPercent: null, windowMinutes: null, remainingSeconds: null },
    secondary: null,
    fiveHour: null,
    weekly
  })

  assert.equal(windows.length, 1)
  assert.equal(windows[0].usage, weekly)
  assert.equal(windows[0].type, 'weekly')
})

test('shows an honest duration label for an unfamiliar quota window', () => {
  const windows = getCodexUsageWindows({
    primary: { usedPercent: 10, windowMinutes: 60, remainingSeconds: 1800 }
  })

  assert.equal(windows.length, 1)
  assert.equal(windows[0].label, '1h')
  assert.equal(windows[0].type, 'other')
})
