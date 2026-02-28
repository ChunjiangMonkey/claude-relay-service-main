'use strict'

const fs = require('fs')
const path = require('path')
const mysql = require('mysql2/promise')

function createMysqlAdapter(config) {
  const pool = mysql.createPool({
    host: config.mysql.host,
    port: config.mysql.port,
    user: config.mysql.user,
    password: config.mysql.password,
    database: config.mysql.database,
    connectionLimit: config.dbPoolMax,
    waitForConnections: true,
    queueLimit: 0,
    charset: 'utf8mb4',
    timezone: 'Z',
    multipleStatements: true,
    ssl: config.mysql.ssl ? {} : undefined
  })

  return {
    backend: 'mysql',
    ensureSchema,
    loadOffsets,
    insertRawEvent,
    upsertRequest,
    upsertNonStreamResponse,
    upsertStreamFinal,
    upsertStreamSummary,
    upsertTransportError,
    persistOffset,
    insertIngestError,
    close
  }

  async function ensureSchema() {
    const sqlPath = path.join(__dirname, 'sql', 'mysql.sql')
    const schemaSql = fs.readFileSync(sqlPath, 'utf8')
    await pool.query(schemaSql)
  }

  async function loadOffsets() {
    const [rows] = await pool.query('SELECT file_path, inode, offset, remainder FROM collector_offsets')
    return rows
  }

  async function insertRawEvent(record) {
    const [result] = await pool.execute(
      `
      INSERT IGNORE INTO upstream_events_raw (event_hash, trace_id, event_type, event_ts, source_file, payload)
      VALUES (?, ?, ?, ?, ?, ?)
      `,
      [
        record.eventHash,
        record.traceId,
        record.eventType,
        toMysqlDateTime(record.eventTs),
        record.sourceFile,
        toJsonString(record.payload)
      ]
    )

    return result.affectedRows > 0
  }

  async function upsertRequest(record) {
    await pool.execute(
      `
      INSERT INTO anthropic_interactions (
        trace_id,
        upstream_request_id,
        model,
        is_stream,
        request_json,
        status,
        first_seen_at,
        last_seen_at,
        created_at,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, 'request_captured', CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3))
      ON DUPLICATE KEY UPDATE
        upstream_request_id = COALESCE(VALUES(upstream_request_id), upstream_request_id),
        model = COALESCE(VALUES(model), model),
        is_stream = COALESCE(VALUES(is_stream), is_stream),
        request_json = COALESCE(VALUES(request_json), request_json),
        last_seen_at = CURRENT_TIMESTAMP(3),
        updated_at = CURRENT_TIMESTAMP(3)
      `,
      [record.traceId, record.upstreamRequestId, record.model, record.isStream, toJsonString(record.requestJson)]
    )
  }

  async function upsertNonStreamResponse(record) {
    await pool.execute(
      `
      INSERT INTO anthropic_interactions (
        trace_id,
        upstream_request_id,
        model,
        is_stream,
        response_json,
        usage,
        stop_reason,
        http_status,
        latency_ms,
        status,
        first_seen_at,
        last_seen_at,
        created_at,
        updated_at
      ) VALUES (
        ?,
        ?,
        ?,
        FALSE,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        CURRENT_TIMESTAMP(3),
        CURRENT_TIMESTAMP(3),
        CURRENT_TIMESTAMP(3),
        CURRENT_TIMESTAMP(3)
      )
      ON DUPLICATE KEY UPDATE
        upstream_request_id = COALESCE(VALUES(upstream_request_id), upstream_request_id),
        model = COALESCE(VALUES(model), model),
        is_stream = FALSE,
        response_json = COALESCE(VALUES(response_json), response_json),
        usage = COALESCE(VALUES(usage), usage),
        stop_reason = COALESCE(VALUES(stop_reason), stop_reason),
        http_status = COALESCE(VALUES(http_status), http_status),
        latency_ms = COALESCE(VALUES(latency_ms), latency_ms),
        status = VALUES(status),
        last_seen_at = CURRENT_TIMESTAMP(3),
        updated_at = CURRENT_TIMESTAMP(3)
      `,
      [
        record.traceId,
        record.upstreamRequestId,
        record.model,
        toJsonString(record.responseJson),
        toJsonString(record.usage),
        record.stopReason,
        record.httpStatus,
        record.latencyMs,
        record.status
      ]
    )
  }

  async function upsertStreamFinal(record) {
    await pool.execute(
      `
      INSERT INTO anthropic_interactions (
        trace_id,
        upstream_request_id,
        model,
        is_stream,
        assistant_text_full,
        tool_calls,
        usage,
        stop_reason,
        http_status,
        latency_ms,
        status,
        first_seen_at,
        last_seen_at,
        created_at,
        updated_at
      ) VALUES (
        ?,
        ?,
        ?,
        TRUE,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        ?,
        CURRENT_TIMESTAMP(3),
        CURRENT_TIMESTAMP(3),
        CURRENT_TIMESTAMP(3),
        CURRENT_TIMESTAMP(3)
      )
      ON DUPLICATE KEY UPDATE
        upstream_request_id = COALESCE(VALUES(upstream_request_id), upstream_request_id),
        model = COALESCE(VALUES(model), model),
        is_stream = TRUE,
        assistant_text_full = COALESCE(VALUES(assistant_text_full), assistant_text_full),
        tool_calls = COALESCE(VALUES(tool_calls), tool_calls),
        usage = COALESCE(VALUES(usage), usage),
        stop_reason = COALESCE(VALUES(stop_reason), stop_reason),
        http_status = COALESCE(VALUES(http_status), http_status),
        latency_ms = COALESCE(VALUES(latency_ms), latency_ms),
        status = VALUES(status),
        last_seen_at = CURRENT_TIMESTAMP(3),
        updated_at = CURRENT_TIMESTAMP(3)
      `,
      [
        record.traceId,
        record.upstreamRequestId,
        record.model,
        record.assistantTextFull,
        toJsonString(record.toolCalls),
        toJsonString(record.usage),
        record.stopReason,
        record.httpStatus,
        record.latencyMs,
        record.status
      ]
    )
  }

  async function upsertStreamSummary(record) {
    await pool.execute(
      `
      INSERT INTO anthropic_interactions (
        trace_id,
        is_stream,
        usage,
        stop_reason,
        latency_ms,
        status,
        first_seen_at,
        last_seen_at,
        created_at,
        updated_at
      ) VALUES (?, TRUE, ?, ?, ?, ?, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3))
      ON DUPLICATE KEY UPDATE
        is_stream = TRUE,
        usage = COALESCE(VALUES(usage), usage),
        stop_reason = COALESCE(VALUES(stop_reason), stop_reason),
        latency_ms = COALESCE(VALUES(latency_ms), latency_ms),
        status = COALESCE(status, VALUES(status)),
        last_seen_at = CURRENT_TIMESTAMP(3),
        updated_at = CURRENT_TIMESTAMP(3)
      `,
      [record.traceId, toJsonString(record.usage), record.stopReason, record.latencyMs, record.status]
    )
  }

  async function upsertTransportError(record) {
    await pool.execute(
      `
      INSERT INTO anthropic_interactions (
        trace_id,
        upstream_request_id,
        model,
        is_stream,
        http_status,
        latency_ms,
        status,
        first_seen_at,
        last_seen_at,
        created_at,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, 'transport_error', CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3))
      ON DUPLICATE KEY UPDATE
        upstream_request_id = COALESCE(VALUES(upstream_request_id), upstream_request_id),
        model = COALESCE(VALUES(model), model),
        is_stream = COALESCE(VALUES(is_stream), is_stream),
        http_status = COALESCE(VALUES(http_status), http_status),
        latency_ms = COALESCE(VALUES(latency_ms), latency_ms),
        status = 'transport_error',
        last_seen_at = CURRENT_TIMESTAMP(3),
        updated_at = CURRENT_TIMESTAMP(3)
      `,
      [
        record.traceId,
        record.upstreamRequestId,
        record.model,
        record.isStream,
        record.httpStatus,
        record.latencyMs
      ]
    )
  }

  async function persistOffset(record) {
    await pool.execute(
      `
      INSERT INTO collector_offsets (file_path, inode, offset, remainder, updated_at)
      VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP(3))
      ON DUPLICATE KEY UPDATE
        inode = VALUES(inode),
        offset = VALUES(offset),
        remainder = VALUES(remainder),
        updated_at = CURRENT_TIMESTAMP(3)
      `,
      [record.filePath, record.inode, record.offset, record.remainder]
    )
  }

  async function insertIngestError(record) {
    await pool.execute(
      `
      INSERT INTO ingest_errors (source_file, raw_line, error)
      VALUES (?, ?, ?)
      `,
      [record.sourceFile, record.rawLine, record.error]
    )
  }

  async function close() {
    await pool.end()
  }
}

function toJsonString(value) {
  if (value === null || value === undefined) {
    return null
  }
  return JSON.stringify(value)
}

function toMysqlDateTime(value) {
  if (!value) {
    return null
  }

  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return null
  }

  const yyyy = date.getUTCFullYear()
  const mm = String(date.getUTCMonth() + 1).padStart(2, '0')
  const dd = String(date.getUTCDate()).padStart(2, '0')
  const hh = String(date.getUTCHours()).padStart(2, '0')
  const mi = String(date.getUTCMinutes()).padStart(2, '0')
  const ss = String(date.getUTCSeconds()).padStart(2, '0')
  const mmm = String(date.getUTCMilliseconds()).padStart(3, '0')

  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}.${mmm}`
}

module.exports = {
  createMysqlAdapter
}
