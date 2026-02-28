'use strict'

const fs = require('fs')
const path = require('path')
const { Pool } = require('pg')

function createPostgresAdapter(config) {
  const pool = new Pool({
    connectionString: config.databaseUrl,
    max: config.dbPoolMax
  })

  return {
    backend: 'postgres',
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
    const sqlPath = path.join(__dirname, 'sql', 'postgres.sql')
    const schemaSql = fs.readFileSync(sqlPath, 'utf8')
    await pool.query(schemaSql)
  }

  async function loadOffsets() {
    const result = await pool.query('SELECT file_path, inode, offset, remainder FROM collector_offsets')
    return result.rows
  }

  async function insertRawEvent(record) {
    const result = await pool.query(
      `
      INSERT INTO upstream_events_raw (event_hash, trace_id, event_type, event_ts, source_file, payload)
      VALUES ($1, $2, $3, $4, $5, $6::jsonb)
      ON CONFLICT (event_hash) DO NOTHING
      RETURNING id
      `,
      [
        record.eventHash,
        record.traceId,
        record.eventType,
        record.eventTs,
        record.sourceFile,
        JSON.stringify(record.payload)
      ]
    )

    return result.rowCount > 0
  }

  async function upsertRequest(record) {
    await pool.query(
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
      ) VALUES ($1, $2, $3, $4, $5::jsonb, 'request_captured', NOW(), NOW(), NOW(), NOW())
      ON CONFLICT (trace_id) DO UPDATE SET
        upstream_request_id = COALESCE(EXCLUDED.upstream_request_id, anthropic_interactions.upstream_request_id),
        model = COALESCE(EXCLUDED.model, anthropic_interactions.model),
        is_stream = COALESCE(EXCLUDED.is_stream, anthropic_interactions.is_stream),
        request_json = COALESCE(EXCLUDED.request_json, anthropic_interactions.request_json),
        last_seen_at = NOW(),
        updated_at = NOW()
      `,
      [
        record.traceId,
        record.upstreamRequestId,
        record.model,
        record.isStream,
        toJsonString(record.requestJson)
      ]
    )
  }

  async function upsertNonStreamResponse(record) {
    await pool.query(
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
        $1,
        $2,
        $3,
        false,
        $4::jsonb,
        $5::jsonb,
        $6,
        $7,
        $8,
        $9,
        NOW(),
        NOW(),
        NOW(),
        NOW()
      )
      ON CONFLICT (trace_id) DO UPDATE SET
        upstream_request_id = COALESCE(EXCLUDED.upstream_request_id, anthropic_interactions.upstream_request_id),
        model = COALESCE(EXCLUDED.model, anthropic_interactions.model),
        is_stream = FALSE,
        response_json = COALESCE(EXCLUDED.response_json, anthropic_interactions.response_json),
        usage = COALESCE(EXCLUDED.usage, anthropic_interactions.usage),
        stop_reason = COALESCE(EXCLUDED.stop_reason, anthropic_interactions.stop_reason),
        http_status = COALESCE(EXCLUDED.http_status, anthropic_interactions.http_status),
        latency_ms = COALESCE(EXCLUDED.latency_ms, anthropic_interactions.latency_ms),
        status = EXCLUDED.status,
        last_seen_at = NOW(),
        updated_at = NOW()
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
    await pool.query(
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
        $1,
        $2,
        $3,
        true,
        $4,
        $5::jsonb,
        $6::jsonb,
        $7,
        $8,
        $9,
        $10,
        NOW(),
        NOW(),
        NOW(),
        NOW()
      )
      ON CONFLICT (trace_id) DO UPDATE SET
        upstream_request_id = COALESCE(EXCLUDED.upstream_request_id, anthropic_interactions.upstream_request_id),
        model = COALESCE(EXCLUDED.model, anthropic_interactions.model),
        is_stream = TRUE,
        assistant_text_full = COALESCE(EXCLUDED.assistant_text_full, anthropic_interactions.assistant_text_full),
        tool_calls = COALESCE(EXCLUDED.tool_calls, anthropic_interactions.tool_calls),
        usage = COALESCE(EXCLUDED.usage, anthropic_interactions.usage),
        stop_reason = COALESCE(EXCLUDED.stop_reason, anthropic_interactions.stop_reason),
        http_status = COALESCE(EXCLUDED.http_status, anthropic_interactions.http_status),
        latency_ms = COALESCE(EXCLUDED.latency_ms, anthropic_interactions.latency_ms),
        status = EXCLUDED.status,
        last_seen_at = NOW(),
        updated_at = NOW()
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
    await pool.query(
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
      ) VALUES ($1, true, $2::jsonb, $3, $4, $5, NOW(), NOW(), NOW(), NOW())
      ON CONFLICT (trace_id) DO UPDATE SET
        is_stream = TRUE,
        usage = COALESCE(EXCLUDED.usage, anthropic_interactions.usage),
        stop_reason = COALESCE(EXCLUDED.stop_reason, anthropic_interactions.stop_reason),
        latency_ms = COALESCE(EXCLUDED.latency_ms, anthropic_interactions.latency_ms),
        status = COALESCE(anthropic_interactions.status, EXCLUDED.status),
        last_seen_at = NOW(),
        updated_at = NOW()
      `,
      [record.traceId, toJsonString(record.usage), record.stopReason, record.latencyMs, record.status]
    )
  }

  async function upsertTransportError(record) {
    await pool.query(
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
      ) VALUES ($1, $2, $3, $4, $5, $6, 'transport_error', NOW(), NOW(), NOW(), NOW())
      ON CONFLICT (trace_id) DO UPDATE SET
        upstream_request_id = COALESCE(EXCLUDED.upstream_request_id, anthropic_interactions.upstream_request_id),
        model = COALESCE(EXCLUDED.model, anthropic_interactions.model),
        is_stream = COALESCE(EXCLUDED.is_stream, anthropic_interactions.is_stream),
        http_status = COALESCE(EXCLUDED.http_status, anthropic_interactions.http_status),
        latency_ms = COALESCE(EXCLUDED.latency_ms, anthropic_interactions.latency_ms),
        status = 'transport_error',
        last_seen_at = NOW(),
        updated_at = NOW()
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
    await pool.query(
      `
      INSERT INTO collector_offsets (file_path, inode, offset, remainder, updated_at)
      VALUES ($1, $2, $3, $4, NOW())
      ON CONFLICT (file_path) DO UPDATE SET
        inode = EXCLUDED.inode,
        offset = EXCLUDED.offset,
        remainder = EXCLUDED.remainder,
        updated_at = NOW()
      `,
      [record.filePath, record.inode, record.offset, record.remainder]
    )
  }

  async function insertIngestError(record) {
    await pool.query(
      `
      INSERT INTO ingest_errors (source_file, raw_line, error)
      VALUES ($1, $2, $3)
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

module.exports = {
  createPostgresAdapter
}
