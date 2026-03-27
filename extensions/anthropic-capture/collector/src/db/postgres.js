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
    upsertOpenaiRequest,
    upsertOpenaiResponse,
    persistOffset,
    insertIngestError,
    close
  }

  async function ensureSchema() {
    const sqlPath = path.join(__dirname, 'sql', 'postgres.sql')
    const schemaSql = fs.readFileSync(sqlPath, 'utf8')
    await pool.query(schemaSql)
    await pool.query(
      'ALTER TABLE anthropic_interactions ADD COLUMN IF NOT EXISTS thought_text_full TEXT'
    )
    await pool.query(
      'ALTER TABLE anthropic_interactions ADD COLUMN IF NOT EXISTS response_message_id TEXT'
    )
    await pool.query(
      'ALTER TABLE anthropic_interactions ADD COLUMN IF NOT EXISTS relay_key_id TEXT'
    )
    await pool.query('ALTER TABLE openai_interactions ADD COLUMN IF NOT EXISTS relay_key_id TEXT')
    await pool.query(
      'CREATE INDEX IF NOT EXISTS idx_anthropic_relay_key ON anthropic_interactions(relay_key_id)'
    )
    await pool.query(
      'CREATE INDEX IF NOT EXISTS idx_openai_relay_key ON openai_interactions(relay_key_id)'
    )
  }

  async function loadOffsets() {
    const result = await pool.query(
      'SELECT file_path, inode, offset, remainder FROM collector_offsets'
    )
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
        relay_key_id,
        status,
        first_seen_at,
        last_seen_at,
        created_at,
        updated_at
      ) VALUES ($1, $2, $3, $4, $5::jsonb, $6, 'request_captured', NOW(), NOW(), NOW(), NOW())
      ON CONFLICT (trace_id) DO UPDATE SET
        upstream_request_id = COALESCE(EXCLUDED.upstream_request_id, anthropic_interactions.upstream_request_id),
        model = COALESCE(EXCLUDED.model, anthropic_interactions.model),
        is_stream = COALESCE(EXCLUDED.is_stream, anthropic_interactions.is_stream),
        request_json = COALESCE(EXCLUDED.request_json, anthropic_interactions.request_json),
        relay_key_id = COALESCE(EXCLUDED.relay_key_id, anthropic_interactions.relay_key_id),
        last_seen_at = NOW(),
        updated_at = NOW()
      `,
      [
        record.traceId,
        record.upstreamRequestId,
        record.model,
        record.isStream,
        toJsonString(record.requestJson),
        record.relayKeyId
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
        relay_key_id,
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
        $10,
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
        relay_key_id = COALESCE(EXCLUDED.relay_key_id, anthropic_interactions.relay_key_id),
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
        record.relayKeyId,
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
        thought_text_full,
        response_message_id,
        tool_calls,
        usage,
        stop_reason,
        http_status,
        latency_ms,
        relay_key_id,
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
        $5,
        $6,
        $7::jsonb,
        $8::jsonb,
        $9,
        $10,
        $11,
        $12,
        $13,
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
        thought_text_full = COALESCE(EXCLUDED.thought_text_full, anthropic_interactions.thought_text_full),
        response_message_id = COALESCE(EXCLUDED.response_message_id, anthropic_interactions.response_message_id),
        tool_calls = COALESCE(EXCLUDED.tool_calls, anthropic_interactions.tool_calls),
        usage = COALESCE(EXCLUDED.usage, anthropic_interactions.usage),
        stop_reason = COALESCE(EXCLUDED.stop_reason, anthropic_interactions.stop_reason),
        http_status = COALESCE(EXCLUDED.http_status, anthropic_interactions.http_status),
        latency_ms = COALESCE(EXCLUDED.latency_ms, anthropic_interactions.latency_ms),
        relay_key_id = COALESCE(EXCLUDED.relay_key_id, anthropic_interactions.relay_key_id),
        status = EXCLUDED.status,
        last_seen_at = NOW(),
        updated_at = NOW()
      `,
      [
        record.traceId,
        record.upstreamRequestId,
        record.model,
        record.assistantTextFull,
        record.thoughtTextFull,
        record.responseMessageId,
        toJsonString(record.toolCalls),
        toJsonString(record.usage),
        record.stopReason,
        record.httpStatus,
        record.latencyMs,
        record.relayKeyId,
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
        relay_key_id,
        status,
        first_seen_at,
        last_seen_at,
        created_at,
        updated_at
      ) VALUES ($1, true, $2::jsonb, $3, $4, $5, $6, NOW(), NOW(), NOW(), NOW())
      ON CONFLICT (trace_id) DO UPDATE SET
        is_stream = TRUE,
        usage = COALESCE(EXCLUDED.usage, anthropic_interactions.usage),
        stop_reason = COALESCE(EXCLUDED.stop_reason, anthropic_interactions.stop_reason),
        latency_ms = COALESCE(EXCLUDED.latency_ms, anthropic_interactions.latency_ms),
        relay_key_id = COALESCE(EXCLUDED.relay_key_id, anthropic_interactions.relay_key_id),
        status = COALESCE(anthropic_interactions.status, EXCLUDED.status),
        last_seen_at = NOW(),
        updated_at = NOW()
      `,
      [
        record.traceId,
        toJsonString(record.usage),
        record.stopReason,
        record.latencyMs,
        record.relayKeyId,
        record.status
      ]
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
        relay_key_id,
        status,
        first_seen_at,
        last_seen_at,
        created_at,
        updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'transport_error', NOW(), NOW(), NOW(), NOW())
      ON CONFLICT (trace_id) DO UPDATE SET
        upstream_request_id = COALESCE(EXCLUDED.upstream_request_id, anthropic_interactions.upstream_request_id),
        model = COALESCE(EXCLUDED.model, anthropic_interactions.model),
        is_stream = COALESCE(EXCLUDED.is_stream, anthropic_interactions.is_stream),
        http_status = COALESCE(EXCLUDED.http_status, anthropic_interactions.http_status),
        latency_ms = COALESCE(EXCLUDED.latency_ms, anthropic_interactions.latency_ms),
        relay_key_id = COALESCE(EXCLUDED.relay_key_id, anthropic_interactions.relay_key_id),
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
        record.latencyMs,
        record.relayKeyId
      ]
    )
  }

  async function upsertOpenaiRequest(record) {
    await pool.query(
      `
      INSERT INTO openai_interactions (
        trace_id,
        provider_kind,
        model,
        is_stream,
        request_json,
        relay_key_id,
        status,
        first_seen_at,
        last_seen_at,
        created_at,
        updated_at
      ) VALUES ($1, $2, $3, $4, $5::jsonb, $6, 'request_captured', NOW(), NOW(), NOW(), NOW())
      ON CONFLICT (trace_id) DO UPDATE SET
        provider_kind = COALESCE(EXCLUDED.provider_kind, openai_interactions.provider_kind),
        model = COALESCE(EXCLUDED.model, openai_interactions.model),
        is_stream = COALESCE(EXCLUDED.is_stream, openai_interactions.is_stream),
        request_json = COALESCE(EXCLUDED.request_json, openai_interactions.request_json),
        relay_key_id = COALESCE(EXCLUDED.relay_key_id, openai_interactions.relay_key_id),
        last_seen_at = NOW(),
        updated_at = NOW()
      `,
      [
        record.traceId,
        record.providerKind,
        record.model,
        record.isStream,
        toJsonString(record.requestJson),
        record.relayKeyId
      ]
    )
  }

  async function upsertOpenaiResponse(record) {
    await pool.query(
      `
      INSERT INTO openai_interactions (
        trace_id,
        provider_kind,
        model,
        is_stream,
        response_id,
        assistant_text_full,
        reasoning_text_full,
        tool_calls,
        usage_json,
        input_tokens,
        output_tokens,
        total_tokens,
        cached_tokens,
        reasoning_tokens,
        http_status,
        latency_ms,
        relay_key_id,
        status,
        first_seen_at,
        last_seen_at,
        created_at,
        updated_at
      ) VALUES (
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8::jsonb,
        $9::jsonb,
        $10,
        $11,
        $12,
        $13,
        $14,
        $15,
        $16,
        $17,
        $18,
        NOW(),
        NOW(),
        NOW(),
        NOW()
      )
      ON CONFLICT (trace_id) DO UPDATE SET
        provider_kind = COALESCE(EXCLUDED.provider_kind, openai_interactions.provider_kind),
        model = COALESCE(EXCLUDED.model, openai_interactions.model),
        is_stream = COALESCE(EXCLUDED.is_stream, openai_interactions.is_stream),
        response_id = COALESCE(EXCLUDED.response_id, openai_interactions.response_id),
        assistant_text_full = COALESCE(EXCLUDED.assistant_text_full, openai_interactions.assistant_text_full),
        reasoning_text_full = COALESCE(EXCLUDED.reasoning_text_full, openai_interactions.reasoning_text_full),
        tool_calls = COALESCE(EXCLUDED.tool_calls, openai_interactions.tool_calls),
        usage_json = COALESCE(EXCLUDED.usage_json, openai_interactions.usage_json),
        input_tokens = COALESCE(EXCLUDED.input_tokens, openai_interactions.input_tokens),
        output_tokens = COALESCE(EXCLUDED.output_tokens, openai_interactions.output_tokens),
        total_tokens = COALESCE(EXCLUDED.total_tokens, openai_interactions.total_tokens),
        cached_tokens = COALESCE(EXCLUDED.cached_tokens, openai_interactions.cached_tokens),
        reasoning_tokens = COALESCE(EXCLUDED.reasoning_tokens, openai_interactions.reasoning_tokens),
        http_status = COALESCE(EXCLUDED.http_status, openai_interactions.http_status),
        latency_ms = COALESCE(EXCLUDED.latency_ms, openai_interactions.latency_ms),
        relay_key_id = COALESCE(EXCLUDED.relay_key_id, openai_interactions.relay_key_id),
        status = EXCLUDED.status,
        last_seen_at = NOW(),
        updated_at = NOW()
      `,
      [
        record.traceId,
        record.providerKind,
        record.model,
        Boolean(record.isStream),
        record.responseId,
        record.assistantTextFull,
        record.reasoningTextFull,
        toJsonString(record.toolCalls),
        toJsonString(record.usageJson),
        record.inputTokens,
        record.outputTokens,
        record.totalTokens,
        record.cachedTokens,
        record.reasoningTokens,
        record.httpStatus,
        record.latencyMs,
        record.relayKeyId,
        record.status
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
