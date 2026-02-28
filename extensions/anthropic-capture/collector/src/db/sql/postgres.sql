CREATE TABLE IF NOT EXISTS upstream_events_raw (
  id BIGSERIAL PRIMARY KEY,
  event_hash TEXT NOT NULL UNIQUE,
  trace_id TEXT,
  event_type TEXT NOT NULL,
  event_ts TIMESTAMPTZ,
  source_file TEXT NOT NULL,
  payload JSONB NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_upstream_events_raw_trace_id ON upstream_events_raw(trace_id);
CREATE INDEX IF NOT EXISTS idx_upstream_events_raw_event_ts ON upstream_events_raw(event_ts DESC);

CREATE TABLE IF NOT EXISTS anthropic_interactions (
  trace_id TEXT PRIMARY KEY,
  upstream_request_id TEXT,
  model TEXT,
  is_stream BOOLEAN,
  request_json JSONB,
  response_json JSONB,
  assistant_text_full TEXT,
  tool_calls JSONB,
  usage JSONB,
  stop_reason TEXT,
  http_status INTEGER,
  latency_ms INTEGER,
  status TEXT,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_anthropic_interactions_status ON anthropic_interactions(status);
CREATE INDEX IF NOT EXISTS idx_anthropic_interactions_model ON anthropic_interactions(model);
CREATE INDEX IF NOT EXISTS idx_anthropic_interactions_last_seen ON anthropic_interactions(last_seen_at DESC);

CREATE TABLE IF NOT EXISTS collector_offsets (
  file_path TEXT PRIMARY KEY,
  inode TEXT,
  offset BIGINT NOT NULL,
  remainder TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ingest_errors (
  id BIGSERIAL PRIMARY KEY,
  source_file TEXT,
  raw_line TEXT,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
