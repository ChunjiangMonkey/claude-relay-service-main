CREATE TABLE IF NOT EXISTS upstream_events_raw (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  event_hash VARCHAR(64) NOT NULL,
  trace_id VARCHAR(255),
  event_type VARCHAR(128) NOT NULL,
  event_ts DATETIME(3),
  source_file TEXT NOT NULL,
  payload JSON NOT NULL,
  ingested_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_upstream_events_raw_hash (event_hash),
  KEY idx_upstream_events_raw_trace_id (trace_id),
  KEY idx_upstream_events_raw_event_ts (event_ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS anthropic_interactions (
  trace_id VARCHAR(255) PRIMARY KEY,
  upstream_request_id VARCHAR(255),
  model VARCHAR(255),
  is_stream BOOLEAN,
  request_json JSON,
  response_json JSON,
  assistant_text_full LONGTEXT,
  tool_calls JSON,
  usage_json JSON,
  stop_reason VARCHAR(255),
  http_status INT,
  latency_ms INT,
  status VARCHAR(64),
  first_seen_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  last_seen_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  KEY idx_anthropic_interactions_status (status),
  KEY idx_anthropic_interactions_model (model),
  KEY idx_anthropic_interactions_last_seen (last_seen_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS collector_offsets (
  file_path VARCHAR(512) PRIMARY KEY,
  inode VARCHAR(64),
  offset BIGINT NOT NULL,
  remainder LONGTEXT,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ingest_errors (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  source_file TEXT,
  raw_line LONGTEXT,
  error TEXT,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
