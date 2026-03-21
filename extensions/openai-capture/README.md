# OpenAI Upstream Capture (Phase 1)

This extension captures **OpenAI upstream** traffic into JSONL files only. It does not write to MySQL/PostgreSQL yet.

## Scope

- `chatgpt.com/backend-api/codex/responses`
- `chatgpt.com/backend-api/codex/responses/compact`
- `api.openai.com` / compatible `baseApi` response endpoints that match:
  - `/v1/responses`
  - `/responses`

Only `HTTPS` is supported in phase 1.

## Output files

- `openai-upstream-requests.jsonl`
- `openai-upstream-responses.jsonl`
- `openai-upstream-stream-final.jsonl`

## Env

```bash
OPENAI_CAPTURE_ENABLED=true
OPENAI_CAPTURE_DIR=/data/relay-capture
OPENAI_CAPTURE_HOSTS=api.openai.com,chatgpt.com
OPENAI_CAPTURE_METHODS=POST
OPENAI_CAPTURE_PATH_PREFIXES=/v1/responses,/responses,/backend-api/codex/responses
OPENAI_CAPTURE_MAX_RECORD_BYTES=16777216
OPENAI_CAPTURE_MAX_FILE_BYTES=268435456
OPENAI_CAPTURE_BACKUP_FILES=3
OPENAI_CAPTURE_INCLUDE_REASONING=false
OPENAI_CAPTURE_DEBUG=false
```

When Anthropic and OpenAI capture are both enabled, keep `ANTHROPIC_CAPTURE_DIR` and `OPENAI_CAPTURE_DIR` the same.

## Runtime injection

Use the shared bootstrap so Anthropic and OpenAI hooks can coexist:

```bash
NODE_OPTIONS=--require /app/extensions/upstream-capture/bootstrap.js
```

Optional link helper:

```bash
sh /app/extensions/upstream-capture/setup-capture-links.sh
```

## Record notes

- Request records include:
  - `authorization_raw`
  - `authorization_sha256`
  - `provider_kind`
  - `request_body_raw`
  - `request_body_json`
- Response records include:
  - non-stream `body_raw` / `body_json`
  - stream final reconstruction
  - `usage`
  - `response_id`
  - `model`
  - `status`
  - `assistant_text_full`
  - `reasoning_text_full` (if enabled)
  - `tool_calls`

Sensitive headers remain masked in the `headers` object. The raw credential is stored separately in `authorization_raw` by design for OpenAI phase 1.
