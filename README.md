# Async Data Request API (FastAPI + Redis Queue + SQLite)

![Build](https://img.shields.io/badge/build-local%20verified-brightgreen)
![Tests](https://img.shields.io/badge/tests-31%20passed-brightgreen)
![Coverage](https://img.shields.io/badge/coverage-not%20configured-lightgrey)
![License](https://img.shields.io/badge/license-unlicensed-lightgrey)

> A queue-based FastAPI service for async dimension and fact-data retrieval with idempotent submissions, structured errors, request correlation IDs, and paginated results.

## Quick Start

```bash
# 1) Install dependencies
uv sync

# 2) Start Redis/Valkey + generate sample SQLite DB
docker compose up -d && python Sample_data_generator/generate_sample_data.py

# 3) Run API (terminal 1) and worker (terminal 2)
uv run uvicorn app.main:app --reload
PYTHONPATH=. uv run python -m rq.cli worker dimension_requests --url redis://127.0.0.1:6379/0
```

Verify:

```bash
curl -s http://127.0.0.1:8000/health
# Expected: {"status":"ok"}
```

## Usage

### 1) Submit a dimension job

```bash
curl -s -X POST "http://127.0.0.1:8000/v1/jobs/advertisers" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: advertisers-demo-1" \
  -H "X-Request-ID: req-demo-1" \
  -d '{"search": "Co"}'
```

Expected response shape:

```json
{
  "job_id": "idem-...",
  "entity": "advertisers",
  "status": "queued",
  "message": "Job submitted successfully",
  "submitted_at": "2026-04-26T09:15:23+00:00",
  "request_id": "req-demo-1"
}
```

### 2) Poll for result with pagination

```bash
curl -s "http://127.0.0.1:8000/v1/jobs/advertisers/<job_id>?page=1&page_size=100" \
  -H "X-Request-ID: req-demo-2"
```

Expected response shape:

```json
{
  "job_id": "...",
  "entity": "advertisers",
  "status": "finished",
  "total": 234,
  "items": [
    {"id": "ADV1", "name": "Alpha Co"}
  ],
  "pagination": {
    "page": 1,
    "page_size": 100,
    "total_items": 234,
    "total_pages": 3,
    "has_next": true,
    "has_previous": false
  },
  "request_id": "req-demo-2"
}
```

### 3) Submit ad-metrics fact retrieval

```bash
curl -s -X POST "http://127.0.0.1:8000/v1/jobs/ad-metrics" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: facts-demo-1" \
  -d '{
    "advertiser_id": "ADV1",
    "report_start_date": "2026-01-01",
    "report_end_date": "2026-01-31"
  }'
```

## Configuration

All configuration is environment-driven.

| Variable | Description | Default | Required |
|---|---|---|---|
| APP_NAME | API display name | Redis Queue Data Request API | No |
| APP_VERSION | API version label | 0.1.0 | No |
| APP_ENV | Runtime environment (development/staging/production) | development | No |
| DEBUG | Enables debug behavior | true in local/dev | No |
| LOG_LEVEL | Log level (DEBUG/INFO/WARNING/ERROR/CRITICAL) | env-based | No |
| LOG_JSON | Use JSON structured logs | true | No |
| ENABLE_OPENTELEMETRY | Toggle tracing instrumentation | true in production, or when DEBUG=true | No |
| SLOW_REQUEST_WARNING_MS | Warn if request latency exceeds this | 5000 | No |
| SQLITE_DB_PATH | SQLite DB file path | master_clientdata.db | No |
| DEFAULT_PAGE_SIZE | Default result page size | 250 | No |
| MAX_PAGE_SIZE | Maximum allowed page size | 1000 | No |
| REDIS_HOST | Redis host | 127.0.0.1 | No |
| REDIS_PORT | Redis port | 6379 | No |
| REDIS_DB | Redis database index | 0 | No |
| REDIS_PASSWORD | Redis password | - | No |
| REDIS_QUEUE_NAME | Queue name | dimension_requests | No |
| REDIS_RETRY_ATTEMPTS | Redis connect retries | 3 | No |
| REDIS_RETRY_BACKOFF_MS | Backoff between retries (ms) | 200 | No |
| REDIS_CONNECT_TIMEOUT_SECONDS | Connect timeout | 3 | No |
| REDIS_SOCKET_TIMEOUT_SECONDS | Socket timeout | 5 | No |
| JOB_TIMEOUT_SECONDS | Worker job timeout | 60 | No |
| JOB_RESULT_TTL_SECONDS | Finished job result retention | 600 | No |
| JOB_FAILURE_TTL_SECONDS | Failed job retention | 1200 | No |

Example `.env`:

```bash
APP_ENV=development
LOG_LEVEL=DEBUG
SQLITE_DB_PATH=master_clientdata.db
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
DEFAULT_PAGE_SIZE=250
MAX_PAGE_SIZE=1000
```

## API Reference

Base path: `/v1`

### Health

#### GET /health

Response:

```json
{"status":"ok"}
```

### Dimension Jobs

#### POST /v1/jobs/advertisers
#### POST /v1/jobs/campaigns
#### POST /v1/jobs/placements
#### POST /v1/jobs/creatives

Request body:

```json
{
  "search": "optional case-insensitive text"
}
```

Response (202 Accepted):

```json
{
  "job_id": "...",
  "entity": "advertisers",
  "status": "queued",
  "message": "Job submitted successfully",
  "submitted_at": "...",
  "request_id": "..."
}
```

#### GET /v1/jobs/advertisers/{job_id}
#### GET /v1/jobs/campaigns/{job_id}
#### GET /v1/jobs/placements/{job_id}
#### GET /v1/jobs/creatives/{job_id}

Query parameters:
- `page` (default 1)
- `page_size` (default from config)

### Fact Metrics Jobs

#### POST /v1/jobs/ad-metrics

Request body:

```json
{
  "advertiser_id": "ADV1",
  "campaign_id": null,
  "placement_id": null,
  "creative_id": null,
  "report_start_date": "2026-01-01",
  "report_end_date": "2026-01-31"
}
```

#### GET /v1/jobs/ad-metrics/{job_id}

Same pagination query parameters as dimension retrieval endpoints.

### Error Response Contract

All API errors follow this structure:

```json
{
  "error": {
    "code": "INVALID_PAGE_SIZE",
    "message": "page_size must be <= 1000",
    "request_id": "req-123",
    "details": {
      "field": "page_size"
    }
  }
}
```

Common error codes and statuses:
- `INVALID_REQUEST` -> 422
- `INVALID_PAGE_SIZE` -> 422
- `JOB_NOT_FOUND` -> 404
- `QUEUE_UNAVAILABLE` -> 503
- `DB_NOT_FOUND` -> 500
- `INTERNAL_SERVER_ERROR` -> 500

## Development Notes

- Swagger UI may prefill optional string fields with `"string"`; submit `{}` or clear that value if you want an unfiltered query.
- Idempotency deduplicates repeated submit calls only when key and payload are the same.
- Request IDs can be passed via `X-Request-ID`; if omitted, one is generated and returned in response headers.
