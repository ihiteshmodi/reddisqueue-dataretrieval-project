# reddisqueue-datarequest-project

## Sample Data Generator

Run from workspace root:

```bash
python Sample_data_generator/generate_sample_data.py
```

This creates `master_clientdata.db` in the working directory and recreates it on every run.

## FastAPI Queue Flow

This project now exposes 8 endpoints:

- 4 submit endpoints (`POST`) to enqueue a Redis job.
- 4 retrieval endpoints (`GET`) to fetch status/result by `job_id`.

Request headers:

- `Idempotency-Key` (optional): deduplicates repeated submit requests with same payload.
- `X-Request-ID` (optional): correlation ID propagated to response and worker logs.

Retrieval endpoints support pagination query params:

- `page` (default `1`)
- `page_size` (default from config, currently `250`)

### Endpoints

- `POST /v1/jobs/advertisers`
- `GET /v1/jobs/advertisers/{job_id}`
- `POST /v1/jobs/campaigns`
- `GET /v1/jobs/campaigns/{job_id}`
- `POST /v1/jobs/placements`
- `GET /v1/jobs/placements/{job_id}`
- `POST /v1/jobs/creatives`
- `GET /v1/jobs/creatives/{job_id}`

### Request Body for Submit Endpoints

Body is optional. If omitted, endpoint returns all available values.

```json
{
	"search": null
}
```

### Run Locally

1. Start Valkey/Redis:

```bash
docker compose up -d
```

2. Install dependencies:

```bash
uv sync
```

3. Ensure DB exists (or generate it):

```bash
python Sample_data_generator/generate_sample_data.py
```

4. Start API:

```bash
uv run uvicorn app.main:app --reload
```

5. Start worker (separate terminal):

```bash
PYTHONPATH=. uv run python -m rq.cli worker dimension_requests --url redis://127.0.0.1:6379/0
```

### Example

Submit advertisers request:

```bash
curl -s -X POST http://127.0.0.1:8000/v1/jobs/advertisers \
	-H "Content-Type: application/json" \
	-d '{}'
```

Response (example):

```json
{
	"job_id": "a0f4783b-650e-4b8f-b20b-ec77c52e3caf",
	"entity": "advertisers",
	"status": "queued",
	"message": "Job submitted successfully",
	"submitted_at": "2026-04-26T09:15:23+00:00",
	"request_id": "9f8c6fa7-bb63-4eb9-b06d-c5e11236a5f0"
}
```

Fetch status/result:

```bash
curl -s "http://127.0.0.1:8000/v1/jobs/advertisers/<job_id>?page=1&page_size=50"
```
