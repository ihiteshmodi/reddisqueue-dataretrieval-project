from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app.interfaces.api as api_module
import app.main as app_main
from app.infrastructure.redis_queue import QueueUnavailableError
from app.interfaces.schemas import JobResultResponse, JobSubmissionResponse, PaginationMeta


class _HappyPathManager:
    def submit(self, entity, request, idempotency_key=None, request_id=None):
        return JobSubmissionResponse(
            job_id="job-123",
            entity=entity,
            status="queued",
            message="Job submitted successfully",
            submitted_at=datetime.now(UTC),
            request_id=request_id,
        )

    def submit_fact_metrics(self, request, idempotency_key=None, request_id=None):
        return JobSubmissionResponse(
            job_id="fact-123",
            entity="ad_metrics_daily",
            status="queued",
            message="Job submitted successfully",
            submitted_at=datetime.now(UTC),
            request_id=request_id,
        )

    def get_result(self, entity, job_id, page, page_size):
        items = [
            {"id": "ADV1", "name": "Alpha Co"},
            {"id": "ADV2", "name": "Beta Co"},
        ]
        return JobResultResponse(
            job_id=job_id,
            entity=entity,
            status="finished",
            total=2,
            items=items,
            pagination=PaginationMeta(
                page=page,
                page_size=page_size,
                total_items=2,
                total_pages=1,
                has_next=False,
                has_previous=False,
            ),
        )


class _StatefulFlowManager:
    def __init__(self) -> None:
        self._jobs: dict[str, list[dict[str, str]]] = {}
        self._idem_key_to_job_id: dict[tuple[str, str, str | None], str] = {}
        self._counter = 0

    def submit(self, entity, request, idempotency_key=None, request_id=None):
        idem_key = (entity, request.search, idempotency_key)
        if idempotency_key and idem_key in self._idem_key_to_job_id:
            job_id = self._idem_key_to_job_id[idem_key]
            return JobSubmissionResponse(
                job_id=job_id,
                entity=entity,
                status="queued",
                message="Existing job returned for idempotency key",
                submitted_at=datetime.now(UTC),
                request_id=request_id,
            )

        self._counter += 1
        job_id = f"job-{self._counter}"
        self._jobs[job_id] = [
            {"id": "ADV1", "name": "Alpha Co"},
            {"id": "ADV2", "name": "Beta Co"},
            {"id": "ADV3", "name": "Gamma Labs"},
        ]
        if idempotency_key:
            self._idem_key_to_job_id[idem_key] = job_id
        return JobSubmissionResponse(
            job_id=job_id,
            entity=entity,
            status="queued",
            message="Job submitted successfully",
            submitted_at=datetime.now(UTC),
            request_id=request_id,
        )

    def submit_fact_metrics(self, request, idempotency_key=None, request_id=None):
        return JobSubmissionResponse(
            job_id="fact-1",
            entity="ad_metrics_daily",
            status="queued",
            message="Job submitted successfully",
            submitted_at=datetime.now(UTC),
            request_id=request_id,
        )

    def get_result(self, entity, job_id, page, page_size):
        if job_id not in self._jobs:
            return JobResultResponse(job_id=job_id, entity=entity, status="not_found", error="Job not found")

        all_items = self._jobs[job_id]
        start = (page - 1) * page_size
        end = start + page_size
        page_items = all_items[start:end]
        total = len(all_items)
        total_pages = (total + page_size - 1) // page_size
        return JobResultResponse(
            job_id=job_id,
            entity=entity,
            status="finished",
            total=total,
            items=page_items,
            pagination=PaginationMeta(
                page=page,
                page_size=page_size,
                total_items=total,
                total_pages=total_pages,
                has_next=page < total_pages,
                has_previous=page > 1,
            ),
        )


def _build_test_client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, raise_server_exceptions: bool = True) -> TestClient:
    db_path = tmp_path / "test.db"
    db_path.write_text("ready")

    patched_settings = replace(app_main.settings, sqlite_db_path=db_path)
    monkeypatch.setattr(app_main, "settings", patched_settings)
    monkeypatch.setattr(app_main, "create_redis_connection", lambda _settings: None)

    return TestClient(app_main.app, raise_server_exceptions=raise_server_exceptions)


def test_submit_endpoint_returns_202_and_request_id(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(api_module, "_get_cached_job_manager", lambda: _HappyPathManager())
    with _build_test_client(monkeypatch, tmp_path) as client:
        response = client.post(
            "/v1/jobs/advertisers",
            json={},
            headers={"X-Request-ID": "req-submit-1"},
        )

    assert response.status_code == 202
    body = response.json()
    assert response.headers["X-Request-ID"] == "req-submit-1"
    assert body["job_id"] == "job-123"
    assert body["status"] == "queued"
    assert body["request_id"] == "req-submit-1"


def test_get_result_returns_paginated_items(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(api_module, "_get_cached_job_manager", lambda: _HappyPathManager())
    with _build_test_client(monkeypatch, tmp_path) as client:
        response = client.get(
            "/v1/jobs/advertisers/job-123?page=1&page_size=2",
            headers={"X-Request-ID": "req-result-1"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "finished"
    assert body["total"] == 2
    assert len(body["items"]) == 2
    assert body["pagination"]["page"] == 1
    assert body["pagination"]["page_size"] == 2
    assert body["request_id"] == "req-result-1"


def test_invalid_page_size_returns_structured_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(api_module, "_get_cached_job_manager", lambda: _HappyPathManager())
    with _build_test_client(monkeypatch, tmp_path) as client:
        response = client.get("/v1/jobs/advertisers/job-123?page=1&page_size=1001")

    assert response.status_code == 422
    body = response.json()
    assert body["error"]["code"] == "INVALID_PAGE_SIZE"


def test_queue_unavailable_returns_503_structured_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def _raise_queue_unavailable():
        raise QueueUnavailableError("queue down")

    monkeypatch.setattr(api_module, "_get_cached_job_manager", _raise_queue_unavailable)
    with _build_test_client(monkeypatch, tmp_path) as client:
        response = client.get("/v1/jobs/advertisers/job-123")

    assert response.status_code == 503
    body = response.json()
    assert body["error"]["code"] == "QUEUE_UNAVAILABLE"


def test_validation_error_uses_structured_error_schema(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(api_module, "_get_cached_job_manager", lambda: _HappyPathManager())
    with _build_test_client(monkeypatch, tmp_path) as client:
        response = client.post("/v1/jobs/advertisers", json={"search": "x" * 101})

    assert response.status_code == 422
    body = response.json()
    assert body["error"]["code"] == "INVALID_REQUEST"
    assert "validation_errors" in body["error"]["details"]


def test_unhandled_exception_returns_safe_500(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    class _BoomManager(_HappyPathManager):
        def get_result(self, entity, job_id, page, page_size):
            raise RuntimeError("exploded")

    monkeypatch.setattr(api_module, "_get_cached_job_manager", lambda: _BoomManager())
    with _build_test_client(monkeypatch, tmp_path, raise_server_exceptions=False) as client:
        response = client.get("/v1/jobs/advertisers/job-123", headers={"X-Request-ID": "req-err-1"})

    assert response.status_code == 500
    body = response.json()
    assert body["error"]["code"] == "INTERNAL_SERVER_ERROR"
    assert body["error"]["request_id"] == "req-err-1"


def test_e2e_style_submit_then_poll_and_idempotency(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    manager = _StatefulFlowManager()
    monkeypatch.setattr(api_module, "_get_cached_job_manager", lambda: manager)

    with _build_test_client(monkeypatch, tmp_path) as client:
        first = client.post(
            "/v1/jobs/advertisers",
            json={"search": "Co"},
            headers={"Idempotency-Key": "idem-e2e-1"},
        )
        second = client.post(
            "/v1/jobs/advertisers",
            json={"search": "Co"},
            headers={"Idempotency-Key": "idem-e2e-1"},
        )

        assert first.status_code == 202
        assert second.status_code == 202
        first_body = first.json()
        second_body = second.json()
        assert first_body["job_id"] == second_body["job_id"]
        assert second_body["message"] == "Existing job returned for idempotency key"

        result = client.get(f"/v1/jobs/advertisers/{first_body['job_id']}?page=1&page_size=2")

    assert result.status_code == 200
    result_body = result.json()
    assert result_body["status"] == "finished"
    assert result_body["total"] == 3
    assert len(result_body["items"]) == 2
    assert result_body["pagination"]["has_next"] is True
