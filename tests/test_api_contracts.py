from __future__ import annotations

from typing import Any, cast

from fastapi import Request

from app.application.job_manager import _build_idempotency_job_id
from app.interfaces.api import _error_detail


def test_idempotency_job_id_is_stable_for_same_input() -> None:
    payload = {"search": None, "_request_id": "req-1"}
    first = _build_idempotency_job_id("idem-key", "advertisers", payload)
    second = _build_idempotency_job_id("idem-key", "advertisers", payload)
    assert first == second


def test_idempotency_job_id_changes_when_payload_changes() -> None:
    payload_a = {"search": None}
    payload_b = {"search": "Acme"}
    first = _build_idempotency_job_id("idem-key", "advertisers", payload_a)
    second = _build_idempotency_job_id("idem-key", "advertisers", payload_b)
    assert first != second


def test_error_detail_has_code_message_and_request_id() -> None:
    scope = {"type": "http", "method": "GET", "path": "/", "headers": []}
    request = Request(scope)
    request.state.request_id = "req-123"

    payload = _error_detail(request, "JOB_NOT_FOUND", "Job not found")
    error = cast(dict[str, Any], payload["error"])

    assert error["code"] == "JOB_NOT_FOUND"
    assert error["message"] == "Job not found"
    assert error["request_id"] == "req-123"
