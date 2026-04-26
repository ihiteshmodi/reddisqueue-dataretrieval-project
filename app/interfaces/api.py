from __future__ import annotations

from functools import lru_cache

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Query, Request, status

from app.application.job_manager import JobManager, get_job_manager
from app.infrastructure.config import get_settings
from app.infrastructure.redis_queue import QueueUnavailableError
from app.interfaces.schemas import (
	ApiErrorResponse,
	DimensionQueryRequest,
	FactMetricsQueryRequest,
	JobResultResponse,
	JobSubmissionResponse,
)

router = APIRouter(prefix="/v1", tags=["dimension-requests"])


@lru_cache(maxsize=1)
def _get_cached_job_manager() -> JobManager:
	settings = get_settings()
	return get_job_manager(settings)


def _request_id(request: Request) -> str | None:
	return getattr(request.state, "request_id", None)


def _error_detail(
	request: Request,
	code: str,
	message: str,
	details: dict[str, str | int | float | bool | None] | None = None,
) -> dict[str, object]:
	error = ApiErrorResponse.model_validate(
		{
			"error": {
				"code": code,
				"message": message,
				"request_id": _request_id(request),
				"details": details,
			}
		}
	)
	return error.model_dump(exclude_none=True)


def _manager_dependency(request: Request) -> JobManager:
	try:
		return _get_cached_job_manager()
	except QueueUnavailableError as exc:
		raise HTTPException(
			status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
			detail=_error_detail(request, "QUEUE_UNAVAILABLE", "Queue unavailable"),
		) from exc


def _submit_dimension_request(
	entity: str,
	request: DimensionQueryRequest,
	request_context: Request,
	idempotency_key: str | None,
	manager: JobManager,
) -> JobSubmissionResponse:
	try:
		response = manager.submit(
			entity=entity,
			request=request,
			idempotency_key=idempotency_key,
			request_id=_request_id(request_context),
		)
		response.request_id = _request_id(request_context)
		return response
	except ValueError as exc:
		raise HTTPException(
			status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
			detail=_error_detail(request_context, "INVALID_REQUEST", str(exc)),
		) from exc
	except FileNotFoundError as exc:
		raise HTTPException(
			status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
			detail=_error_detail(request_context, "DB_NOT_FOUND", str(exc)),
		) from exc


def _submit_fact_request(
	request: FactMetricsQueryRequest,
	request_context: Request,
	idempotency_key: str | None,
	manager: JobManager,
) -> JobSubmissionResponse:
	try:
		response = manager.submit_fact_metrics(
			request=request,
			idempotency_key=idempotency_key,
			request_id=_request_id(request_context),
		)
		response.request_id = _request_id(request_context)
		return response
	except ValueError as exc:
		raise HTTPException(
			status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
			detail=_error_detail(request_context, "INVALID_REQUEST", str(exc)),
		) from exc
	except FileNotFoundError as exc:
		raise HTTPException(
			status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
			detail=_error_detail(request_context, "DB_NOT_FOUND", str(exc)),
		) from exc


def _get_dimension_result(
	entity: str,
	job_id: str,
	request_context: Request,
	page: int,
	page_size: int | None,
	manager: JobManager,
) -> JobResultResponse:
	settings = get_settings()
	effective_page_size = page_size or settings.default_page_size
	if effective_page_size > settings.max_page_size:
		raise HTTPException(
			status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
			detail=_error_detail(
				request_context,
				"INVALID_PAGE_SIZE",
				f"page_size must be <= {settings.max_page_size}",
			),
		)
	response = manager.get_result(
		entity=entity,
		job_id=job_id,
		page=page,
		page_size=effective_page_size,
	)
	if response.status == "not_found":
		raise HTTPException(
			status_code=status.HTTP_404_NOT_FOUND,
			detail=_error_detail(request_context, "JOB_NOT_FOUND", response.error or "Job not found"),
		)
	response.request_id = _request_id(request_context)
	return response


@router.post("/jobs/advertisers", response_model=JobSubmissionResponse, status_code=status.HTTP_202_ACCEPTED)
def submit_advertisers_request(
	request_context: Request,
	request: DimensionQueryRequest = Body(default_factory=DimensionQueryRequest),
	idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
	manager: JobManager = Depends(_manager_dependency),
) -> JobSubmissionResponse:
	return _submit_dimension_request("advertisers", request, request_context, idempotency_key, manager)


@router.get("/jobs/advertisers/{job_id}", response_model=JobResultResponse)
def get_advertisers_result(
	request_context: Request,
	job_id: str,
	page: int = Query(default=1, ge=1),
	page_size: int | None = Query(default=None, ge=1),
	manager: JobManager = Depends(_manager_dependency),
) -> JobResultResponse:
	return _get_dimension_result("advertisers", job_id, request_context, page, page_size, manager)


@router.post("/jobs/campaigns", response_model=JobSubmissionResponse, status_code=status.HTTP_202_ACCEPTED)
def submit_campaigns_request(
	request_context: Request,
	request: DimensionQueryRequest = Body(default_factory=DimensionQueryRequest),
	idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
	manager: JobManager = Depends(_manager_dependency),
) -> JobSubmissionResponse:
	return _submit_dimension_request("campaigns", request, request_context, idempotency_key, manager)


@router.get("/jobs/campaigns/{job_id}", response_model=JobResultResponse)
def get_campaigns_result(
	request_context: Request,
	job_id: str,
	page: int = Query(default=1, ge=1),
	page_size: int | None = Query(default=None, ge=1),
	manager: JobManager = Depends(_manager_dependency),
) -> JobResultResponse:
	return _get_dimension_result("campaigns", job_id, request_context, page, page_size, manager)


@router.post("/jobs/placements", response_model=JobSubmissionResponse, status_code=status.HTTP_202_ACCEPTED)
def submit_placements_request(
	request_context: Request,
	request: DimensionQueryRequest = Body(default_factory=DimensionQueryRequest),
	idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
	manager: JobManager = Depends(_manager_dependency),
) -> JobSubmissionResponse:
	return _submit_dimension_request("placements", request, request_context, idempotency_key, manager)


@router.get("/jobs/placements/{job_id}", response_model=JobResultResponse)
def get_placements_result(
	request_context: Request,
	job_id: str,
	page: int = Query(default=1, ge=1),
	page_size: int | None = Query(default=None, ge=1),
	manager: JobManager = Depends(_manager_dependency),
) -> JobResultResponse:
	return _get_dimension_result("placements", job_id, request_context, page, page_size, manager)


@router.post("/jobs/creatives", response_model=JobSubmissionResponse, status_code=status.HTTP_202_ACCEPTED)
def submit_creatives_request(
	request_context: Request,
	request: DimensionQueryRequest = Body(default_factory=DimensionQueryRequest),
	idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
	manager: JobManager = Depends(_manager_dependency),
) -> JobSubmissionResponse:
	return _submit_dimension_request("creatives", request, request_context, idempotency_key, manager)


@router.get("/jobs/creatives/{job_id}", response_model=JobResultResponse)
def get_creatives_result(
	request_context: Request,
	job_id: str,
	page: int = Query(default=1, ge=1),
	page_size: int | None = Query(default=None, ge=1),
	manager: JobManager = Depends(_manager_dependency),
) -> JobResultResponse:
	return _get_dimension_result("creatives", job_id, request_context, page, page_size, manager)


@router.post("/jobs/ad-metrics", response_model=JobSubmissionResponse, status_code=status.HTTP_202_ACCEPTED)
def submit_fact_metrics_request(
	request_context: Request,
	request: FactMetricsQueryRequest = Body(default_factory=FactMetricsQueryRequest),
	idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
	manager: JobManager = Depends(_manager_dependency),
) -> JobSubmissionResponse:
	return _submit_fact_request(request, request_context, idempotency_key, manager)


@router.get("/jobs/ad-metrics/{job_id}", response_model=JobResultResponse)
def get_fact_metrics_result(
	request_context: Request,
	job_id: str,
	page: int = Query(default=1, ge=1),
	page_size: int | None = Query(default=None, ge=1),
	manager: JobManager = Depends(_manager_dependency),
) -> JobResultResponse:
	return _get_dimension_result("ad_metrics_daily", job_id, request_context, page, page_size, manager)
