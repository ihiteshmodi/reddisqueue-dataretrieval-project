from __future__ import annotations

from functools import lru_cache

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status

from app.application.job_manager import JobManager, get_job_manager
from app.infrastructure.config import get_settings
from app.infrastructure.redis_queue import QueueUnavailableError
from app.interfaces.schemas import (
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


def _manager_dependency() -> JobManager:
	try:
		return _get_cached_job_manager()
	except QueueUnavailableError as exc:
		raise HTTPException(
			status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
			detail="Queue unavailable",
		) from exc


def _submit_dimension_request(
	entity: str,
	request: DimensionQueryRequest,
	manager: JobManager,
) -> JobSubmissionResponse:
	try:
		return manager.submit(entity=entity, request=request)
	except ValueError as exc:
		raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
	except FileNotFoundError as exc:
		raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


def _submit_fact_request(
	request: FactMetricsQueryRequest,
	manager: JobManager,
) -> JobSubmissionResponse:
	try:
		return manager.submit_fact_metrics(request=request)
	except ValueError as exc:
		raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
	except FileNotFoundError as exc:
		raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


def _get_dimension_result(
	entity: str,
	job_id: str,
	page: int,
	page_size: int | None,
	manager: JobManager,
) -> JobResultResponse:
	settings = get_settings()
	effective_page_size = page_size or settings.default_page_size
	if effective_page_size > settings.max_page_size:
		raise HTTPException(
			status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
			detail=f"page_size must be <= {settings.max_page_size}",
		)
	response = manager.get_result(
		entity=entity,
		job_id=job_id,
		page=page,
		page_size=effective_page_size,
	)
	if response.status == "not_found":
		raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=response.error)
	return response


@router.post("/jobs/advertisers", response_model=JobSubmissionResponse, status_code=status.HTTP_202_ACCEPTED)
def submit_advertisers_request(
	request: DimensionQueryRequest = Body(default_factory=DimensionQueryRequest),
	manager: JobManager = Depends(_manager_dependency),
) -> JobSubmissionResponse:
	return _submit_dimension_request("advertisers", request, manager)


@router.get("/jobs/advertisers/{job_id}", response_model=JobResultResponse)
def get_advertisers_result(
	job_id: str,
	page: int = Query(default=1, ge=1),
	page_size: int | None = Query(default=None, ge=1),
	manager: JobManager = Depends(_manager_dependency),
) -> JobResultResponse:
	return _get_dimension_result("advertisers", job_id, page, page_size, manager)


@router.post("/jobs/campaigns", response_model=JobSubmissionResponse, status_code=status.HTTP_202_ACCEPTED)
def submit_campaigns_request(
	request: DimensionQueryRequest = Body(default_factory=DimensionQueryRequest),
	manager: JobManager = Depends(_manager_dependency),
) -> JobSubmissionResponse:
	return _submit_dimension_request("campaigns", request, manager)


@router.get("/jobs/campaigns/{job_id}", response_model=JobResultResponse)
def get_campaigns_result(
	job_id: str,
	page: int = Query(default=1, ge=1),
	page_size: int | None = Query(default=None, ge=1),
	manager: JobManager = Depends(_manager_dependency),
) -> JobResultResponse:
	return _get_dimension_result("campaigns", job_id, page, page_size, manager)


@router.post("/jobs/placements", response_model=JobSubmissionResponse, status_code=status.HTTP_202_ACCEPTED)
def submit_placements_request(
	request: DimensionQueryRequest = Body(default_factory=DimensionQueryRequest),
	manager: JobManager = Depends(_manager_dependency),
) -> JobSubmissionResponse:
	return _submit_dimension_request("placements", request, manager)


@router.get("/jobs/placements/{job_id}", response_model=JobResultResponse)
def get_placements_result(
	job_id: str,
	page: int = Query(default=1, ge=1),
	page_size: int | None = Query(default=None, ge=1),
	manager: JobManager = Depends(_manager_dependency),
) -> JobResultResponse:
	return _get_dimension_result("placements", job_id, page, page_size, manager)


@router.post("/jobs/creatives", response_model=JobSubmissionResponse, status_code=status.HTTP_202_ACCEPTED)
def submit_creatives_request(
	request: DimensionQueryRequest = Body(default_factory=DimensionQueryRequest),
	manager: JobManager = Depends(_manager_dependency),
) -> JobSubmissionResponse:
	return _submit_dimension_request("creatives", request, manager)


@router.get("/jobs/creatives/{job_id}", response_model=JobResultResponse)
def get_creatives_result(
	job_id: str,
	page: int = Query(default=1, ge=1),
	page_size: int | None = Query(default=None, ge=1),
	manager: JobManager = Depends(_manager_dependency),
) -> JobResultResponse:
	return _get_dimension_result("creatives", job_id, page, page_size, manager)


@router.post("/jobs/ad-metrics", response_model=JobSubmissionResponse, status_code=status.HTTP_202_ACCEPTED)
def submit_fact_metrics_request(
	request: FactMetricsQueryRequest = Body(default_factory=FactMetricsQueryRequest),
	manager: JobManager = Depends(_manager_dependency),
) -> JobSubmissionResponse:
	return _submit_fact_request(request, manager)


@router.get("/jobs/ad-metrics/{job_id}", response_model=JobResultResponse)
def get_fact_metrics_result(
	job_id: str,
	page: int = Query(default=1, ge=1),
	page_size: int | None = Query(default=None, ge=1),
	manager: JobManager = Depends(_manager_dependency),
) -> JobResultResponse:
	return _get_dimension_result("ad_metrics_daily", job_id, page, page_size, manager)
