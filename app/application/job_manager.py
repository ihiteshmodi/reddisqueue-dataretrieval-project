from __future__ import annotations

from datetime import datetime
from typing import Any, cast

from app.application.pagination import paginate_items
from app.infrastructure.config import Settings
from app.infrastructure.redis_queue import (
	create_queue,
	enqueue_dimension_job,
	fetch_job,
)
from app.infrastructure.sqlite_db import normalize_entity
from app.interfaces.schemas import (
	DimensionItem,
	DimensionQueryRequest,
	EntityType,
	JobStatus,
	JobResultResponse,
	JobSubmissionResponse,
	PaginationMeta,
)


def _normalize_datetime(value: datetime | None) -> datetime | None:
	return value


def _extract_error(exc_info: str | None) -> str:
	if not exc_info:
		return "Job failed"
	lines = [line.strip() for line in exc_info.splitlines() if line.strip()]
	if not lines:
		return "Job failed"
	return lines[-1]


def _normalize_status(raw_status: Any) -> str:
	if hasattr(raw_status, "value"):
		return str(raw_status.value)
	status_text = str(raw_status)
	if "." in status_text:
		return status_text.split(".")[-1].lower()
	return status_text.lower()


ALLOWED_JOB_STATUSES = {
	"queued",
	"started",
	"finished",
	"failed",
	"not_found",
	"deferred",
	"scheduled",
	"stopped",
	"canceled",
}


class JobManager:
	def __init__(self, settings: Settings) -> None:
		self._settings = settings
		self._queue = create_queue(settings)

	def submit(self, entity: str, request: DimensionQueryRequest) -> JobSubmissionResponse:
		normalized_entity = normalize_entity(entity)
		typed_entity = cast(EntityType, normalized_entity)
		payload: dict[str, Any] = {
			"search": request.search,
		}
		job = enqueue_dimension_job(
			queue=self._queue,
			settings=self._settings,
			entity=normalized_entity,
			payload=payload,
		)
		return JobSubmissionResponse(
			job_id=job.id,
			entity=typed_entity,
			status="queued",
			message="Job submitted successfully",
			submitted_at=_normalize_datetime(job.enqueued_at),
		)

	def get_result(
		self,
		entity: str,
		job_id: str,
		page: int,
		page_size: int,
	) -> JobResultResponse:
		normalized_entity = normalize_entity(entity)
		typed_entity = cast(EntityType, normalized_entity)
		job = fetch_job(self._queue, job_id)
		if job is None:
			return JobResultResponse(
				job_id=job_id,
				entity=typed_entity,
				status="not_found",
				error="Job not found",
			)

		status = _normalize_status(job.get_status(refresh=True))
		if status not in ALLOWED_JOB_STATUSES:
			status = "failed"
		typed_status = cast(JobStatus, status)
		response = JobResultResponse(
			job_id=job.id,
			entity=typed_entity,
			status=typed_status,
			submitted_at=_normalize_datetime(job.enqueued_at),
			started_at=_normalize_datetime(job.started_at),
			ended_at=_normalize_datetime(job.ended_at),
		)

		if status == "failed":
			response.error = _extract_error(job.exc_info)
			return response

		if status == "finished":
			raw_result = job.result if isinstance(job.result, dict) else {}
			result_entity = str(raw_result.get("entity", normalized_entity))
			if result_entity != normalized_entity:
				response.status = "failed"
				response.error = "Job ID does not match requested entity"
				return response

			all_items = raw_result.get("items", [])
			total = int(raw_result.get("total", len(all_items)))
			paged_items, page_meta = paginate_items(all_items, page=page, page_size=page_size)
			response.total = total
			response.items = [DimensionItem(**item) for item in paged_items]
			response.pagination = PaginationMeta(
				page=int(page_meta["page"]),
				page_size=int(page_meta["page_size"]),
				total_items=int(page_meta["total_items"]),
				total_pages=int(page_meta["total_pages"]),
				has_next=bool(page_meta["has_next"]),
				has_previous=bool(page_meta["has_previous"]),
			)

		return response


def get_job_manager(settings: Settings) -> JobManager:
	return JobManager(settings)
