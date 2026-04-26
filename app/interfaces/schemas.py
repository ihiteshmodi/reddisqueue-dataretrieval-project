from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

EntityType = Literal["advertisers", "campaigns", "placements", "creatives"]
JobStatus = Literal[
	"queued",
	"started",
	"finished",
	"failed",
	"not_found",
	"deferred",
	"scheduled",
	"stopped",
	"canceled",
]


class DimensionQueryRequest(BaseModel):
	model_config = ConfigDict(
		json_schema_extra={
			"example": {},
		}
	)

	search: str | None = Field(
		default=None,
		max_length=100,
		description="Optional case-insensitive contains filter by name.",
	)


class DimensionItem(BaseModel):
	id: str
	name: str


class JobSubmissionResponse(BaseModel):
	job_id: str
	entity: EntityType
	status: Literal["queued"]
	message: str
	submitted_at: datetime | None = None


class JobResultResponse(BaseModel):
	job_id: str
	entity: EntityType
	status: JobStatus
	submitted_at: datetime | None = None
	started_at: datetime | None = None
	ended_at: datetime | None = None
	total: int | None = None
	items: list[DimensionItem] | None = None
	error: str | None = None
