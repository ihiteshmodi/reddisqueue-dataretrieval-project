from __future__ import annotations

from datetime import date
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

EntityType = Literal["advertisers", "campaigns", "placements", "creatives", "ad_metrics_daily"]
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


class FactMetricsQueryRequest(BaseModel):
	advertiser_id: str | None = Field(default=None, max_length=64)
	campaign_id: str | None = Field(default=None, max_length=64)
	placement_id: str | None = Field(default=None, max_length=64)
	creative_id: str | None = Field(default=None, max_length=64)
	report_start_date: date | None = Field(default=None)
	report_end_date: date | None = Field(default=None)


class FactMetricItem(BaseModel):
	report_date: str
	creative_id: str
	placement_id: str
	campaign_id: str
	advertiser_id: str
	spend: float
	impressions: int
	clicks: int
	conversions: int
	revenue: float
	created_at: str


class DimensionItem(BaseModel):
	id: str
	name: str


class PaginationMeta(BaseModel):
	page: int
	page_size: int
	total_items: int
	total_pages: int
	has_next: bool
	has_previous: bool


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
	items: list[DimensionItem | FactMetricItem] | None = None
	pagination: PaginationMeta | None = None
	error: str | None = None
