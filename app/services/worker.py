from __future__ import annotations

from pathlib import Path
from typing import Any

from rq import Worker

from app.infrastructure.config import get_settings
from app.infrastructure.redis_queue import create_queue
from app.infrastructure.sqlite_db import (
	fetch_distinct_dimension_rows,
	fetch_fact_metrics_rows,
	normalize_entity,
)


def run_dimension_extract_job(
	entity: str,
	request_payload: dict[str, Any],
	db_path: str,
) -> dict[str, Any]:
	normalized_entity = normalize_entity(entity)
	search = request_payload.get("search")

	items = fetch_distinct_dimension_rows(
		db_path=Path(db_path),
		entity=normalized_entity,
		search=search,
	)

	return {
		"entity": normalized_entity,
		"total": len(items),
		"items": items,
	}


def run_fact_metrics_job(
	request_payload: dict[str, Any],
	db_path: str,
) -> dict[str, Any]:
	items = fetch_fact_metrics_rows(
		db_path=Path(db_path),
		advertiser_id=request_payload.get("advertiser_id"),
		campaign_id=request_payload.get("campaign_id"),
		placement_id=request_payload.get("placement_id"),
		creative_id=request_payload.get("creative_id"),
		report_start_date=request_payload.get("report_start_date"),
		report_end_date=request_payload.get("report_end_date"),
	)

	return {
		"entity": "ad_metrics_daily",
		"total": len(items),
		"items": items,
	}


def main() -> None:
	settings = get_settings()
	queue = create_queue(settings)
	worker = Worker([queue], connection=queue.connection)
	worker.work(with_scheduler=False)


if __name__ == "__main__":
	main()
