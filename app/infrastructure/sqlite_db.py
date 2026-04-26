from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

ENTITY_TABLE_MAP: dict[str, tuple[str, str, str]] = {
	"advertisers": ("advertisers", "advertiser_id", "advertiser_name"),
	"campaigns": ("campaigns", "campaign_id", "campaign_name"),
	"placements": ("placements", "placement_id", "placement_name"),
	"creatives": ("creatives", "creative_id", "creative_name"),
}

SWAGGER_PLACEHOLDER_VALUES = {"", "string", "null", "none"}


def normalize_entity(entity: str) -> str:
	normalized = entity.strip().lower()
	if normalized not in ENTITY_TABLE_MAP:
		raise ValueError(f"Unsupported entity: {entity}")
	return normalized


def fetch_distinct_dimension_rows(
	db_path: Path,
	entity: str,
	search: str | None,
) -> list[dict[str, str]]:
	normalized_entity = normalize_entity(entity)
	table_name, id_column, name_column = ENTITY_TABLE_MAP[normalized_entity]
	search_term = None
	if search:
		cleaned = search.strip()
		if cleaned and cleaned.lower() not in SWAGGER_PLACEHOLDER_VALUES:
			search_term = f"%{cleaned}%"

	if not db_path.exists():
		raise FileNotFoundError(f"Database file not found: {db_path}")

	sql = f"""
	SELECT DISTINCT {id_column} AS id, {name_column} AS name
	FROM {table_name}
	WHERE (? IS NULL OR {name_column} LIKE ?)
	ORDER BY {name_column} ASC
	"""

	with sqlite3.connect(db_path) as conn:
		conn.row_factory = sqlite3.Row
		rows = conn.execute(sql, (search_term, search_term)).fetchall()

	return [{"id": str(row["id"]), "name": str(row["name"])} for row in rows]


def fetch_fact_metrics_rows(
	db_path: Path,
	advertiser_id: str | None,
	campaign_id: str | None,
	placement_id: str | None,
	creative_id: str | None,
	report_start_date: str | None,
	report_end_date: str | None,
) -> list[dict[str, str | int | float]]:
	if not db_path.exists():
		raise FileNotFoundError(f"Database file not found: {db_path}")

	start_date = report_start_date
	end_date = report_end_date
	if not (start_date and end_date):
		today = datetime.now(UTC).date()
		start_date = (today - timedelta(days=3)).isoformat()
		end_date = today.isoformat()

	where_clauses = ["report_date BETWEEN ? AND ?"]
	params: list[str] = [start_date, end_date]

	if advertiser_id:
		where_clauses.append("advertiser_id = ?")
		params.append(advertiser_id)
	if campaign_id:
		where_clauses.append("campaign_id = ?")
		params.append(campaign_id)
	if placement_id:
		where_clauses.append("placement_id = ?")
		params.append(placement_id)
	if creative_id:
		where_clauses.append("creative_id = ?")
		params.append(creative_id)

	if len(params) == 2:
		raise ValueError(
			"At least one ID filter is required: advertiser_id, campaign_id, placement_id, or creative_id"
		)

	sql = f"""
	SELECT report_date, creative_id, placement_id, campaign_id, advertiser_id,
	       spend, impressions, clicks, conversions, revenue, created_at
	FROM ad_metrics_daily
	WHERE {' AND '.join(where_clauses)}
	ORDER BY report_date ASC, creative_id ASC
	"""

	with sqlite3.connect(db_path) as conn:
		conn.row_factory = sqlite3.Row
		rows = conn.execute(sql, params).fetchall()

	return [
		{
			"report_date": str(row["report_date"]),
			"creative_id": str(row["creative_id"]),
			"placement_id": str(row["placement_id"]),
			"campaign_id": str(row["campaign_id"]),
			"advertiser_id": str(row["advertiser_id"]),
			"spend": float(row["spend"]),
			"impressions": int(row["impressions"]),
			"clicks": int(row["clicks"]),
			"conversions": int(row["conversions"]),
			"revenue": float(row["revenue"]),
			"created_at": str(row["created_at"]),
		}
		for row in rows
	]
