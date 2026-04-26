from __future__ import annotations

import sqlite3
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
