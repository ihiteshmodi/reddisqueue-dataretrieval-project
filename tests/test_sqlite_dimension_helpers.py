from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.interfaces.schemas import DimensionQueryRequest
from app.infrastructure.sqlite_db import fetch_distinct_dimension_rows, normalize_entity


def _build_test_db(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE advertisers (
                advertiser_id TEXT PRIMARY KEY,
                advertiser_name TEXT NOT NULL
            );
            INSERT INTO advertisers(advertiser_id, advertiser_name) VALUES
                ('ADV1', 'Alpha Co'),
                ('ADV2', 'Beta Co'),
                ('ADV3', 'Gamma Labs');
            """
        )


def test_normalize_entity_accepts_supported_values() -> None:
    assert normalize_entity(" Advertisers ") == "advertisers"


@pytest.mark.parametrize("value", ["publisher", "", "ad_groups"])
def test_normalize_entity_rejects_unsupported(value: str) -> None:
    with pytest.raises(ValueError):
        normalize_entity(value)


def test_dimension_query_request_defaults_to_search_only() -> None:
    request = DimensionQueryRequest()
    assert request.search is None


def test_fetch_distinct_dimension_rows_returns_all_rows_when_search_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "unit.db"
    _build_test_db(db_path)

    rows = fetch_distinct_dimension_rows(
        db_path=db_path,
        entity="advertisers",
        search=None,
    )
    assert len(rows) == 3
    assert [row["name"] for row in rows] == ["Alpha Co", "Beta Co", "Gamma Labs"]


def test_fetch_distinct_dimension_rows_with_search_filter(tmp_path: Path) -> None:
    db_path = tmp_path / "unit.db"
    _build_test_db(db_path)

    filtered_rows = fetch_distinct_dimension_rows(
        db_path=db_path,
        entity="advertisers",
        search="Co",
    )
    assert len(filtered_rows) == 2
    assert [row["name"] for row in filtered_rows] == ["Alpha Co", "Beta Co"]
