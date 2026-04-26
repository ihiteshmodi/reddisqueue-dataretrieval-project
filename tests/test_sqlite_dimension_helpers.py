from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from app.interfaces.schemas import DimensionQueryRequest
from app.infrastructure.sqlite_db import (
    fetch_distinct_dimension_rows,
    fetch_fact_metrics_rows,
    normalize_entity,
)


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

            CREATE TABLE ad_metrics_daily (
                report_date TEXT NOT NULL,
                creative_id TEXT NOT NULL,
                placement_id TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                advertiser_id TEXT NOT NULL,
                spend REAL NOT NULL,
                impressions INTEGER NOT NULL,
                clicks INTEGER NOT NULL,
                conversions INTEGER NOT NULL,
                revenue REAL NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )

        today = datetime.now(UTC).date()
        dates = [
            (today - timedelta(days=4)).isoformat(),
            (today - timedelta(days=2)).isoformat(),
            today.isoformat(),
        ]
        rows = [
            (dates[0], "CRT1", "PLC1", "CMP1", "ADV1", 10.0, 100, 10, 2, 12.5, "2026-01-01T00:00:00"),
            (dates[1], "CRT1", "PLC1", "CMP1", "ADV1", 12.0, 120, 12, 3, 15.0, "2026-01-02T00:00:00"),
            (dates[2], "CRT2", "PLC2", "CMP2", "ADV2", 20.0, 200, 20, 5, 24.0, "2026-01-03T00:00:00"),
        ]
        conn.executemany(
            """
            INSERT INTO ad_metrics_daily(
                report_date, creative_id, placement_id, campaign_id, advertiser_id,
                spend, impressions, clicks, conversions, revenue, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
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


@pytest.mark.parametrize("placeholder", ["string", " STRING ", "null", "none", ""])
def test_fetch_distinct_dimension_rows_ignores_placeholder_search_values(
    tmp_path: Path,
    placeholder: str,
) -> None:
    db_path = tmp_path / "unit.db"
    _build_test_db(db_path)

    rows = fetch_distinct_dimension_rows(
        db_path=db_path,
        entity="advertisers",
        search=placeholder,
    )
    assert len(rows) == 3


def test_fetch_fact_metrics_rows_uses_default_last_three_days_window(tmp_path: Path) -> None:
    db_path = tmp_path / "unit.db"
    _build_test_db(db_path)

    rows = fetch_fact_metrics_rows(
        db_path=db_path,
        advertiser_id="ADV1",
        campaign_id=None,
        placement_id=None,
        creative_id=None,
        report_start_date=None,
        report_end_date=None,
    )
    assert len(rows) == 1
    assert rows[0]["advertiser_id"] == "ADV1"


def test_fetch_fact_metrics_rows_honors_explicit_date_range(tmp_path: Path) -> None:
    db_path = tmp_path / "unit.db"
    _build_test_db(db_path)

    rows = fetch_fact_metrics_rows(
        db_path=db_path,
        advertiser_id="ADV1",
        campaign_id=None,
        placement_id=None,
        creative_id=None,
        report_start_date="1900-01-01",
        report_end_date="2999-12-31",
    )
    assert len(rows) == 2


def test_fetch_fact_metrics_rows_requires_one_id_filter(tmp_path: Path) -> None:
    db_path = tmp_path / "unit.db"
    _build_test_db(db_path)

    with pytest.raises(ValueError):
        fetch_fact_metrics_rows(
            db_path=db_path,
            advertiser_id=None,
            campaign_id=None,
            placement_id=None,
            creative_id=None,
            report_start_date=None,
            report_end_date=None,
        )
