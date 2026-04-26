from __future__ import annotations

import json
import random
import sqlite3
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Iterable

# Hardcoded run configuration (no CLI).
DB_FILENAME = "master_clientdata.db"
SEED = 42
BATCH_SIZE = 5000
ADVERTISER_COUNT = 25
CAMPAIGNS_PER_ADVERTISER = 1
PLACEMENTS_PER_CAMPAIGN = 1
CREATIVES_PER_PLACEMENT = 2
MONTHS_BACK = 6
EXPORT_SAMPLE_JSON = False
SAMPLE_JSON_FILENAME = "sample_metrics_preview.json"
SAMPLE_JSON_ROWS = 25


@dataclass(frozen=True)
class DimensionRows:
    advertisers: list[tuple[str, str]]
    campaigns: list[tuple[str, str, str]]
    placements: list[tuple[str, str, str]]
    creatives: list[tuple[str, str, str]]


@dataclass(frozen=True)
class GenerationStats:
    fact_rows: int
    start_date: date
    end_date: date
    elapsed_seconds: float


def subtract_months(value: date, months: int) -> date:
    if months < 0:
        raise ValueError("months must be non-negative")
    year = value.year
    month = value.month - months
    while month <= 0:
        year -= 1
        month += 12

    if month == 2:
        leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
        max_day = 29 if leap else 28
    elif month in {4, 6, 9, 11}:
        max_day = 30
    else:
        max_day = 31

    return date(year, month, min(value.day, max_day))


def date_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current = current.fromordinal(current.toordinal() + 1)


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    return conn


def recreate_db(db_path: Path) -> sqlite3.Connection:
    if db_path.exists():
        db_path.unlink()
    return connect(db_path)


def create_schema(conn: sqlite3.Connection) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS advertisers (
        advertiser_id TEXT PRIMARY KEY,
        advertiser_name TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS campaigns (
        campaign_id TEXT PRIMARY KEY,
        campaign_name TEXT NOT NULL,
        advertiser_id TEXT NOT NULL,
        FOREIGN KEY (advertiser_id) REFERENCES advertisers(advertiser_id)
    );

    CREATE TABLE IF NOT EXISTS placements (
        placement_id TEXT PRIMARY KEY,
        placement_name TEXT NOT NULL,
        campaign_id TEXT NOT NULL,
        FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id)
    );

    CREATE TABLE IF NOT EXISTS creatives (
        creative_id TEXT PRIMARY KEY,
        creative_name TEXT NOT NULL,
        placement_id TEXT NOT NULL,
        FOREIGN KEY (placement_id) REFERENCES placements(placement_id)
    );

    CREATE TABLE IF NOT EXISTS ad_metrics_daily (
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
        created_at TEXT NOT NULL,
        UNIQUE (creative_id, report_date),
        FOREIGN KEY (creative_id) REFERENCES creatives(creative_id),
        FOREIGN KEY (placement_id) REFERENCES placements(placement_id),
        FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id),
        FOREIGN KEY (advertiser_id) REFERENCES advertisers(advertiser_id)
    );
    """
    conn.executescript(ddl)


def create_indexes(conn: sqlite3.Connection) -> None:
    ddl = """
    CREATE INDEX IF NOT EXISTS idx_metrics_advertiser ON ad_metrics_daily(advertiser_id);
    CREATE INDEX IF NOT EXISTS idx_metrics_campaign ON ad_metrics_daily(campaign_id);
    CREATE INDEX IF NOT EXISTS idx_metrics_placement ON ad_metrics_daily(placement_id);
    CREATE INDEX IF NOT EXISTS idx_metrics_creative ON ad_metrics_daily(creative_id);
    CREATE INDEX IF NOT EXISTS idx_metrics_report_date ON ad_metrics_daily(report_date);

    CREATE INDEX IF NOT EXISTS idx_metrics_date_advertiser ON ad_metrics_daily(report_date, advertiser_id);
    CREATE INDEX IF NOT EXISTS idx_metrics_date_campaign ON ad_metrics_daily(report_date, campaign_id);
    CREATE INDEX IF NOT EXISTS idx_metrics_date_placement ON ad_metrics_daily(report_date, placement_id);
    CREATE INDEX IF NOT EXISTS idx_metrics_date_creative ON ad_metrics_daily(report_date, creative_id);

    CREATE INDEX IF NOT EXISTS idx_campaigns_advertiser ON campaigns(advertiser_id);
    CREATE INDEX IF NOT EXISTS idx_placements_campaign ON placements(campaign_id);
    CREATE INDEX IF NOT EXISTS idx_creatives_placement ON creatives(placement_id);
    """
    conn.executescript(ddl)


def build_dimensions() -> tuple[DimensionRows, dict[str, tuple[str, str, str]]]:
    advertisers: list[tuple[str, str]] = []
    campaigns: list[tuple[str, str, str]] = []
    placements: list[tuple[str, str, str]] = []
    creatives: list[tuple[str, str, str]] = []
    creative_parents: dict[str, tuple[str, str, str]] = {}

    campaign_counter = 1
    placement_counter = 1
    creative_counter = 1

    for a in range(1, ADVERTISER_COUNT + 1):
        advertiser_id = f"ADV{a:04d}"
        advertisers.append((advertiser_id, f"Advertiser {a}"))

        for _ in range(CAMPAIGNS_PER_ADVERTISER):
            campaign_id = f"CMP{campaign_counter:06d}"
            campaign_counter += 1
            campaigns.append((campaign_id, f"Campaign {campaign_id}", advertiser_id))

            for _ in range(PLACEMENTS_PER_CAMPAIGN):
                placement_id = f"PLC{placement_counter:06d}"
                placement_counter += 1
                placements.append((placement_id, f"Placement {placement_id}", campaign_id))

                for _ in range(CREATIVES_PER_PLACEMENT):
                    creative_id = f"CRT{creative_counter:06d}"
                    creative_counter += 1
                    creatives.append((creative_id, f"Creative {creative_id}", placement_id))
                    creative_parents[creative_id] = (advertiser_id, campaign_id, placement_id)

    return DimensionRows(advertisers, campaigns, placements, creatives), creative_parents


def insert_dimensions(conn: sqlite3.Connection, rows: DimensionRows) -> None:
    conn.executemany(
        "INSERT INTO advertisers(advertiser_id, advertiser_name) VALUES (?, ?)",
        rows.advertisers,
    )
    conn.executemany(
        "INSERT INTO campaigns(campaign_id, campaign_name, advertiser_id) VALUES (?, ?, ?)",
        rows.campaigns,
    )
    conn.executemany(
        "INSERT INTO placements(placement_id, placement_name, campaign_id) VALUES (?, ?, ?)",
        rows.placements,
    )
    conn.executemany(
        "INSERT INTO creatives(creative_id, creative_name, placement_id) VALUES (?, ?, ?)",
        rows.creatives,
    )
    conn.commit()


def generate_fact_row(
    rng: random.Random,
    report_date: date,
    creative_id: str,
    advertiser_id: str,
    campaign_id: str,
    placement_id: str,
) -> tuple[str, str, str, str, str, float, int, int, int, float, str]:
    weekday_multiplier = 1.08 if report_date.weekday() < 5 else 0.9
    base_impressions = rng.randint(900, 4200)
    noise = max(0.8, min(1.25, rng.gauss(1.0, 0.08)))

    impressions = int(base_impressions * weekday_multiplier * noise)
    impressions = max(10, impressions)

    ctr = max(0.005, min(0.2, rng.uniform(0.01, 0.12) * noise))
    clicks = int(impressions * ctr)
    clicks = max(0, min(clicks, impressions))

    cvr = max(0.01, min(0.5, rng.uniform(0.03, 0.2) * (2 - noise)))
    conversions = int(clicks * cvr)
    conversions = max(0, min(conversions, clicks))

    cpc = max(0.05, rng.uniform(0.2, 4.5) * noise)
    spend = round(clicks * cpc, 2)

    roas = max(0.0, rng.uniform(0.6, 2.4))
    revenue = round(spend * roas, 2)

    created_at = datetime.now(UTC).isoformat(timespec="seconds")

    return (
        report_date.isoformat(),
        creative_id,
        placement_id,
        campaign_id,
        advertiser_id,
        spend,
        impressions,
        clicks,
        conversions,
        revenue,
        created_at,
    )


def insert_fact_rows(
    conn: sqlite3.Connection,
    creative_parents: dict[str, tuple[str, str, str]],
    start_date: date,
    end_date: date,
) -> int:
    rng = random.Random(SEED)
    sql = """
    INSERT INTO ad_metrics_daily(
        report_date, creative_id, placement_id, campaign_id, advertiser_id,
        spend, impressions, clicks, conversions, revenue, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    buffer: list[tuple[str, str, str, str, str, float, int, int, int, float, str]] = []
    total = 0

    for report_day in date_range(start_date, end_date):
        for creative_id, (advertiser_id, campaign_id, placement_id) in creative_parents.items():
            buffer.append(
                generate_fact_row(
                    rng,
                    report_day,
                    creative_id,
                    advertiser_id,
                    campaign_id,
                    placement_id,
                )
            )
            if len(buffer) >= BATCH_SIZE:
                conn.executemany(sql, buffer)
                conn.commit()
                total += len(buffer)
                buffer.clear()

    if buffer:
        conn.executemany(sql, buffer)
        conn.commit()
        total += len(buffer)

    return total


def fetch_count(conn: sqlite3.Connection, table: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    if row is None:
        return 0
    return int(row[0])


def run_validations(conn: sqlite3.Connection) -> None:
    checks = {
        "campaign_orphans": """
            SELECT COUNT(*)
            FROM campaigns c
            LEFT JOIN advertisers a ON a.advertiser_id = c.advertiser_id
            WHERE a.advertiser_id IS NULL
        """,
        "placement_orphans": """
            SELECT COUNT(*)
            FROM placements p
            LEFT JOIN campaigns c ON c.campaign_id = p.campaign_id
            WHERE c.campaign_id IS NULL
        """,
        "creative_orphans": """
            SELECT COUNT(*)
            FROM creatives c
            LEFT JOIN placements p ON p.placement_id = c.placement_id
            WHERE p.placement_id IS NULL
        """,
        "fact_orphans": """
            SELECT COUNT(*)
            FROM ad_metrics_daily f
            LEFT JOIN creatives c ON c.creative_id = f.creative_id
            LEFT JOIN placements p ON p.placement_id = f.placement_id
            LEFT JOIN campaigns m ON m.campaign_id = f.campaign_id
            LEFT JOIN advertisers a ON a.advertiser_id = f.advertiser_id
            WHERE c.creative_id IS NULL
               OR p.placement_id IS NULL
               OR m.campaign_id IS NULL
               OR a.advertiser_id IS NULL
        """,
        "duplicate_creative_date": """
            SELECT COUNT(*)
            FROM (
                SELECT creative_id, report_date, COUNT(*) c
                FROM ad_metrics_daily
                GROUP BY creative_id, report_date
                HAVING c > 1
            )
        """,
        "invalid_metric_values": """
            SELECT COUNT(*)
            FROM ad_metrics_daily
            WHERE spend < 0
               OR revenue < 0
               OR impressions < 0
               OR clicks < 0
               OR conversions < 0
               OR clicks > impressions
               OR conversions > clicks
        """,
    }

    for check_name, sql in checks.items():
        count = int(conn.execute(sql).fetchone()[0])
        if count != 0:
            raise RuntimeError(f"Validation failed for {check_name}: {count}")


def explain_index_usage(conn: sqlite3.Connection, start_date: date, end_date: date) -> None:
    queries = {
        "date+advertiser": (
            """
            EXPLAIN QUERY PLAN
            SELECT SUM(spend)
            FROM ad_metrics_daily
            WHERE report_date BETWEEN ? AND ?
              AND advertiser_id = ?
            """,
            (start_date.isoformat(), end_date.isoformat(), "ADV0001"),
        ),
        "date+campaign": (
            """
            EXPLAIN QUERY PLAN
            SELECT SUM(clicks)
            FROM ad_metrics_daily
            WHERE report_date BETWEEN ? AND ?
              AND campaign_id = ?
            """,
            (start_date.isoformat(), end_date.isoformat(), "CMP000001"),
        ),
        "date+placement": (
            """
            EXPLAIN QUERY PLAN
            SELECT SUM(conversions)
            FROM ad_metrics_daily
            WHERE report_date BETWEEN ? AND ?
              AND placement_id = ?
            """,
            (start_date.isoformat(), end_date.isoformat(), "PLC000001"),
        ),
        "date+creative": (
            """
            EXPLAIN QUERY PLAN
            SELECT SUM(revenue)
            FROM ad_metrics_daily
            WHERE report_date BETWEEN ? AND ?
              AND creative_id = ?
            """,
            (start_date.isoformat(), end_date.isoformat(), "CRT000001"),
        ),
    }

    print("\nIndex usage checks (EXPLAIN QUERY PLAN):")
    for name, (sql, params) in queries.items():
        plan_rows = conn.execute(sql, params).fetchall()
        plan_text = " | ".join(str(row) for row in plan_rows)
        print(f"- {name}: {plan_text}")


def maybe_export_sample_json(conn: sqlite3.Connection, output_path: Path) -> None:
    if not EXPORT_SAMPLE_JSON:
        return

    rows = conn.execute(
        """
        SELECT report_date, advertiser_id, campaign_id, placement_id, creative_id,
               spend, impressions, clicks, conversions, revenue
        FROM ad_metrics_daily
        ORDER BY report_date, creative_id
        LIMIT ?
        """,
        (SAMPLE_JSON_ROWS,),
    ).fetchall()

    payload = [
        {
            "report_date": r[0],
            "advertiser_id": r[1],
            "campaign_id": r[2],
            "placement_id": r[3],
            "creative_id": r[4],
            "spend": r[5],
            "impressions": r[6],
            "clicks": r[7],
            "conversions": r[8],
            "revenue": r[9],
        }
        for r in rows
    ]

    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Sample JSON written: {output_path}")


def summarize(conn: sqlite3.Connection, stats: GenerationStats, db_path: Path) -> None:
    table_counts = {
        "advertisers": fetch_count(conn, "advertisers"),
        "campaigns": fetch_count(conn, "campaigns"),
        "placements": fetch_count(conn, "placements"),
        "creatives": fetch_count(conn, "creatives"),
        "ad_metrics_daily": fetch_count(conn, "ad_metrics_daily"),
    }

    throughput = 0.0
    if stats.elapsed_seconds > 0:
        throughput = stats.fact_rows / stats.elapsed_seconds

    print("\nGeneration complete")
    print(f"Database file: {db_path}")
    print(f"Date span: {stats.start_date} -> {stats.end_date}")
    print(f"Elapsed: {stats.elapsed_seconds:.2f}s")
    print(f"Throughput: {throughput:.2f} fact rows/sec")
    print("Rows by table:")
    for table_name, count in table_counts.items():
        print(f"- {table_name}: {count}")


def main() -> None:
    workspace_root = Path.cwd()
    db_path = workspace_root / DB_FILENAME

    end_date = date.today()
    start_date = subtract_months(end_date, MONTHS_BACK)
    if start_date > end_date:
        raise ValueError("Invalid date range")

    start_time = time.perf_counter()

    conn = recreate_db(db_path)
    try:
        create_schema(conn)
        create_indexes(conn)

        dimensions, creative_parents = build_dimensions()
        insert_dimensions(conn, dimensions)

        total_facts = insert_fact_rows(conn, creative_parents, start_date, end_date)

        run_validations(conn)
        explain_index_usage(conn, start_date, end_date)

        maybe_export_sample_json(conn, workspace_root / SAMPLE_JSON_FILENAME)

        stats = GenerationStats(
            fact_rows=total_facts,
            start_date=start_date,
            end_date=end_date,
            elapsed_seconds=time.perf_counter() - start_time,
        )
        summarize(conn, stats, db_path)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
