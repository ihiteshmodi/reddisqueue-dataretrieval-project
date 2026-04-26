from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from os import getenv
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _get_int(name: str, default: int) -> int:
	raw_value = getenv(name)
	if raw_value is None:
		return default
	try:
		return int(raw_value)
	except ValueError as exc:
		raise ValueError(f"Invalid integer for {name}: {raw_value}") from exc


def _resolve_db_path(raw_path: str) -> Path:
	db_path = Path(raw_path)
	if db_path.is_absolute():
		return db_path
	return (PROJECT_ROOT / db_path).resolve()


@dataclass(frozen=True)
class Settings:
	app_name: str
	app_version: str
	sqlite_db_path: Path
	default_page_size: int
	max_page_size: int
	redis_host: str
	redis_port: int
	redis_db: int
	redis_password: str | None
	redis_queue_name: str
	redis_connect_timeout_seconds: int
	redis_socket_timeout_seconds: int
	job_timeout_seconds: int
	job_result_ttl_seconds: int
	job_failure_ttl_seconds: int


@lru_cache(maxsize=1)
def get_settings() -> Settings:
	return Settings(
		app_name=getenv("APP_NAME", "Redis Queue Data Request API"),
		app_version=getenv("APP_VERSION", "0.1.0"),
		sqlite_db_path=_resolve_db_path(getenv("SQLITE_DB_PATH", "master_clientdata.db")),
		default_page_size=_get_int("DEFAULT_PAGE_SIZE", 250),
		max_page_size=_get_int("MAX_PAGE_SIZE", 1000),
		redis_host=getenv("REDIS_HOST", "127.0.0.1"),
		redis_port=_get_int("REDIS_PORT", 6379),
		redis_db=_get_int("REDIS_DB", 0),
		redis_password=getenv("REDIS_PASSWORD"),
		redis_queue_name=getenv("REDIS_QUEUE_NAME", "dimension_requests"),
		redis_connect_timeout_seconds=_get_int("REDIS_CONNECT_TIMEOUT_SECONDS", 3),
		redis_socket_timeout_seconds=_get_int("REDIS_SOCKET_TIMEOUT_SECONDS", 5),
		job_timeout_seconds=_get_int("JOB_TIMEOUT_SECONDS", 60),
		job_result_ttl_seconds=_get_int("JOB_RESULT_TTL_SECONDS", 600),
		job_failure_ttl_seconds=_get_int("JOB_FAILURE_TTL_SECONDS", 1200),
	)
