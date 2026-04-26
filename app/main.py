from __future__ import annotations

from fastapi import FastAPI, status

from app.infrastructure.config import get_settings
from app.infrastructure.redis_queue import create_redis_connection
from app.interfaces.api import router as dimension_router

settings = get_settings()

app = FastAPI(
	title=settings.app_name,
	version=settings.app_version,
)


@app.on_event("startup")
def startup_check() -> None:
	if not settings.sqlite_db_path.exists():
		raise RuntimeError(f"SQLite database not found: {settings.sqlite_db_path}")
	create_redis_connection(settings)


@app.get("/health", status_code=status.HTTP_200_OK)
def health() -> dict[str, str]:
	return {"status": "ok"}


app.include_router(dimension_router)
