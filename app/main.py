from __future__ import annotations

from contextlib import asynccontextmanager
import logging
from uuid import uuid4

from fastapi import FastAPI, Request, status
from starlette.responses import Response

from app.infrastructure.config import get_settings
from app.infrastructure.redis_queue import create_redis_connection
from app.interfaces.api import router as dimension_router

settings = get_settings()
logger = logging.getLogger("app.main")


@asynccontextmanager
async def lifespan(_app: FastAPI):
	if not settings.sqlite_db_path.exists():
		raise RuntimeError(f"SQLite database not found: {settings.sqlite_db_path}")
	create_redis_connection(settings)
	yield


app = FastAPI(
	title=settings.app_name,
	version=settings.app_version,
	lifespan=lifespan,
)


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
	request_id = request.headers.get("X-Request-ID") or str(uuid4())
	request.state.request_id = request_id

	response: Response = await call_next(request)
	response.headers["X-Request-ID"] = request_id
	logger.info(
		"request_completed",
		extra={
			"request_id": request_id,
			"method": request.method,
			"path": request.url.path,
			"status_code": response.status_code,
		},
	)
	return response

@app.get("/health", status_code=status.HTTP_200_OK)
def health() -> dict[str, str]:
	return {"status": "ok"}


app.include_router(dimension_router)
