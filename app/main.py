from __future__ import annotations

from contextlib import asynccontextmanager
from time import perf_counter
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.responses import Response

from app.infrastructure.config import get_settings
from app.infrastructure.redis_queue import create_redis_connection
from app.interfaces.api import router as dimension_router
from app.observability.logging import get_logger, configure_logging

settings = get_settings()
configure_logging(settings.log_level, settings.log_json)
logger = get_logger("app.main")


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

	started = perf_counter()
	response: Response = await call_next(request)
	duration_ms = round((perf_counter() - started) * 1000, 2)
	response.headers["X-Request-ID"] = request_id
	log_fn = logger.warning if duration_ms >= settings.slow_request_warning_ms else logger.info
	log_fn(
		"request_completed",
		extra={
			"request_id": request_id,
			"method": request.method,
			"path": request.url.path,
			"status_code": response.status_code,
			"duration_ms": duration_ms,
		},
	)
	return response


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
	request_id = getattr(request.state, "request_id", None)
	errors = [{"loc": err.get("loc"), "msg": err.get("msg"), "type": err.get("type")} for err in exc.errors()]
	logger.warning(
		"request_validation_failed",
		extra={
			"request_id": request_id,
			"path": request.url.path,
			"method": request.method,
			"error_count": len(errors),
		},
	)
	return JSONResponse(
		status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
		content={
			"error": {
				"code": "INVALID_REQUEST",
				"message": "Invalid request",
				"request_id": request_id,
				"details": {"validation_errors": errors},
			}
		},
	)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
	request_id = getattr(request.state, "request_id", None)
	detail = exc.detail
	if isinstance(detail, dict) and "error" in detail:
		error_detail = detail["error"]
		if isinstance(error_detail, dict) and error_detail.get("request_id") is None:
			error_detail["request_id"] = request_id
		payload = detail
	else:
		payload = {
			"error": {
				"code": "HTTP_ERROR",
				"message": str(detail),
				"request_id": request_id,
			}
		}

	logger.warning(
		"http_exception",
		extra={
			"request_id": request_id,
			"path": request.url.path,
			"method": request.method,
			"status_code": exc.status_code,
		},
	)
	return JSONResponse(status_code=exc.status_code, content=payload)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
	request_id = getattr(request.state, "request_id", None)
	logger.exception(
		"unhandled_exception",
		extra={
			"request_id": request_id,
			"path": request.url.path,
			"method": request.method,
		},
	)
	return JSONResponse(
		status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
		content={
			"error": {
				"code": "INTERNAL_SERVER_ERROR",
				"message": "Internal server error",
				"request_id": request_id,
			}
		},
	)

@app.get("/health", status_code=status.HTTP_200_OK)
def health() -> dict[str, str]:
	return {"status": "ok"}


app.include_router(dimension_router)
