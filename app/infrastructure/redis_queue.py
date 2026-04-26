from __future__ import annotations

import time
from typing import Any

from redis import Redis
from redis.exceptions import RedisError
from rq import Queue
from rq.exceptions import NoSuchJobError
from rq.job import Job

from app.infrastructure.config import Settings


class QueueUnavailableError(RuntimeError):
	"""Raised when the Redis queue is unavailable."""


def create_redis_connection(settings: Settings) -> Redis:
	connection = Redis(
		host=settings.redis_host,
		port=settings.redis_port,
		db=settings.redis_db,
		password=settings.redis_password,
		socket_connect_timeout=settings.redis_connect_timeout_seconds,
		socket_timeout=settings.redis_socket_timeout_seconds,
		health_check_interval=30,
	)
	last_error: RedisError | None = None
	for attempt in range(1, settings.redis_retry_attempts + 1):
		try:
			connection.ping()
			return connection
		except RedisError as exc:
			last_error = exc
			if attempt < settings.redis_retry_attempts:
				time.sleep(settings.redis_retry_backoff_ms / 1000)
	if last_error is not None:
		raise QueueUnavailableError("Unable to connect to Redis") from last_error
	return connection


def create_queue(settings: Settings) -> Queue:
	connection = create_redis_connection(settings)
	return Queue(
		name=settings.redis_queue_name,
		connection=connection,
		default_timeout=settings.job_timeout_seconds,
	)


def enqueue_dimension_job(
	queue: Queue,
	settings: Settings,
	entity: str,
	payload: dict[str, Any],
	job_id: str | None = None,
) -> Job:
	return queue.enqueue(
		"app.services.worker.run_dimension_extract_job",
		kwargs={
			"entity": entity,
			"request_payload": payload,
			"db_path": str(settings.sqlite_db_path),
		},
		result_ttl=settings.job_result_ttl_seconds,
		failure_ttl=settings.job_failure_ttl_seconds,
		job_timeout=settings.job_timeout_seconds,
		job_id=job_id,
	)


def enqueue_fact_metrics_job(
	queue: Queue,
	settings: Settings,
	payload: dict[str, Any],
	job_id: str | None = None,
) -> Job:
	return queue.enqueue(
		"app.services.worker.run_fact_metrics_job",
		kwargs={
			"request_payload": payload,
			"db_path": str(settings.sqlite_db_path),
		},
		result_ttl=settings.job_result_ttl_seconds,
		failure_ttl=settings.job_failure_ttl_seconds,
		job_timeout=settings.job_timeout_seconds,
		job_id=job_id,
	)


def fetch_job(queue: Queue, job_id: str) -> Job | None:
	try:
		return Job.fetch(job_id, connection=queue.connection)
	except NoSuchJobError:
		return None
