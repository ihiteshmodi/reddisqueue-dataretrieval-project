from __future__ import annotations

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
	try:
		connection.ping()
	except RedisError as exc:
		raise QueueUnavailableError("Unable to connect to Redis") from exc
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
	)


def fetch_job(queue: Queue, job_id: str) -> Job | None:
	try:
		return Job.fetch(job_id, connection=queue.connection)
	except NoSuchJobError:
		return None
