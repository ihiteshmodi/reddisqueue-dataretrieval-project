from __future__ import annotations

from importlib import import_module
import json
from functools import lru_cache
from datetime import datetime, timezone

_stdlib_logging = import_module("logging")

class JsonLogFormatter(_stdlib_logging.Formatter):  # type: ignore[name-defined]
	def format(self, record) -> str:
		payload: dict[str, object] = {
			"timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
			"level": record.levelname,
			"logger": record.name,
			"message": record.getMessage(),
		}

		for key, value in record.__dict__.items():
			if key in {
				"name",
				"msg",
				"args",
				"levelname",
				"levelno",
				"pathname",
				"filename",
				"module",
				"exc_info",
				"exc_text",
				"stack_info",
				"lineno",
				"funcName",
				"created",
				"msecs",
				"relativeCreated",
				"thread",
				"threadName",
				"processName",
				"process",
				"taskName",
			}:
				continue
			payload[key] = value

		if record.exc_info:
			payload["exception"] = self.formatException(record.exc_info)

		return json.dumps(payload, default=str)


@lru_cache(maxsize=8)
def configure_logging(log_level: str = "INFO", log_json: bool = True) -> None:
	root_logger = _stdlib_logging.getLogger()
	root_logger.handlers.clear()
	root_logger.setLevel(getattr(_stdlib_logging, log_level.upper(), _stdlib_logging.INFO))

	handler = _stdlib_logging.StreamHandler()
	if log_json:
		handler.setFormatter(JsonLogFormatter())
	else:
		handler.setFormatter(_stdlib_logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))

	root_logger.addHandler(handler)


def get_logger(name: str):
	configure_logging()
	return _stdlib_logging.getLogger(name)
