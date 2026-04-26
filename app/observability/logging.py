from __future__ import annotations

import logging
from functools import lru_cache


@lru_cache(maxsize=1)
def _configure_logging() -> None:
	logging.basicConfig(
		level=logging.INFO,
		format="%(asctime)s %(levelname)s %(name)s %(message)s",
	)


def get_logger(name: str) -> logging.Logger:
	_configure_logging()
	return logging.getLogger(name)
