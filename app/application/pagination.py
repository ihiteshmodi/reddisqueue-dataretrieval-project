from __future__ import annotations

from math import ceil
from typing import Any


def paginate_items(
	items: list[dict[str, Any]],
	page: int,
	page_size: int,
) -> tuple[list[dict[str, Any]], dict[str, int | bool]]:
	total_items = len(items)
	if page_size <= 0:
		raise ValueError("page_size must be positive")
	if page <= 0:
		raise ValueError("page must be positive")

	total_pages = ceil(total_items / page_size) if total_items else 0
	start_idx = (page - 1) * page_size
	end_idx = start_idx + page_size
	paged_items = items[start_idx:end_idx]

	meta = {
		"page": page,
		"page_size": page_size,
		"total_items": total_items,
		"total_pages": total_pages,
		"has_next": page < total_pages,
		"has_previous": page > 1 and total_pages > 0,
	}
	return paged_items, meta
