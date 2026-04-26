from __future__ import annotations

from app.application.pagination import paginate_items


def test_paginate_items_slices_expected_page() -> None:
    items = [{"id": str(i), "name": f"Item {i}"} for i in range(1, 121)]

    page_items, meta = paginate_items(items, page=2, page_size=50)

    assert len(page_items) == 50
    assert page_items[0]["id"] == "51"
    assert page_items[-1]["id"] == "100"
    assert meta["page"] == 2
    assert meta["page_size"] == 50
    assert meta["total_items"] == 120
    assert meta["total_pages"] == 3
    assert meta["has_next"] is True
    assert meta["has_previous"] is True


def test_paginate_items_uses_empty_page_when_overflow() -> None:
    items = [{"id": str(i), "name": f"Item {i}"} for i in range(1, 11)]

    page_items, meta = paginate_items(items, page=3, page_size=5)

    assert page_items == []
    assert meta["total_pages"] == 2
    assert meta["has_next"] is False
    assert meta["has_previous"] is True
