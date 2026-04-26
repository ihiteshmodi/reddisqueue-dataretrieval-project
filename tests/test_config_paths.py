from __future__ import annotations

from pathlib import Path

from app.infrastructure.config import PROJECT_ROOT, _resolve_db_path


def test_resolve_db_path_uses_project_root_for_relative_path() -> None:
    resolved = _resolve_db_path("master_clientdata.db")
    assert resolved == (PROJECT_ROOT / "master_clientdata.db").resolve()


def test_resolve_db_path_keeps_absolute_path() -> None:
    absolute = Path("/tmp/custom.db")
    assert _resolve_db_path(str(absolute)) == absolute
