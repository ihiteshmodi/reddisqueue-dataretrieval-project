from __future__ import annotations

from app.main import app


EXPECTED_PATHS = {
    "/health",
    "/v1/jobs/advertisers",
    "/v1/jobs/advertisers/{job_id}",
    "/v1/jobs/campaigns",
    "/v1/jobs/campaigns/{job_id}",
    "/v1/jobs/placements",
    "/v1/jobs/placements/{job_id}",
    "/v1/jobs/creatives",
    "/v1/jobs/creatives/{job_id}",
}


def test_expected_paths_are_registered() -> None:
    app_paths = {route.path for route in app.routes}
    assert EXPECTED_PATHS.issubset(app_paths)
