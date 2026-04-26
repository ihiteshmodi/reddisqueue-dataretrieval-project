from __future__ import annotations

import main


def test_main_uses_root_app_launcher(monkeypatch):
    captured: dict[str, object] = {}

    def fake_run(app_path: str, host: str, port: int, reload: bool) -> None:
        captured["app_path"] = app_path
        captured["host"] = host
        captured["port"] = port
        captured["reload"] = reload

    monkeypatch.setattr(main.uvicorn, "run", fake_run)

    main.main()

    assert captured["app_path"] == "main:app"
    assert captured["host"] == "0.0.0.0"
    assert captured["port"] == 8000
    assert captured["reload"] is True
