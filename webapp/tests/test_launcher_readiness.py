from __future__ import annotations

import inspect
import os
from pathlib import Path
from typing import Any
from unittest.mock import patch

from gridedge_web import launcher


class _Process:
    returncode: int | None = None

    def poll(self) -> None:
        return None


class _Response:
    status = 200

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


def test_launcher_waits_for_readiness_then_authenticated_business_read() -> None:
    requested: list[tuple[str, str | None]] = []

    def open_request(request: Any, timeout: float) -> _Response:
        assert timeout == 0.5
        url = request.full_url if hasattr(request, "full_url") else str(request)
        token = (
            request.get_header("X-gridedge-api-token")
            if hasattr(request, "get_header")
            else None
        )
        requested.append((url, token))
        return _Response()

    with patch.object(launcher.urllib.request, "urlopen", side_effect=open_request):
        launcher._wait_for_core("http://127.0.0.1:8790", _Process(), "secret-token")

    assert requested == [
        ("http://127.0.0.1:8790/ready", None),
        ("http://127.0.0.1:8790/api/v1/runs", "secret-token"),
    ]


def test_ci_smoke_uses_readiness_and_authenticated_runs_not_liveness() -> None:
    workspace = os.environ.get("GITHUB_WORKSPACE")
    root = Path(workspace) if workspace else Path(__file__).resolve().parents[2]
    workflow = (root / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    smoke = workflow[workflow.index("endpoints =") :]
    assert "/ready" in smoke
    assert "/api/v1/runs" in smoke
    assert "X-GridEdge-Api-Token" in smoke
    assert "/health" not in smoke.split("PY", 1)[0]


def test_launcher_source_does_not_treat_liveness_as_readiness() -> None:
    source = inspect.getsource(launcher._wait_for_core)
    assert "/ready" in source
    assert "/api/v1/runs" in source
    assert "X-GridEdge-Api-Token" in source
    assert '"/health"' not in source
