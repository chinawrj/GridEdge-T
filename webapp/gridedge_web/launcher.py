from __future__ import annotations

import os
import secrets
import signal
import subprocess
import time
import urllib.request
from pathlib import Path


def _wait_for_core(url: str, process: subprocess.Popen[bytes], token: str) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Rust core exited with status {process.returncode}")
        try:
            with urllib.request.urlopen(f"{url}/ready", timeout=0.5) as response:
                if response.status != 200:
                    continue
            request = urllib.request.Request(
                f"{url}/api/v1/runs",
                headers={"X-GridEdge-Api-Token": token},
            )
            with urllib.request.urlopen(request, timeout=0.5) as response:
                if response.status == 200:
                    return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError("Rust core database did not become ready")


def main() -> None:
    project = Path(__file__).resolve().parents[2]
    core_port = int(os.getenv("GRIDEDGE_CORE_PORT", "8790"))
    web_port = int(os.getenv("GRIDEDGE_WEB_PORT", "8787"))
    token = os.getenv("GRIDEDGE_API_TOKEN") or secrets.token_urlsafe(32)
    binary = Path(os.getenv("GRIDEDGE_BINARY", project / "target/debug/gridedge"))
    config = Path(
        os.getenv(
            "GRIDEDGE_CONFIG",
            project / "configs/zhaoxin_5m_quantity_v10.yaml",
        )
    )
    data = Path(
        os.getenv(
            "GRIDEDGE_DATA",
            project / "data/processed/002256.SZ_5m_raw_20230814_20260814.csv",
        )
    )
    if not binary.exists():
        raise FileNotFoundError(f"Rust binary is missing: {binary}")

    environment = os.environ.copy()
    environment.update(
        {
            "GRIDEDGE_API_TOKEN": token,
            "GRIDEDGE_CORE_URL": f"http://127.0.0.1:{core_port}",
            "GRIDEDGE_WEB_PORT": str(web_port),
            "GRIDEDGE_DATA": str(data),
        }
    )
    core = subprocess.Popen(
        [
            str(binary),
            "web",
            "--config",
            str(config),
            "--data",
            str(data),
            "--host",
            "127.0.0.1",
            "--port",
            str(core_port),
        ],
        cwd=project,
        env=environment,
    )
    try:
        _wait_for_core(environment["GRIDEDGE_CORE_URL"], core, token)
        os.environ.update(environment)
        from .app import main as run_web

        run_web()
    finally:
        if core.poll() is None:
            core.send_signal(signal.SIGINT)
            try:
                core.wait(timeout=5)
            except subprocess.TimeoutExpired:
                core.kill()
                core.wait(timeout=2)
