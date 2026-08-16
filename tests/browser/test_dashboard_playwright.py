"""Real-browser contract for the installed GridEdge platform.

This CI-gated test has no mocked HTTP boundary: it starts the Rust core and the
installed NiceGUI console entry point, then drives Chromium as a user would.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import signal
import socket
import sqlite3
import subprocess
import time
import urllib.request
from decimal import Decimal
from pathlib import Path

from playwright.sync_api import Page, sync_playwright


ROOT = Path(__file__).resolve().parents[2]
STATUS = re.compile(r"(已暂停|自动播放中)\s*·\s*([\d,]+)\s*/\s*21\s*根")


def _unused_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _wait_ready(url: str, process: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + 25
    while time.monotonic() < deadline:
        status = process.poll()
        assert status is None, f"platform exited before browser readiness: {status}"
        try:
            with urllib.request.urlopen(url, timeout=0.5) as response:
                if response.status == 200:
                    return
        except OSError:
            pass
        time.sleep(0.1)
    raise AssertionError("browser-facing platform did not become ready")


def _wait_core_ready(
    core_port: int, token: str, process: subprocess.Popen[str]
) -> None:
    deadline = time.monotonic() + 15
    pending = {"ready", "runs"}
    while pending and time.monotonic() < deadline:
        assert process.poll() is None, "platform exited before core readiness"
        for endpoint in list(pending):
            path = "/ready" if endpoint == "ready" else "/api/v1/runs"
            headers = {} if endpoint == "ready" else {"X-GridEdge-Api-Token": token}
            request = urllib.request.Request(
                f"http://127.0.0.1:{core_port}{path}", headers=headers
            )
            try:
                with urllib.request.urlopen(request, timeout=0.5) as response:
                    if response.status == 200:
                        pending.remove(endpoint)
            except OSError:
                pass
        if pending:
            time.sleep(0.1)
    assert not pending, f"core did not become ready: {sorted(pending)}"


def _wait_status(page: Page, *, action: str, minimum_cursor: int) -> int:
    deadline = time.monotonic() + 12
    last_body = ""
    while time.monotonic() < deadline:
        last_body = page.locator("body").inner_text()
        for match in STATUS.finditer(last_body):
            cursor = int(match.group(2).replace(",", ""))
            if match.group(1) == action and cursor >= minimum_cursor:
                return cursor
        page.wait_for_timeout(100)
    raise AssertionError(
        f"dashboard never showed {action} at cursor >= {minimum_cursor}; body={last_body}"
    )


def _core_snapshot(core_port: int, token: str, run_id: str) -> dict[str, object]:
    request = urllib.request.Request(
        f"http://127.0.0.1:{core_port}/api/v1/runs/{run_id}",
        headers={"X-GridEdge-Api-Token": token},
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        assert response.status == 200
        return json.loads(response.read())


def _money_text(value: str) -> str:
    return f"¥{Decimal(value):,.2f}"


def _metric_card_text(page: Page, label: str) -> str:
    return page.get_by_text(label, exact=True).locator("..").inner_text()


def _stop(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.send_signal(signal.SIGINT)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)


def test_dashboard_step_play_pause_is_reactive_without_page_navigation(
    tmp_path: Path,
) -> None:
    binary = Path(os.environ.get("GRIDEDGE_BINARY", ROOT / "target/debug/gridedge"))
    platform = Path(
        os.environ.get(
            "GRIDEDGE_PLATFORM_EXECUTABLE",
            ROOT / "webapp/.venv/bin/gridedge-platform",
        )
    )
    assert binary.is_file(), f"build the Rust binary before browser testing: {binary}"
    assert (
        platform.is_file()
    ), f"install the BFF wheel before browser testing: {platform}"

    database = tmp_path / "browser.db"
    source = (ROOT / "configs/default.yaml").read_text(encoding="utf-8")
    expected = 'database: "gridedge.db"'
    assert source.count(expected) == 1
    config = tmp_path / "config.yaml"
    config.write_text(
        source.replace(expected, f'database: "{database.as_posix()}"'),
        encoding="utf-8",
    )
    core_port = _unused_port()
    web_port = _unused_port()
    url = f"http://127.0.0.1:{web_port}/"
    log_path = tmp_path / "platform.log"
    log = log_path.open("w", encoding="utf-8")
    environment = os.environ.copy()
    api_token = "playwright-browser-api-token"
    environment.update(
        {
            "GRIDEDGE_BINARY": str(binary.resolve()),
            "GRIDEDGE_CONFIG": str(config),
            "GRIDEDGE_DATA": str((ROOT / "tests/fixtures/sample.csv").resolve()),
            "GRIDEDGE_CORE_PORT": str(core_port),
            "GRIDEDGE_WEB_PORT": str(web_port),
            "GRIDEDGE_WEB_SECRET": "playwright-browser-contract",
            "GRIDEDGE_API_TOKEN": api_token,
        }
    )
    process = subprocess.Popen(
        [str(platform)],
        cwd=tmp_path,
        env=environment,
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
    )
    failed = True
    artifact_dir = Path(
        os.environ.get("GRIDEDGE_BROWSER_ARTIFACTS", tmp_path / "artifacts")
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)

    try:
        _wait_ready(url, process)
        _wait_core_ready(core_port, api_token, process)
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            context = browser.new_context(viewport={"width": 1280, "height": 720})
            context.tracing.start(screenshots=True, snapshots=True, sources=True)
            page = context.new_page()
            document_requests: list[str] = []
            page_errors: list[str] = []
            console_errors: list[str] = []
            page.on(
                "request",
                lambda request: document_requests.append(request.url)
                if request.resource_type == "document"
                else None,
            )
            page.on("pageerror", lambda error: page_errors.append(str(error)))
            page.on(
                "console",
                lambda message: console_errors.append(message.text)
                if message.type == "error"
                else None,
            )

            page.goto(url, wait_until="networkidle")
            with sqlite3.connect(database) as connection:
                connection.executescript(
                    """
                    CREATE TRIGGER fail_browser_start_once
                    BEFORE INSERT ON events
                    WHEN NEW.run_id='browser-reactive-contract'
                     AND NEW.event_type='REPLAY_INITIALIZED'
                    BEGIN SELECT RAISE(ABORT, 'injected browser START failure'); END;
                    """
                )
            page.get_by_label("新建单步回放").fill("browser-reactive-contract")
            page.get_by_role("button", name="创建").click()
            page.get_by_text("发现待恢复命令", exact=True).wait_for(timeout=10_000)
            with sqlite3.connect(database) as connection:
                connection.execute("DROP TRIGGER fail_browser_start_once")
            page.get_by_role("button", name="重试原命令").click()
            assert _wait_status(page, action="已暂停", minimum_cursor=0) == 0

            page.get_by_role("button", name="下一根").click()
            assert _wait_status(page, action="已暂停", minimum_cursor=1) == 1
            chart = page.locator("canvas").first
            assert chart.is_visible(), "ECharts did not mount a visible canvas"
            box = chart.bounding_box()
            assert box is not None and box["width"] > 100 and box["height"] > 100

            page.get_by_label("播放速度").click()
            page.get_by_text("0.25 秒", exact=True).click()
            page.get_by_role("button", name="自动播放").click()
            _wait_status(page, action="自动播放中", minimum_cursor=3)
            page.get_by_role("button", name="暂停").click()
            paused_cursor = _wait_status(page, action="已暂停", minimum_cursor=3)
            page.wait_for_timeout(1_250)
            assert (
                _wait_status(page, action="已暂停", minimum_cursor=paused_cursor)
                == paused_cursor
            )

            core_snapshot = _core_snapshot(
                core_port, api_token, "browser-reactive-contract"
            )
            performance = core_snapshot["performance"]
            assert performance["valuation_policy_version"] == "LOT_CONSERVATIVE_EXIT_V1"
            mark_unrealized = performance["mark_to_market_unrealized_grid_pnl"]
            exit_unrealized = performance["conservative_exit_unrealized_grid_pnl"]
            mark_total = performance["total_mark_to_market_grid_pnl"]
            exit_total = performance["total_conservative_exit_grid_pnl"]
            assert all(
                isinstance(value, str)
                for value in (mark_unrealized, exit_unrealized, mark_total, exit_total)
            )
            assert Decimal(exit_unrealized) < Decimal(mark_unrealized)
            expected_metric_cards = {
                "盯市总网格收益": _money_text(mark_total),
                "盯市未实现（未扣退出成本）": _money_text(mark_unrealized),
                "逐 lot 保守退出总收益": _money_text(exit_total),
                "逐 lot 保守退出未实现": _money_text(exit_unrealized),
            }
            for label, expected in expected_metric_cards.items():
                assert expected in _metric_card_text(page, label)
            assert "不代表当前可卖" in page.locator("body").inner_text()

            assert page.url == url
            assert document_requests == [url], (
                "interactive commands performed a top-level navigation: "
                f"{document_requests}"
            )
            assert not page_errors, page_errors
            assert not console_errors, console_errors

            page.reload(wait_until="networkidle")
            assert (
                _wait_status(page, action="已暂停", minimum_cursor=paused_cursor)
                == paused_cursor
            )
            assert document_requests == [url, url]
            for label, expected in expected_metric_cards.items():
                assert expected in _metric_card_text(page, label)

            finish_button = page.get_by_role("button", name="运行至结束", exact=True)
            assert finish_button.is_visible()
            assert finish_button.is_enabled()
            finish_button.click()
            assert _wait_status(page, action="已暂停", minimum_cursor=21) == 21
            assert document_requests == [url, url]
            finished_snapshot = _core_snapshot(
                core_port, api_token, "browser-reactive-contract"
            )
            assert finished_snapshot["progress"]["processed_bars"] == 21
            assert finished_snapshot["playback"]["active"] is False
            with sqlite3.connect(database) as connection:
                finish_facts = connection.execute(
                    """
                    SELECT
                      (SELECT COUNT(*) FROM web_command_inbox
                        WHERE run_id='browser-reactive-contract'
                          AND command='finish'),
                      (SELECT COUNT(*) FROM web_command_inbox
                        WHERE run_id='browser-reactive-contract'
                          AND command='finish' AND receipt_state='COMPLETED'),
                      (SELECT COUNT(*) FROM events
                        WHERE run_id='browser-reactive-contract'
                          AND event_type='MARKET_DATA_RECEIVED'),
                      (SELECT COUNT(*) FROM events
                        WHERE run_id='browser-reactive-contract'
                          AND event_type='MARKET_BAR_DECISIONS_COMMITTED'),
                      (SELECT COUNT(*) FROM events
                        WHERE run_id='browser-reactive-contract'
                          AND event_type='MARKET_BAR_PROCESSED'),
                      (SELECT COUNT(*) FROM events
                        WHERE run_id='browser-reactive-contract'
                          AND event_type='SERVICE_STOPPED')
                    """
                ).fetchone()
            assert finish_facts == (1, 1, 21, 21, 21, 1)
            page.get_by_test_id("opportunity-total").wait_for(timeout=5_000)
            opportunity_counts = {
                "total": page.get_by_test_id("opportunity-total").inner_text(),
                "granted": page.get_by_test_id("opportunity-granted").inner_text(),
                "skipped": page.get_by_test_id("opportunity-skipped").inner_text(),
            }
            assert "12" in opportunity_counts["total"]
            assert "8" in opportunity_counts["granted"]
            assert "4" in opportunity_counts["skipped"]
            opportunity_history = page.get_by_test_id("opportunity-history")
            history_text = opportunity_history.inner_text()
            assert "SKIPPED" in history_text
            assert "OBSERVATION_BOUNDARY" in history_text
            assert "跳过原因" in history_text
            assert any(
                disposition in history_text
                for disposition in ["RESERVED", "DEFERRED", "BLOCKED", "RESIDUAL_HELD"]
            ), "granted opportunities were reduced to an unauditable GRANTED label"

            page.reload(wait_until="networkidle")
            assert _wait_status(page, action="已暂停", minimum_cursor=21) == 21
            assert document_requests == [url, url, url]
            refreshed_terminal = _core_snapshot(
                core_port, api_token, "browser-reactive-contract"
            )
            assert refreshed_terminal["sequence"] == finished_snapshot["sequence"]
            assert refreshed_terminal["progress"] == finished_snapshot["progress"]
            assert {
                "total": page.get_by_test_id("opportunity-total").inner_text(),
                "granted": page.get_by_test_id("opportunity-granted").inner_text(),
                "skipped": page.get_by_test_id("opportunity-skipped").inner_text(),
            } == opportunity_counts
            refreshed_history = page.get_by_test_id("opportunity-history").inner_text()
            assert "SKIPPED" in refreshed_history
            assert "OBSERVATION_BOUNDARY" in refreshed_history

            page.set_viewport_size({"width": 390, "height": 844})
            page.wait_for_timeout(250)
            dimensions = page.evaluate(
                "() => ({scroll: document.documentElement.scrollWidth, "
                "client: document.documentElement.clientWidth})"
            )
            assert dimensions["scroll"] <= dimensions["client"] + 1, dimensions

            failed = False
            context.tracing.stop()
            context.close()
            browser.close()
    except BaseException:
        if "context" in locals():
            try:
                page.screenshot(
                    path=str(artifact_dir / "dashboard-failure.png"), full_page=True
                )
                context.tracing.stop(path=str(artifact_dir / "dashboard-trace.zip"))
            except BaseException:
                pass
        raise
    finally:
        _stop(process)
        log.close()
        if failed and log_path.exists():
            shutil.copy2(log_path, artifact_dir / "platform.log")
