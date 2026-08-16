import asyncio
import json

import httpx

from gridedge_web.client import GridEdgeCoreClient


def test_read_timeout_queries_the_durable_receipt_without_reposting_the_command() -> (
    None
):
    requests: list[tuple[str, dict[str, object]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.url.path, json.loads(request.content)))
        if len(requests) == 1:
            raise httpx.ReadTimeout("long FINISH response timed out", request=request)
        assert request.url.path == "/api/v1/pending-commands/retry"
        assert json.loads(request.content) == {
            "run_id": "run-1",
            "request_id": "finish-timeout-1",
        }
        return httpx.Response(
            200,
            json={
                "api_version": "gridedge.api/v1",
                "request_id": "finish-timeout-1",
                "command": "finish",
                "run_id": "run-1",
                "accepted": True,
                "message": "replay completed",
                "accepted_sequence": 4_242,
                "accepted_version": 3,
            },
        )

    async def exercise() -> None:
        client = GridEdgeCoreClient("http://core.invalid", "test-token")
        await client.close()
        client._client = httpx.AsyncClient(  # noqa: SLF001 - transport boundary fixture
            base_url="http://core.invalid",
            headers={"x-gridedge-api-token": "test-token"},
            transport=httpx.MockTransport(handler),
        )
        try:
            result = await client.command(
                "finish",
                "run-1",
                expected_sequence=40,
                expected_version=2,
                request_id="finish-timeout-1",
            )
            assert result.accepted_sequence == 4_242
            assert result.accepted_version == 3
        finally:
            await client.close()

    asyncio.run(exercise())
    assert len(requests) == 2
    assert requests[0][0] == "/api/v1/commands"
    assert requests[0][1]["api_version"] == "gridedge.api/v1"
    assert requests[0][1]["command"] == "finish"
    assert requests[0][1]["expected_sequence"] == 40
    assert requests[0][1]["expected_version"] == 2
    assert requests[0][1]["request_id"] == "finish-timeout-1"


def test_http_conflict_is_not_retried_as_a_transport_failure() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            409, request=request, json={"error": "stale command version"}
        )

    async def exercise() -> None:
        client = GridEdgeCoreClient("http://core.invalid", "test-token")
        await client.close()
        client._client = httpx.AsyncClient(  # noqa: SLF001 - transport boundary fixture
            base_url="http://core.invalid",
            headers={"x-gridedge-api-token": "test-token"},
            transport=httpx.MockTransport(handler),
        )
        try:
            try:
                await client.command(
                    "step",
                    "run-1",
                    expected_sequence=40,
                    expected_version=2,
                    request_id="stale-step",
                )
            except httpx.HTTPStatusError as error:
                assert error.response.status_code == 409
            else:
                raise AssertionError("HTTP conflict was accepted")
        finally:
            await client.close()

    asyncio.run(exercise())
    assert calls == 1


def test_pending_discovery_and_retry_use_dedicated_minimal_endpoints() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET":
            assert request.url.path == "/api/v1/pending-commands"
            return httpx.Response(
                200,
                request=request,
                json={
                    "api_version": "gridedge.api/v1",
                    "commands": [
                        {
                            "run_id": "no-event-start",
                            "request_id": "start-request-1",
                            "command": "start",
                            "accepted_version": 1,
                            "recovery_state": "retryable",
                        }
                    ],
                },
            )
        assert request.url.path == "/api/v1/pending-commands/retry"
        assert json.loads(request.content) == {
            "run_id": "no-event-start",
            "request_id": "start-request-1",
        }
        return httpx.Response(
            200,
            request=request,
            json={
                "api_version": "gridedge.api/v1",
                "request_id": "start-request-1",
                "command": "start",
                "run_id": "no-event-start",
                "accepted": True,
                "message": "step replay created",
                "accepted_sequence": 2,
                "accepted_version": 1,
            },
        )

    async def exercise() -> None:
        client = GridEdgeCoreClient("http://core.invalid", "test-token")
        await client.close()
        client._client = httpx.AsyncClient(  # noqa: SLF001 - transport boundary fixture
            base_url="http://core.invalid",
            headers={"x-gridedge-api-token": "test-token"},
            transport=httpx.MockTransport(handler),
        )
        try:
            assert hasattr(client, "pending_commands")
            assert hasattr(client, "retry_pending")
            pending = await client.pending_commands()
            assert pending.commands[0].run_id == "no-event-start"
            result = await client.retry_pending("no-event-start", "start-request-1")
            assert result.accepted_version == 1
        finally:
            await client.close()

    asyncio.run(exercise())
    assert [request.method for request in requests] == ["GET", "POST"]


def test_retry_pending_does_not_regenerate_the_original_command_envelope() -> None:
    captured: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(json.loads(request.content))
        return httpx.Response(
            409,
            request=request,
            json={"code": "PENDING_PLAN_CONFLICT", "error": "stored plan is invalid"},
        )

    async def exercise() -> None:
        client = GridEdgeCoreClient("http://core.invalid", "test-token")
        await client.close()
        client._client = httpx.AsyncClient(  # noqa: SLF001 - transport boundary fixture
            base_url="http://core.invalid",
            headers={"x-gridedge-api-token": "test-token"},
            transport=httpx.MockTransport(handler),
        )
        try:
            assert hasattr(client, "retry_pending")
            try:
                await client.retry_pending("tampered", "request-1")
            except httpx.HTTPStatusError as error:
                assert error.response.status_code == 409
            else:
                raise AssertionError("tampered pending plan was accepted")
        finally:
            await client.close()

    asyncio.run(exercise())
    assert captured == [{"run_id": "tampered", "request_id": "request-1"}]


def test_bars_client_requests_the_run_bound_sampled_prefix() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            request=request,
            json={
                "api_version": "gridedge.api/v1",
                "run_id": "run-1",
                "dataset_id": "sample.csv",
                "data_sha256": "abc123",
                "visible_bars": 1,
                "total_bars": 21,
                "sampled": [
                    {
                        "timestamp": "2026-01-05 09:30:00",
                        "symbol": "600000.SH",
                        "open": "10.00",
                        "high": "10.99",
                        "low": "9.01",
                        "close": "10.00",
                        "volume": 1000,
                        "amount": "10000",
                    }
                ],
            },
        )

    async def exercise() -> None:
        client = GridEdgeCoreClient("http://core.invalid", "test-token")
        await client.close()
        client._client = httpx.AsyncClient(  # noqa: SLF001 - transport boundary fixture
            base_url="http://core.invalid",
            headers={"x-gridedge-api-token": "test-token"},
            transport=httpx.MockTransport(handler),
        )
        try:
            batch = await client.bars("run-1", max_points=321)
            assert batch.dataset_id == "sample.csv"
            assert batch.data_sha256 == "abc123"
            assert batch.sampled[0].high == "10.99"
            assert batch.sampled[0].low == "9.01"
        finally:
            await client.close()

    asyncio.run(exercise())
    assert len(requests) == 1
    assert requests[0].url.path == "/api/v1/runs/run-1/bars"
    assert requests[0].url.params["max_points"] == "321"
