from __future__ import annotations

from uuid import uuid4

import httpx

from .models import (
    BarBatch,
    CommandResult,
    EventBatch,
    OpportunityHistory,
    OpportunityPage,
    PendingCommandBatch,
    RunSnapshot,
)


class GridEdgeCoreClient:
    def __init__(
        self,
        base_url: str,
        api_token: str,
        timeout: float = 15.0,
        finish_timeout: float = 240.0,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={"x-gridedge-api-token": api_token},
            timeout=timeout,
        )
        self._finish_timeout = httpx.Timeout(
            finish_timeout,
            connect=timeout,
            write=timeout,
            pool=timeout,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def runs(self) -> list[str]:
        response = await self._client.get("/api/v1/runs")
        response.raise_for_status()
        return list(response.json())

    async def snapshot(self, run_id: str) -> RunSnapshot:
        response = await self._client.get(f"/api/v1/runs/{run_id}")
        response.raise_for_status()
        return RunSnapshot.model_validate(response.json())

    async def events(self, run_id: str, after: int, limit: int = 250) -> EventBatch:
        response = await self._client.get(
            f"/api/v1/runs/{run_id}/events",
            params={"after": after, "limit": limit},
        )
        response.raise_for_status()
        return EventBatch.model_validate(response.json())

    async def opportunities(
        self,
        run_id: str,
        *,
        through_sequence: int,
        page_limit: int = 250,
    ) -> OpportunityHistory:
        if through_sequence < 0 or not 1 <= page_limit <= 250:
            raise ValueError("invalid opportunity history prefix")
        after = 0
        records = []
        counts = None
        standard_quantity = None
        seen_ids: set[str] = set()
        for _ in range(100_000):
            response = await self._client.get(
                f"/api/v1/runs/{run_id}/opportunities",
                params={
                    "after": after,
                    "through": through_sequence,
                    "limit": page_limit,
                },
            )
            response.raise_for_status()
            page = OpportunityPage.model_validate(response.json())
            if page.run_id != run_id or page.through_sequence != through_sequence:
                raise RuntimeError("opportunity page changed its frozen run prefix")
            if standard_quantity is None:
                standard_quantity = page.standard_quantity
            elif page.standard_quantity != standard_quantity:
                raise RuntimeError("opportunity unit changed between pages")
            if counts is None:
                counts = page.counts
            elif page.counts != counts:
                raise RuntimeError("opportunity totals changed between pages")
            for record in page.opportunities:
                if record.run_id != run_id or record.opportunity_id in seen_ids:
                    raise RuntimeError(
                        "opportunity history contains a duplicate or foreign row"
                    )
                if record.touch_sequence <= after:
                    raise RuntimeError(
                        "opportunity page did not advance in journal order"
                    )
                seen_ids.add(record.opportunity_id)
                records.append(record)
            if page.complete:
                break
            if page.next_sequence <= after:
                raise RuntimeError("opportunity cursor did not advance")
            after = page.next_sequence
        else:
            raise RuntimeError("opportunity pagination did not terminate")
        if counts is None or standard_quantity is None:
            raise RuntimeError("opportunity endpoint returned no page totals")
        if len(records) != counts.touched:
            raise RuntimeError("opportunity history is incomplete")
        granted = sum(record.resolution == "GRANTED" for record in records)
        skipped = sum(record.resolution == "SKIPPED" for record in records)
        legacy_unbound = sum(
            record.resolution == "LEGACY_UNBOUND" for record in records
        )
        if (granted, skipped, legacy_unbound) != (
            counts.granted,
            counts.skipped,
            counts.legacy_unbound,
        ):
            raise RuntimeError("opportunity history does not match its totals")
        return OpportunityHistory(
            api_version="gridedge.api/v1",
            run_id=run_id,
            through_sequence=through_sequence,
            standard_quantity=standard_quantity,
            opportunities=records,
            counts=counts,
        )

    async def bars(self, run_id: str, max_points: int = 1_000) -> BarBatch:
        response = await self._client.get(
            f"/api/v1/runs/{run_id}/bars",
            params={"max_points": max_points},
        )
        response.raise_for_status()
        return BarBatch.model_validate(response.json())

    async def pending_commands(self) -> PendingCommandBatch:
        response = await self._client.get("/api/v1/pending-commands")
        response.raise_for_status()
        return PendingCommandBatch.model_validate(response.json())

    async def retry_pending(
        self,
        run_id: str,
        request_id: str,
        *,
        timeout: httpx.Timeout | None = None,
    ) -> CommandResult:
        response = await self._client.post(
            "/api/v1/pending-commands/retry",
            json={
                "run_id": run_id,
                "request_id": request_id,
            },
            timeout=timeout or self._finish_timeout,
        )
        response.raise_for_status()
        return CommandResult.model_validate(response.json())

    async def command(
        self,
        command: str,
        run_id: str,
        *,
        dataset: str | None = None,
        speed_ms: int | None = None,
        expected_sequence: int,
        expected_version: int,
        request_id: str | None = None,
    ) -> CommandResult:
        durable_request_id = request_id or str(uuid4())
        payload = {
            "api_version": "gridedge.api/v1",
            "request_id": durable_request_id,
            "command": command,
            "run_id": run_id,
            "dataset": dataset,
            "speed_ms": speed_ms,
            "expected_sequence": expected_sequence,
            "expected_version": expected_version,
        }
        try:
            response = await self._client.post(
                "/api/v1/commands",
                json=payload,
                timeout=(
                    self._finish_timeout
                    if command == "finish"
                    else self._client.timeout
                ),
            )
        except httpx.TransportError:
            try:
                return await self.retry_pending(
                    run_id,
                    durable_request_id,
                    timeout=self._finish_timeout,
                )
            except httpx.HTTPStatusError as error:
                if error.response.status_code != 404:
                    raise
            response = await self._client.post(
                "/api/v1/commands",
                json=payload,
                timeout=(
                    self._finish_timeout
                    if command == "finish"
                    else self._client.timeout
                ),
            )
        response.raise_for_status()
        return CommandResult.model_validate(response.json())
