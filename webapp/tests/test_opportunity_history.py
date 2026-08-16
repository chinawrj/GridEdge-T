from __future__ import annotations

import asyncio
import inspect
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from gridedge_web import app as dashboard_app
from gridedge_web import models
from gridedge_web.client import GridEdgeCoreClient


def _opportunity(index: int, resolution: str) -> dict[str, Any]:
    granted = resolution == "GRANTED"
    return {
        "opportunity_id": f"opportunity-{index}",
        "run_id": "complete-history",
        "cycle_id": "cycle-1",
        "symbol": "600000.SH",
        "event_time": f"2026-01-05 09:{30 + index:02}:00",
        "grid_index": -index,
        "grid_price": f"{10 - index / 10:.2f}",
        "touch_sequence": index * 10,
        "resolution_sequence": index * 10 + 1,
        "processed_sequence": index * 10 + 2,
        "resolution": resolution,
        "semantics": "CURRENT_V3",
        "standard_quantity": 1500,
        "direction": "BUY",
        "right_id": f"right-{index}" if granted else None,
        "decision_id": f"decision-{index}" if granted else None,
        "reason": None if granted else "OBSERVATION_BOUNDARY",
        "reason_audit_status": None if granted else "RECORDED_UNVERIFIED",
        "terminal_kind": "RESERVED" if granted else None,
        "terminal_reason": None,
        "algorithm_succeeded": True if granted else None,
        "pre_trade_capacity": (
            {
                "eligible_quantity": 0,
                "eligible_lot_ids": [],
                "t_plus_one_blocked_quantity": 0,
                "t_plus_one_blocked_lot_ids": [],
                "risk_blocked_quantity": 0,
                "risk_blocked_lot_ids": [],
                "no_profit_blocked_quantity": 0,
                "no_profit_blocked_lot_ids": [],
                "source_right_ids": [],
                "tranche_ids": [f"tranche-{index}"],
            }
            if granted
            else None
        ),
        "partial_blocks": [],
        "decision_contract_version": 3 if granted else None,
        "gross_available_quantity": 1500 if granted else None,
        "platform_residual_quantity": 0 if granted else None,
        "algorithm_authorized_quantity": 1500 if granted else None,
        "exercise_quantity": 1500 if granted else None,
        "defer_quantity": 0 if granted else None,
        "platform_blocked_quantity": 0 if granted else None,
        "order_intent_quantity": 1500 if granted else None,
        "remaining_decision_quantity": 0 if granted else None,
    }


def _model(name: str) -> type[Any]:
    candidate = getattr(models, name, None)
    assert candidate is not None, f"strict BFF model {name} is missing"
    return candidate


def test_opportunity_dtos_are_strict_and_resolution_complete() -> None:
    record_model = _model("OpportunityRecord")
    counts_model = _model("OpportunityCounts")
    page_model = _model("OpportunityPage")
    history_model = _model("OpportunityHistory")

    granted = record_model.model_validate(_opportunity(1, "GRANTED"))
    skipped = record_model.model_validate(_opportunity(2, "SKIPPED"))
    assert granted.right_id == "right-1"
    assert granted.reason is None
    assert skipped.right_id is None
    assert skipped.reason == "OBSERVATION_BOUNDARY"
    deferred_payload = {
        **_opportunity(3, "GRANTED"),
        "terminal_kind": "DEFERRED",
        "algorithm_succeeded": True,
        "gross_available_quantity": 1500,
        "platform_residual_quantity": 0,
        "algorithm_authorized_quantity": 1500,
        "exercise_quantity": 0,
        "defer_quantity": 1500,
        "platform_blocked_quantity": 0,
        "order_intent_quantity": 0,
        "remaining_decision_quantity": 1500,
    }
    algorithm_failure_payload = {
        **deferred_payload,
        "terminal_kind": "BLOCKED",
        "terminal_reason": "ALGORITHM_TIMEOUT",
        "algorithm_succeeded": False,
    }
    deferred = record_model.model_validate(deferred_payload)
    algorithm_failure = record_model.model_validate(algorithm_failure_payload)
    assert deferred.terminal_kind == "DEFERRED"
    assert deferred.algorithm_succeeded is True
    assert algorithm_failure.terminal_kind == "BLOCKED"
    assert algorithm_failure.terminal_reason == "ALGORITHM_TIMEOUT"
    assert algorithm_failure.algorithm_succeeded is False
    assert deferred.model_dump() != algorithm_failure.model_dump()

    partial_payload = {
        **_opportunity(4, "GRANTED"),
        "pre_trade_capacity": {
            **_opportunity(4, "GRANTED")["pre_trade_capacity"],
            "t_plus_one_blocked_quantity": 700,
            "t_plus_one_blocked_lot_ids": ["lot-today"],
        },
        "partial_blocks": [
            {
                "sequence": 43,
                "reason": "T_PLUS_ONE_RESTRICTION",
                "t_plus_one_blocked_quantity": 700,
                "t_plus_one_blocked_lot_ids": ["lot-today"],
                "risk_blocked_quantity": 0,
                "risk_blocked_lot_ids": [],
                "no_profit_blocked_quantity": 0,
                "no_profit_blocked_lot_ids": [],
            }
        ],
    }
    partial = record_model.model_validate(partial_payload)
    assert partial.partial_blocks[0].t_plus_one_blocked_lot_ids == ["lot-today"]

    full_t1_payload = {
        **_opportunity(7, "GRANTED"),
        "grid_index": 1,
        "direction": "SELL",
        "terminal_kind": "BLOCKED",
        "terminal_reason": "SELL_BLOCKED_T_PLUS_ONE",
        "pre_trade_capacity": {
            "eligible_quantity": 0,
            "eligible_lot_ids": [],
            "t_plus_one_blocked_quantity": 1500,
            "t_plus_one_blocked_lot_ids": ["same-day-lot"],
            "risk_blocked_quantity": 0,
            "risk_blocked_lot_ids": [],
            "no_profit_blocked_quantity": 0,
            "no_profit_blocked_lot_ids": [],
            "source_right_ids": [],
            "tranche_ids": [],
        },
        "gross_available_quantity": 0,
        "platform_residual_quantity": 0,
        "algorithm_authorized_quantity": 0,
        "exercise_quantity": 0,
        "defer_quantity": 0,
        "platform_blocked_quantity": 0,
        "order_intent_quantity": 0,
        "remaining_decision_quantity": 0,
    }
    full_t1 = record_model.model_validate(full_t1_payload)
    assert full_t1.pre_trade_capacity is not None
    assert full_t1.pre_trade_capacity.t_plus_one_blocked_quantity == 1500
    assert full_t1.pre_trade_capacity.t_plus_one_blocked_lot_ids == ["same-day-lot"]
    assert full_t1.partial_blocks == []
    audit_note = dashboard_app._opportunity_audit_note(full_t1)  # noqa: SLF001
    assert "SELL_BLOCKED_T_PLUS_ONE" in audit_note
    assert "T+1阻断 1500 股 / 1 lot" in audit_note

    legacy_payload = {
        **_opportunity(5, "GRANTED"),
        "semantics": "LEGACY_RECORDED",
        "decision_contract_version": 2,
        "algorithm_succeeded": None,
        "pre_trade_capacity": None,
        "gross_available_quantity": None,
        "platform_residual_quantity": None,
        "algorithm_authorized_quantity": None,
        "exercise_quantity": 1200,
        "defer_quantity": 1800,
        "platform_blocked_quantity": None,
        "order_intent_quantity": 1200,
        "remaining_decision_quantity": None,
    }
    legacy = record_model.model_validate(legacy_payload)
    assert legacy.semantics == "LEGACY_RECORDED"
    assert legacy.exercise_quantity == 1200
    assert legacy.gross_available_quantity is None
    legacy_unbound_payload = {
        **_opportunity(6, "SKIPPED"),
        "resolution": "LEGACY_UNBOUND",
        "semantics": "LEGACY_RECORDED",
        "standard_quantity": 0,
        "reason": "LEGACY_TOUCH_RESOLUTION_UNBOUND",
        "reason_audit_status": "LEGACY_UNBOUND",
    }
    unbound = record_model.model_validate(legacy_unbound_payload)
    assert unbound.resolution == "LEGACY_UNBOUND"
    counts = counts_model.model_validate(
        {"touched": 12, "granted": 8, "skipped": 4, "legacy_unbound": 0}
    )
    assert counts.touched == counts.granted + counts.skipped

    page_model.model_validate(
        {
            "api_version": "gridedge.api/v1",
            "run_id": "complete-history",
            "through_sequence": 233,
            "standard_quantity": 1500,
            "opportunities": [
                _opportunity(1, "GRANTED"),
                _opportunity(2, "SKIPPED"),
            ],
            "next_sequence": 21,
            "complete": False,
            "counts": {"touched": 12, "granted": 8, "skipped": 4, "legacy_unbound": 0},
        }
    )
    with pytest.raises(ValidationError):
        page_model.model_validate(
            {
                "api_version": "gridedge.api/v1",
                "run_id": "complete-history",
                "through_sequence": 233,
                "standard_quantity": 1000,
                "opportunities": [_opportunity(1, "GRANTED")],
                "next_sequence": 10,
                "complete": True,
                "counts": {
                    "touched": 1,
                    "granted": 1,
                    "skipped": 0,
                    "legacy_unbound": 0,
                },
            }
        )
    history_model.model_validate(
        {
            "api_version": "gridedge.api/v1",
            "run_id": "complete-history",
            "through_sequence": 233,
            "standard_quantity": 1500,
            "opportunities": [
                *[_opportunity(index, "GRANTED") for index in range(1, 9)],
                *[_opportunity(index, "SKIPPED") for index in range(9, 13)],
            ],
            "counts": {"touched": 12, "granted": 8, "skipped": 4, "legacy_unbound": 0},
        }
    )
    with pytest.raises(ValidationError):
        history_model.model_validate(
            {
                "api_version": "gridedge.api/v1",
                "run_id": "complete-history",
                "through_sequence": 233,
                "standard_quantity": 1500,
                "opportunities": [
                    {**_opportunity(1, "GRANTED"), "standard_quantity": 1000}
                ],
                "counts": {
                    "touched": 1,
                    "granted": 1,
                    "skipped": 0,
                    "legacy_unbound": 0,
                },
            }
        )

    invalid_cases = [
        {**_opportunity(1, "GRANTED"), "extra": "silently accepted"},
        {**_opportunity(9, "SKIPPED"), "reason": None},
        {**_opportunity(1, "GRANTED"), "right_id": None},
        {**_opportunity(1, "GRANTED"), "resolution_sequence": 9},
        {**_opportunity(1, "GRANTED"), "terminal_kind": None},
        {**_opportunity(1, "GRANTED"), "algorithm_succeeded": None},
        {**_opportunity(2, "SKIPPED"), "reason_audit_status": None},
        {
            **_opportunity(1, "GRANTED"),
            "platform_blocked_quantity": 1500,
            "order_intent_quantity": 1500,
        },
        {
            **partial_payload,
            "partial_blocks": [
                {
                    **partial_payload["partial_blocks"][0],
                    "t_plus_one_blocked_quantity": 600,
                }
            ],
        },
        {
            **partial_payload,
            "partial_blocks": [
                {
                    **partial_payload["partial_blocks"][0],
                    "t_plus_one_blocked_lot_ids": ["different-lot"],
                }
            ],
        },
    ]
    for invalid in invalid_cases:
        with pytest.raises(ValidationError):
            record_model.model_validate(invalid)
    for legacy_version in (1, 2):
        with pytest.raises(ValidationError):
            record_model.model_validate(
                {
                    **legacy_payload,
                    "decision_contract_version": legacy_version,
                    "gross_available_quantity": 0,
                }
            )
    with pytest.raises(ValidationError):
        counts_model.model_validate({"touched": 12, "granted": 8, "skipped": 3})


def test_client_fetches_every_opportunity_page_at_one_frozen_prefix() -> None:
    records = [
        *[_opportunity(index, "GRANTED") for index in range(1, 9)],
        *[_opportunity(index, "SKIPPED") for index in range(9, 13)],
    ]
    calls: list[tuple[int, int, int]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/runs/complete-history/opportunities"
        after = int(request.url.params["after"])
        through = int(request.url.params["through"])
        limit = int(request.url.params["limit"])
        calls.append((after, through, limit))
        assert through == 233
        remaining = [
            record for record in records if record["resolution_sequence"] > after
        ]
        page_records = remaining[:limit]
        complete = len(page_records) == len(remaining)
        next_sequence = (
            int(page_records[-1]["resolution_sequence"]) if page_records else after
        )
        return httpx.Response(
            200,
            request=request,
            json={
                "api_version": "gridedge.api/v1",
                "run_id": "complete-history",
                "through_sequence": through,
                "standard_quantity": 1500,
                "opportunities": page_records,
                "next_sequence": next_sequence,
                "complete": complete,
                "counts": {
                    "touched": 12,
                    "granted": 8,
                    "skipped": 4,
                    "legacy_unbound": 0,
                },
            },
        )

    async def exercise() -> Any:
        client = GridEdgeCoreClient("http://core.invalid", "test-token")
        await client.close()
        client._client = httpx.AsyncClient(  # noqa: SLF001 - transport boundary fixture
            base_url="http://core.invalid",
            headers={"x-gridedge-api-token": "test-token"},
            transport=httpx.MockTransport(handler),
        )
        try:
            assert hasattr(
                client, "opportunities"
            ), "full-history client method is missing"
            return await client.opportunities(
                "complete-history", through_sequence=233, page_limit=3
            )
        finally:
            await client.close()

    history = asyncio.run(exercise())
    assert history.counts.touched == 12
    assert history.counts.granted == 8
    assert history.counts.skipped == 4
    assert [record.opportunity_id for record in history.opportunities] == [
        f"opportunity-{index}" for index in range(1, 13)
    ]
    assert len(calls) == 4
    assert {through for _, through, _ in calls} == {233}
    assert all(limit == 3 for _, _, limit in calls)
    assert [after for after, _, _ in calls] == [0, 31, 61, 91]


def test_client_rejects_a_nonadvancing_opportunity_cursor() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            request=request,
            json={
                "api_version": "gridedge.api/v1",
                "run_id": "stalled-history",
                "through_sequence": 100,
                "standard_quantity": 1500,
                "opportunities": [],
                "next_sequence": 0,
                "complete": False,
                "counts": {
                    "touched": 1,
                    "granted": 0,
                    "skipped": 1,
                    "legacy_unbound": 0,
                },
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
            assert hasattr(
                client, "opportunities"
            ), "full-history client method is missing"
            with pytest.raises(RuntimeError, match="cursor|advance"):
                await client.opportunities(
                    "stalled-history", through_sequence=100, page_limit=20
                )
        finally:
            await client.close()

    asyncio.run(exercise())
    assert calls == 1


def test_dashboard_uses_complete_opportunity_history_and_exposes_auditable_totals() -> (
    None
):
    source = inspect.getsource(dashboard_app)
    assert "await core.opportunities(" in source
    for contract in [
        "机械机会总数",
        "授予机会",
        "跳过机会",
        "跳过原因",
        "opportunity-total",
        "opportunity-granted",
        "opportunity-skipped",
        "opportunity-history",
        "terminal_kind",
        "terminal_reason",
        "partial_blocks",
        "旧版记录",
    ]:
        assert (
            contract in source
        ), f"dashboard opportunity contract is missing: {contract}"
    assert "snapshot.sequence - 80" not in source
