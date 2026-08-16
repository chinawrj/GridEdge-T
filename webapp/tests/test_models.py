import gridedge_web.models as models
import pytest
from pydantic import ValidationError

from gridedge_web.models import BarBatch, CommandResult, Performance, RunSnapshot


def exact_performance_payload() -> dict[str, object]:
    return {
        "realized_grid_pnl": "12.34",
        "unrealized_grid_pnl": "5.00",
        "total_grid_pnl": "17.34",
        "valuation_policy_version": "LOT_CONSERVATIVE_EXIT_V1",
        "mark_price": "10.10",
        "mark_to_market_unrealized_grid_pnl": "5.00",
        "total_mark_to_market_grid_pnl": "17.34",
        "conservative_exit_unrealized_grid_pnl": "-2.01",
        "total_conservative_exit_grid_pnl": "10.33",
        "conservative_exit_adjustment": "-7.01",
        "unpriced_open_quantity": 0,
    }


def test_snapshot_contract_rejects_unknown_api_versions() -> None:
    payload = {
        "api_version": "gridedge.api/v2",
        "sequence": 1,
        "lot_size": 100,
        "standard_quantity": 6000,
        "state": {},
        "performance": exact_performance_payload(),
        "progress": None,
        "playback": {"active": False, "interval_ms": 0, "command_version": 0},
        "command_version": 0,
    }
    try:
        RunSnapshot.model_validate(payload)
    except ValueError:
        return
    raise AssertionError("unknown API version must fail closed")


def test_snapshot_contract_accepts_v1_decimal_strings() -> None:
    snapshot = RunSnapshot.model_validate(
        {
            "api_version": "gridedge.api/v1",
            "sequence": 42,
            "lot_size": 100,
            "standard_quantity": 6000,
            "state": {"realized_pnl": "12.34"},
            "performance": exact_performance_payload(),
            "progress": None,
            "playback": {"active": False, "interval_ms": 0, "command_version": 7},
            "command_version": 7,
        }
    )
    assert snapshot.sequence == 42
    assert snapshot.state["realized_pnl"] == "12.34"
    assert (
        snapshot.performance.unrealized_grid_pnl
        == snapshot.performance.mark_to_market_unrealized_grid_pnl
    )
    assert (
        snapshot.performance.total_grid_pnl
        == snapshot.performance.total_mark_to_market_grid_pnl
    )
    assert snapshot.performance.mark_to_market_unrealized_grid_pnl == "5.00"
    assert snapshot.performance.conservative_exit_unrealized_grid_pnl == "-2.01"
    assert snapshot.performance.conservative_exit_adjustment == "-7.01"
    assert snapshot.command_version == 7


def test_performance_contract_requires_every_named_field_and_exact_decimal_text() -> (
    None
):
    performance = Performance.model_validate(exact_performance_payload())
    assert performance.total_mark_to_market_grid_pnl == "17.34"
    assert performance.total_conservative_exit_grid_pnl == "10.33"

    for field in exact_performance_payload():
        missing = exact_performance_payload()
        del missing[field]
        with pytest.raises(ValidationError, match=field):
            Performance.model_validate(missing)

    for field in (
        "realized_grid_pnl",
        "mark_price",
        "mark_to_market_unrealized_grid_pnl",
        "conservative_exit_unrealized_grid_pnl",
        "conservative_exit_adjustment",
    ):
        numeric = exact_performance_payload()
        numeric[field] = 1.25
        with pytest.raises(ValidationError, match=field):
            Performance.model_validate(numeric)

    extra = exact_performance_payload()
    extra["ambiguous_profit"] = "999"
    with pytest.raises(ValidationError, match="ambiguous_profit"):
        Performance.model_validate(extra)


def test_unavailable_valuation_is_explicit_null_not_an_implicit_zero() -> None:
    unavailable = exact_performance_payload()
    for field in (
        "unrealized_grid_pnl",
        "total_grid_pnl",
        "mark_price",
        "mark_to_market_unrealized_grid_pnl",
        "total_mark_to_market_grid_pnl",
        "conservative_exit_unrealized_grid_pnl",
        "total_conservative_exit_grid_pnl",
        "conservative_exit_adjustment",
    ):
        unavailable[field] = None
    unavailable["valuation_policy_version"] = None
    unavailable["unpriced_open_quantity"] = 100
    performance = Performance.model_validate(unavailable)
    assert performance.unrealized_grid_pnl is None
    assert performance.total_grid_pnl is None
    assert performance.mark_to_market_unrealized_grid_pnl is None
    assert performance.conservative_exit_unrealized_grid_pnl is None
    assert performance.unpriced_open_quantity == 100

    snapshot_payload = {
        "api_version": "gridedge.api/v1",
        "sequence": 1,
        "lot_size": 100,
        "standard_quantity": 1500,
        "state": {},
        "progress": None,
        "playback": {"active": False, "interval_ms": 0, "command_version": 0},
        "command_version": 0,
    }
    with pytest.raises(ValidationError, match="performance"):
        RunSnapshot.model_validate(snapshot_payload)


def test_command_result_requires_the_durable_acceptance_coordinates() -> None:
    payload = {
        "api_version": "gridedge.api/v1",
        "request_id": "step-1",
        "command": "step",
        "run_id": "run-1",
        "accepted": True,
        "message": "advanced one bar",
    }
    try:
        CommandResult.model_validate(payload)
    except ValueError:
        pass
    else:
        raise AssertionError(
            "a command response without durable coordinates was accepted"
        )

    payload.update({"accepted_sequence": 42, "accepted_version": 3})
    result = CommandResult.model_validate(payload)
    assert result.accepted_sequence == 42
    assert result.accepted_version == 3


def test_bar_batch_contract_keeps_decimal_ohlc_and_run_dataset_identity() -> None:
    batch = BarBatch.model_validate(
        {
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
        }
    )
    assert batch.dataset_id == "sample.csv"
    assert batch.data_sha256 == "abc123"
    assert batch.sampled[0].high == "10.99"
    assert batch.sampled[0].low == "9.01"

    invalid = batch.model_dump()
    invalid["unexpected"] = "must fail closed"
    try:
        BarBatch.model_validate(invalid)
    except ValueError:
        return
    raise AssertionError("bar batch accepted an unknown field")


def test_pending_command_discovery_contract_is_versioned_strict_and_never_leaks_request() -> (
    None
):
    assert hasattr(models, "PendingCommandBatch"), "pending recovery DTO is missing"
    batch_type = models.PendingCommandBatch
    batch = batch_type.model_validate(
        {
            "api_version": "gridedge.api/v1",
            "commands": [
                {
                    "run_id": "no-event-start",
                    "request_id": "start-request-1",
                    "command": "start",
                    "accepted_version": 1,
                    "recovery_state": "retryable",
                },
                {
                    "run_id": "legacy-start",
                    "request_id": "legacy-request-1",
                    "command": "start",
                    "accepted_version": 1,
                    "recovery_state": "blocked",
                },
            ],
        }
    )
    assert batch.commands[0].recovery_state == "retryable"
    assert batch.commands[1].recovery_state == "blocked"
    assert not hasattr(batch.commands[0], "request_json")

    for mutation in (
        {"api_version": "gridedge.api/v2"},
        {"unexpected": "must fail closed"},
    ):
        payload = batch.model_dump()
        payload.update(mutation)
        try:
            batch_type.model_validate(payload)
        except ValueError:
            pass
        else:
            raise AssertionError(f"invalid pending command batch accepted: {mutation}")
