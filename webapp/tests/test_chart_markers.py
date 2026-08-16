from gridedge_web.app import _chart_markers, _echarts_time, _right_capacity_text
from gridedge_web.models import RunSnapshot


def exact_unavailable_performance() -> dict[str, object]:
    return {
        "realized_grid_pnl": "0",
        "unrealized_grid_pnl": None,
        "total_grid_pnl": None,
        "valuation_policy_version": None,
        "mark_price": None,
        "mark_to_market_unrealized_grid_pnl": None,
        "total_mark_to_market_grid_pnl": None,
        "conservative_exit_unrealized_grid_pnl": None,
        "total_conservative_exit_grid_pnl": None,
        "conservative_exit_adjustment": None,
        "unpriced_open_quantity": 0,
    }


def test_echarts_time_normalizes_bar_and_marker_serialization_to_one_axis() -> None:
    assert _echarts_time("2026-01-05 10:05:00") == "2026-01-05T10:05:00"
    assert _echarts_time("2026-01-05T10:05:00") == "2026-01-05T10:05:00"


def test_right_capacity_uses_directional_shares_and_never_treats_string_zero_as_truthy() -> (
    None
):
    sell = {
        "direction": "SELL",
        "capacity": {
            "available_quantity": 0,
            "available_budget": "0",
            "eligible_quantity": 1700,
        },
    }
    assert _right_capacity_text(sell, 1500) == "1 份；残余 200 股；总计 1,700 股"
    assert _right_capacity_text(
        {**sell, "capacity": {"eligible_quantity": 0}}, 1500
    ) == ("0 份；总计 0 股")
    buy = {"direction": "BUY", "capacity": {"available_quantity": 3000}}
    assert _right_capacity_text(buy, 1500) == "2 份；总计 3,000 股"
    legacy_buy = {"direction": "BUY", "capacity": {"available_budget": "10000"}}
    assert _right_capacity_text(legacy_buy, 0) == "历史预算 ¥10,000.00"


def test_decided_capacity_uses_frozen_c_a_p_and_keeps_pretrade_blocks_separate() -> (
    None
):
    current = {
        "direction": "SELL",
        "decision_contract_version": 3,
        "decision_gross_available_quantity": 1800,
        "decision_algorithm_authorized_quantity": 1500,
        "decision_platform_residual_quantity": 300,
        "decision_platform_blocked_quantity": 1500,
        "capacity": {
            "available_quantity": 0,
            "available_budget": "0",
            "eligible_quantity": 9999,
            "t_plus_one_blocked_quantity": 1500,
        },
    }
    assert _right_capacity_text(current, 1500) == (
        "授权 1 份；残余 300 股；总计 1,800 股；平台阻断 1 份；T+1阻断 1,500 股"
    )


def test_chart_markers_encode_exercised_and_voluntarily_deferred_authorization() -> (
    None
):
    snapshot = RunSnapshot.model_validate(
        {
            "api_version": "gridedge.api/v1",
            "sequence": 20,
            "lot_size": 100,
            "standard_quantity": 600,
            "progress": None,
            "playback": {"active": False, "interval_ms": 0, "command_version": 0},
            "command_version": 0,
            "performance": exact_unavailable_performance(),
            "state": {
                "orders": {
                    "buy": {
                        "filled_quantity": 100,
                        "intent": {
                            "right_id": "buy-right",
                            "direction": "BUY",
                            "quantity": 600,
                            "created_at": "2026-01-05T10:00:00",
                            "limit_price": "2.50",
                        },
                    },
                    "sell": {
                        "filled_quantity": 0,
                        "intent": {
                            "right_id": "sell-right",
                            "direction": "SELL",
                            "quantity": 600,
                            "created_at": "2026-01-06T10:00:00",
                            "limit_price": "2.70",
                        },
                    },
                },
                "grid_rights": {
                    "buy-right": {
                        "direction": "BUY",
                        "status": "PARTIALLY_EXERCISED",
                        "decision_contract_version": 3,
                        "decision_gross_available_quantity": 1200,
                        "decision_platform_residual_quantity": 0,
                        "decision_algorithm_authorized_quantity": 1200,
                        "decided_exercise_quantity": 600,
                        "decided_defer_quantity": 600,
                        "decision_platform_blocked_quantity": 0,
                        "decision_intent_quantity": 600,
                        "decision_remaining_quantity": 600,
                        "decision_is_algorithm": True,
                        "grid_price": "2.50",
                        "granted_at": "2026-01-05T10:05:00",
                        "capacity": {"available_quantity": 1200},
                    },
                    "sell-right": {
                        "direction": "SELL",
                        "status": "PARTIALLY_EXERCISED",
                        "decision_contract_version": 3,
                        "decision_gross_available_quantity": 1200,
                        "decision_platform_residual_quantity": 0,
                        "decision_algorithm_authorized_quantity": 1200,
                        "decided_exercise_quantity": 600,
                        "decided_defer_quantity": 600,
                        "decision_platform_blocked_quantity": 0,
                        "decision_intent_quantity": 600,
                        "decision_remaining_quantity": 600,
                        "decision_is_algorithm": True,
                        "grid_price": "2.70",
                        "granted_at": "2026-01-06T10:05:00",
                        "capacity": {
                            "eligible_quantity": 1200,
                            "tranche_ids": ["sell-tranche"],
                            "t_plus_one_blocked_quantity": 0,
                            "risk_blocked_quantity": 0,
                            "no_profit_blocked_quantity": 0,
                        },
                    },
                },
                "right_tranches": {},
            },
        }
    )
    markers = _chart_markers(snapshot)
    assert markers["buy"][0]["label"]["formatter"] == "1"
    assert markers["sell"][0]["label"]["formatter"] == "1"
    assert markers["buy_right"][0]["label"]["formatter"] == "1"
    assert markers["sell_right"][0]["label"]["formatter"] == "1"
    assert markers["buy_right"][0]["itemStyle"]["borderWidth"] == 2
    assert markers["sell_right"][0]["itemStyle"]["borderWidth"] == 2
    assert markers["buy"][0]["symbolOffset"] == [-14, 0]
    assert markers["sell"][0]["symbolOffset"] == [-14, 0]
    assert markers["buy_right"][0]["symbolOffset"] == [14, 0]
    assert markers["sell_right"][0]["symbolOffset"] == [14, 0]
    assert "行权数量" in markers["buy"][0]["name"]
    assert "剩余可行权数量" in markers["buy_right"][0]["name"]


def test_platform_blocked_quantity_is_not_mislabeled_as_algorithm_defer() -> None:
    snapshot = RunSnapshot.model_validate(
        {
            "api_version": "gridedge.api/v1",
            "sequence": 1,
            "lot_size": 100,
            "standard_quantity": 600,
            "progress": None,
            "playback": {"active": False, "interval_ms": 0, "command_version": 0},
            "command_version": 0,
            "performance": exact_unavailable_performance(),
            "state": {
                "orders": {},
                "grid_rights": {
                    "blocked": {
                        "direction": "SELL",
                        "status": "BLOCKED",
                        "grid_price": "2.70",
                        "granted_at": "2026-01-06T10:05:00",
                        "capacity": {
                            "eligible_quantity": 0,
                            "t_plus_one_blocked_quantity": 300,
                            "risk_blocked_quantity": 200,
                            "no_profit_blocked_quantity": 100,
                        },
                    }
                },
                "right_tranches": {},
            },
        }
    )
    markers = _chart_markers(snapshot)
    assert markers["sell_right"] == []


def test_fully_exercised_decision_still_displays_zero_remaining_units() -> None:
    snapshot = RunSnapshot.model_validate(
        {
            "api_version": "gridedge.api/v1",
            "sequence": 2,
            "lot_size": 100,
            "standard_quantity": 800,
            "progress": None,
            "playback": {"active": False, "interval_ms": 0, "command_version": 0},
            "command_version": 0,
            "performance": exact_unavailable_performance(),
            "state": {
                "orders": {
                    "order": {
                        "intent": {
                            "right_id": "right",
                            "direction": "BUY",
                            "quantity": 800,
                        }
                    }
                },
                "grid_rights": {
                    "right": {
                        "direction": "BUY",
                        "status": "EXERCISED",
                        "decision_contract_version": 3,
                        "decision_gross_available_quantity": 800,
                        "decision_platform_residual_quantity": 0,
                        "decision_algorithm_authorized_quantity": 800,
                        "decided_exercise_quantity": 800,
                        "decided_defer_quantity": 0,
                        "decision_platform_blocked_quantity": 0,
                        "decision_intent_quantity": 800,
                        "decision_remaining_quantity": 0,
                        "decision_is_algorithm": True,
                        "grid_price": "2.50",
                        "granted_at": "2026-01-05T10:00:00",
                        "capacity": {
                            "available_quantity": 800,
                            "tranche_ids": ["tranche"],
                        },
                    }
                },
                "right_tranches": {},
            },
        }
    )
    markers = _chart_markers(snapshot)
    assert markers["buy"][0]["label"]["formatter"] == "1"
    assert markers["buy_right"][0]["label"]["formatter"] == "0"


def test_buy_platform_block_is_never_drawn_as_algorithm_defer() -> None:
    snapshot = RunSnapshot.model_validate(
        {
            "api_version": "gridedge.api/v1",
            "sequence": 3,
            "lot_size": 100,
            "standard_quantity": 6000,
            "progress": None,
            "playback": {"active": False, "interval_ms": 0, "command_version": 0},
            "command_version": 0,
            "performance": exact_unavailable_performance(),
            "state": {
                "orders": {},
                "grid_rights": {
                    "blocked-buy": {
                        "direction": "BUY",
                        "status": "BLOCKED",
                        "grid_price": "2.54",
                        "granted_at": "2026-01-05T10:00:00",
                        "decision_contract_version": 3,
                        "decision_gross_available_quantity": 6000,
                        "decision_platform_residual_quantity": 0,
                        "decision_algorithm_authorized_quantity": 6000,
                        "decided_exercise_quantity": 6000,
                        "decided_defer_quantity": 0,
                        "decision_platform_blocked_quantity": 6000,
                        "decision_intent_quantity": 0,
                        "decision_remaining_quantity": 6000,
                        "decision_is_algorithm": True,
                        "capacity": {"available_quantity": 6000},
                    }
                },
                "right_tranches": {},
            },
        }
    )
    markers = _chart_markers(snapshot)
    assert markers["buy"][0]["label"]["formatter"] == "1"
    assert markers["buy_right"][0]["label"]["formatter"] == "0"
    assert markers["platform_blocked"][0]["label"]["formatter"] == "1"


def test_algorithm_failure_fallback_is_not_drawn_as_voluntary_defer() -> None:
    snapshot = RunSnapshot.model_validate(
        {
            "api_version": "gridedge.api/v1",
            "sequence": 4,
            "lot_size": 100,
            "standard_quantity": 6000,
            "progress": None,
            "playback": {"active": False, "interval_ms": 0, "command_version": 0},
            "command_version": 0,
            "performance": exact_unavailable_performance(),
            "state": {
                "orders": {},
                "grid_rights": {
                    "failed-buy": {
                        "direction": "BUY",
                        "status": "BLOCKED",
                        "grid_price": "2.54",
                        "granted_at": "2026-01-05T10:00:00",
                        "decided_exercise_quantity": 0,
                        "decided_defer_quantity": 6000,
                        "decision_is_algorithm": False,
                        "capacity": {"available_quantity": 6000},
                    }
                },
                "right_tranches": {},
            },
        }
    )
    markers = _chart_markers(snapshot)
    assert markers["buy"] == []
    assert markers["buy_right"] == []


def test_platform_residual_shares_never_become_fractional_algorithm_markers() -> None:
    snapshot = RunSnapshot.model_validate(
        {
            "api_version": "gridedge.api/v1",
            "sequence": 5,
            "lot_size": 100,
            "standard_quantity": 10_000,
            "progress": None,
            "playback": {"active": False, "interval_ms": 0, "command_version": 0},
            "command_version": 0,
            "performance": exact_unavailable_performance(),
            "state": {
                "orders": {},
                "grid_rights": {
                    "residual-only": {
                        "direction": "SELL",
                        "status": "DEFERRED",
                        "grid_price": "2.70",
                        "granted_at": "2026-01-06T10:05:00",
                        "decision_contract_version": 3,
                        "decision_gross_available_quantity": 8700,
                        "decision_platform_residual_quantity": 8700,
                        "decision_algorithm_authorized_quantity": 0,
                        "decided_exercise_quantity": 0,
                        "decided_defer_quantity": 0,
                        "decision_platform_blocked_quantity": 0,
                        "decision_intent_quantity": 0,
                        "decision_remaining_quantity": 0,
                        "decision_is_algorithm": True,
                        "decision_recorded": True,
                        "capacity": {
                            "eligible_quantity": 8700,
                            "tranche_ids": ["lot-residual"],
                        },
                    }
                },
                "right_tranches": {},
            },
        }
    )
    markers = _chart_markers(snapshot)
    assert markers["buy"] == []
    assert markers["sell"] == []
    assert markers["buy_right"] == []
    assert markers["sell_right"] == []
    assert markers["platform_blocked"] == []
    assert markers["legacy"] == []
    assert len(markers["platform_residual"]) == 1
    assert markers["platform_residual"][0]["label"]["formatter"] == "8,700股"


def test_legacy_fractional_decision_is_not_masqueraded_as_current_unit_marker() -> None:
    snapshot = RunSnapshot.model_validate(
        {
            "api_version": "gridedge.api/v1",
            "sequence": 6,
            "lot_size": 100,
            "standard_quantity": 600,
            "progress": None,
            "playback": {"active": False, "interval_ms": 0, "command_version": 0},
            "command_version": 0,
            "performance": exact_unavailable_performance(),
            "state": {
                "orders": {},
                "grid_rights": {
                    "legacy-v2": {
                        "direction": "BUY",
                        "status": "DEFERRED",
                        "grid_price": "2.50",
                        "granted_at": "2026-01-05T10:05:00",
                        "decision_contract_version": 2,
                        "decided_exercise_quantity": 300,
                        "decided_defer_quantity": 600,
                        "decision_is_algorithm": True,
                        "decision_recorded": True,
                        "capacity": {"available_quantity": 900},
                    }
                },
                "right_tranches": {},
            },
        }
    )
    markers = _chart_markers(snapshot)
    assert markers["buy"] == []
    assert markers["sell"] == []
    assert markers["buy_right"] == []
    assert markers["sell_right"] == []
    assert markers["platform_blocked"] == []
    assert markers["platform_residual"] == []
    assert len(markers["legacy"]) == 1
    assert markers["legacy"][0]["label"]["formatter"] == "900股"
