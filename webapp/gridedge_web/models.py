from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, model_validator


class ReplayDescriptor(BaseModel):
    dataset_id: str
    data_sha256: str
    symbol: str = ""
    total_bars: int
    first_timestamp: str
    last_timestamp: str


class ReplayProgress(BaseModel):
    descriptor: ReplayDescriptor
    processed_bars: int


class Playback(BaseModel):
    active: bool = False
    interval_ms: int = 0
    command_version: int


class Performance(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    realized_grid_pnl: str
    unrealized_grid_pnl: str | None
    total_grid_pnl: str | None
    valuation_policy_version: str | None
    mark_price: str | None
    mark_to_market_unrealized_grid_pnl: str | None
    total_mark_to_market_grid_pnl: str | None
    conservative_exit_unrealized_grid_pnl: str | None
    total_conservative_exit_grid_pnl: str | None
    conservative_exit_adjustment: str | None
    unpriced_open_quantity: int


class RunSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    api_version: Literal["gridedge.api/v1"]
    sequence: int
    lot_size: int = 100
    standard_quantity: int = 0
    state: dict[str, Any]
    performance: Performance
    progress: ReplayProgress | None = None
    playback: Playback
    command_version: int


class EventBatch(BaseModel):
    api_version: Literal["gridedge.api/v1"]
    events: list[dict[str, Any]]
    next_sequence: int


class OpportunityCounts(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    touched: int
    granted: int
    skipped: int
    legacy_unbound: int

    @model_validator(mode="after")
    def validate_conservation(self) -> OpportunityCounts:
        if min(self.touched, self.granted, self.skipped, self.legacy_unbound) < 0:
            raise ValueError("opportunity counts cannot be negative")
        if self.touched != self.granted + self.skipped + self.legacy_unbound:
            raise ValueError("opportunity counts do not conserve touches")
        return self


class OpportunityPartialBlock(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    sequence: int
    reason: str
    t_plus_one_blocked_quantity: int
    t_plus_one_blocked_lot_ids: list[str]
    risk_blocked_quantity: int
    risk_blocked_lot_ids: list[str]
    no_profit_blocked_quantity: int
    no_profit_blocked_lot_ids: list[str]

    @model_validator(mode="after")
    def validate_quantities(self) -> OpportunityPartialBlock:
        if (
            self.sequence < 0
            or not self.reason
            or min(
                self.t_plus_one_blocked_quantity,
                self.risk_blocked_quantity,
                self.no_profit_blocked_quantity,
            )
            < 0
        ):
            raise ValueError("partial block is invalid")
        return self


class OpportunityPreTradeCapacity(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    eligible_quantity: int
    eligible_lot_ids: list[str]
    t_plus_one_blocked_quantity: int
    t_plus_one_blocked_lot_ids: list[str]
    risk_blocked_quantity: int
    risk_blocked_lot_ids: list[str]
    no_profit_blocked_quantity: int
    no_profit_blocked_lot_ids: list[str]
    source_right_ids: list[str]
    tranche_ids: list[str]

    @model_validator(mode="after")
    def validate_quantities(self) -> OpportunityPreTradeCapacity:
        if (
            min(
                self.eligible_quantity,
                self.t_plus_one_blocked_quantity,
                self.risk_blocked_quantity,
                self.no_profit_blocked_quantity,
            )
            < 0
        ):
            raise ValueError("pre-trade capacity contains a negative quantity")
        return self


class OpportunityRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    opportunity_id: str
    run_id: str
    cycle_id: str
    symbol: str
    event_time: str
    grid_index: int
    grid_price: str | None
    touch_sequence: int
    resolution_sequence: int
    processed_sequence: int
    resolution: Literal["GRANTED", "SKIPPED", "LEGACY_UNBOUND"]
    semantics: Literal["CURRENT_V3", "LEGACY_RECORDED"]
    standard_quantity: int
    direction: Literal["BUY", "SELL"]
    right_id: str | None
    decision_id: str | None
    reason: str | None
    reason_audit_status: Literal["RECORDED_UNVERIFIED", "LEGACY_UNBOUND"] | None
    terminal_kind: Literal["DEFERRED", "RESIDUAL_HELD", "BLOCKED", "RESERVED"] | None
    terminal_reason: str | None
    algorithm_succeeded: bool | None
    pre_trade_capacity: OpportunityPreTradeCapacity | None
    partial_blocks: list[OpportunityPartialBlock]
    decision_contract_version: int | None
    gross_available_quantity: int | None
    platform_residual_quantity: int | None
    algorithm_authorized_quantity: int | None
    exercise_quantity: int | None
    defer_quantity: int | None
    platform_blocked_quantity: int | None
    order_intent_quantity: int | None
    remaining_decision_quantity: int | None

    @model_validator(mode="after")
    def validate_resolution(self) -> OpportunityRecord:
        if (
            not self.opportunity_id
            or self.touch_sequence < 0
            or self.standard_quantity < 0
        ):
            raise ValueError("opportunity identity is invalid")
        if self.resolution_sequence <= self.touch_sequence:
            raise ValueError("opportunity resolution must follow its touch")
        if self.processed_sequence < self.resolution_sequence:
            raise ValueError("opportunity cannot resolve after its processed bar")
        if self.semantics == "CURRENT_V3" and (
            self.standard_quantity <= 0 or self.grid_price is None
        ):
            raise ValueError("current opportunity lacks its audited grid facts")
        if self.resolution == "GRANTED":
            if (
                not self.right_id
                or not self.decision_id
                or self.reason is not None
                or self.reason_audit_status is not None
                or self.terminal_kind is None
            ):
                raise ValueError("granted opportunity lacks its right and disposition")
            if self.semantics == "CURRENT_V3" and self.decision_contract_version != 3:
                raise ValueError("current opportunity lacks its decision contract")
            if self.decision_contract_version == 3:
                quantities = [
                    self.gross_available_quantity,
                    self.platform_residual_quantity,
                    self.algorithm_authorized_quantity,
                    self.exercise_quantity,
                    self.defer_quantity,
                    self.platform_blocked_quantity,
                    self.order_intent_quantity,
                    self.remaining_decision_quantity,
                ]
                if (
                    self.decision_contract_version != 3
                    or self.algorithm_succeeded is None
                    or self.pre_trade_capacity is None
                ):
                    raise ValueError("current opportunity lacks its algorithm contract")
                if any(quantity is None or quantity < 0 for quantity in quantities):
                    raise ValueError(
                        "current opportunity lacks its exact quantity partition"
                    )
                c, p, a, e, d, b, i, r = (int(quantity) for quantity in quantities)
                if (
                    c != p + a
                    or a != e + d
                    or i != e - b
                    or r != d + b
                    or p >= self.standard_quantity
                    or b > e
                    or any(
                        quantity % self.standard_quantity
                        for quantity in [a, e, d, b, i, r]
                    )
                ):
                    raise ValueError(
                        "current opportunity quantity partition is invalid"
                    )
                if self.partial_blocks:
                    if len(self.partial_blocks) != 1:
                        raise ValueError(
                            "current opportunity has duplicate partial blocks"
                        )
                    block = self.partial_blocks[0]
                    capacity = self.pre_trade_capacity
                    if (
                        block.t_plus_one_blocked_quantity
                        != capacity.t_plus_one_blocked_quantity
                        or block.t_plus_one_blocked_lot_ids
                        != capacity.t_plus_one_blocked_lot_ids
                        or block.risk_blocked_quantity != capacity.risk_blocked_quantity
                        or block.risk_blocked_lot_ids != capacity.risk_blocked_lot_ids
                        or block.no_profit_blocked_quantity
                        != capacity.no_profit_blocked_quantity
                        or block.no_profit_blocked_lot_ids
                        != capacity.no_profit_blocked_lot_ids
                    ):
                        raise ValueError(
                            "partial block differs from its pre-trade capacity"
                        )
            elif (
                self.semantics != "LEGACY_RECORDED"
                or self.decision_contract_version not in (1, 2)
                or self.pre_trade_capacity is not None
                or self.gross_available_quantity is not None
                or self.platform_residual_quantity is not None
                or self.algorithm_authorized_quantity is not None
                or self.platform_blocked_quantity is not None
                or self.remaining_decision_quantity is not None
            ):
                raise ValueError("legacy opportunity invents current quantity facts")
        elif self.resolution == "LEGACY_UNBOUND":
            if (
                self.semantics != "LEGACY_RECORDED"
                or self.right_id is not None
                or self.decision_id is not None
                or self.reason != "LEGACY_TOUCH_RESOLUTION_UNBOUND"
                or self.reason_audit_status != "LEGACY_UNBOUND"
                or self.terminal_kind is not None
                or self.terminal_reason is not None
                or self.algorithm_succeeded is not None
                or self.pre_trade_capacity is not None
                or self.partial_blocks
                or any(
                    quantity is not None
                    for quantity in [
                        self.decision_contract_version,
                        self.gross_available_quantity,
                        self.platform_residual_quantity,
                        self.algorithm_authorized_quantity,
                        self.exercise_quantity,
                        self.defer_quantity,
                        self.platform_blocked_quantity,
                        self.order_intent_quantity,
                        self.remaining_decision_quantity,
                    ]
                )
            ):
                raise ValueError(
                    "unbound legacy opportunity must not invent a resolution"
                )
        elif (
            self.right_id is not None
            or self.decision_id is not None
            or not self.reason
            or self.reason_audit_status != "RECORDED_UNVERIFIED"
            or self.terminal_kind is not None
            or self.terminal_reason is not None
            or self.algorithm_succeeded is not None
            or self.pre_trade_capacity is not None
            or self.partial_blocks
            or any(
                quantity is not None
                for quantity in [
                    self.decision_contract_version,
                    self.gross_available_quantity,
                    self.platform_residual_quantity,
                    self.algorithm_authorized_quantity,
                    self.exercise_quantity,
                    self.defer_quantity,
                    self.platform_blocked_quantity,
                    self.order_intent_quantity,
                    self.remaining_decision_quantity,
                ]
            )
        ):
            raise ValueError("skipped opportunity must expose only its recorded reason")
        return self


class OpportunityPage(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    api_version: Literal["gridedge.api/v1"]
    run_id: str
    through_sequence: int
    standard_quantity: int
    opportunities: list[OpportunityRecord]
    next_sequence: int
    complete: bool
    counts: OpportunityCounts

    @model_validator(mode="after")
    def bind_record_units(self) -> OpportunityPage:
        if any(
            record.standard_quantity != self.standard_quantity
            for record in self.opportunities
        ):
            raise ValueError("opportunity page changed its audited share unit")
        return self


class OpportunityHistory(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    api_version: Literal["gridedge.api/v1"]
    run_id: str
    through_sequence: int
    standard_quantity: int
    opportunities: list[OpportunityRecord]
    counts: OpportunityCounts

    @model_validator(mode="after")
    def bind_record_units(self) -> OpportunityHistory:
        if any(
            record.standard_quantity != self.standard_quantity
            for record in self.opportunities
        ):
            raise ValueError("opportunity history changed its audited share unit")
        return self


class MarketBar(BaseModel):
    timestamp: str
    symbol: str
    open: str
    high: str
    low: str
    close: str
    volume: int
    amount: str | None = None


class BarBatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    api_version: Literal["gridedge.api/v1"]
    run_id: str
    dataset_id: str
    data_sha256: str
    visible_bars: int
    total_bars: int
    sampled: list[MarketBar]


class CommandResult(BaseModel):
    api_version: Literal["gridedge.api/v1"]
    request_id: str
    command: str
    run_id: str
    accepted: bool
    message: str
    accepted_sequence: int
    accepted_version: int


class PendingCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    request_id: str
    command: Literal["start", "step", "play", "pause", "finish"]
    accepted_version: int
    recovery_state: Literal["retryable", "blocked"]


class PendingCommandBatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    api_version: Literal["gridedge.api/v1"]
    commands: list[PendingCommand]
