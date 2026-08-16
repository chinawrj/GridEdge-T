from __future__ import annotations

import asyncio
import os
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from nicegui import app, ui

from .client import GridEdgeCoreClient
from .models import OpportunityHistory, OpportunityRecord, RunSnapshot


CORE_URL = os.getenv("GRIDEDGE_CORE_URL", "http://127.0.0.1:8790")
API_TOKEN = os.getenv("GRIDEDGE_API_TOKEN", "")
DATASET_ID = Path(
    os.getenv(
        "GRIDEDGE_DATA",
        "data/processed/002256.SZ_5m_raw_20230814_20260814.csv",
    )
).name

core = GridEdgeCoreClient(CORE_URL, API_TOKEN)


def _money(value: Any) -> str:
    if value is None:
        return "—"
    try:
        return f"¥{Decimal(str(value)):,.2f}"
    except (InvalidOperation, TypeError, ValueError):
        return "—"


def _table_columns(*names: tuple[str, str]) -> list[dict[str, str]]:
    return [
        {"name": key, "label": label, "field": key, "align": "left"}
        for key, label in names
    ]


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or 0))
    except (InvalidOperation, ValueError):
        return Decimal(0)


def _unit_text(value: Decimal) -> str:
    rounded = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return format(rounded, "f").rstrip("0").rstrip(".") or "0"


def _share_quantity(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _right_capacity_text(right: dict[str, Any], standard_quantity: int) -> str:
    capacity = right.get("capacity", {})
    direction = right.get("direction")
    contract = _share_quantity(right.get("decision_contract_version"))
    notes: list[str] = []

    if contract >= 3 and standard_quantity > 0:
        gross = _share_quantity(right.get("decision_gross_available_quantity"))
        authorized = _share_quantity(
            right.get("decision_algorithm_authorized_quantity")
        )
        residual = _share_quantity(right.get("decision_platform_residual_quantity"))
        notes.append(f"授权 {authorized // standard_quantity:,} 份")
        if residual:
            notes.append(f"残余 {residual:,} 股")
        notes.append(f"总计 {gross:,} 股")
        blocked = _share_quantity(right.get("decision_platform_blocked_quantity"))
        if blocked:
            notes.append(f"平台阻断 {blocked // standard_quantity:,} 份")
    elif direction == "SELL":
        eligible = _share_quantity(capacity.get("eligible_quantity"))
        if standard_quantity > 0:
            units, residual = divmod(eligible, standard_quantity)
            notes.append(f"{units:,} 份")
            if residual:
                notes.append(f"残余 {residual:,} 股")
            notes.append(f"总计 {eligible:,} 股")
        else:
            notes.append(f"历史股数 {eligible:,} 股")
    elif standard_quantity > 0:
        available = _share_quantity(capacity.get("available_quantity"))
        units, residual = divmod(available, standard_quantity)
        notes.append(f"{units:,} 份")
        if residual:
            notes.append(f"残余 {residual:,} 股")
        notes.append(f"总计 {available:,} 股")
    else:
        notes.append(f"历史预算 {_money(capacity.get('available_budget'))}")

    for label, key in [
        ("T+1阻断", "t_plus_one_blocked_quantity"),
        ("风险阻断", "risk_blocked_quantity"),
        ("保本阻断", "no_profit_blocked_quantity"),
    ]:
        quantity = _share_quantity(capacity.get(key))
        if quantity:
            notes.append(f"{label} {quantity:,} 股")
    return "；".join(notes)


def _echarts_time(value: Any) -> str:
    """Normalize the project's naive Shanghai timestamps for ECharts' time axis."""
    return str(value or "").strip().replace(" ", "T", 1)


def _opportunity_audit_note(record: OpportunityRecord) -> str:
    notes = [value for value in [record.reason, record.terminal_reason] if value]
    capacity = record.pre_trade_capacity
    if capacity is not None:
        for label, quantity, lot_ids in [
            (
                "T+1阻断",
                capacity.t_plus_one_blocked_quantity,
                capacity.t_plus_one_blocked_lot_ids,
            ),
            ("风险阻断", capacity.risk_blocked_quantity, capacity.risk_blocked_lot_ids),
            (
                "保本阻断",
                capacity.no_profit_blocked_quantity,
                capacity.no_profit_blocked_lot_ids,
            ),
        ]:
            if quantity > 0:
                notes.append(f"{label} {quantity} 股 / {len(lot_ids)} lot")
    notes.extend(
        block.reason for block in record.partial_blocks if block.reason not in notes
    )
    return "；".join(notes) or "—"


def _marker(
    timestamp: str,
    price: Any,
    units: Decimal,
    label: str,
    color: str,
    *,
    hollow: bool = False,
    offset: list[int] | None = None,
) -> dict[str, Any]:
    unit_text = _unit_text(units)
    marker = {
        "name": f"{label} {unit_text} 份",
        "value": [_echarts_time(timestamp), float(price), float(units)],
        "label": {
            "show": True,
            "position": "inside",
            "formatter": unit_text,
            "color": color if hollow else "#ffffff",
            "fontSize": 9,
            "fontWeight": 700,
        },
        "itemStyle": {
            "color": "rgba(255,255,255,0.92)" if hollow else color,
            "borderColor": color,
            "borderWidth": 2 if hollow else 1,
        },
    }
    if offset is not None:
        marker["symbolOffset"] = offset
    return marker


def _share_marker(
    timestamp: str,
    price: Any,
    shares: int,
    label: str,
    color: str,
    *,
    offset: list[int] | None = None,
) -> dict[str, Any]:
    share_text = f"{shares:,}股"
    marker = {
        "name": f"{label} {share_text}",
        "value": [_echarts_time(timestamp), float(price), shares],
        "label": {
            "show": True,
            "position": "inside",
            "formatter": share_text,
            "color": "#ffffff",
            "fontSize": 8,
            "fontWeight": 700,
        },
        "itemStyle": {"color": color, "borderColor": color, "borderWidth": 1},
    }
    if offset is not None:
        marker["symbolOffset"] = offset
    return marker


def _chart_markers(snapshot: RunSnapshot) -> dict[str, list[dict[str, Any]]]:
    state = snapshot.state
    red = "#d84d5b"
    green = "#178b68"
    markers: dict[str, list[dict[str, Any]]] = {
        "buy": [],
        "sell": [],
        "buy_right": [],
        "sell_right": [],
        "platform_blocked": [],
        "platform_residual": [],
        "legacy": [],
    }
    chosen_by_right: dict[str, int] = {}
    for order in state.get("orders", {}).values():
        intent = order.get("intent", {})
        quantity = int(intent.get("quantity", 0))
        if quantity <= 0:
            continue
        right_id = intent.get("right_id")
        if right_id:
            chosen_by_right[right_id] = chosen_by_right.get(right_id, 0) + quantity

    lot_size = max(1, snapshot.lot_size)
    standard_quantity = snapshot.standard_quantity
    for right_id, right in state.get("grid_rights", {}).items():
        direction = right.get("direction")
        status = right.get("status")
        price_decimal = _decimal(right.get("grid_price", 0))
        price = float(price_decimal)
        capacity = right.get("capacity", {})
        if direction == "BUY":
            if standard_quantity > 0:
                planned = int(capacity.get("available_quantity", 0) or 0)
            else:
                planned = (
                    int(_decimal(capacity.get("available_budget", 0)) / price_decimal)
                    if price_decimal > 0
                    else 0
                )
                planned = planned // lot_size * lot_size
            action_key, defer_key = "buy", "buy_right"
            color = red
            action_label, defer_label = "买入行权数量", "买入剩余可行权数量"
        else:
            # Blocked quantities were never granted to the algorithm and therefore
            # must not be presented as a voluntary Defer decision.
            planned = int(capacity.get("eligible_quantity", 0) or 0)
            action_key, defer_key = "sell", "sell_right"
            color = green
            action_label, defer_label = "卖出行权数量", "卖出剩余可行权数量"

        decision_contract_version = int(right.get("decision_contract_version", 0) or 0)
        exact_exercise = int(right.get("decided_exercise_quantity", 0) or 0)
        exact_defer = int(right.get("decided_defer_quantity", 0) or 0)
        if decision_contract_version >= 3:
            planned = int(right.get("decision_algorithm_authorized_quantity", 0) or 0)
            residual = int(right.get("decision_platform_residual_quantity", 0) or 0)
            platform_blocked = int(
                right.get("decision_platform_blocked_quantity", 0) or 0
            )
            if residual > 0:
                markers["platform_residual"].append(
                    _share_marker(
                        right.get("granted_at", ""),
                        price,
                        residual,
                        "平台残余",
                        "#b7791f",
                        offset=[0, 18],
                    )
                )
            if platform_blocked > 0 and standard_quantity > 0:
                markers["platform_blocked"].append(
                    _marker(
                        right.get("granted_at", ""),
                        price,
                        Decimal(platform_blocked) / Decimal(standard_quantity),
                        "平台阻断",
                        "#4a5568",
                        offset=[0, -18],
                    )
                )
            if not bool(right.get("decision_is_algorithm", False)) or planned <= 0:
                continue
            chosen = exact_exercise
            deferred = exact_defer
        elif exact_exercise + exact_defer > 0:
            # Contract v1/v2 is retained for replay only. Never draw a legacy
            # fractional-share decision as a current red/green strategy marker.
            markers["legacy"].append(
                _share_marker(
                    right.get("granted_at", ""),
                    price,
                    exact_exercise + exact_defer,
                    "旧版决策",
                    "#718096",
                )
            )
            continue
        if exact_exercise + exact_defer > 0:
            if not bool(right.get("decision_is_algorithm", False)):
                continue
            chosen = exact_exercise
            deferred = exact_defer
            planned = chosen + deferred
        else:
            # Read-only display fallback for pre-contract-v2 ledgers.
            if status == "BLOCKED":
                continue
            chosen = min(planned, chosen_by_right.get(right_id, 0))
            deferred = max(0, planned - chosen)
        if standard_quantity > 0:
            chosen_units = Decimal(chosen) / Decimal(standard_quantity)
            deferred_units = Decimal(deferred) / Decimal(standard_quantity)
        else:
            tranche_count = len(set(capacity.get("tranche_ids", [])))
            if tranche_count == 0:
                tranche_count = len(set(capacity.get("accumulated_grid_indices", [])))
            authorized_units = (
                Decimal(max(1, tranche_count)) if planned > 0 else Decimal(0)
            )
            chosen_units = (
                authorized_units * Decimal(chosen) / Decimal(planned)
                if planned > 0
                else Decimal(0)
            )
            deferred_units = authorized_units - chosen_units
        if planned > 0:
            markers[action_key].append(
                _marker(
                    right.get("granted_at", ""),
                    price,
                    chosen_units,
                    action_label,
                    color,
                    offset=[-14, 0],
                )
            )
            markers[defer_key].append(
                _marker(
                    right.get("granted_at", ""),
                    price,
                    deferred_units,
                    defer_label,
                    color,
                    hollow=True,
                    offset=[14, 0],
                )
            )
    return markers


@ui.page("/")
async def dashboard() -> None:
    runs = await core.runs()
    selected: dict[str, Any] = {
        "run_id": runs[0] if runs else "",
        "busy": False,
        "snapshot": None,
        "opportunity_history": None,
    }

    ui.add_css(
        """
        :root { --nav:#101c2b; --ink:#102033; --muted:#718096; --orange:#f36b35; }
        body { background:#f3f5f7; color:var(--ink); }
        .ge-header { background:var(--nav); color:white; }
        .ge-brand { font-family:Georgia,serif; font-size:25px; font-weight:700; }
        .metric { background:white; border:1px solid #dfe5eb; border-radius:12px; padding:16px; min-width:150px; flex:1; }
        .metric .q-label:first-child { color:var(--muted); font-size:11px; }
        .metric-value { font-family:Georgia,serif; font-size:23px; margin-top:7px; }
        .panel { background:white; border:1px solid #dfe5eb; border-radius:12px; padding:18px; }
        .section-title { font-family:Georgia,serif; font-size:20px; font-weight:700; }
        .live-dot { color:#29bb83; }
        .q-table th { background:#f5f7f9; }
        """
    )

    with ui.header().classes("ge-header items-center px-6 py-3"):
        ui.label("GridEdge-T").classes("ge-brand")
        ui.label("RESEARCH / PAPER · Rust Core + Python Reactive Web").classes(
            "text-xs text-blue-grey-3 ml-3"
        )
        ui.space()
        connection = ui.label("正在连接核心…").classes("text-sm")

    with ui.column().classes("w-full max-w-[1700px] mx-auto p-5 gap-4"):
        with ui.row().classes("w-full items-end gap-3"):
            run_select = ui.select(
                options=runs,
                value=selected["run_id"] or None,
                label="模拟运行",
            ).classes("min-w-[320px]")
            new_run = ui.input("新建单步回放", value="web-python-0815").classes(
                "min-w-[250px]"
            )
            create_button = ui.button("创建", icon="add")
            speed = ui.select(
                options={250: "0.25 秒", 500: "0.5 秒", 1000: "1 秒", 2000: "2 秒"},
                value=1000,
                label="播放速度",
            ).classes("w-36")
            play_button = ui.button("自动播放", icon="play_arrow")
            pause_button = ui.button("暂停", icon="pause").props("outline")
            step_button = ui.button("下一根", icon="skip_next").props("outline")
            finish_button = ui.button("运行至结束", icon="fast_forward")
            finish_button.props("outline")

        status_line = ui.label("请选择一个运行").classes("text-sm text-grey-7")
        pending_panel = ui.column().classes(
            "w-full rounded-xl border border-amber-4 bg-amber-1 p-4 gap-2"
        )
        pending_panel.set_visibility(False)
        progress = ui.linear_progress(value=0, show_value=False).props(
            "rounded color=deep-orange"
        )

        metric_values: dict[str, Any] = {}
        with ui.row().classes("w-full gap-3 flex-wrap"):
            for key, label in [
                ("cash", "可用现金"),
                ("position", "总持仓"),
                ("pnl_total", "盯市总网格收益"),
                ("pnl_unrealized", "盯市未实现（未扣退出成本）"),
                ("pnl_exit_total", "逐 lot 保守退出总收益"),
                ("pnl_exit_unrealized", "逐 lot 保守退出未实现"),
                ("pnl", "已实现网格收益"),
                ("fees", "累计费用"),
                ("events", "账本事件"),
                ("guard", "保本拦截"),
            ]:
                with ui.column().classes("metric gap-0"):
                    ui.label(label)
                    metric_values[key] = ui.label("—").classes("metric-value")
        valuation_note = ui.label(
            "盯市收益按最新已处理收盘价计价，未扣未来退出成本；逐 lot 保守退出估值包含不利滑点、卖出佣金和印花税，不代表当前可卖。"
        ).classes("text-xs text-grey-7")

        with ui.column().classes("panel w-full gap-3"):
            ui.label("完整机械机会历史").classes("section-title")
            with ui.row().classes("w-full gap-3 flex-wrap"):
                with ui.column().classes("metric gap-0"):
                    ui.label("机械机会总数")
                    opportunity_total = ui.label("—").classes("metric-value")
                    opportunity_total.props("data-testid=opportunity-total")
                with ui.column().classes("metric gap-0"):
                    ui.label("授予机会")
                    opportunity_granted = ui.label("—").classes("metric-value")
                    opportunity_granted.props("data-testid=opportunity-granted")
                with ui.column().classes("metric gap-0"):
                    ui.label("跳过机会")
                    opportunity_skipped = ui.label("—").classes("metric-value")
                    opportunity_skipped.props("data-testid=opportunity-skipped")
                with ui.column().classes("metric gap-0"):
                    ui.label("旧版未绑定")
                    opportunity_legacy_unbound = ui.label("—").classes("metric-value")
                    opportunity_legacy_unbound.props(
                        "data-testid=opportunity-legacy-unbound"
                    )
            opportunity_table = ui.table(
                columns=_table_columns(
                    ("sequence", "触发序号"),
                    ("time", "时间"),
                    ("grid", "网格"),
                    ("side", "方向"),
                    ("resolution", "结果"),
                    ("reason", "跳过原因（账本记录）"),
                ),
                rows=[],
                row_key="id",
                pagination={"rowsPerPage": 12},
            ).classes("w-full")
            opportunity_table.props("data-testid=opportunity-history")

        with ui.grid(columns=2).classes("w-full gap-4"):
            with ui.column().classes("panel col-span-2 xl:col-span-1"):
                ui.label("行情与回放进度").classes("section-title")
                chart = ui.echart(
                    {
                        "animation": False,
                        "legend": {
                            "data": [
                                "卖出行权量",
                                "买入行权量",
                                "卖出剩余授权",
                                "买入剩余授权",
                                "平台阻断",
                                "平台残余股",
                                "旧版决策股数",
                                "行情",
                            ],
                            "top": 0,
                        },
                        "grid": {"left": 48, "right": 18, "top": 55, "bottom": 40},
                        "xAxis": {
                            "type": "time",
                            "boundaryGap": ["1%", "1%"],
                        },
                        "yAxis": {"type": "value", "scale": True},
                        "dataZoom": [
                            {"type": "inside", "start": 0, "end": 100},
                            {"type": "slider", "start": 0, "end": 100},
                        ],
                        "toolbox": {
                            "feature": {
                                "dataZoom": {},
                                "restore": {},
                                "saveAsImage": {},
                            }
                        },
                        "series": [
                            {
                                "name": "卖出行权量",
                                "type": "scatter",
                                "data": [],
                                "symbolSize": 24,
                                "z": 10,
                                "itemStyle": {"color": "#178b68"},
                            },
                            {
                                "name": "买入行权量",
                                "type": "scatter",
                                "data": [],
                                "symbolSize": 24,
                                "z": 10,
                                "itemStyle": {"color": "#d84d5b"},
                            },
                            {
                                "name": "卖出剩余授权",
                                "type": "scatter",
                                "data": [],
                                "symbolSize": 26,
                                "z": 10,
                                "itemStyle": {
                                    "color": "#ffffff",
                                    "borderColor": "#178b68",
                                    "borderWidth": 2,
                                },
                            },
                            {
                                "name": "买入剩余授权",
                                "type": "scatter",
                                "data": [],
                                "symbolSize": 26,
                                "z": 10,
                                "itemStyle": {
                                    "color": "#ffffff",
                                    "borderColor": "#d84d5b",
                                    "borderWidth": 2,
                                },
                            },
                            {
                                "name": "平台阻断",
                                "type": "scatter",
                                "data": [],
                                "symbolSize": 25,
                                "z": 11,
                                "itemStyle": {"color": "#4a5568"},
                            },
                            {
                                "name": "平台残余股",
                                "type": "scatter",
                                "data": [],
                                "symbolSize": 27,
                                "z": 11,
                                "itemStyle": {"color": "#b7791f"},
                            },
                            {
                                "name": "旧版决策股数",
                                "type": "scatter",
                                "data": [],
                                "symbolSize": 26,
                                "z": 9,
                                "itemStyle": {"color": "#718096"},
                            },
                            {
                                "name": "行情",
                                "type": "candlestick",
                                "data": [],
                                "z": 1,
                                "silent": True,
                                "itemStyle": {
                                    "color": "#d84d5b",
                                    "color0": "#178b68",
                                    "borderColor": "#d84d5b",
                                    "borderColor0": "#178b68",
                                },
                            },
                        ],
                        "tooltip": {"trigger": "axis"},
                    }
                ).classes("w-full h-[360px]")
                ui.label(
                    "每个现行决策点并列显示：左侧 ● 实心＝本次行权整数份；右侧 ○ 空心＝算法主动递延整数份（含0）。平台阻断单独显示深灰整数份，不能凑成1份的残余只用琥珀色显示股数；旧版非整份只作灰色历史审计。绿色卖出，红色买入；ECharts K 线位于圆点下层。"
                ).classes("text-xs text-grey-7")
            with ui.column().classes("panel col-span-2 xl:col-span-1"):
                ui.label("网格权利与来源").classes("section-title")
                rights_table = ui.table(
                    columns=_table_columns(
                        ("grid", "网格"),
                        ("side", "方向"),
                        ("status", "状态"),
                        ("capacity", "容量"),
                        ("decision", "决策"),
                    ),
                    rows=[],
                    row_key="id",
                    pagination={"rowsPerPage": 8},
                ).classes("w-full")

        with ui.column().classes("panel w-full"):
            ui.label("订单、份额与事件账本").classes("section-title")
            with ui.tabs().classes("w-full") as tabs:
                orders_tab = ui.tab("订单")
                lots_tab = ui.tab("份额")
                events_tab = ui.tab("事件")
            with ui.tab_panels(tabs, value=orders_tab).classes("w-full"):
                with ui.tab_panel(orders_tab):
                    orders_table = ui.table(
                        columns=_table_columns(
                            ("grid", "网格"),
                            ("side", "方向"),
                            ("quantity", "数量"),
                            ("price", "限价"),
                            ("status", "状态"),
                        ),
                        rows=[],
                        row_key="id",
                        pagination={"rowsPerPage": 10},
                    ).classes("w-full")
                with ui.tab_panel(lots_tab):
                    lots_table = ui.table(
                        columns=_table_columns(
                            ("grid", "网格"),
                            ("opened", "开仓日"),
                            ("original", "原始数量"),
                            ("remaining", "剩余"),
                            ("pnl", "已实现收益"),
                        ),
                        rows=[],
                        row_key="id",
                        pagination={"rowsPerPage": 10},
                    ).classes("w-full")
                with ui.tab_panel(events_tab):
                    events_table = ui.table(
                        columns=_table_columns(
                            ("sequence", "序号"),
                            ("time", "时间"),
                            ("type", "事件"),
                            ("key", "幂等键"),
                        ),
                        rows=[],
                        row_key="sequence",
                        pagination={"rowsPerPage": 12},
                    ).classes("w-full")

    def render(
        snapshot: RunSnapshot,
        events: list[dict[str, Any]],
        bars: list[dict[str, Any]],
        opportunities: OpportunityHistory,
    ) -> None:
        selected["snapshot"] = snapshot
        state = snapshot.state
        cash = state.get("cash", {})
        position = state.get("position", {})
        metric_values["cash"].set_text(_money(cash.get("available")))
        metric_values["position"].set_text(f"{position.get('total', 0):,} 股")
        metric_values["pnl_total"].set_text(
            _money(snapshot.performance.total_mark_to_market_grid_pnl)
        )
        metric_values["pnl_unrealized"].set_text(
            _money(snapshot.performance.mark_to_market_unrealized_grid_pnl)
        )
        metric_values["pnl_exit_total"].set_text(
            _money(snapshot.performance.total_conservative_exit_grid_pnl)
        )
        metric_values["pnl_exit_unrealized"].set_text(
            _money(snapshot.performance.conservative_exit_unrealized_grid_pnl)
        )
        metric_values["pnl"].set_text(_money(snapshot.performance.realized_grid_pnl))
        metric_values["fees"].set_text(_money(cash.get("total_fees")))
        metric_values["events"].set_text(f"{snapshot.sequence:,}")
        unpriced = snapshot.performance.unpriced_open_quantity
        unavailable = "" if unpriced == 0 else f"；另有 {unpriced:,} 股因成本未知未估值"
        valuation_note.set_text(
            "盯市收益按最新已处理收盘价计价，未扣未来退出成本；逐 lot 保守退出估值包含不利滑点、卖出佣金和印花税，不代表当前可卖"
            f"{unavailable}。"
        )

        rights = list(state.get("grid_rights", {}).values())
        blocked = sum(
            int(right.get("capacity", {}).get("no_profit_blocked_quantity", 0))
            for right in rights
        )
        metric_values["guard"].set_text(f"{blocked:,} 股")

        replay = snapshot.progress
        markers = _chart_markers(snapshot)
        if replay:
            total = replay.descriptor.total_bars
            processed = min(replay.processed_bars, total)
            progress.set_value(processed / total if total else 1)
            action = "自动播放中" if snapshot.playback.active else "已暂停"
            status_line.set_text(
                f"{action} · {processed:,} / {total:,} 根 · 序号 {snapshot.sequence:,}"
            )
            chart.options["series"][7]["data"] = [
                [
                    _echarts_time(row.get("timestamp", "")),
                    float(row.get("open", 0) or 0),
                    float(row.get("close", 0) or 0),
                    float(row.get("low", 0) or 0),
                    float(row.get("high", 0) or 0),
                ]
                for row in bars
            ]
        else:
            progress.set_value(0)
            status_line.set_text(
                f"旧运行缺少数据绑定 · 不显示默认行情 · 序号 {snapshot.sequence:,}"
            )
            chart.options["series"][7]["data"] = []
        chart.options["series"][0]["data"] = markers["sell"]
        chart.options["series"][1]["data"] = markers["buy"]
        chart.options["series"][2]["data"] = markers["sell_right"]
        chart.options["series"][3]["data"] = markers["buy_right"]
        chart.options["series"][4]["data"] = markers["platform_blocked"]
        chart.options["series"][5]["data"] = markers["platform_residual"]
        chart.options["series"][6]["data"] = markers["legacy"]
        chart.update()

        rights_table.rows = [
            {
                "id": right.get("right_id"),
                "grid": f"{int(right.get('grid_index', 0)):+}",
                "side": right.get("direction"),
                "status": right.get("status"),
                "capacity": _right_capacity_text(right, snapshot.standard_quantity),
                "decision": (right.get("decision_id") or "—")[:12],
            }
            for right in sorted(
                rights, key=lambda item: item.get("granted_at", ""), reverse=True
            )[:80]
        ]
        rights_table.update()

        orders = list(state.get("orders", {}).values())
        orders_table.rows = [
            {
                "id": order.get("order_id"),
                "grid": f"{int(order.get('intent', {}).get('grid_index', 0)):+}",
                "side": order.get("intent", {}).get("direction"),
                "quantity": order.get("intent", {}).get("quantity", 0),
                "price": order.get("intent", {}).get("limit_price", "0"),
                "status": order.get("status"),
            }
            for order in reversed(orders[-100:])
        ]
        orders_table.update()

        lots = list(state.get("lots", {}).values())
        lots_table.rows = [
            {
                "id": lot.get("lot_id"),
                "grid": f"{int(lot.get('grid_index', 0)):+}",
                "opened": lot.get("opened_on"),
                "original": lot.get("original_quantity", 0),
                "remaining": lot.get("remaining_quantity", 0),
                "pnl": _money(lot.get("realized_pnl")),
            }
            for lot in reversed(lots[-100:])
        ]
        lots_table.update()

        events_table.rows = [
            {
                "sequence": event.get("sequence_number"),
                "time": event.get("event_time"),
                "type": event.get("event_type"),
                "key": event.get("idempotency_key"),
            }
            for event in reversed(events)
        ]
        events_table.update()

        opportunity_total.set_text(f"{opportunities.counts.touched:,}")
        opportunity_granted.set_text(f"{opportunities.counts.granted:,}")
        opportunity_skipped.set_text(f"{opportunities.counts.skipped:,}")
        opportunity_legacy_unbound.set_text(f"{opportunities.counts.legacy_unbound:,}")
        opportunity_table.rows = [
            {
                "id": record.opportunity_id,
                "sequence": record.touch_sequence,
                "time": record.event_time,
                "grid": f"{record.grid_index:+}",
                "side": record.direction,
                "resolution": (
                    record.terminal_kind
                    if record.resolution == "GRANTED"
                    else record.resolution
                )
                + (" · 旧版记录" if record.semantics == "LEGACY_RECORDED" else ""),
                "reason": _opportunity_audit_note(record),
            }
            for record in reversed(opportunities.opportunities)
        ]
        opportunity_table.update()

    def render_pending(commands: list[Any]) -> None:
        pending_panel.clear()
        pending_panel.set_visibility(bool(commands))
        if not commands:
            return
        with pending_panel:
            ui.label("发现待恢复命令").classes("font-bold text-amber-10")
            ui.label(
                "账本没有猜测执行它；请复用原请求继续，避免生成新请求后永久冲突。"
            ).classes("text-sm text-amber-9")
            for pending in commands:
                with ui.row().classes("w-full items-center gap-3"):
                    ui.label(
                        f"{pending.run_id} · {pending.command.upper()} · 控制版本 {pending.accepted_version}"
                    ).classes("grow text-sm")
                    button = ui.button(
                        "重试原命令",
                        icon="restart_alt",
                        on_click=lambda _event=None,
                        run_id=pending.run_id,
                        request_id=pending.request_id: retry_pending(
                            run_id, request_id
                        ),
                    ).props("outline color=amber-9")
                    if pending.recovery_state != "retryable":
                        button.disable()
                        button.tooltip("旧版待处理命令未保存原请求，已安全停止")

    async def refresh() -> None:
        if selected["busy"]:
            return
        selected["busy"] = True
        try:
            pending = await core.pending_commands()
            render_pending(pending.commands)
            if not selected["run_id"]:
                connection.set_text("● 核心已连接")
                connection.classes(replace="text-sm live-dot")
                return
            snapshot = await core.snapshot(selected["run_id"])
            cached_opportunities = selected.get("opportunity_history")
            if (
                isinstance(cached_opportunities, OpportunityHistory)
                and cached_opportunities.run_id == selected["run_id"]
                and cached_opportunities.through_sequence == snapshot.sequence
            ):
                opportunities = cached_opportunities
            else:
                opportunities = await core.opportunities(
                    selected["run_id"], through_sequence=snapshot.sequence
                )
                selected["opportunity_history"] = opportunities
            batch = await core.events(
                selected["run_id"], max(0, snapshot.sequence - 250), limit=250
            )
            bars = []
            if snapshot.progress is not None:
                bar_batch = await core.bars(selected["run_id"], max_points=1_000)
                if (
                    bar_batch.dataset_id != snapshot.progress.descriptor.dataset_id
                    or bar_batch.data_sha256 != snapshot.progress.descriptor.data_sha256
                ):
                    raise RuntimeError("行情数据与运行账本绑定不一致")
                bars = [bar.model_dump() for bar in bar_batch.sampled]
            render(snapshot, batch.events, bars, opportunities)
            connection.set_text("● 核心已连接")
            connection.classes(replace="text-sm live-dot")
        except Exception as error:  # UI boundary: display, never mutate the ledger.
            connection.set_text(f"核心连接失败：{error}")
            connection.classes(replace="text-sm text-red-4")
        finally:
            selected["busy"] = False

    async def retry_pending(run_id: str, request_id: str) -> None:
        if selected["busy"]:
            ui.notify("页面正在同步，请稍后重试", type="warning")
            return
        selected["busy"] = True
        try:
            result = await core.retry_pending(run_id, request_id)
            latest = await core.runs()
            run_select.options = latest
            run_select.set_value(run_id)
            selected["run_id"] = run_id
            selected["snapshot"] = None
            selected["opportunity_history"] = None
            ui.notify(result.message, type="positive")
        except Exception as error:
            ui.notify(f"恢复失败：{error}", type="negative")
        finally:
            selected["busy"] = False
        await refresh()

    async def run_command(command: str) -> None:
        run_id = selected["run_id"]
        if not run_id:
            ui.notify("请先选择运行", type="warning")
            return
        try:
            current = selected.get("snapshot")
            if not isinstance(current, RunSnapshot):
                current = await core.snapshot(run_id)
            result = await core.command(
                command,
                run_id,
                speed_ms=int(speed.value) if command == "play" else None,
                expected_sequence=current.sequence,
                expected_version=current.command_version,
            )
            ui.notify(result.message, type="positive")
            await asyncio.sleep(0.15)
            await refresh()
        except Exception as error:
            ui.notify(f"命令失败：{error}", type="negative")

    async def create_run() -> None:
        run_id = str(new_run.value or "").strip()
        if not run_id:
            ui.notify("请输入运行名称", type="warning")
            return
        try:
            await core.command(
                "start",
                run_id,
                dataset=DATASET_ID,
                expected_sequence=0,
                expected_version=0,
            )
            latest = await core.runs()
            run_select.options = latest
            run_select.set_value(run_id)
            selected["run_id"] = run_id
            selected["snapshot"] = None
            selected["opportunity_history"] = None
            ui.notify("单步回放已创建", type="positive")
            await refresh()
        except Exception as error:
            ui.notify(f"创建失败：{error}", type="negative")

    async def select_run(event: Any) -> None:
        selected["run_id"] = str(event.value or "")
        selected["snapshot"] = None
        selected["opportunity_history"] = None
        await refresh()

    run_select.on_value_change(select_run)
    create_button.on_click(create_run)
    play_button.on_click(lambda: run_command("play"))
    pause_button.on_click(lambda: run_command("pause"))
    step_button.on_click(lambda: run_command("step"))
    finish_button.on_click(lambda: run_command("finish"))
    ui.timer(0.75, refresh)
    await refresh()


app.on_shutdown(core.close)


def main() -> None:
    ui.run(
        host=os.getenv("GRIDEDGE_WEB_HOST", "127.0.0.1"),
        port=int(os.getenv("GRIDEDGE_WEB_PORT", "8787")),
        title="GridEdge-T 量化运行平台",
        favicon="📈",
        reload=False,
        show=False,
        storage_secret=os.getenv("GRIDEDGE_WEB_SECRET", "local-research-only"),
    )


if __name__ in {"__main__", "__mp_main__"}:
    main()
