import inspect
import json
import subprocess

import nicegui

from gridedge_web.app import _money, dashboard


def test_dashboard_fetches_run_bound_bars_and_never_falls_back_to_global_file() -> None:
    source = inspect.getsource(dashboard)
    assert "bars = []" in source
    assert "if snapshot.progress is not None:" in source
    assert 'await core.bars(selected["run_id"]' in source
    assert "bar_batch.dataset_id != snapshot.progress.descriptor.dataset_id" in source
    assert "bar_batch.data_sha256" in source
    assert "行情数据与运行账本绑定不一致" in source
    assert "旧运行缺少数据绑定 · 不显示默认行情" in source
    assert "read_text(" not in source
    assert "open(" not in source
    assert "csv." not in source


def test_dashboard_passes_server_aggregated_ohlc_in_echarts_candlestick_order() -> None:
    source = inspect.getsource(dashboard)
    assert '"type": "candlestick"' in source
    assert '"type": "time"' in source
    assert '"type": "category"' not in source
    assert '_echarts_time(row.get("timestamp", ""))' in source
    assert 'float(row.get("open", 0) or 0)' in source
    assert 'float(row.get("close", 0) or 0)' in source
    assert 'float(row.get("low", 0) or 0)' in source
    assert 'float(row.get("high", 0) or 0)' in source


def test_echarts_time_axis_renders_markers_inside_aggregated_candle_intervals() -> None:
    echarts = (
        __import__("pathlib").Path(nicegui.__file__).parent
        / "elements/lib/echarts/echarts.min.js"
    )
    script = f"""
const fs=require('fs'), vm=require('vm');
const context={{console,setTimeout,clearTimeout}};
context.global=context; context.window=context; context.self=context;
vm.createContext(context);
vm.runInContext(fs.readFileSync({json.dumps(str(echarts))},'utf8'),context);
const chart=context.echarts.init(null,null,{{renderer:'svg',ssr:true,width:900,height:420}});
chart.setOption({{
  animation:false,
  xAxis:{{type:'time'}}, yAxis:{{type:'value'}},
  series:[
    {{type:'scatter',symbolSize:20,label:{{show:true,formatter:p=>p.data.name}},data:[
      {{name:'M_START',value:['2026-01-05T10:01:00',10,1]}},
      {{name:'M_MIDDLE',value:['2026-01-05T10:05:00',10.5,1]}},
      {{name:'M_END',value:['2026-01-05T10:09:00',11,1]}}
    ]}},
    {{type:'candlestick',data:[
      ['2026-01-05T10:00:00',9,10,8,11],
      ['2026-01-05T10:10:00',10,11,9,12]
    ]}}
  ]
}});
const svg=chart.renderToSVGString();
const markerData=chart.getOption().series[0].data;
const markerPixels=markerData.map(row=>chart.convertToPixel({{seriesIndex:0}},row.value));
const bucketPixels=['2026-01-05T10:00:00','2026-01-05T10:10:00']
  .map(value=>chart.convertToPixel({{xAxisIndex:0}},value));
const finite=markerPixels.flat().every(Number.isFinite) && bucketPixels.every(Number.isFinite);
const ordered=markerPixels[0][0] < markerPixels[1][0] && markerPixels[1][0] < markerPixels[2][0];
const inside=markerPixels.every(pixel=>bucketPixels[0] < pixel[0] && pixel[0] < bucketPixels[1]);
const ok=!svg.includes('NaN') && ['M_START','M_MIDDLE','M_END'].every(x=>svg.includes(x))
  && finite && ordered && inside;
process.stdout.write(JSON.stringify({{ok,length:svg.length,markerPixels,bucketPixels}}));
process.exit(ok?0:2);
"""
    result = subprocess.run(
        ["node", "-e", script],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    assert json.loads(result.stdout)["ok"] is True


def test_dashboard_distinguishes_mark_to_market_from_per_lot_conservative_exit() -> (
    None
):
    source = inspect.getsource(dashboard)
    for label in (
        "盯市总网格收益",
        "盯市未实现（未扣退出成本）",
        "逐 lot 保守退出总收益",
        "逐 lot 保守退出未实现",
        "已实现网格收益",
    ):
        assert label in source
    assert "包含不利滑点、卖出佣金和印花税" in source
    assert "不代表当前可卖" in source
    assert "因成本未知未估值" in source
    assert "total_mark_to_market_grid_pnl" in source
    assert "conservative_exit_unrealized_grid_pnl" in source
    assert _money(None) == "—"
    assert _money("5.00") == "¥5.00"
    assert _money("-2.01") == "¥-2.01"
