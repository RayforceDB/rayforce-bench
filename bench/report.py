"""Report generator for benchmark results."""

import json
import re
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .runner import BenchmarkRun


# Stable colors per engine — matches teide-bench palette so cross-repo
# screenshots stay visually consistent.
ENGINE_COLORS = {
    "rayforce":   "#ff7f0e",
    "duckdb":     "#1f77b4",
    "polars":     "#2ca02c",
    "chdb":       "#e377c2",
    "datafusion": "#17becf",
    "pandas":     "#8c564b",
    "questdb":    "#9467bd",
    "timescale":  "#d62728",
}


def _compute_boxplot(values: list[float]) -> list[float]:
    """Compute boxplot stats: [min, q1, median, q3, max]."""
    if not values:
        return [0, 0, 0, 0, 0]
    sorted_vals = sorted(values)
    n = len(sorted_vals)

    min_val = sorted_vals[0]
    max_val = sorted_vals[-1]

    # Median
    if n % 2 == 0:
        median = (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2
    else:
        median = sorted_vals[n // 2]

    # Q1 (25th percentile)
    q1_idx = n // 4
    q1 = sorted_vals[q1_idx]

    # Q3 (75th percentile)
    q3_idx = (3 * n) // 4
    q3 = sorted_vals[q3_idx]

    return [min_val, q1, median, q3, max_val]


def generate_html_report(
    results: list["BenchmarkRun"],
    output_path: Path,
    title: str = "RayforceDB Benchmarks",
) -> None:
    """Generate benchmark report in the format expected by the original HTML.

    Saves data.json and updates index.html with embedded chartData.
    """
    output_path = Path(output_path)
    json_path = output_path.parent / "data.json"

    # Get unique adapters and tasks
    adapters = sorted(set(r.adapter for r in results))
    tasks = sorted(set(r.benchmark for r in results))

    # Build comparison data
    comparison_values = []
    for r in results:
        comparison_values.append({
            "adapter": r.adapter,
            "task": r.benchmark,
            "median_ms": r.median_ms,
        })

    # Build tasks data (with individual run values and boxplot)
    tasks_data = {}
    for task in tasks:
        task_results = []
        for adapter in adapters:
            # Find the result for this adapter/task combination
            matching = [r for r in results if r.benchmark == task and r.adapter == adapter]
            if matching:
                run = matching[0]
                values = [res.time_ms for res in run.results]
                task_results.append({
                    "adapter": adapter,
                    "values": values,
                    "boxplot": _compute_boxplot(values),
                })
        tasks_data[task] = task_results

    # Build chartData in the format expected by the original HTML
    chart_data = {
        "comparison": {
            "adapters": adapters,
            "tasks": tasks,
            "values": comparison_values,
        },
        "tasks": tasks_data,
    }

    # Save data.json
    output_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(chart_data, indent=2))

    # Update index.html with embedded chartData
    html_path = output_path.parent / "index.html"
    if html_path.exists():
        html_content = html_path.read_text()
        # Replace chartData value
        html_content = re.sub(
            r"const chartData = \{.*?\};",
            f"const chartData = {json.dumps(chart_data)};",
            html_content,
            flags=re.DOTALL
        )
        html_path.write_text(html_content)

    print(f"Results saved: {json_path}")


def generate_histogram_html(results: list["BenchmarkRun"],
                             output_path: Path,
                             title: str = "RayforceDB H2O Benchmark") -> None:
    """Generate a self-contained Plotly bar chart, log-Y, grouped by adapter.

    Same shape as teide-bench/results/bench.html — easy quick-look that
    works without docs/index.html infrastructure.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    by_bench: dict[str, dict[str, float]] = {}
    for r in results:
        if r.error or not r.results:
            continue
        by_bench.setdefault(r.benchmark, {})[r.adapter] = r.median_ms

    benches = sorted(by_bench.keys())
    adapters = sorted({a for d in by_bench.values() for a in d.keys()})

    values = {a: [by_bench.get(b, {}).get(a) for b in benches] for a in adapters}
    colors = [ENGINE_COLORS.get(a, "#666") for a in adapters]

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>{title}</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>body{{font-family:sans-serif;padding:20px}}</style>
</head><body>
<h2>{title}</h2>
<div id="chart"></div>
<script>
const benches = {json.dumps(benches)};
const adapters = {json.dumps(adapters)};
const values = {json.dumps(values)};
const colors = {json.dumps(colors)};
const traces = adapters.map((a, i) => ({{
  x: benches,
  y: values[a],
  name: a,
  type: 'bar',
  marker: {{ color: colors[i] }},
  text: values[a].map(v => v == null ? '' :
        v >= 1000 ? (v/1000).toFixed(2)+'s' : v.toFixed(1)+'ms'),
  textposition: 'outside',
  textfont: {{ size: 9 }},
}}));
Plotly.newPlot('chart', traces, {{
  barmode: 'group',
  height: 500,
  yaxis: {{ title: 'Time (ms)', type: 'log' }},
  template: 'plotly_white',
  legend: {{ orientation: 'h', y: 1.1 }},
}});
</script></body></html>"""

    output_path.write_text(html)
    print(f"Histogram saved: {output_path}")


def generate_sort_grid_html(input_json: Path, output_path: Path,
                             title: str = "Sort grid — typed scaling curve") -> None:
    """Render a log-log scaling curve from sort_grid_runner output.

    One trace per (adapter, dtype) pair — legend lets the viewer toggle
    individual curves. Loads the same docs/sort_data.json the runner
    writes, so the page can be re-rendered without re-running benchmarks.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    data = json.loads(Path(input_json).read_text())
    rows = data.get("results", [])
    by_pair: dict[tuple[str, str], list[tuple[int, float]]] = {}
    for r in rows:
        by_pair.setdefault((r["adapter"], r["dtype"]), []).append(
            (r["length"], r["median_ms"])
        )

    traces = []
    for (adapter, dtype), points in sorted(by_pair.items()):
        points.sort()
        xs = [p[0] for p in points]
        ys = [p[1] for p in points]
        traces.append({
            "x": xs, "y": ys,
            "name": f"{adapter} / {dtype}",
            "mode": "lines+markers",
            "type": "scatter",
            "legendgroup": adapter,
            "marker": {"color": ENGINE_COLORS.get(adapter, "#666")},
            "line": {"color": ENGINE_COLORS.get(adapter, "#666"),
                     "dash": _dash_for_dtype(dtype)},
        })

    meta = data.get("meta", {})
    label = meta.get("rayforce_label", "rayforce")
    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>{title}</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>body{{font-family:sans-serif;padding:20px}}</style>
</head><body>
<h2>{title}</h2>
<p>{label} — {meta.get('iterations', '?')} iterations, {meta.get('warmup', '?')} warmup</p>
<div id="chart" style="height: 700px"></div>
<script>
const traces = {json.dumps(traces)};
Plotly.newPlot('chart', traces, {{
  xaxis: {{ title: 'Length (rows)', type: 'log', autorange: true }},
  yaxis: {{ title: 'Median time (ms)', type: 'log', autorange: true }},
  template: 'plotly_white',
  legend: {{ groupclick: 'togglegroup' }},
}});
</script></body></html>"""

    output_path.write_text(html)
    print(f"Sort grid HTML saved: {output_path}")


# Dashed/dotted lines distinguish dtypes within an engine's color group.
_DTYPE_DASH = {
    "u8": "dot", "i16": "dashdot", "i32": "dash",
    "i64": "solid", "f64": "longdash",
    "str8": "longdashdot", "str16": "5px,2px,1px,2px",
}


def _dash_for_dtype(dtype: str) -> str:
    return _DTYPE_DASH.get(dtype, "solid")


# Group ops into colour buckets so the legend reads well.
_OP_GROUPS = {
    "groupby_q1": "groupby", "groupby_q2": "groupby", "groupby_q3": "groupby",
    "groupby_q4": "groupby", "groupby_q5": "groupby", "groupby_q6": "groupby",
    "groupby_q7": "groupby",
    "join_inner": "join", "join_left": "join",
    "sort_single": "sort", "sort_multi": "sort",
    "sort_u8": "sort_typed", "sort_i16": "sort_typed",
    "sort_i32": "sort_typed", "sort_i64": "sort_typed",
    "sort_f64": "sort_typed", "sort_str8": "sort_typed",
    "sort_str16": "sort_typed",
}


def generate_scaling_html(input_json: Path, output_path: Path,
                          title: str = "Rayforce-bench scaling curve") -> None:
    """Render an interactive log-log scaling chart with engine+op filters.

    UI mirrors teide-bench/sort_bench_plot.py: checkbox groups for engines
    and ops, each with All/None buttons, Plotly.react redraw on change.

    Reads docs/scaling_data.json (produced by bench.scaling_runner) and
    emits one trace per (adapter, op) pair: engine encoded by colour, op
    encoded by line-dash and marker symbol.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    payload = json.loads(Path(input_json).read_text())
    rows = payload.get("results", [])
    meta = payload.get("meta", {})

    engines = sorted({r["adapter"] for r in rows})
    ops = sorted({r["op"] for r in rows}, key=_op_sort_key)

    # Group rows into (adapter, op) → [(size, median_ms), ...]
    series = []
    for adapter in engines:
        for op in ops:
            pts = sorted(
                ((r["size"], r["median_ms"]) for r in rows
                 if r["adapter"] == adapter and r["op"] == op
                 and r["size"] > 0 and r["median_ms"] > 0),
                key=lambda p: p[0],
            )
            if not pts:
                continue
            series.append({
                "engine": adapter,
                "op": op,
                "x": [p[0] for p in pts],
                "y": [p[1] for p in pts],
            })

    payload_dump = {
        "series":   series,
        "engines":  engines,
        "ops":      ops,
        "engine_colors": ENGINE_COLORS,
        "op_dashes":     {op: _dash_for_op(op) for op in ops},
        "op_markers":    {op: _marker_for_op(op) for op in ops},
        "rayforce_label": meta.get("rayforce_label", ""),
    }

    html = _SCALING_TEMPLATE.replace("__TITLE__", title) \
                            .replace("__PAYLOAD__", json.dumps(payload_dump))
    output_path.write_text(html)
    print(f"Scaling HTML saved: {output_path}")


def _op_sort_key(op: str):
    """Stable order: groupby first, then join, then sort, then sort_typed."""
    bucket_order = {"groupby": 0, "join": 1, "sort": 2, "sort_typed": 3}
    return (bucket_order.get(_OP_GROUPS.get(op, "z"), 9), op)


_OP_DASH = {
    "groupby_q1": "solid",   "groupby_q2": "dot",     "groupby_q3": "dash",
    "groupby_q4": "longdash", "groupby_q5": "dashdot", "groupby_q6": "longdashdot",
    "groupby_q7": "solid",
    "join_inner": "solid",   "join_left": "dot",
    "sort_single": "solid",  "sort_multi": "dash",
    "sort_u8":  "dot",       "sort_i16":  "dash",     "sort_i32":  "longdash",
    "sort_i64": "solid",     "sort_f64":  "dashdot",
    "sort_str8":  "longdashdot", "sort_str16": "longdashdot",
}


def _dash_for_op(op: str) -> str:
    return _OP_DASH.get(op, "solid")


_OP_MARKER = {
    "groupby_q1": "circle", "groupby_q2": "square", "groupby_q3": "diamond",
    "groupby_q4": "triangle-up", "groupby_q5": "triangle-down",
    "groupby_q6": "x", "groupby_q7": "star",
    "join_inner": "circle-open", "join_left": "square-open",
    "sort_single": "diamond-open", "sort_multi": "star-open",
    "sort_u8": "circle", "sort_i16": "square", "sort_i32": "diamond",
    "sort_i64": "triangle-up", "sort_f64": "x",
    "sort_str8": "cross", "sort_str16": "hexagon",
}


def _marker_for_op(op: str) -> str:
    return _OP_MARKER.get(op, "circle")


_SCALING_TEMPLATE = r"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<title>__TITLE__</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         margin: 0; padding: 20px; background: #fafafa; }
  h2 { margin: 0 0 6px 0; }
  .meta { color: #666; font-size: 13px; margin-bottom: 16px; }
  .controls { display: flex; gap: 30px; flex-wrap: wrap; margin-bottom: 20px;
              padding: 15px; background: white; border-radius: 8px;
              box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
  .control-group { display: flex; flex-direction: column; gap: 4px; min-width: 180px; }
  .control-group h3 { margin: 0 0 6px 0; font-size: 12px; color: #666;
                       text-transform: uppercase; letter-spacing: 0.5px; }
  .control-group label { font-size: 13px; cursor: pointer; display: flex;
                          align-items: center; gap: 6px; }
  .swatch { display: inline-block; width: 14px; height: 3px; border-radius: 1px;
             vertical-align: middle; }
  .btn-row { display: flex; gap: 6px; margin-bottom: 6px; }
  .btn-row button { font-size: 11px; padding: 2px 8px; cursor: pointer;
                     border: 1px solid #ccc; border-radius: 3px; background: #f5f5f5; }
  .btn-row button:hover { background: #e0e0e0; }
  #chart { background: white; border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1); padding: 8px; }
</style>
</head><body>

<h2>__TITLE__</h2>
<div class="meta" id="meta"></div>

<div class="controls">
  <div class="control-group">
    <h3>Engines</h3>
    <div class="btn-row">
      <button onclick="toggleAll('engine', true)">All</button>
      <button onclick="toggleAll('engine', false)">None</button>
    </div>
    <div id="engine-checks"></div>
  </div>
  <div class="control-group">
    <h3>Operations</h3>
    <div class="btn-row">
      <button onclick="toggleAll('op', true)">All</button>
      <button onclick="toggleAll('op', false)">None</button>
      <button onclick="presetOps('groupby')">Groupby</button>
      <button onclick="presetOps('join')">Join</button>
      <button onclick="presetOps('sort')">Sort H2O</button>
      <button onclick="presetOps('sort_typed')">Sort typed</button>
    </div>
    <div id="op-checks"></div>
  </div>
</div>

<div id="chart" style="height: 700px"></div>

<script>
const PAYLOAD = __PAYLOAD__;
const SERIES = PAYLOAD.series;
const ENGINES = PAYLOAD.engines;
const OPS = PAYLOAD.ops;
const ENGINE_COLORS = PAYLOAD.engine_colors;
const OP_DASHES = PAYLOAD.op_dashes;
const OP_MARKERS = PAYLOAD.op_markers;

document.getElementById('meta').textContent =
  PAYLOAD.rayforce_label ? "rayforce: " + PAYLOAD.rayforce_label : "";

const OP_GROUPS = {
  "groupby_q1": "groupby", "groupby_q2": "groupby", "groupby_q3": "groupby",
  "groupby_q4": "groupby", "groupby_q5": "groupby", "groupby_q6": "groupby",
  "groupby_q7": "groupby",
  "join_inner": "join", "join_left": "join",
  "sort_single": "sort", "sort_multi": "sort",
  "sort_u8": "sort_typed", "sort_i16": "sort_typed",
  "sort_i32": "sort_typed", "sort_i64": "sort_typed",
  "sort_f64": "sort_typed", "sort_str8": "sort_typed",
  "sort_str16": "sort_typed",
};

const enabled = { engine: {}, op: {} };
ENGINES.forEach(e => enabled.engine[e] = true);
// Default-on a useful starter set: groupby_q1 + sort_i64 + sort_str8.
const DEFAULT_ON = new Set(["groupby_q1", "sort_i64", "sort_str8"]);
OPS.forEach(o => enabled.op[o] = DEFAULT_ON.has(o));
if (![...DEFAULT_ON].some(o => OPS.includes(o))) {
  OPS.forEach(o => enabled.op[o] = true);
}

function buildChecks(containerId, items, category, swatchFn) {
  const el = document.getElementById(containerId);
  items.forEach(item => {
    const label = document.createElement('label');
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.checked = !!enabled[category][item];
    cb.dataset.category = category;
    cb.dataset.item = item;
    cb.addEventListener('change', () => {
      enabled[category][item] = cb.checked;
      redraw();
    });
    label.appendChild(cb);
    if (swatchFn) {
      const sw = document.createElement('span');
      sw.className = 'swatch';
      sw.style.background = swatchFn(item);
      label.appendChild(sw);
    }
    label.appendChild(document.createTextNode(' ' + item));
    el.appendChild(label);
  });
}

buildChecks('engine-checks', ENGINES, 'engine',
  e => ENGINE_COLORS[e] || '#666');
buildChecks('op-checks', OPS, 'op', null);

function toggleAll(category, val) {
  const items = category === 'engine' ? ENGINES : OPS;
  items.forEach(item => enabled[category][item] = val);
  document.querySelectorAll(`input[data-category="${category}"]`)
    .forEach(cb => cb.checked = val);
  redraw();
}

function presetOps(group) {
  OPS.forEach(o => enabled.op[o] = (OP_GROUPS[o] === group));
  document.querySelectorAll('input[data-category="op"]').forEach(cb => {
    cb.checked = enabled.op[cb.dataset.item];
  });
  redraw();
}

function redraw() {
  const filtered = SERIES.filter(s =>
    enabled.engine[s.engine] && enabled.op[s.op]);

  const traces = filtered.map(s => ({
    x: s.x, y: s.y,
    mode: 'lines+markers',
    name: `${s.engine} / ${s.op}`,
    legendgroup: s.engine,
    line: {
      color: ENGINE_COLORS[s.engine] || '#666',
      dash: OP_DASHES[s.op] || 'solid',
      width: 2,
    },
    marker: {
      symbol: OP_MARKERS[s.op] || 'circle',
      size: 7,
    },
    hovertemplate:
      `<b>${s.engine} / ${s.op}</b><br>n=%{x:,}<br>%{y:.3f} ms<extra></extra>`,
  }));

  Plotly.react('chart', traces, {
    xaxis: { type: 'log', title: 'Rows', exponentformat: 'power' },
    yaxis: { type: 'log', title: 'Median time (ms)' },
    template: 'plotly_white',
    legend: { groupclick: 'togglegroup', font: { size: 10 } },
    margin: { t: 30, r: 30 },
    hovermode: 'closest',
  }, { responsive: true });
}

redraw();
</script>
</body></html>
"""
