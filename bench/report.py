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
