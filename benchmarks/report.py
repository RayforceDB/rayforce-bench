"""
Static HTML report generation using Plotly.

Generates interactive reports with:
- Overview comparison tables
- Per-task box/violin plots
- Reproducibility metadata
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .runner import BenchmarkResults
from .stats import BenchmarkStatistics, compute_statistics


def generate_report(
    results: BenchmarkResults,
    output_dir: Path,
    report_name: str | None = None,
) -> Path:
    """Generate a static HTML report from benchmark results.
    
    Args:
        results: Raw results from BenchmarkRunner.
        output_dir: Directory to write report files.
        report_name: Optional custom name (default: suite_timestamp).
    
    Returns:
        Path to generated HTML file.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    stats = compute_statistics(results)
    
    if report_name is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_name = f"{results.suite_name}_{timestamp}"
    
    # Generate HTML content
    html_content = _generate_html(results, stats)
    
    # Write HTML file
    html_path = output_dir / f"{report_name}.html"
    with open(html_path, "w") as f:
        f.write(html_content)
    
    # Write JSON data for programmatic access
    json_path = output_dir / f"{report_name}.json"
    with open(json_path, "w") as f:
        json.dump(_results_to_dict(results, stats), f, indent=2)
    
    return html_path


def _generate_html(results: BenchmarkResults, stats: BenchmarkStatistics) -> str:
    """Generate complete HTML report content."""
    
    # Build comparison chart
    comparison_chart = _build_comparison_chart(stats)
    
    # Build box plots for each task
    task_charts = _build_task_charts(results, stats)
    
    # Build summary table
    summary_table = _build_summary_table(stats)
    
    # Generate HTML
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Benchmark Report: {results.suite_name}</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        :root {{
            --bg-primary: #0a0a0f;
            --bg-secondary: #12121a;
            --bg-tertiary: #1a1a24;
            --text-primary: #e8e8ed;
            --text-secondary: #9898a8;
            --accent-primary: #e9a033;
            --accent-secondary: #1565C0;
            --success: #4CAF50;
            --error: #f44336;
            --border: #2a2a36;
        }}
        
        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}
        
        body {{
            font-family: 'JetBrains Mono', 'SF Mono', 'Fira Code', monospace;
            background: var(--bg-primary);
            color: var(--text-primary);
            line-height: 1.6;
            padding: 2rem;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        
        header {{
            text-align: center;
            margin-bottom: 3rem;
            padding: 2rem;
            background: linear-gradient(135deg, var(--bg-secondary) 0%, var(--bg-tertiary) 100%);
            border-radius: 12px;
            border: 1px solid var(--border);
        }}
        
        h1 {{
            font-size: 2.5rem;
            font-weight: 600;
            color: var(--accent-primary);
            margin-bottom: 0.5rem;
        }}
        
        .subtitle {{
            color: var(--text-secondary);
            font-size: 1.1rem;
        }}
        
        section {{
            margin-bottom: 3rem;
            background: var(--bg-secondary);
            border-radius: 12px;
            padding: 2rem;
            border: 1px solid var(--border);
        }}
        
        h2 {{
            font-size: 1.5rem;
            color: var(--accent-primary);
            margin-bottom: 1.5rem;
            padding-bottom: 0.5rem;
            border-bottom: 2px solid var(--border);
        }}
        
        .chart-container {{
            background: var(--bg-tertiary);
            border-radius: 8px;
            padding: 1rem;
            margin-bottom: 2rem;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }}
        
        th, td {{
            padding: 0.75rem 1rem;
            text-align: left;
            border-bottom: 1px solid var(--border);
        }}
        
        th {{
            background: var(--bg-tertiary);
            color: var(--accent-primary);
            font-weight: 600;
            position: sticky;
            top: 0;
        }}
        
        tr:hover {{
            background: rgba(233, 160, 51, 0.05);
        }}
        
        .valid {{
            color: var(--success);
        }}
        
        .invalid {{
            color: var(--error);
        }}
        
        .metadata {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 1rem;
        }}
        
        .metadata-card {{
            background: var(--bg-tertiary);
            padding: 1rem;
            border-radius: 8px;
            border: 1px solid var(--border);
        }}
        
        .metadata-card h3 {{
            color: var(--accent-primary);
            font-size: 0.9rem;
            margin-bottom: 0.5rem;
        }}
        
        .metadata-card pre {{
            font-size: 0.8rem;
            color: var(--text-secondary);
            white-space: pre-wrap;
            word-break: break-all;
        }}
        
        .best-value {{
            color: var(--success);
            font-weight: 600;
        }}
        
        footer {{
            text-align: center;
            color: var(--text-secondary);
            font-size: 0.8rem;
            margin-top: 2rem;
            padding: 1rem;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>⚡ Benchmark Report</h1>
            <p class="subtitle">Suite: {results.suite_name} | Dataset: {results.dataset_name}</p>
            <p class="subtitle" style="font-size: 0.9rem; margin-top: 0.5rem;">
                {results.started_at} — {results.finished_at}
            </p>
        </header>
        
        <section>
            <h2>📊 Overview Comparison</h2>
            <div class="chart-container">
                <div id="comparison-chart"></div>
            </div>
        </section>
        
        <section>
            <h2>📈 Per-Task Performance</h2>
            {task_charts}
        </section>
        
        <section>
            <h2>📋 Results Summary</h2>
            {summary_table}
        </section>
        
        <section>
            <h2>🔧 Reproducibility</h2>
            <div class="metadata">
                <div class="metadata-card">
                    <h3>System Info</h3>
                    <pre>{json.dumps(results.system_info, indent=2)}</pre>
                </div>
                {_build_adapter_cards(results)}
            </div>
        </section>
        
        <footer>
            Generated by Rayforce Benchmark Framework v0.1.0
        </footer>
    </div>
    
    <script>
        {comparison_chart}
    </script>
</body>
</html>
"""
    return html


def _build_comparison_chart(stats: BenchmarkStatistics) -> str:
    """Build JavaScript for comparison bar chart."""
    if stats.summary_df is None or len(stats.summary_df) == 0:
        return ""
    
    df = stats.summary_df
    adapters = df["adapter"].unique().tolist()
    tasks = df["task"].unique().tolist()
    
    traces = []
    for adapter in adapters:
        adapter_data = df[df["adapter"] == adapter]
        values = []
        for task in tasks:
            task_data = adapter_data[adapter_data["task"] == task]
            if len(task_data) > 0:
                values.append(task_data["median_ms"].iloc[0])
            else:
                values.append(None)
        
        traces.append(f"""{{
            name: '{adapter}',
            type: 'bar',
            x: {json.dumps(tasks)},
            y: {json.dumps(values)}
        }}""")
    
    return f"""
        Plotly.newPlot('comparison-chart', [{', '.join(traces)}], {{
            title: 'Median Execution Time by Task',
            barmode: 'group',
            xaxis: {{ title: 'Task' }},
            yaxis: {{ title: 'Time (ms)', type: 'log' }},
            paper_bgcolor: 'transparent',
            plot_bgcolor: 'rgba(26, 26, 36, 0.8)',
            font: {{ color: '#e8e8ed' }},
            legend: {{ orientation: 'h', y: -0.2 }}
        }}, {{ responsive: true }});
    """


def _build_task_charts(results: BenchmarkResults, stats: BenchmarkStatistics) -> str:
    """Build HTML for per-task box plots."""
    if not results.task_results:
        return "<p>No task results available.</p>"
    
    charts_html = []
    chart_scripts = []
    
    # Group results by task
    tasks = {}
    for tr in results.task_results:
        if tr.task_id not in tasks:
            tasks[tr.task_id] = []
        tasks[tr.task_id].append(tr)
    
    for idx, (task_id, task_results) in enumerate(tasks.items()):
        chart_id = f"task-chart-{idx}"
        charts_html.append(f'<div class="chart-container"><div id="{chart_id}"></div></div>')
        
        traces = []
        for tr in task_results:
            if tr.timings_ns:
                times_ms = [t / 1_000_000 for t in tr.timings_ns]
                traces.append(f"""{{
                    name: '{tr.adapter_name}',
                    type: 'box',
                    y: {json.dumps(times_ms)},
                    boxpoints: 'all',
                    jitter: 0.3
                }}""")
        
        if traces:
            chart_scripts.append(f"""
                Plotly.newPlot('{chart_id}', [{', '.join(traces)}], {{
                    title: 'Task: {task_id}',
                    yaxis: {{ title: 'Time (ms)' }},
                    paper_bgcolor: 'transparent',
                    plot_bgcolor: 'rgba(26, 26, 36, 0.8)',
                    font: {{ color: '#e8e8ed' }}
                }}, {{ responsive: true }});
            """)
    
    return "\n".join(charts_html) + f"\n<script>{chr(10).join(chart_scripts)}</script>"


def _build_summary_table(stats: BenchmarkStatistics) -> str:
    """Build HTML summary table."""
    if stats.summary_df is None or len(stats.summary_df) == 0:
        return "<p>No summary data available.</p>"
    
    df = stats.summary_df
    
    # Find best (minimum) median for each task to highlight
    best_by_task = df.groupby("task")["median_ms"].min().to_dict()
    
    rows = []
    for _, row in df.iterrows():
        is_best = abs(row["median_ms"] - best_by_task.get(row["task"], float("inf"))) < 0.01
        median_class = "best-value" if is_best else ""
        valid_class = "valid" if row["valid"] else "invalid"
        valid_text = "✓" if row["valid"] else "✗"
        
        rows_per_sec = f"{row['rows/s']:,.0f}" if row['rows/s'] else "N/A"
        
        rows.append(f"""
            <tr>
                <td>{row['task']}</td>
                <td>{row['adapter']}</td>
                <td>{row['n']}</td>
                <td>{row['min_ms']:.2f}</td>
                <td class="{median_class}">{row['median_ms']:.2f}</td>
                <td>{row['p95_ms']:.2f}</td>
                <td>{row['max_ms']:.2f}</td>
                <td>{row['std_ms']:.2f}</td>
                <td>{rows_per_sec}</td>
                <td>{row['cache']}</td>
                <td class="{valid_class}">{valid_text}</td>
            </tr>
        """)
    
    return f"""
        <div style="overflow-x: auto;">
            <table>
                <thead>
                    <tr>
                        <th>Task</th>
                        <th>Adapter</th>
                        <th>N</th>
                        <th>Min (ms)</th>
                        <th>Median (ms)</th>
                        <th>P95 (ms)</th>
                        <th>Max (ms)</th>
                        <th>Std (ms)</th>
                        <th>Rows/s</th>
                        <th>Cache</th>
                        <th>Valid</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(rows)}
                </tbody>
            </table>
        </div>
    """


def _build_adapter_cards(results: BenchmarkResults) -> str:
    """Build HTML for adapter metadata cards."""
    cards = []
    for adapter_name, info in results.adapter_info.items():
        cards.append(f"""
            <div class="metadata-card">
                <h3>{adapter_name}</h3>
                <pre>{json.dumps(info, indent=2)}</pre>
            </div>
        """)
    return "\n".join(cards)


def _results_to_dict(results: BenchmarkResults, stats: BenchmarkStatistics) -> dict:
    """Convert results and stats to JSON-serializable dictionary."""
    return {
        "suite_name": results.suite_name,
        "dataset_name": results.dataset_name,
        "started_at": results.started_at,
        "finished_at": results.finished_at,
        "system_info": results.system_info,
        "adapter_info": results.adapter_info,
        "task_results": [
            {
                "task_id": tr.task_id,
                "adapter_name": tr.adapter_name,
                "timings_ns": tr.timings_ns,
                "row_counts": tr.row_counts,
                "validation_passed": tr.validation_passed,
                "validation_error": tr.validation_error,
                "cache_mode": tr.cache_mode,
                "errors": tr.errors,
            }
            for tr in results.task_results
        ],
        "statistics": [
            {
                "task_id": ts.task_id,
                "adapter_name": ts.adapter_name,
                "n_samples": ts.n_samples,
                "min_ms": ts.min_ms,
                "median_ms": ts.median_ms,
                "p95_ms": ts.p95_ms,
                "max_ms": ts.max_ms,
                "std_ms": ts.std_ms,
                "rows_per_sec": ts.rows_per_sec,
            }
            for ts in stats.task_stats
        ],
    }
