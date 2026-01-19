"""Report generator for benchmark results."""

import json
import re
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .runner import BenchmarkRun


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
