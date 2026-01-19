#!/usr/bin/env python3
"""Benchmark runner CLI."""

import argparse
import gc
import json
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

from .adapters import (
    Adapter,
    BenchmarkResult,
    DuckDBAdapter,
    PolarsAdapter,
    QuestDBAdapter,
    RayforceAdapter,
    TimescaleAdapter,
)
from .report import generate_html_report


@dataclass
class BenchmarkRun:
    """Results of a benchmark run."""
    adapter: str
    version: str
    benchmark: str
    iterations: int
    results: list[BenchmarkResult]
    timestamp: str

    @property
    def median_ms(self) -> float:
        times = sorted(r.time_ms for r in self.results)
        n = len(times)
        if n % 2 == 0:
            return (times[n // 2 - 1] + times[n // 2]) / 2
        return times[n // 2]

    @property
    def min_ms(self) -> float:
        return min(r.time_ms for r in self.results)

    @property
    def max_ms(self) -> float:
        return max(r.time_ms for r in self.results)


class BenchmarkRunner:
    """Run benchmarks across multiple adapters."""

    BENCHMARKS = {
        "groupby": ["groupby_q1", "groupby_q2", "groupby_q3", "groupby_q4", "groupby_q5", "groupby_q6"],
        "join": ["join_inner", "join_left"],
        "sort": ["sort_single", "sort_multi"],
    }

    def __init__(
        self,
        adapters: list[str],
        rayforce_local: str | None = None,
        iterations: int = 5,
        warmup: int = 2,
    ):
        self.iterations = iterations
        self.warmup = warmup
        self._adapters: dict[str, Adapter] = {}

        for name in adapters:
            if name == "polars":
                self._adapters[name] = PolarsAdapter()
            elif name == "duckdb":
                self._adapters[name] = DuckDBAdapter()
            elif name == "questdb":
                self._adapters[name] = QuestDBAdapter()
            elif name == "timescale":
                self._adapters[name] = TimescaleAdapter()
            elif name == "rayforce":
                self._adapters[name] = RayforceAdapter(local_path=rayforce_local)
            else:
                raise ValueError(f"Unknown adapter: {name}")

    def run_groupby(self, data_path: Path) -> list[BenchmarkRun]:
        """Run groupby benchmarks."""
        results = []

        for name, adapter in self._adapters.items():
            print(f"{name}", end="", flush=True)
            adapter.load_data(data_path / "data.csv")

            for bench_name in self.BENCHMARKS["groupby"]:
                method = getattr(adapter, f"run_{bench_name}")
                run = self._run_benchmark(name, adapter, bench_name, method)
                results.append(run)
                print(".", end="", flush=True)

            adapter.close()
            gc.collect()
            print()

        return results

    def run_join(self, data_path: Path) -> list[BenchmarkRun]:
        """Run join benchmarks."""
        results = []
        left_path = data_path / "left.csv"
        right_path = data_path / "right.csv"

        for name, adapter in self._adapters.items():
            print(f"{name}", end="", flush=True)
            adapter.load_data(left_path, "left")

            for bench_name in self.BENCHMARKS["join"]:
                method = getattr(adapter, f"run_{bench_name}")
                run = self._run_benchmark(
                    name, adapter, bench_name,
                    lambda m=method: m(right_path)
                )
                results.append(run)
                print(".", end="", flush=True)

            adapter.close()
            gc.collect()
            print()

        return results

    def run_sort(self, data_path: Path) -> list[BenchmarkRun]:
        """Run sort benchmarks."""
        results = []

        for name, adapter in self._adapters.items():
            print(f"{name}", end="", flush=True)
            adapter.load_data(data_path / "data.csv")

            for bench_name in self.BENCHMARKS["sort"]:
                method = getattr(adapter, f"run_{bench_name}")
                run = self._run_benchmark(name, adapter, bench_name, method)
                results.append(run)
                print(".", end="", flush=True)

            adapter.close()
            gc.collect()
            print()

        return results

    def _run_benchmark(
        self,
        adapter_name: str,
        adapter: Adapter,
        bench_name: str,
        method: Callable[[], BenchmarkResult],
    ) -> BenchmarkRun:
        """Run a single benchmark with warmup and iterations."""
        # Warmup
        for _ in range(self.warmup):
            method()

        # Measured runs
        results = []
        for _ in range(self.iterations):
            result = method()
            results.append(result)

        return BenchmarkRun(
            adapter=adapter_name,
            version=adapter.version,
            benchmark=bench_name,
            iterations=self.iterations,
            results=results,
            timestamp=datetime.now().isoformat(),
        )

    def _print_result(self, run: BenchmarkRun) -> None:
        """Print benchmark result."""
        rows = run.results[0].rows if run.results else 0
        print(
            f"  {run.benchmark}: "
            f"median={run.median_ms:.2f}ms "
            f"min={run.min_ms:.2f}ms "
            f"max={run.max_ms:.2f}ms "
            f"({rows} rows)"
        )


def save_results(results: list[BenchmarkRun], output_path: Path) -> None:
    """Save results to JSON file."""
    data = []
    for run in results:
        run_data = {
            "adapter": run.adapter,
            "version": run.version,
            "benchmark": run.benchmark,
            "iterations": run.iterations,
            "median_ms": run.median_ms,
            "min_ms": run.min_ms,
            "max_ms": run.max_ms,
            "timestamp": run.timestamp,
            "results": [
                {"time_ms": r.time_ms, "rows": r.rows, "error": r.error}
                for r in run.results
            ],
        }
        data.append(run_data)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"\nResults saved to {output_path}")


def print_comparison(results: list[BenchmarkRun]) -> None:
    """Print comparison table."""
    # Group by benchmark
    by_benchmark: dict[str, dict[str, BenchmarkRun]] = {}
    for run in results:
        if run.benchmark not in by_benchmark:
            by_benchmark[run.benchmark] = {}
        by_benchmark[run.benchmark][run.adapter] = run

    # Put rayforce first, then sort the rest
    all_adapters = set(r.adapter for r in results)
    adapters = []
    if "rayforce" in all_adapters:
        adapters.append("rayforce")
        all_adapters.remove("rayforce")
    adapters.extend(sorted(all_adapters))

    print("\n" + "=" * 60)
    print("COMPARISON (median ms)")
    print("=" * 60)

    # Header
    header = f"{'Benchmark':<15}"
    for adapter in adapters:
        header += f" {adapter:>12}"
    print(header)
    print("-" * 60)

    # Track speedups relative to rayforce
    speedups: dict[str, list[float]] = {a: [] for a in adapters}

    # Rows
    for bench_name, adapter_results in sorted(by_benchmark.items()):
        row = f"{bench_name:<15}"
        rf_time = adapter_results.get("rayforce", None)
        rf_ms = rf_time.median_ms if rf_time else None

        for adapter in adapters:
            if adapter in adapter_results:
                t = adapter_results[adapter].median_ms
                row += f" {t:>12.2f}"
                if rf_ms and rf_ms > 0:
                    speedups[adapter].append(t / rf_ms)
            else:
                row += f" {'N/A':>12}"
        print(row)

    # Average speedup line
    print("-" * 60)
    row = f"{'(avg speedup)':<15}"
    for adapter in adapters:
        if speedups[adapter]:
            avg = sum(speedups[adapter]) / len(speedups[adapter])
            row += f" {avg:>11.2f}x"
        else:
            row += f" {'N/A':>12}"
    print(row)
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Run benchmarks for rayforce-bench")

    parser.add_argument(
        "benchmark",
        nargs="?",
        choices=["groupby", "join", "sort", "all"],
        help="Benchmark suite to run",
    )
    parser.add_argument(
        "-d", "--data",
        help="Path to dataset directory",
    )
    parser.add_argument(
        "-a", "--adapters",
        nargs="+",
        default=["rayforce", "polars", "duckdb"],
        help="Adapters: polars, duckdb, questdb, timescale, rayforce",
    )
    parser.add_argument(
        "--rayforce-local",
        help="Path to local rayforce-py repo for dev builds",
    )
    parser.add_argument(
        "-i", "--iterations",
        type=int,
        default=5,
        help="Number of measured iterations (default: 5)",
    )
    parser.add_argument(
        "-w", "--warmup",
        type=int,
        default=2,
        help="Number of warmup iterations (default: 2)",
    )
    parser.add_argument(
        "-o", "--output",
        help="Output JSON file for results",
    )
    parser.add_argument(
        "--html",
        default="docs/index.html",
        help="Output HTML report path (default: docs/index.html)",
    )
    parser.add_argument(
        "--no-html",
        action="store_true",
        help="Skip HTML report generation",
    )
    parser.add_argument(
        "--check-deps",
        action="store_true",
        help="Check dependencies and exit",
    )
    parser.add_argument(
        "--no-docker",
        action="store_true",
        help="Don't auto-start Docker containers for questdb/timescale",
    )
    parser.add_argument(
        "--stop-infra",
        action="store_true",
        help="Stop infrastructure containers after benchmarks",
    )

    args = parser.parse_args()

    # Check dependencies first
    from .adapters import print_dependency_status
    if args.check_deps:
        print_dependency_status(quiet=False)
        sys.exit(0)

    # Validate required args for actual benchmark runs
    if not args.benchmark:
        parser.error("benchmark is required (choose from: groupby, join, sort, all)")
    if not args.data:
        parser.error("-d/--data is required")

    if not print_dependency_status(quiet=True):
        sys.exit(1)

    # Start required infrastructure (Docker containers)
    if not args.no_docker:
        from .infra import start_required_infrastructure
        if not start_required_infrastructure(args.adapters, quiet=True):
            from .infra import CONTAINERS, is_container_running
            failed = [a for a in args.adapters if a in CONTAINERS and not is_container_running(CONTAINERS[a]["name"])]
            if failed:
                args.adapters = [a for a in args.adapters if a not in failed]
                if not args.adapters:
                    print("No adapters available.")
                    sys.exit(1)

    data_path = Path(args.data)

    if not data_path.exists():
        print(f"Error: {data_path} not found")
        sys.exit(1)

    runner = BenchmarkRunner(
        adapters=args.adapters,
        rayforce_local=args.rayforce_local,
        iterations=args.iterations,
        warmup=args.warmup,
    )

    results = []

    if args.benchmark in ("groupby", "all"):
        results.extend(runner.run_groupby(data_path))

    if args.benchmark in ("join", "all"):
        results.extend(runner.run_join(data_path))

    if args.benchmark in ("sort", "all"):
        results.extend(runner.run_sort(data_path))

    print_comparison(results)

    if args.output:
        save_results(results, Path(args.output))

    if not args.no_html:
        generate_html_report(results, Path(args.html))

    if args.stop_infra:
        from .infra import stop_infrastructure
        stop_infrastructure(args.adapters, quiet=True)


if __name__ == "__main__":
    main()
