#!/usr/bin/env python3
"""Benchmark orchestrator — spawns one subprocess per (adapter, op).

Each operation runs in its own child process via bench.worker, so memory
is guaranteed released between runs. Pattern borrowed from teide-bench.
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from .adapters import BenchmarkResult
from .engine_source import engine_label, resolve_rayforce_py
from .report import generate_histogram_html, generate_html_report
from .swapcheck import SwapSample, warn_if_already_used, warn_if_grew


WORKER_TIMEOUT_S = 600


@dataclass
class BenchmarkRun:
    """Aggregated result for one (adapter, benchmark) pair."""
    adapter: str
    version: str
    benchmark: str
    iterations: int
    results: list[BenchmarkResult]
    timestamp: str
    error: str | None = None

    @property
    def median_ms(self) -> float:
        if not self.results:
            return 0.0
        times = sorted(r.time_ms for r in self.results)
        n = len(times)
        if n % 2 == 0:
            return (times[n // 2 - 1] + times[n // 2]) / 2
        return times[n // 2]

    @property
    def min_ms(self) -> float:
        return min((r.time_ms for r in self.results), default=0.0)

    @property
    def max_ms(self) -> float:
        return max((r.time_ms for r in self.results), default=0.0)


BENCHMARKS = {
    # Canonical H2O groupby suite (q1..q10 per db-benchmark/polars/groupby-polars.py).
    # Engines that cannot run a particular op raise NotImplementedError;
    # the worker reports it as "NYI: <reason>" without aborting the run.
    "groupby":        [f"groupby_q{i}" for i in range(1, 11)],
    # Canonical H2O join suite (q1..q5 per db-benchmark/polars/join-polars.py).
    # Single-key joins on 3 right-table sizes (small=1e3, medium=N/1e3,
    # big=N). Adapter loads x/small/medium/big from the canonical-join
    # dataset directory (data path is the dir, not a CSV).
    "canonical_join": [f"join_q{i}" for i in range(1, 6)],
    # Bonus stress tests — *outside* of canonical H2O. Kept as separate
    # ops so the report can label them distinctly:
    #   join_inner / join_left : 3-key compound joins on equal-size tables
    #   sort_single / sort_multi: full-table sort (1-key / 3-key)
    "join":           ["join_inner", "join_left"],
    "sort":           ["sort_single", "sort_multi"],
}

# Suite category — "canonical" vs "bonus". Used by reporters to group
# ops in the comparison table and HTML report.
SUITE_CATEGORY = {
    "groupby":        "canonical",
    "canonical_join": "canonical",
    "join":           "bonus",
    "sort":           "bonus",
}

# Logical "bonus" group — alias used by `make bench-bonus` so users can
# run all bonus ops in one shot.
BONUS_OPS = BENCHMARKS["join"] + BENCHMARKS["sort"]


@dataclass
class OrchestratorConfig:
    adapters: list[str]
    iterations: int = 5
    warmup: int = 2
    rayforce_local: str | None = None
    labels: dict[str, str] = field(default_factory=dict)


def _run_worker(cfg: OrchestratorConfig, adapter: str, benchmark: str,
                data: Path, right_data: Path | None) -> BenchmarkRun:
    """Spawn a child process for one (adapter, benchmark) pair."""
    fd, result_path = tempfile.mkstemp(prefix=f"bench_{adapter}_{benchmark}_",
                                       suffix=".json")
    os.close(fd)

    cmd = [
        sys.executable, "-m", "bench.worker",
        "--adapter", adapter,
        "--benchmark", benchmark,
        "--data", str(data),
        "--iterations", str(cfg.iterations),
        "--warmup", str(cfg.warmup),
        "--result", result_path,
    ]
    if right_data is not None:
        cmd += ["--right-data", str(right_data)]
    if cfg.rayforce_local:
        cmd += ["--rayforce-local", cfg.rayforce_local]

    swap_before = SwapSample.now()
    try:
        subprocess.run(cmd, timeout=WORKER_TIMEOUT_S, check=False)
        with open(result_path) as f:
            data_out = json.load(f)
    except subprocess.TimeoutExpired:
        data_out = {
            "adapter": adapter, "benchmark": benchmark,
            "version": "?", "iterations": 0, "results": [],
            "timestamp": datetime.now().isoformat(),
            "error": f"timeout after {WORKER_TIMEOUT_S}s",
        }
    except (FileNotFoundError, json.JSONDecodeError) as e:
        data_out = {
            "adapter": adapter, "benchmark": benchmark,
            "version": "?", "iterations": 0, "results": [],
            "timestamp": datetime.now().isoformat(),
            "error": f"worker produced no result: {e}",
        }
    finally:
        if os.path.exists(result_path):
            os.unlink(result_path)
    warn_if_grew(swap_before, SwapSample.now(), f"{adapter}/{benchmark}")

    results = [
        BenchmarkResult(name=r["name"], time_ns=r["time_ns"],
                        rows=r["rows"], error=r.get("error"))
        for r in data_out.get("results", [])
    ]
    return BenchmarkRun(
        adapter=adapter,
        version=data_out.get("version") or "?",
        benchmark=benchmark,
        iterations=data_out.get("iterations", 0),
        results=results,
        timestamp=data_out.get("timestamp", datetime.now().isoformat()),
        error=data_out.get("error"),
    )


def _print_op_result(run: BenchmarkRun) -> None:
    if run.error and run.error.startswith("NYI"):
        # Canonical query the engine cannot run — report cleanly so the
        # user sees the gap, not a stack trace.
        print(f"  {run.benchmark:<20s} NYI: {run.error[len('NYI: '):]}")
    elif run.error:
        print(f"  {run.benchmark:<20s} ERROR: {run.error}")
    elif not run.results:
        print(f"  {run.benchmark:<20s} (no results)")
    else:
        print(f"  {run.benchmark:<20s} median={run.median_ms:>8.2f}ms  "
              f"min={run.min_ms:>8.2f}ms  result={run.results[0].rows} rows")


def run_suite(cfg: OrchestratorConfig, suite: str, data_path: Path,
              join_data: Path | None = None,
              canonical_join_data: Path | None = None) -> list[BenchmarkRun]:
    """Run a named suite across all adapters.

    Suite → data layout:
      groupby / sort  : <data_path>/data.csv (single CSV)
      join (bonus)    : <data_path>/left.csv + right.csv (or join_data dir)
      canonical_join  : <data_path>/{x,small,medium,big}.csv (directory)
    """
    if suite == "canonical_join":
        d = canonical_join_data if canonical_join_data is not None else data_path
        # The worker takes "--data <dir>" for canonical join and calls
        # adapter.load_canonical_join(dir) — pass the directory directly.
        primary = d
        right = None
    elif suite == "join":
        d = join_data if join_data is not None else data_path
        primary = d / "left.csv"
        right = d / "right.csv"
    else:
        primary = data_path / "data.csv"
        right = None

    ops = BENCHMARKS[suite]
    runs = []
    for adapter in cfg.adapters:
        print(f"[{adapter}]")
        for op in ops:
            run = _run_worker(cfg, adapter, op, primary, right)
            _print_op_result(run)
            runs.append(run)
    return runs


def save_results(results: list[BenchmarkRun], output_path: Path) -> None:
    data = []
    for run in results:
        data.append({
            "adapter": run.adapter,
            "version": run.version,
            "benchmark": run.benchmark,
            "iterations": run.iterations,
            "median_ms": run.median_ms,
            "min_ms": run.min_ms,
            "max_ms": run.max_ms,
            "timestamp": run.timestamp,
            "error": run.error,
            "results": [
                {"time_ms": r.time_ms, "rows": r.rows, "error": r.error}
                for r in run.results
            ],
        })
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"\nResults saved to {output_path}")




def print_comparison(results: list[BenchmarkRun]) -> None:
    by_bench: dict[str, dict[str, BenchmarkRun]] = {}
    for run in results:
        by_bench.setdefault(run.benchmark, {})[run.adapter] = run

    all_adapters = set(r.adapter for r in results)
    adapters = []
    if "rayforce" in all_adapters:
        adapters.append("rayforce")
        all_adapters.remove("rayforce")
    adapters.extend(sorted(all_adapters))

    print("\n" + "=" * 70)
    print("COMPARISON (median ms)")
    print("=" * 70)

    header = f"{'Benchmark':<18}"
    for a in adapters:
        header += f" {a:>12}"
    print(header)
    print("-" * 70)

    speedups: dict[str, list[float]] = {a: [] for a in adapters}
    for bench_name, ar in sorted(by_bench.items()):
        row = f"{bench_name:<18}"
        rf_ms = ar["rayforce"].median_ms if "rayforce" in ar and not ar["rayforce"].error else None
        for a in adapters:
            r = ar.get(a)
            if r is None or r.error or not r.results:
                row += f" {'N/A':>12}"
                continue
            t = r.median_ms
            row += f" {t:>12.2f}"
            if rf_ms and rf_ms > 0:
                speedups[a].append(t / rf_ms)
        print(row)

    print("-" * 70)
    avg = f"{'(avg vs rayforce)':<18}"
    for a in adapters:
        if speedups[a]:
            avg += f" {sum(speedups[a]) / len(speedups[a]):>11.2f}x"
        else:
            avg += f" {'N/A':>12}"
    print(avg)
    print("=" * 70)


def main():
    ap = argparse.ArgumentParser(description="Run rayforce benchmarks")
    ap.add_argument("benchmark", nargs="?",
                    choices=["groupby", "join", "canonical_join",
                             "sort", "all"],
                    help="Benchmark suite to run")
    ap.add_argument("-d", "--data", help="Path to dataset directory")
    ap.add_argument("--join-data", help="Path to join dataset directory "
                    "(needed when running 'all' or when join's left/right "
                    "live elsewhere from -d)")
    ap.add_argument("--canonical-join-data", help="Path to canonical-join "
                    "dataset directory (containing x.csv, small.csv, "
                    "medium.csv, big.csv) — used by `bench all`")
    ap.add_argument("-a", "--adapters", nargs="+",
                    default=["rayforce", "polars", "duckdb", "chdb",
                             "datafusion", "pandas"])
    ap.add_argument("--rayforce-local",
                    help="Path to local rayforce-py for dev builds")
    ap.add_argument("--rayforce-branch",
                    help="Clone rayforce-py from this git branch and use it")
    ap.add_argument("-i", "--iterations", type=int, default=5)
    ap.add_argument("-w", "--warmup", type=int, default=2)
    ap.add_argument("-o", "--output", help="Output JSON path")
    ap.add_argument("--html", default="docs/index.html",
                    help="Output HTML report path")
    ap.add_argument("--no-html", action="store_true")
    ap.add_argument("--check-deps", action="store_true")
    ap.add_argument("--no-docker", action="store_true",
                    help="Skip Docker auto-start for questdb/timescale")
    ap.add_argument("--stop-infra", action="store_true",
                    help="Stop infrastructure after run")
    args = ap.parse_args()

    from .adapters import print_dependency_status
    if args.check_deps:
        print_dependency_status(quiet=False)
        sys.exit(0)

    if not args.benchmark:
        ap.error("benchmark is required (groupby, join, sort, all)")
    if not args.data:
        ap.error("-d/--data is required")
    # Soft check: warn about missing deps but only fail if a requested
    # adapter is unavailable (the worker will surface a clean error then).
    print_dependency_status(quiet=True)

    if not args.no_docker:
        from .infra import start_required_infrastructure, CONTAINERS, is_container_running
        if not start_required_infrastructure(args.adapters, quiet=True):
            failed = [a for a in args.adapters
                      if a in CONTAINERS and not is_container_running(CONTAINERS[a]["name"])]
            if failed:
                args.adapters = [a for a in args.adapters if a not in failed]
                if not args.adapters:
                    print("No adapters available.")
                    sys.exit(1)

    data_path = Path(args.data)
    if not data_path.exists():
        print(f"Error: {data_path} not found")
        sys.exit(1)

    rayforce_src = resolve_rayforce_py(args.rayforce_local, args.rayforce_branch)
    labels: dict[str, str] = {}
    if "rayforce" in args.adapters:
        labels["rayforce"] = engine_label("rayforce", rayforce_src)
        if rayforce_src is not None:
            print(f"Using rayforce: {labels['rayforce']}")

    cfg = OrchestratorConfig(
        adapters=list(args.adapters),
        iterations=args.iterations,
        warmup=args.warmup,
        rayforce_local=str(rayforce_src) if rayforce_src else None,
        labels=labels,
    )

    warn_if_already_used(SwapSample.now())

    suites = (["groupby", "canonical_join", "join", "sort"]
              if args.benchmark == "all" else [args.benchmark])
    join_path = Path(args.join_data) if args.join_data else None
    canonical_join_path = (Path(args.canonical_join_data)
                           if getattr(args, "canonical_join_data", None)
                           else None)
    results: list[BenchmarkRun] = []
    for suite in suites:
        results.extend(run_suite(cfg, suite, data_path,
                                 join_data=join_path,
                                 canonical_join_data=canonical_join_path))

    print_comparison(results)

    if args.output:
        save_results(results, Path(args.output))
    if not args.no_html:
        generate_html_report(results, Path(args.html))
        generate_histogram_html(results, Path(args.html).parent / "histogram.html")

    if args.stop_infra:
        from .infra import stop_infrastructure
        stop_infrastructure(args.adapters, quiet=True)


if __name__ == "__main__":
    main()
