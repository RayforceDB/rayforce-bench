#!/usr/bin/env python3
"""Benchmark worker — runs a single (adapter, benchmark) in an isolated process.

Child entrypoint invoked by the orchestrator (bench.runner). Memory is
guaranteed to be released between operations because each invocation runs
in its own process. Result is written as JSON to --result.
"""

import argparse
import json
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path


def _make_adapter(name, args):
    """Lazy-import adapter to avoid loading every engine in every worker."""
    if name == "polars":
        from .adapters import PolarsAdapter
        return PolarsAdapter()
    if name == "duckdb":
        from .adapters import DuckDBAdapter
        return DuckDBAdapter()
    if name == "questdb":
        from .adapters import QuestDBAdapter
        return QuestDBAdapter()
    if name == "timescale":
        from .adapters import TimescaleAdapter
        return TimescaleAdapter()
    if name == "rayforce":
        from .adapters import RayforceAdapter
        return RayforceAdapter(local_path=args.rayforce_local)
    raise ValueError(f"Unknown adapter: {name}")


def _bind_method(adapter, benchmark, args):
    """Resolve adapter.run_<benchmark> and adapt its signature."""
    method = getattr(adapter, f"run_{benchmark}")
    if benchmark.startswith("join_"):
        right = Path(args.right_data) if args.right_data else None
        if right is None:
            raise ValueError(f"join benchmark {benchmark} requires --right-data")
        return lambda: method(right)
    return method


def main():
    ap = argparse.ArgumentParser(description="Single-benchmark worker (subprocess)")
    ap.add_argument("--adapter", required=True)
    ap.add_argument("--benchmark", required=True,
                    help="Benchmark op name, e.g. groupby_q1, join_inner, sort_single")
    ap.add_argument("--data", required=True, help="Path to primary CSV (or directory)")
    ap.add_argument("--right-data", help="Path to right-side CSV (join only)")
    ap.add_argument("--iterations", type=int, default=5)
    ap.add_argument("--warmup", type=int, default=2)
    ap.add_argument("--result", required=True, help="Where to write JSON result")
    ap.add_argument("--rayforce-local", help="Path to local rayforce-py for dev builds")
    args = ap.parse_args()

    output = {
        "adapter": args.adapter,
        "benchmark": args.benchmark,
        "iterations": args.iterations,
        "warmup": args.warmup,
        "timestamp": datetime.now().isoformat(),
        "version": None,
        "results": [],
        "error": None,
    }

    adapter = None
    try:
        adapter = _make_adapter(args.adapter, args)
        output["version"] = adapter.version

        data_path = Path(args.data)
        if args.benchmark.startswith("join_"):
            adapter.load_data(data_path, "left")
        else:
            adapter.load_data(data_path)

        invoke = _bind_method(adapter, args.benchmark, args)

        for _ in range(args.warmup):
            invoke()

        for _ in range(args.iterations):
            r = invoke()
            output["results"].append({
                "name": r.name,
                "time_ns": r.time_ns,
                "rows": r.rows,
                "error": r.error,
            })
    except Exception as e:
        output["error"] = f"{type(e).__name__}: {e}"
        output["traceback"] = traceback.format_exc()
    finally:
        if adapter is not None:
            try:
                adapter.close()
            except Exception:
                pass

    Path(args.result).write_text(json.dumps(output, indent=2))
    # Skip Python-level cleanup — some engines (rayforce-py) segfault on exit.
    os._exit(0 if output["error"] is None else 1)


if __name__ == "__main__":
    main()
