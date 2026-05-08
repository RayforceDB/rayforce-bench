#!/usr/bin/env python3
"""Sort-grid worker — runs one (adapter, dtype, csv) bench in isolation.

Same isolation pattern as bench.worker, but with a single-column typed
CSV input and run_sort_typed_full() rather than the named-bench dispatch.
"""

import argparse
import json
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path


def _make_adapter(name, args):
    if name == "polars":
        from .adapters.polars_adapter import PolarsAdapter
        return PolarsAdapter()
    if name == "duckdb":
        from .adapters.duckdb_adapter import DuckDBAdapter
        return DuckDBAdapter()
    if name == "pandas":
        from .adapters.pandas_adapter import PandasAdapter
        return PandasAdapter()
    if name == "chdb":
        from .adapters.chdb_adapter import ChdbAdapter
        return ChdbAdapter()
    if name == "datafusion":
        from .adapters.datafusion_adapter import DataFusionAdapter
        return DataFusionAdapter()
    if name == "rayforce":
        from .adapters.rayforce_adapter import RayforceAdapter
        return RayforceAdapter(local_path=args.rayforce_local)
    if name == "questdb":
        from .adapters.questdb_adapter import QuestDBAdapter
        return QuestDBAdapter()
    if name == "timescale":
        from .adapters.timescale_adapter import TimescaleAdapter
        return TimescaleAdapter()
    raise ValueError(f"Unsupported adapter for sort grid: {name}")


def main():
    ap = argparse.ArgumentParser(description="Sort-grid worker (subprocess)")
    ap.add_argument("--adapter", required=True)
    ap.add_argument("--csv", required=True)
    ap.add_argument("--dtype", required=True)
    ap.add_argument("--length", type=int, required=True)
    ap.add_argument("--iterations", type=int, default=3)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--result", required=True)
    ap.add_argument("--rayforce-local")
    args = ap.parse_args()

    output = {
        "adapter": args.adapter,
        "dtype": args.dtype,
        "length": args.length,
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
        results = adapter.run_sort_typed_full(
            Path(args.csv), args.dtype, args.warmup, args.iterations,
        )
        for r in results:
            output["results"].append({
                "name": r.name, "time_ns": r.time_ns,
                "rows": r.rows, "error": r.error,
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
    os._exit(0 if output["error"] is None else 1)


if __name__ == "__main__":
    main()
