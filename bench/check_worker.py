#!/usr/bin/env python3
"""Subprocess worker for `make check`.

Runs one (adapter, op, size) and pipes the materialized result to the
parent as an Arrow IPC stream over stdout (binary). No disk involved —
parent reads it straight into memory.

Stderr carries human log/error lines. Empty stdout + exit != 0 means
the adapter failed.
"""

from __future__ import annotations

import argparse
import os
import sys
import traceback
from pathlib import Path


def _make_adapter(name, rayforce_local: str | None = None):
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
    if name == "questdb":
        from .adapters.questdb_adapter import QuestDBAdapter
        return QuestDBAdapter()
    if name == "timescale":
        from .adapters.timescale_adapter import TimescaleAdapter
        return TimescaleAdapter()
    if name == "rayforce":
        from .adapters.rayforce_adapter import RayforceAdapter
        return RayforceAdapter(local_path=rayforce_local)
    raise ValueError(f"Unknown adapter: {name}")


def main() -> int:
    ap = argparse.ArgumentParser(description="check worker")
    ap.add_argument("--adapter", required=True)
    ap.add_argument("--op", required=True)
    ap.add_argument("--data", required=True)
    ap.add_argument("--right", help="Right-side CSV for join_*")
    ap.add_argument("--rayforce-local", default=None)
    args = ap.parse_args()

    adapter = None
    code = 0
    try:
        adapter = _make_adapter(args.adapter, rayforce_local=args.rayforce_local)
        if args.op.startswith("join_"):
            adapter.load_data(Path(args.data), table_name="left")
            df = adapter.materialize(args.op, right_path=Path(args.right))
        else:
            adapter.load_data(Path(args.data))
            df = adapter.materialize(args.op)

        import polars as pl
        if df is None:
            df = pl.DataFrame()
        df.write_ipc(sys.stdout.buffer)
        sys.stdout.buffer.flush()
    except Exception as e:
        sys.stderr.write(f"{type(e).__name__}: {e}\n")
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()
        code = 2

    if adapter is not None:
        try:
            adapter.close()
        except Exception:
            pass
    # Skip Python finalizers — rayforce-py occasionally crashes during
    # interpreter shutdown. Stdout has already been flushed above, so the
    # parent will see a complete IPC stream (or empty stream + nonzero rc).
    os._exit(code)


if __name__ == "__main__":
    main()
