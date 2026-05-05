#!/usr/bin/env python3
"""Sort-grid orchestrator — runs the typed-column sort grid.

Each (adapter, dtype, length) triple is a separate subprocess so memory
is released between runs and a single failure doesn't take down the
whole grid. Results are aggregated into JSON suitable for the log-log
scaling-curve plot in docs/sort.html.
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

from .engine_source import engine_label, resolve_rayforce_py
from .generators.sort_grid import DTYPES, gen_grid, scaling_lengths
from .report import generate_sort_grid_html
from .swapcheck import SwapSample, warn_if_already_used, warn_if_grew


WORKER_TIMEOUT_S = 600
DEFAULT_GRID_ADAPTERS = ["rayforce", "duckdb", "polars", "chdb",
                         "datafusion", "pandas"]


def parse_size(s: str) -> int:
    s = s.lower().strip()
    mult = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}
    if s[-1] in mult:
        return int(float(s[:-1]) * mult[s[-1]])
    if "e" in s:
        return int(float(s))
    return int(s)


@dataclass
class GridConfig:
    adapters: list[str]
    dtypes: list[str]
    lengths: list[int]
    iterations: int = 3
    warmup: int = 1
    rayforce_local: str | None = None


def _spawn(cfg: GridConfig, adapter: str, dtype: str, length: int,
           csv_path: Path) -> dict:
    fd, result_path = tempfile.mkstemp(prefix=f"sg_{adapter}_{dtype}_{length}_",
                                       suffix=".json")
    os.close(fd)

    cmd = [
        sys.executable, "-m", "bench.sort_grid_worker",
        "--adapter", adapter,
        "--csv", str(csv_path),
        "--dtype", dtype,
        "--length", str(length),
        "--iterations", str(cfg.iterations),
        "--warmup", str(cfg.warmup),
        "--result", result_path,
    ]
    if cfg.rayforce_local:
        cmd += ["--rayforce-local", cfg.rayforce_local]

    try:
        subprocess.run(cmd, timeout=WORKER_TIMEOUT_S, check=False)
        with open(result_path) as f:
            return json.load(f)
    except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError) as e:
        return {
            "adapter": adapter, "dtype": dtype, "length": length,
            "results": [], "error": f"worker failed: {e}",
        }
    finally:
        if os.path.exists(result_path):
            os.unlink(result_path)


def _median(vals: list[float]) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    n = len(s)
    return (s[n // 2 - 1] + s[n // 2]) / 2 if n % 2 == 0 else s[n // 2]


def run_grid(cfg: GridConfig, data_root: Path) -> list[dict]:
    """Run the full grid. data_root has <dtype>/<length>.csv layout."""
    total = len(cfg.adapters) * len(cfg.dtypes) * len(cfg.lengths)
    done = 0
    results = []
    for adapter in cfg.adapters:
        print(f"\n[{adapter}]")
        for dtype in cfg.dtypes:
            for n in cfg.lengths:
                done += 1
                csv_path = data_root / dtype / f"{n}.csv"
                if not csv_path.exists():
                    print(f"  ({done}/{total}) {dtype}/{n}: MISSING CSV")
                    continue

                swap_before = SwapSample.now()
                data = _spawn(cfg, adapter, dtype, n, csv_path)
                warn_if_grew(swap_before, SwapSample.now(),
                             f"{adapter}/{dtype}/{n}")

                if data.get("error"):
                    print(f"  ({done}/{total}) {dtype}/{n}: ERROR — {data['error']}")
                    continue
                times = [r["time_ns"] / 1_000_000 for r in data["results"]]
                med = _median(times)
                print(f"  ({done}/{total}) {dtype:<6s} n={n:<10d} median={med:>8.2f}ms")
                results.append({
                    "adapter": adapter,
                    "version": data.get("version") or "?",
                    "dtype": dtype,
                    "length": n,
                    "rows": data["results"][0]["rows"] if data["results"] else 0,
                    "median_ms": med,
                    "times_ms": times,
                    "timestamp": data.get("timestamp", datetime.now().isoformat()),
                })
    return results


def main():
    ap = argparse.ArgumentParser(description="Extended sort-grid runner")
    ap.add_argument("-a", "--adapters", nargs="+",
                    default=list(DEFAULT_GRID_ADAPTERS),
                    help=f"Adapters (default: {','.join(DEFAULT_GRID_ADAPTERS)})")
    ap.add_argument("--dtypes", default=",".join(DTYPES),
                    help=f"Comma-separated dtypes (default: {','.join(DTYPES)})")
    ap.add_argument("--max", default="1m",
                    help="Max length on the scaling curve (default: 1m, e.g. 1e6)")
    ap.add_argument("--data-dir", default="data/sort_grid",
                    help="Where to read/generate per-dtype CSVs")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("-i", "--iterations", type=int, default=3)
    ap.add_argument("-w", "--warmup", type=int, default=1)
    ap.add_argument("-o", "--output", default="docs/sort_data.json",
                    help="Where to write aggregated results")
    ap.add_argument("--gen-only", action="store_true",
                    help="Generate CSVs and exit; don't run benchmarks")
    ap.add_argument("--rayforce-local")
    ap.add_argument("--rayforce-branch")
    args = ap.parse_args()

    dtypes = [d.strip() for d in args.dtypes.split(",") if d.strip()]
    max_n = parse_size(args.max)
    lengths = scaling_lengths(max_n)
    data_root = Path(args.data_dir).resolve()

    print(f"Sort grid: {len(args.adapters)} adapters × {len(dtypes)} dtypes "
          f"× {len(lengths)} lengths (max {max_n:,})")

    print(f"\nGenerating CSVs at {data_root}...")
    gen_grid(data_root, dtypes, lengths, seed=args.seed, verbose=False)
    print("CSV generation done.")

    if args.gen_only:
        return

    rayforce_src = resolve_rayforce_py(args.rayforce_local, args.rayforce_branch)
    label = engine_label("rayforce", rayforce_src)
    if rayforce_src is not None:
        print(f"Using rayforce: {label}")

    cfg = GridConfig(
        adapters=list(args.adapters),
        dtypes=dtypes,
        lengths=lengths,
        iterations=args.iterations,
        warmup=args.warmup,
        rayforce_local=str(rayforce_src) if rayforce_src else None,
    )

    warn_if_already_used(SwapSample.now())
    results = run_grid(cfg, data_root)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({
        "meta": {
            "max_n": max_n,
            "iterations": args.iterations,
            "warmup": args.warmup,
            "rayforce_label": label,
            "timestamp": datetime.now().isoformat(),
        },
        "results": results,
    }, indent=2))
    print(f"\nResults saved to {out_path} ({len(results)} entries)")

    html_path = out_path.parent / "sort.html"
    generate_sort_grid_html(out_path, html_path)


if __name__ == "__main__":
    main()
