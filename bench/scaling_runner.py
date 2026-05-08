#!/usr/bin/env python3
"""Scaling-curve orchestrator.

Sweeps sizes from 10 rows up to whatever the user asks for, runs each
H2O groupby/join/sort op plus the typed sort grid at each point, and
produces a single JSON suitable for the interactive Plotly chart in
docs/scaling.html.

Each (adapter, op, size) triple is a separate subprocess so memory is
released between runs. Iteration count adapts to size — small inputs
need many runs to beat the noise floor; huge inputs need few because
each run is already slow.
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
from .generators.groupby import GroupByGenerator
from .generators.join import JoinGenerator
from .generators.sort_grid import DTYPES as SORT_DTYPES, gen_grid as gen_sort_grid
from .swapcheck import SwapSample, warn_if_already_used, warn_if_grew


WORKER_TIMEOUT_S = 1200
WORKER_RETRIES   = 1   # Retry once on empty/missing result (rayforce-py
                       # wrapper sometimes crashes mid-run; second attempt
                       # in a fresh subprocess usually succeeds).

DEFAULT_ADAPTERS = ["rayforce", "duckdb", "polars", "chdb",
                    "datafusion", "pandas"]

# Adapters that implement run_sort_typed_full (typed-sort grid).
SORT_GRID_ADAPTERS = {"rayforce", "duckdb", "polars", "chdb",
                      "datafusion", "pandas", "questdb", "timescale"}

H2O_OPS = [
    "groupby_q1", "groupby_q2", "groupby_q3", "groupby_q4",
    "groupby_q5", "groupby_q6", "groupby_q7",
    "join_inner", "join_left",
    "sort_single", "sort_multi",
]

# Skip join under this row count — both sides are tiny, the timing is
# pure overhead and the curve adds nothing.
JOIN_MIN_ROWS = 1000


def parse_size(s: str) -> int:
    s = s.lower().strip()
    mult = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}
    if s[-1] in mult:
        return int(float(s[:-1]) * mult[s[-1]])
    if "e" in s:
        return int(float(s))
    return int(s)


def adaptive_iter(n: int) -> tuple[int, int]:
    """Return (n_iter, n_warmup) for a given row count.

    Same staircase as teide-bench/sort_bench_multi.iter_counts: tiny
    inputs need many runs to drown out the perf_counter noise floor;
    huge inputs already cost seconds per iteration so we cut down.
    """
    if n <= 100:        return 21, 5
    if n <= 100_000:    return 7,  3
    if n <= 10_000_000: return 5,  2
    return 3, 1


# CLI-level override (set in main); when not None, replaces adaptive_iter.
_FORCED_ITER: int | None = None
_FORCED_WARMUP: int | None = None


def iter_counts(n: int) -> tuple[int, int]:
    if _FORCED_ITER is not None and _FORCED_WARMUP is not None:
        return _FORCED_ITER, _FORCED_WARMUP
    return adaptive_iter(n)


def fmt_size(n: int) -> str:
    if n >= 1_000_000_000: return f"{n // 1_000_000_000}b"
    if n >= 1_000_000:     return f"{n // 1_000_000}m"
    if n >= 1_000:         return f"{n // 1_000}k"
    return str(n)


def _manifest_matches(out: Path, generator: str, want: dict) -> bool:
    """Check the dataset directory's manifest.json against the generator
    parameters we are about to produce. Returns True only when EVERY
    key in `want` is present and equal in the manifest. Missing manifest
    or any mismatch → False (caller should regenerate).
    """
    mp = out / "manifest.json"
    if not mp.exists():
        return False
    try:
        m = json.loads(mp.read_text())
    except Exception:
        return False
    # Generator metadata is stored under m["metadata"] (see DataGenerator).
    meta = m.get("metadata") or {}
    if meta.get("generator") != generator:
        return False
    return all(meta.get(k) == v for k, v in want.items())


def _wipe_stale_csvs(out: Path) -> None:
    """Drop CSV / manifest files in `out` that came from a previous
    generator run with mismatching parameters. Leaves the directory
    around but empty so the new generator can write fresh files."""
    if not out.exists():
        return
    for child in out.iterdir():
        if child.is_file() and (child.suffix == ".csv"
                                or child.name == "manifest.json"):
            child.unlink()


def ensure_groupby(data_root: Path, n: int, k: int, seed: int) -> Path:
    """Generate groupby dataset for size n if missing or stale.

    "Stale" means the manifest disagrees on n_rows / k / seed / schema
    version with what the current generator would produce — files are
    wiped and regenerated. This keeps `make bench` honest after a
    generator change.
    """
    name = f"groupby_{fmt_size(n)}_k{k}"
    out = data_root / name
    want = {"n_rows": n, "k": k, "seed": seed,
            "schema_version": "h2o-canonical-v1"}
    if (out / "data.csv").exists() and _manifest_matches(out, "groupby", want):
        return out
    _wipe_stale_csvs(out)
    gen = GroupByGenerator(n_rows=n, k=k, seed=seed)
    ds = gen.generate()
    ds.write(out, formats=["csv"])
    return out


def ensure_join(data_root: Path, n: int, k: int, seed: int) -> Path:
    name = f"join_{fmt_size(n)}x{fmt_size(n)}"
    out = data_root / name
    want = {"n_rows_left": n, "n_rows_right": n, "k": k, "seed": seed,
            "schema_version": "h2o-canonical-v1"}
    if ((out / "left.csv").exists() and (out / "right.csv").exists()
            and _manifest_matches(out, "join", want)):
        return out
    _wipe_stale_csvs(out)
    gen = JoinGenerator(n_rows_left=n, n_rows_right=n, k=k, seed=seed)
    ds = gen.generate()
    ds.write(out, formats=["csv"])
    return out


def ensure_canonical_join(data_root: Path, n: int, k: int, seed: int) -> Path:
    """Generate canonical H2O J1 (x, small, medium, big) for size n.

    Regenerates if manifest doesn't match (or doesn't exist), so a
    schema-version bump in the generator does not silently reuse old
    CSVs.
    """
    from .generators import CanonicalJoinGenerator
    name = f"join_canonical_{fmt_size(n)}_k{k}"
    out = data_root / name
    files = ["x.csv", "small.csv", "medium.csv", "big.csv"]
    want = {"n_rows_x": n, "k": k, "seed": seed,
            "schema_version": "h2o-canonical-j1-v2"}
    if (all((out / f).exists() for f in files)
            and _manifest_matches(out, "join_canonical_h2o", want)):
        return out
    _wipe_stale_csvs(out)
    gen = CanonicalJoinGenerator(n_rows=n, k=k, seed=seed)
    ds = gen.generate()
    ds.write(out, formats=["csv"])
    return out


@dataclass
class ScalingConfig:
    adapters: list[str]
    sizes: list[int]
    k: int = 100
    seed: int = 0
    h2o_ops: list[str] = field(default_factory=lambda: list(H2O_OPS))
    sort_dtypes: list[str] = field(default_factory=lambda: list(SORT_DTYPES))
    rayforce_local: str | None = None


def _spawn_h2o(cfg: ScalingConfig, adapter: str, op: str, n: int,
               groupby_dir: Path, join_dir: Path | None) -> dict:
    """Run one H2O (adapter, op) at size n via bench.worker."""
    fd, result_path = tempfile.mkstemp(prefix=f"sc_h2o_{adapter}_{op}_{n}_",
                                       suffix=".json")
    os.close(fd)

    n_iter, n_warmup = iter_counts(n)
    if op.startswith("join_"):
        primary = join_dir / "left.csv"
        right = join_dir / "right.csv"
    else:
        primary = groupby_dir / "data.csv"
        right = None

    cmd = [
        sys.executable, "-m", "bench.worker",
        "--adapter", adapter,
        "--benchmark", op,
        "--data", str(primary),
        "--iterations", str(n_iter),
        "--warmup", str(n_warmup),
        "--result", result_path,
    ]
    if right is not None:
        cmd += ["--right-data", str(right)]
    if cfg.rayforce_local:
        cmd += ["--rayforce-local", cfg.rayforce_local]

    return _run_worker_with_retry(
        cmd, result_path,
        identity=f"{adapter}/{op} n={n}",
        on_fail={"adapter": adapter, "benchmark": op, "results": [],
                 "version": "?"},
    )


def _spawn_sort_grid(cfg: ScalingConfig, adapter: str, dtype: str, n: int,
                     csv_path: Path) -> dict:
    """Run one typed-sort point via bench.sort_grid_worker."""
    fd, result_path = tempfile.mkstemp(prefix=f"sc_sort_{adapter}_{dtype}_{n}_",
                                       suffix=".json")
    os.close(fd)

    n_iter, n_warmup = iter_counts(n)
    cmd = [
        sys.executable, "-m", "bench.sort_grid_worker",
        "--adapter", adapter,
        "--csv", str(csv_path),
        "--dtype", dtype,
        "--length", str(n),
        "--iterations", str(n_iter),
        "--warmup", str(n_warmup),
        "--result", result_path,
    ]
    if cfg.rayforce_local:
        cmd += ["--rayforce-local", cfg.rayforce_local]

    return _run_worker_with_retry(
        cmd, result_path,
        identity=f"{adapter}/sort_{dtype} n={n}",
        on_fail={"adapter": adapter, "dtype": dtype, "length": n,
                 "results": [], "version": "?"},
    )


def _run_worker_with_retry(cmd: list, result_path: str,
                            identity: str, on_fail: dict) -> dict:
    """Spawn a worker; retry once if the JSON result is missing or empty.

    Why retry: rayforce-py 1.0.0 occasionally crashes between the timed
    block and the JSON write — the parent then sees "Expecting value"
    on json.load with no actionable info. A second attempt in a fresh
    Python subprocess usually succeeds. Other adapters never need this
    in practice.

    On final failure we attach the worker's stderr tail so the report
    explains why instead of just "Expecting value: line 1 column 1".
    """
    last_err = None
    for attempt in range(1 + WORKER_RETRIES):
        try:
            proc = subprocess.run(cmd, timeout=WORKER_TIMEOUT_S, check=False,
                                  capture_output=True, text=True)
            with open(result_path) as f:
                return json.load(f)
        except subprocess.TimeoutExpired:
            last_err = f"timeout after {WORKER_TIMEOUT_S}s"
            break  # retrying a timeout doesn't change anything
        except (FileNotFoundError, json.JSONDecodeError):
            tail = (proc.stderr or "").strip().splitlines()[-3:]
            last_err = (f"empty result (rc={proc.returncode}; "
                        f"stderr: {' | '.join(tail) if tail else 'silent'})")
        finally:
            if os.path.exists(result_path):
                os.unlink(result_path)
        if attempt < WORKER_RETRIES:
            print(f"  retry [{identity}]: {last_err}")
    return {**on_fail, "error": f"worker failed: {last_err}"}


def _median(vals):
    if not vals:
        return 0.0
    s = sorted(vals)
    n = len(s)
    return (s[n // 2 - 1] + s[n // 2]) / 2 if n % 2 == 0 else s[n // 2]


# CLI override: "min" (best of N) or "median".
_METRIC: str = "median"


def _agg(vals):
    if not vals:
        return 0.0
    return min(vals) if _METRIC == "min" else _median(vals)


def run(cfg: ScalingConfig, data_root: Path) -> list[dict]:
    """Sweep sizes, return aggregated [{adapter, op, size, median_ms, ...}]."""
    results = []
    sort_grid_root = data_root / "sort_grid"

    for size in cfg.sizes:
        print(f"\n══════ size = {size:,} ══════")
        # Generate datasets for this size before any subprocess starts.
        gb_dir = ensure_groupby(data_root, size, cfg.k, cfg.seed)
        join_dir = ensure_join(data_root, size, cfg.k, cfg.seed) \
            if size >= JOIN_MIN_ROWS else None
        # Sort-grid CSVs: one per dtype, only for the sizes we need.
        gen_sort_grid(sort_grid_root, cfg.sort_dtypes, [size],
                      seed=cfg.seed, verbose=False)

        # ── H2O ops ─────────────────────────────────────────────────────
        for adapter in cfg.adapters:
            for op in cfg.h2o_ops:
                if op.startswith("join_") and size < JOIN_MIN_ROWS:
                    continue
                swap_before = SwapSample.now()
                data = _spawn_h2o(cfg, adapter, op, size, gb_dir, join_dir)
                warn_if_grew(swap_before, SwapSample.now(),
                             f"{adapter}/{op}/n={size}")
                if data.get("error"):
                    print(f"  {adapter:<11s} {op:<14s} n={size:<10d} "
                          f"ERROR — {data['error']}")
                    continue
                times = [r["time_ns"] / 1e6 for r in data.get("results", [])]
                if not times:
                    continue
                med = _agg(times)
                rows = data["results"][0]["rows"]
                print(f"  {adapter:<11s} {op:<14s} n={size:<10d} "
                      f"median={med:>9.3f}ms  rows={rows}")
                results.append({
                    "adapter": adapter,
                    "version": data.get("version") or "?",
                    "op": op,
                    "size": size,
                    "rows": rows,
                    "median_ms": med,
                    "times_ms": times,
                })

        # ── Typed sort grid ─────────────────────────────────────────────
        for adapter in cfg.adapters:
            if adapter not in SORT_GRID_ADAPTERS:
                continue
            for dtype in cfg.sort_dtypes:
                csv_path = sort_grid_root / dtype / f"{size}.csv"
                if not csv_path.exists():
                    continue
                swap_before = SwapSample.now()
                data = _spawn_sort_grid(cfg, adapter, dtype, size, csv_path)
                warn_if_grew(swap_before, SwapSample.now(),
                             f"{adapter}/sort_{dtype}/n={size}")
                if data.get("error"):
                    print(f"  {adapter:<11s} sort_{dtype:<9s} n={size:<10d} "
                          f"ERROR — {data['error']}")
                    continue
                times = [r["time_ns"] / 1e6 for r in data.get("results", [])]
                if not times:
                    continue
                med = _agg(times)
                rows = data["results"][0]["rows"]
                op = f"sort_{dtype}"
                print(f"  {adapter:<11s} {op:<14s} n={size:<10d} "
                      f"median={med:>9.3f}ms  rows={rows}")
                results.append({
                    "adapter": adapter,
                    "version": data.get("version") or "?",
                    "op": op,
                    "size": size,
                    "rows": rows,
                    "median_ms": med,
                    "times_ms": times,
                })

    return results


def main():
    ap = argparse.ArgumentParser(description="Scaling-curve runner")
    ap.add_argument("-a", "--adapters", nargs="+",
                    default=list(DEFAULT_ADAPTERS),
                    help=f"Adapters (default: {' '.join(DEFAULT_ADAPTERS)})")
    ap.add_argument("--sizes", default="10,100,1k,10k,100k,1m",
                    help="Comma-separated sizes (default: 10,100,1k,10k,100k,1m)")
    ap.add_argument("-k", type=int, default=100,
                    help="Group cardinality K (default: 100)")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--data-dir", default="data",
                    help="Where to read/generate datasets")
    ap.add_argument("--ops", default=",".join(H2O_OPS),
                    help="H2O ops to run (comma-separated)")
    ap.add_argument("--sort-dtypes", default=",".join(SORT_DTYPES),
                    help="Sort-grid dtypes (comma-separated)")
    ap.add_argument("-o", "--output", default="docs/scaling_data.json",
                    help="Where to write aggregated results")
    ap.add_argument("--no-html", action="store_true")
    ap.add_argument("-i", "--iterations", type=int, default=None,
                    help="Override iter count (default: adaptive 21/7/5/3 by size)")
    ap.add_argument("-w", "--warmup", type=int, default=None,
                    help="Override warmup count (default: adaptive 5/3/2/1 by size)")
    ap.add_argument("--metric", choices=["median", "min"], default="median",
                    help="Aggregate timed iterations as median or min (default median)")
    ap.add_argument("--rayforce-local")
    ap.add_argument("--rayforce-branch")
    ap.add_argument("--stop-infra", action="store_true",
                    help="Stop server containers (questdb / timescale) after the sweep")
    args = ap.parse_args()

    sizes = [parse_size(s) for s in args.sizes.split(",") if s.strip()]
    h2o_ops = [op.strip() for op in args.ops.split(",") if op.strip()]
    sort_dtypes = [d.strip() for d in args.sort_dtypes.split(",") if d.strip()]
    data_root = Path(args.data_dir).resolve()

    global _FORCED_ITER, _FORCED_WARMUP, _METRIC
    if args.iterations is not None and args.warmup is not None:
        _FORCED_ITER = args.iterations
        _FORCED_WARMUP = args.warmup
    _METRIC = args.metric

    rayforce_src = resolve_rayforce_py(args.rayforce_local, args.rayforce_branch)
    label = engine_label("rayforce", rayforce_src)
    if rayforce_src is not None:
        print(f"Using rayforce: {label}")

    cfg = ScalingConfig(
        adapters=list(args.adapters),
        sizes=sizes,
        k=args.k,
        seed=args.seed,
        h2o_ops=h2o_ops,
        sort_dtypes=sort_dtypes,
        rayforce_local=str(rayforce_src) if rayforce_src else None,
    )

    print(f"Sweep: {len(args.adapters)} adapters × "
          f"({len(h2o_ops)} H2O ops + {len(sort_dtypes)} sort dtypes) × "
          f"{len(sizes)} sizes")

    # Auto-start Docker containers for any server-engine adapter — otherwise
    # the per-worker psycopg connect fails with "Connection refused" on
    # every iteration and the curve has nothing but errors for them.
    from .infra import start_required_infrastructure, CONTAINERS, is_container_running
    if not start_required_infrastructure(args.adapters, quiet=True):
        failed = [a for a in args.adapters
                  if a in CONTAINERS and not is_container_running(CONTAINERS[a]["name"])]
        if failed:
            print(f"WARNING: failed to start: {failed} — they'll show as ERROR.")
            cfg.adapters = [a for a in cfg.adapters if a not in failed]

    warn_if_already_used(SwapSample.now())
    results = run(cfg, data_root)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "meta": {
            "sizes": sizes,
            "k": args.k,
            "seed": args.seed,
            "rayforce_label": label,
            "timestamp": datetime.now().isoformat(),
        },
        "results": results,
    }
    out_path.write_text(json.dumps(payload, indent=2))
    print(f"\nResults saved to {out_path} ({len(results)} entries)")

    if not args.no_html:
        from .report import generate_scaling_html
        html_path = out_path.parent / "scaling.html"
        generate_scaling_html(out_path, html_path)

    if args.stop_infra:
        from .infra import stop_infrastructure
        stop_infrastructure(args.adapters, quiet=True)
        print("Stopped server containers.")


if __name__ == "__main__":
    main()
