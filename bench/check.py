#!/usr/bin/env python3
"""Cross-adapter result-equivalence checker.

Runs every (op, size) once per adapter, compares each non-reference
adapter's result against polars (the reference). Adapters are run in
isolated subprocesses (same architecture as bench), but instead of
timing we capture the full materialized polars DataFrame via Arrow IPC
on stdout and compare in the parent.

The check passes when:
  - schema names agree (column-set equality, order doesn't matter)
  - row counts agree
  - canonicalized values agree (rtol=1e-6, atol=1e-9 for floats; exact
    for int and string)

Output is silent on success per (op, size, adapter): a single ✓ tick.
On any failure, prints the diff and continues. Final exit code = 0 if
nothing failed, 1 otherwise.
"""

from __future__ import annotations

import argparse
import io
import math
import os
import subprocess
import sys
from pathlib import Path

import polars as pl
from polars.testing import assert_frame_equal

from .scaling_runner import (
    ensure_groupby, ensure_join, ensure_canonical_join,
    parse_size, fmt_size,
)


OPS = [
    # Canonical H2O groupby (q1..q10)
    "groupby_q1", "groupby_q2", "groupby_q3", "groupby_q4",
    "groupby_q5", "groupby_q6", "groupby_q7", "groupby_q8",
    "groupby_q9", "groupby_q10",
    # Canonical H2O joins (q1..q5)
    "join_q1", "join_q2", "join_q3", "join_q4", "join_q5",
    # Bonus stress tests
    "join_inner", "join_left",
    "sort_single", "sort_multi",
]

REFERENCE_ADAPTER = "polars"

RTOL = 1e-6
ATOL = 1e-9


def _data_paths(data_root: Path, op: str, n: int) -> tuple[Path, Path | None]:
    """Return (primary_path, right_csv_or_None) for op at size n.

    Canonical H2O join (join_q1..q5) needs 4 tables (x/small/medium/big)
    pre-loaded — primary_path is the dataset directory, right_csv is None
    (the worker calls adapter.load_canonical_join(dir)).

    Bonus 3-key joins (join_inner/join_left) use a single (left, right)
    pair; primary_path is left.csv, right_csv is right.csv.

    Groupby/sort use a single CSV; primary_path is data.csv, right None.
    """
    is_canonical_join = (op.startswith("join_q")
                         and op[len("join_q"):].isdigit())
    if is_canonical_join:
        cj = ensure_canonical_join(data_root, n, k=100, seed=0)
        return (cj, None)
    if op.startswith("join_"):
        join_dir = ensure_join(data_root, n, k=100, seed=0)
        return (join_dir / "left.csv", join_dir / "right.csv")
    gb_dir = ensure_groupby(data_root, n, k=100, seed=0)
    return (gb_dir / "data.csv", None)


def _run_worker(adapter: str, op: str, data: Path,
                right: Path | None,
                rayforce_local: str | None) -> tuple[pl.DataFrame | None, str | None]:
    """Spawn check_worker, return (DataFrame, error_msg).

    On success: (df, None). On failure: (None, "stderr text").
    """
    cmd = [
        sys.executable, "-m", "bench.check_worker",
        "--adapter", adapter,
        "--op", op,
        "--data", str(data),
    ]
    if right is not None:
        cmd += ["--right", str(right)]
    if rayforce_local:
        cmd += ["--rayforce-local", rayforce_local]

    proc = subprocess.run(cmd, capture_output=True)
    if proc.returncode != 0:
        err = proc.stderr.decode(errors="replace").strip()
        # Detect NotImplementedError as a special NYI status, distinct
        # from a real failure. The string appears in worker's traceback
        # output; first line of stderr is the formatted exception.
        if "NotImplementedError:" in err.splitlines()[0] if err else False:
            msg = err.splitlines()[0].split("NotImplementedError:", 1)[1].strip()
            return None, f"NYI: {msg}"
        return None, err or f"exit {proc.returncode}, no stderr"
    if not proc.stdout:
        return None, "worker exited 0 but produced no stdout"
    try:
        df = pl.read_ipc(io.BytesIO(proc.stdout))
    except Exception as e:
        return None, f"failed to parse worker stdout as Arrow IPC: {e}"
    return df, None


def _canonicalize(df: pl.DataFrame) -> pl.DataFrame:
    """Return a sortable canonical form: sort columns by name, sort rows
    by tuple of all columns. This neutralizes engine-specific row order
    in groupby/join (ordering not specified by SQL) so equivalence
    comparison reflects the multiset of result rows, not implementation
    quirks.

    Also coerces Decimal columns to Float64. PostgreSQL's `AVG(int)`
    returns `numeric(38,20)`, which polars surfaces as Decimal[38,20];
    the reference (polars) returns Float64. The values are numerically
    identical, just typed differently — cast both to Float64 so the
    rtol/atol comparison can apply.
    """
    if df.width == 0:
        return df
    # Drop engine-specific duplicate-column suffixes from joins. Polars
    # uses `_right`, duckdb `_1`, pandas `_x`/`_y`, chdb `<table>.col`.
    # The canonical H2O answer is "left-side cols + key + right's v2";
    # the dup-suffix cols are bookkeeping noise that varies per engine.
    import re
    drop = []
    for name in df.columns:
        if (name.endswith("_right")
                or re.search(r"_\d+$", name)
                or name.endswith("_x") or name.endswith("_y")
                or "." in name):
            drop.append(name)
    if drop:
        df = df.drop(drop)
    casts = []
    for name, dtype in df.schema.items():
        if isinstance(dtype, pl.Decimal):
            # Decimal[*,*] (PG numeric) → Float64. See _canonicalize doc.
            casts.append(pl.col(name).cast(pl.Float64))
        elif dtype.is_float():
            # Engines disagree on representing "undefined" for float
            # ops on degenerate input (single-element std, corr of two
            # equal vectors, etc.):
            #   polars  pl.std    → null
            #   polars  pl.corr   → NaN
            #   pandas  .std()    → NaN
            #   pandas  .corr()   → NaN
            #   duckdb  STDDEV    → null  (with PG semantics)
            # Normalize NaN→null on both sides so they compare equal.
            casts.append(pl.col(name).fill_nan(None))
    if casts:
        df = df.with_columns(casts)
    cols = sorted(df.columns)
    df = df.select(cols)
    return df.sort(cols)


def _drop_dup_suffix_cols(df: pl.DataFrame) -> pl.DataFrame:
    """Drop engine-specific duplicate-column suffixes and bookkeeping cols.

    Sources:
        polars        → `_right`
        duckdb        → `_1`
        pandas        → `_x`, `_y`
        chdb          → `<table>.col` (qualified)
        questdb plain → `<col>1` (just trailing digit, no underscore)
        questdb ILP   → implicit `timestamp` partition column
    These artifacts vary per engine and aren't part of the semantic
    canonical H2O result.
    """
    if df.width == 0:
        return df
    import re
    cols = df.columns
    # questdb's "just trailing digit" suffix is ambiguous (could be a
    # real col name like id1, id4). Detect it by: stripping trailing
    # digits, the prefix matches another col already in the df.
    cols_set = set(cols)
    drop = []
    for name in cols:
        if (name.endswith("_right")
                or re.search(r"_\d+$", name)
                or name.endswith("_x") or name.endswith("_y")
                or "." in name
                or name == "timestamp"):
            drop.append(name)
            continue
        # Trailing-digit case (QuestDB-style: id11 = id1+1, id41 = id4+1).
        # Strip just the LAST digit; if the prefix is another col, drop.
        # Doing this for each suffix length up to 2 covers a couple of
        # join chained-suffixes if any.
        for strip_n in (1, 2):
            if len(name) > strip_n and name[-strip_n:].isdigit():
                prefix = name[:-strip_n]
                if prefix in cols_set and prefix != name:
                    drop.append(name)
                    break
    return df.drop(drop) if drop else df


def _compare(ref: pl.DataFrame, other: pl.DataFrame) -> str | None:
    """Return None if equivalent, else a multi-line diff string."""
    # Drop engine-specific dup-suffix cols BEFORE schema check.
    ref = _drop_dup_suffix_cols(ref)
    other = _drop_dup_suffix_cols(other)
    # Schema: same column set?
    ref_cols = set(ref.columns)
    other_cols = set(other.columns)
    if ref_cols != other_cols:
        only_ref = sorted(ref_cols - other_cols)
        only_other = sorted(other_cols - ref_cols)
        msg = ["schema mismatch:"]
        if only_ref:
            msg.append(f"    missing: {only_ref}")
        if only_other:
            msg.append(f"    extra:   {only_other}")
        return "\n".join(msg)

    # Row count
    if ref.height != other.height:
        return f"row count: reference={ref.height}, got={other.height}"

    if ref.height == 0:
        return None

    a = _canonicalize(ref)
    b = _canonicalize(other)

    # Coerce dtypes column-by-column where the values are numerically
    # equivalent but engines disagree on width (Int32 vs Int64, BIGINT
    # vs Int64). check_dtypes=False handles this in assert_frame_equal.
    try:
        assert_frame_equal(
            a, b,
            check_dtypes=False,
            check_column_order=True,  # canonicalize already sorted them
            check_row_order=True,     # ditto for rows
            rel_tol=RTOL, abs_tol=ATOL,
        )
    except AssertionError as e:
        # Polars gives a useful error already; surface its message.
        return _trim_error(str(e))
    return None


def _trim_error(msg: str, lines: int = 8) -> str:
    """Keep the first few lines of polars's assert error so the report
    stays readable when many ops disagree."""
    parts = msg.splitlines()
    if len(parts) <= lines:
        return msg
    return "\n".join(parts[:lines]) + f"\n    ... ({len(parts) - lines} more lines)"


def main() -> int:
    ap = argparse.ArgumentParser(description="cross-adapter check")
    ap.add_argument("--sizes", default="10,100,1k,10k,100k,1m,10m",
                    help="Comma-separated row counts (default: full sweep)")
    ap.add_argument("-a", "--adapters", nargs="+", required=True,
                    help="Adapters to check (must include polars as reference)")
    ap.add_argument("--data-dir", default="data",
                    help="Where to read/generate datasets")
    ap.add_argument("--rayforce-local", default=None)
    ap.add_argument("--ops", default=None,
                    help="Comma-separated subset of ops (default: all 11)")
    ap.add_argument("--stop-infra", action="store_true",
                    help="Stop Docker containers on exit")
    args = ap.parse_args()

    if REFERENCE_ADAPTER not in args.adapters:
        print(f"ERROR: reference adapter '{REFERENCE_ADAPTER}' must be in --adapters",
              file=sys.stderr)
        return 1

    sizes = [parse_size(s) for s in args.sizes.split(",") if s.strip()]
    ops = OPS if args.ops is None else [
        o.strip() for o in args.ops.split(",") if o.strip()
    ]
    test_adapters = [a for a in args.adapters if a != REFERENCE_ADAPTER]
    data_root = Path(args.data_dir).resolve()

    # Bring up Docker if needed (questdb/timescale).
    docker_up = False
    if any(a in ("questdb", "timescale") for a in args.adapters):
        from .infra import start_required_infrastructure
        if not start_required_infrastructure(args.adapters, quiet=True):
            print("ERROR: failed to start required Docker containers",
                  file=sys.stderr)
            return 1
        docker_up = True

    # Width of the largest adapter name, for aligned columns.
    name_w = max(len(a) for a in test_adapters)

    failures: list[tuple[str, int, str, str]] = []  # (op, size, adapter, msg)
    nyi: list[tuple[str, int, str, str]] = []
    total = 0
    passed = 0

    try:
        for op in ops:
            for n in sizes:
                data, right = _data_paths(data_root, op, n)

                # Reference (polars) first; held in memory until we've
                # compared against every test adapter at this (op, size).
                ref_df, ref_err = _run_worker(
                    REFERENCE_ADAPTER, op, data, right, args.rayforce_local)
                if ref_err is not None:
                    print(f"  {op:<14s} / {fmt_size(n):>4s}  "
                          f"REFERENCE polars FAILED: {ref_err}")
                    failures.append((op, n, REFERENCE_ADAPTER, ref_err))
                    total += len(test_adapters)
                    continue

                line = f"  {op:<14s} / {fmt_size(n):>4s} "
                for adapter in test_adapters:
                    total += 1
                    df, err = _run_worker(
                        adapter, op, data, right, args.rayforce_local)
                    if err is not None and err.startswith("NYI:"):
                        line += f" {adapter:<{name_w}s} ⊘"
                        nyi.append((op, n, adapter, err[len("NYI: "):]))
                        continue
                    if err is not None:
                        line += f" {adapter:<{name_w}s} ✗"
                        failures.append((op, n, adapter, err))
                        continue
                    diff = _compare(ref_df, df)
                    del df  # release memory before next adapter
                    if diff is None:
                        line += f" {adapter:<{name_w}s} ✓"
                        passed += 1
                    else:
                        line += f" {adapter:<{name_w}s} ✗"
                        failures.append((op, n, adapter, diff))
                print(line)
                del ref_df
    finally:
        if args.stop_infra and docker_up:
            from .infra import stop_infrastructure
            stop_infrastructure(args.adapters, quiet=True)

    print()
    if nyi:
        # NYI ⊘ — engine doesn't yet support that canonical op. Listed
        # so reviewers see the gap, but does NOT fail the run.
        print(f"NYI — {len(nyi)} (op, size, adapter) combinations not "
              f"yet implemented:")
        # Group by (adapter, reason) for compactness
        by_msg: dict[tuple[str, str], list[tuple[str, int]]] = {}
        for op, n, adapter, msg in nyi:
            by_msg.setdefault((adapter, msg), []).append((op, n))
        for (adapter, msg), pairs in by_msg.items():
            ops_str = ", ".join(sorted({f"{op}@{fmt_size(n)}" for op, n in pairs}))
            print(f"  ⊘ {adapter}: {msg}")
            print(f"      → {ops_str}")
        print()

    if failures:
        print(f"FAIL — {len(failures)} of {total} comparisons failed:")
        for op, n, adapter, msg in failures:
            print(f"  ✗ {op} / {fmt_size(n)} / {adapter}")
            for ln in msg.splitlines():
                print(f"      {ln}")
        return 1

    print(f"pass — {passed}/{total} comparisons matched polars, "
          f"{len(nyi)} NYI (rtol={RTOL}, atol={ATOL})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
