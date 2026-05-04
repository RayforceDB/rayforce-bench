"""Rayforce adapter that drives the native binary via .rfl scripts.

Used when rayforce-py is not installed (e.g. PyPI release pending). Mirrors
how teide-bench tested rayforce2 — generate a temporary .rfl, invoke
~/rayforce/rayforce script.rfl, parse timeit numbers from stdout.

Schema assumes the project's standard groupby dataset: id1..id3 (i64) +
v1..v3 (f64). 7-column J1 left/right tables for joins follow the same
shape as teide-bench (i64 keys + symbol payload + f64).

The .rfl script does load_data outside (timeit ...), then n_warmup blind
runs, then n_iter measured runs each printing its own ms — so a single
process does the full warmup + measurement cycle, as in
~/rayforce/bench/h2o/*.rfl.
"""

import json
import os
import re
import subprocess
import tempfile
from pathlib import Path

from .base import Adapter, BenchmarkResult


DEFAULT_BIN = Path("~/rayforce/rayforce").expanduser()
DEFAULT_HEADER = Path("~/rayforce/include/rayforce.h").expanduser()


# Shared schema — must match what bench.generators.groupby produces.
GROUPBY_SCHEMA = "[I64 I64 I64 F64 F64 F64]"
JOIN_SCHEMA = "[I64 I64 I64 F64 F64 F64]"


# (Body of the rayfall expression — wrapped in (timeit ...) for measurement
# and called bare for warmup.)
GROUPBY_QUERIES = {
    "groupby_q1": "(select {{from: {t} v1: (sum v1) by: id1}})",
    "groupby_q2": "(select {{from: {t} v1: (sum v1) by: {{id1: id1 id2: id2}}}})",
    "groupby_q3": "(select {{from: {t} v1: (sum v1) v3: (avg v3) by: id3}})",
    "groupby_q4": "(select {{from: {t} v1: (avg v1) v2: (avg v2) v3: (avg v3) by: id3}})",
    "groupby_q5": "(select {{from: {t} v1: (sum v1) v2: (sum v2) v3: (sum v3) by: id3}})",
    "groupby_q6": "(select {{from: {t} range: (- (max v1) (min v2)) by: id3}})",
    "sort_single": "(xasc {t} [id1])",
    "sort_multi":  "(xasc {t} [id1 id2 id3])",
}


def _read_version(header: Path) -> str:
    if not header.exists():
        return "rfl"
    major = minor = patch = "0"
    for line in header.read_text().splitlines():
        if "RAY_VERSION_MAJOR" in line and "#define" in line:
            major = line.split()[-1]
        elif "RAY_VERSION_MINOR" in line and "#define" in line:
            minor = line.split()[-1]
        elif "RAY_VERSION_PATCH" in line and "#define" in line:
            patch = line.split()[-1]
    return f"{major}.{minor}.{patch} (rfl)"


class RayforceRflAdapter(Adapter):
    """Drives ./rayforce binary via temporary .rfl scripts."""

    name = "rayforce"

    def __init__(self, binary: str | Path | None = None):
        bin_path = Path(binary).expanduser() if binary else DEFAULT_BIN
        if not bin_path.exists():
            raise FileNotFoundError(
                f"Rayforce binary not found at {bin_path}. "
                f"Build with: cd ~/rayforce && make release"
            )
        self.bin = bin_path
        # Header lives next to the binary in normal layouts.
        header = bin_path.parent / "include" / "rayforce.h"
        self.version = _read_version(header if header.exists() else DEFAULT_HEADER)
        self._left: Path | None = None

    def load_data(self, path: Path, table_name: str = "data") -> None:
        # No actual load happens here — the .rfl script reads CSV at run time
        # (.csv.read outside (timeit ...) so it doesn't enter measurements).
        if table_name in ("data", "left"):
            self._left = Path(path)

    # The per-bench methods are required by the ABC but never get called
    # because run_full() builds a single .rfl per (bench, warmup+iter).
    def _nyi(self, name):
        raise NotImplementedError(
            f"{name}: rfl adapter executes via run_full(); per-iter calls "
            f"would re-launch the binary and re-read the CSV each time."
        )

    def run_groupby_q1(self): self._nyi("groupby_q1")
    def run_groupby_q2(self): self._nyi("groupby_q2")
    def run_groupby_q3(self): self._nyi("groupby_q3")
    def run_groupby_q4(self): self._nyi("groupby_q4")
    def run_groupby_q5(self): self._nyi("groupby_q5")
    def run_groupby_q6(self): self._nyi("groupby_q6")
    def run_sort_single(self): self._nyi("sort_single")
    def run_sort_multi(self):  self._nyi("sort_multi")
    def run_join_inner(self, right_path):
        self._nyi("join_inner")
    def run_join_left(self, right_path):
        self._nyi("join_left")

    def run_full(self, bench_name: str, n_warmup: int, n_iter: int,
                 right_path: Path | None = None) -> list[BenchmarkResult]:
        if self._left is None:
            raise RuntimeError("call load_data() before run_full()")

        if bench_name in GROUPBY_QUERIES:
            script = self._gen_single_table_script(
                bench_name, n_warmup, n_iter, table="df")
        elif bench_name in ("join_inner", "join_left"):
            if right_path is None:
                raise ValueError(f"{bench_name} requires right_path")
            script = self._gen_join_script(bench_name, n_warmup, n_iter,
                                            right_path)
        else:
            raise ValueError(f"Unsupported benchmark: {bench_name}")

        times_ms, rows = self._exec(script)
        return [BenchmarkResult(bench_name, int(ms * 1_000_000), rows)
                for ms in times_ms]

    def _gen_single_table_script(self, bench_name, n_warmup, n_iter, table):
        query = GROUPBY_QUERIES[bench_name].format(t=table)
        warmup_lines = [query for _ in range(n_warmup)]
        timed_lines = [f"(println (timeit {query}))" for _ in range(n_iter)]
        lines = [
            f'(set df (.csv.read {GROUPBY_SCHEMA} "{self._left}"))',
            "(println (count df))",
            *warmup_lines,
            *timed_lines,
            "(exit 0)",
        ]
        return "\n".join(lines)

    def _gen_join_script(self, bench_name, n_warmup, n_iter, right_path):
        # Both sides share the H2O J1 7-column schema.
        op = "inner-join" if bench_name == "join_inner" else "left-join"
        query = f"({op} [id1 id2 id3] x y)"
        warmup_lines = [query for _ in range(n_warmup)]
        timed_lines = [f"(println (timeit {query}))" for _ in range(n_iter)]
        lines = [
            f'(set x (.csv.read {JOIN_SCHEMA} "{self._left}"))',
            f'(set y (.csv.read {JOIN_SCHEMA} "{right_path}"))',
            "(println (count x))",
            *warmup_lines,
            *timed_lines,
            "(exit 0)",
        ]
        return "\n".join(lines)

    def _exec(self, script: str) -> tuple[list[float], int]:
        """Run script, return (timings_ms, row_count). Row count is the
        first non-timing line printed by (println (count ...))."""
        with tempfile.NamedTemporaryFile(suffix=".rfl", mode="w",
                                         delete=False) as f:
            f.write(script)
            rfl_path = f.name
        try:
            proc = subprocess.run(
                [str(self.bin), rfl_path],
                capture_output=True, text=True, timeout=600,
            )
        finally:
            os.unlink(rfl_path)

        if proc.returncode != 0:
            raise RuntimeError(
                f"rayforce exited {proc.returncode}: {proc.stderr.strip()}"
            )

        rows = 0
        times: list[float] = []
        for line in proc.stdout.strip().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                v = float(line)
            except ValueError:
                continue
            if rows == 0 and v == int(v):
                # First numeric line is row count from (println (count df)).
                rows = int(v)
            else:
                times.append(v)
        return times, rows

    _RFL_TYPES = {
        "u8": "U8", "i16": "I16", "i32": "I32",
        "i64": "I64", "f64": "F64",
        "str8": "STR", "str16": "STR",
    }

    def run_sort_typed_full(self, csv_path, dtype: str,
                             n_warmup: int, n_iter: int) -> list[BenchmarkResult]:
        """Sort a single typed column for the extended sort grid (rfl mode)."""
        rfl_type = self._RFL_TYPES[dtype]
        query = "(xasc t [v])"
        warmup_lines = [query for _ in range(n_warmup)]
        timed_lines = [f"(println (timeit {query}))" for _ in range(n_iter)]
        script = "\n".join([
            f'(set t (.csv.read [{rfl_type}] "{csv_path}"))',
            "(println (count t))",
            *warmup_lines,
            *timed_lines,
            "(exit 0)",
        ])
        times_ms, rows = self._exec(script)
        return [BenchmarkResult(f"sort_{dtype}", int(ms * 1_000_000), rows)
                for ms in times_ms]

    def close(self) -> None:
        self._left = None
