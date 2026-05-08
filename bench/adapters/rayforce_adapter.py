"""Rayforce native adapter for benchmarks.

Measures end-to-end execution time with time.perf_counter_ns around
eval_str() — same convention as the other adapters, so the comparison is
fair. Does NOT use the engine's internal (timeit ...), which would hide
Python-binding overhead and tilt results in rayforce's favor.
"""

import time
from pathlib import Path
import subprocess
import sys

from .base import Adapter, BenchmarkResult


class RayforceAdapter(Adapter):
    """Benchmark adapter for rayforce native core.

    Measures: Pure rayforce execution time (no Python overhead).

    Uses eval_str("(timeit ...)") to measure query execution time
    directly in the rayforce runtime.
    """

    name = "rayforce"

    def __init__(self, local_path: str | Path | None = None):
        """Initialize rayforce adapter.

        Args:
            local_path: Path to local rayforce-py repo for dev builds.
                       If None, uses installed package from PyPI.
        """
        self._local_path = Path(local_path) if local_path else None
        self._rayforce = None
        self._eval_str = None
        self._Table = None
        self._I64 = None
        self._F64 = None
        self._table_names: dict[str, str] = {}  # Maps logical name to rayforce symbol

        self._setup_rayforce()

    def _setup_rayforce(self) -> None:
        """Setup rayforce module - either from PyPI or local build."""
        if self._local_path:
            self._setup_local_build()
        else:
            self._setup_pypi()

    def _setup_pypi(self) -> None:
        """Use rayforce from PyPI."""
        try:
            import rayforce
            from rayforce import Table, Column, I64, F64

            self._rayforce = rayforce
            self._eval_str = rayforce.eval_str
            self._Table = Table
            self._Column = Column
            self._I64 = I64
            self._F64 = F64
            self._Symbol = getattr(rayforce, "Symbol", None)
            self._STR = getattr(rayforce, "STR", None) or getattr(rayforce, "Str", None)
            self.version = f"{rayforce.version} (pypi)"
        except ImportError as e:
            raise ImportError(
                "rayforce-py not installed. Install with: pip install rayforce-py"
            ) from e

    def _setup_local_build(self) -> None:
        """Build and use rayforce from local path."""
        if not self._local_path or not self._local_path.exists():
            raise ValueError(f"Local path does not exist: {self._local_path}")

        # Build the package using uv (faster) or pip
        print(f"Building rayforce-py from {self._local_path}...")

        result = subprocess.run(
            ["uv", "pip", "install", "-e", str(self._local_path), "--python", sys.executable],
            capture_output=True,
            text=True,
            cwd=self._local_path,
        )
        if result.returncode != 0:
            result = subprocess.run(
                [sys.executable, "-m", "pip", "install", "-e", str(self._local_path), "-q"],
                capture_output=True,
                text=True,
                cwd=self._local_path,
            )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to build rayforce-py: {result.stderr}")

        # Force reimport
        for mod in list(sys.modules.keys()):
            if mod.startswith("rayforce"):
                del sys.modules[mod]

        import rayforce
        from rayforce import Table, Column, I64, F64

        self._rayforce = rayforce
        self._eval_str = rayforce.eval_str
        self._Table = Table
        self._Column = Column
        self._I64 = I64
        self._F64 = F64
        self._Symbol = getattr(rayforce, "Symbol", None)
        self._STR = getattr(rayforce, "STR", None) or getattr(rayforce, "Str", None)
        self.version = f"{rayforce.version} (local: {self._local_path})"
        print(f"Using rayforce {self.version}")

    def load_data(self, path: Path, table_name: str = "data") -> None:
        """Load CSV data using rayforce native Table.from_csv."""
        symbol_name = f"_bench_{table_name}"

        column_types = self._get_column_types(path)
        rf_table = self._Table.from_csv(column_types, str(path))
        rf_table.save(symbol_name)
        self._table_names[table_name] = symbol_name
        # Keep a Python-level Table handle for the chained API
        # (.select().by().execute(), etc.) — canonical rayforce-py idiom
        # per https://py.rayforcedb.com.
        if not hasattr(self, "_tables"):
            self._tables = {}
        self._tables[table_name] = rf_table

    def _get_table_obj(self, name: str = "data"):
        if not hasattr(self, "_tables") or name not in self._tables:
            raise ValueError(f"Table '{name}' not loaded")
        return self._tables[name]

    def _get_column_types(self, path: Path) -> list:
        """Get column types for canonical H2O CSVs.

        groupby:  id1..id3 string-symbol, id4..id6 i64, v1/v2 i64, v3 f64
        join:     id1..id3 i64,           id4..id6 string-symbol, v float
        """
        with open(path) as f:
            header = [c.strip().strip('"') for c in f.readline().strip().split(",")]

        sym = self._Symbol or self._STR
        if sym is None:
            raise RuntimeError(
                "rayforce-py lacks Symbol/STR types; "
                "canonical H2O schema requires string IDs. "
                "Upgrade rayforce-py."
            )

        # Look at the first data row to disambiguate (groupby vs join layout).
        with open(path) as f:
            f.readline()
            first_row = [c.strip().strip('"') for c in f.readline().strip().split(",")]

        types = []
        for col, val in zip(header, first_row):
            if col.startswith("id"):
                # Strings start with "id" prefix in our generator output.
                types.append(sym if val.startswith("id") else self._I64)
            elif col == "v3":
                types.append(self._F64)
            elif col.startswith("v"):
                # join-side v is float; groupby v1/v2 are int. Sniff.
                try:
                    int(val)
                    types.append(self._I64)
                except ValueError:
                    types.append(self._F64)
            else:
                types.append(self._I64)
        return types

    def _get_symbol(self, name: str = "data") -> str:
        """Get rayforce symbol name for a table."""
        if name not in self._table_names:
            raise ValueError(f"Table '{name}' not loaded")
        return self._table_names[name]

    def _run_timed_query(self, query: str, bench_name: str) -> BenchmarkResult:
        """Execute a query and time it externally with perf_counter_ns.

        Args:
            query: The rayforce query (rayfall expression as a string)
            bench_name: Name of the benchmark

        Returns:
            BenchmarkResult with timing in nanoseconds
        """
        start = time.perf_counter_ns()
        result = self._eval_str(query)
        time_ns = time.perf_counter_ns() - start
        rows = len(result) if result is not None else 0
        return BenchmarkResult(bench_name, time_ns, rows)

    def _timed(self, fn, name: str) -> BenchmarkResult:
        result, time_ns = self._time_it(fn)
        rows = len(result) if result is not None else 0
        return BenchmarkResult(name, time_ns, rows)

    def run_groupby_q1(self) -> BenchmarkResult:
        """Q1: sum(v1) group by id1"""
        t = self._get_table_obj()
        C = self._Column
        return self._timed(
            lambda: t.select(v1=C("v1").sum()).by("id1").execute(),
            "groupby_q1",
        )

    def run_groupby_q2(self) -> BenchmarkResult:
        """Q2: sum(v1) group by id1, id2"""
        t = self._get_table_obj()
        C = self._Column
        return self._timed(
            lambda: t.select(v1=C("v1").sum()).by("id1", "id2").execute(),
            "groupby_q2",
        )

    def run_groupby_q3(self) -> BenchmarkResult:
        """Q3: sum(v1), mean(v3) group by id3"""
        t = self._get_table_obj()
        C = self._Column
        return self._timed(
            lambda: t.select(v1=C("v1").sum(),
                             v3=C("v3").mean()).by("id3").execute(),
            "groupby_q3",
        )

    def run_groupby_q4(self) -> BenchmarkResult:
        """Q4: mean(v1), mean(v2), mean(v3) group by id3"""
        t = self._get_table_obj()
        C = self._Column
        return self._timed(
            lambda: t.select(v1=C("v1").mean(),
                             v2=C("v2").mean(),
                             v3=C("v3").mean()).by("id3").execute(),
            "groupby_q4",
        )

    def run_groupby_q5(self) -> BenchmarkResult:
        """Q5: sum(v1), sum(v2), sum(v3) group by id3"""
        t = self._get_table_obj()
        C = self._Column
        return self._timed(
            lambda: t.select(v1=C("v1").sum(),
                             v2=C("v2").sum(),
                             v3=C("v3").sum()).by("id3").execute(),
            "groupby_q5",
        )

    def run_groupby_q6(self) -> BenchmarkResult:
        """Q6: max(v1) - min(v2) group by id3.

        Two-stage because rayforce does not currently support arithmetic
        across two per-group aggregates inside `.by(...)` — the
        `Column("v1").max() - Column("v2").min()` expression is computed
        globally and then broadcast, not per-group. Workaround: aggregate
        max/min separately per group, then subtract in a follow-up
        select with no `.by`.
        """
        t = self._get_table_obj()
        C = self._Column

        def query():
            agg = t.select(v1m=C("v1").max(),
                           v2m=C("v2").min()).by("id3").execute()
            return agg.select("id3",
                              range=C("v1m") - C("v2m")).execute()

        return self._timed(query, "groupby_q6")

    def run_groupby_q7(self) -> BenchmarkResult:
        """Q7: sum(v3), count(v1) group by id1..id6 (canonical H2O)."""
        t = self._get_table_obj()
        C = self._Column
        return self._timed(
            lambda: t.select(v3=C("v3").sum(),
                             cnt=C("v1").count()
                             ).by("id1", "id2", "id3",
                                  "id4", "id5", "id6").execute(),
            "groupby_q7",
        )

    def _load_table_from_csv(self, path: Path) -> object:
        """Load CSV file using rayforce native Table.from_csv."""
        column_types = self._get_column_types(path)
        return self._Table.from_csv(column_types, str(path))

    def run_join_inner(self, right_path: Path) -> BenchmarkResult:
        """Inner join on (id1, id2, id3) — canonical H2O J1."""
        L = self._get_table_obj("left")
        R = self._load_table_from_csv(right_path)
        return self._timed(
            lambda: L.inner_join(R, on=["id1", "id2", "id3"]).execute(),
            "join_inner",
        )

    def run_join_left(self, right_path: Path) -> BenchmarkResult:
        """Left join on (id1, id2, id3) — canonical H2O J1."""
        L = self._get_table_obj("left")
        R = self._load_table_from_csv(right_path)
        return self._timed(
            lambda: L.left_join(R, on=["id1", "id2", "id3"]).execute(),
            "join_left",
        )

    def run_sort_single(self) -> BenchmarkResult:
        """Sort by single column."""
        t = self._get_table_obj()
        return self._timed(lambda: t.order_by("id1").execute(), "sort_single")

    def run_sort_multi(self) -> BenchmarkResult:
        """Sort by multiple columns."""
        t = self._get_table_obj()
        return self._timed(
            lambda: t.order_by("id1", "id2", "id3").execute(),
            "sort_multi",
        )

    _RF_TYPES_NAME = {
        "u8": "U8", "i16": "I16", "i32": "I32",
        "i64": "I64", "f64": "F64",
        # rayforce-py 1.0.0 has rf.String but it's a Vector wrapper without
        # the .ray_name attribute Table.from_csv() expects, so we can't
        # request a RAY_STR column at load time. Fall back to Symbol — same
        # underlying scan path that ~/rayforce/bench/h2o/q*.rfl uses.
        "str8": "Symbol", "str16": "Symbol",
    }

    def run_sort_typed_full(self, csv_path, dtype: str,
                             n_warmup: int, n_iter: int):
        """Sort a single typed column for the extended sort grid."""
        type_name = self._RF_TYPES_NAME[dtype]
        rf_type = getattr(self._rayforce, type_name)
        t = self._Table.from_csv([rf_type], str(csv_path))
        rows = len(t)

        for _ in range(n_warmup):
            t.order_by("v").execute()

        results = []
        for _ in range(n_iter):
            start = time.perf_counter_ns()
            t.order_by("v").execute()
            time_ns = time.perf_counter_ns() - start
            results.append(BenchmarkResult(f"sort_{dtype}", time_ns, rows))
        return results

    def materialize(self, op: str, right_path: Path | None = None):
        import polars as pl
        C = self._Column
        if op in ("join_inner", "join_left"):
            L = self._get_table_obj("left")
            R = self._load_table_from_csv(right_path)
            kind = L.inner_join if op == "join_inner" else L.left_join
            result = kind(R, on=["id1", "id2", "id3"]).execute()
        else:
            t = self._get_table_obj()
            if op == "groupby_q1":
                result = t.select(v1=C("v1").sum()).by("id1").execute()
            elif op == "groupby_q2":
                result = t.select(v1=C("v1").sum()).by("id1", "id2").execute()
            elif op == "groupby_q3":
                result = t.select(v1=C("v1").sum(),
                                  v3=C("v3").mean()).by("id3").execute()
            elif op == "groupby_q4":
                result = t.select(v1=C("v1").mean(),
                                  v2=C("v2").mean(),
                                  v3=C("v3").mean()).by("id3").execute()
            elif op == "groupby_q5":
                result = t.select(v1=C("v1").sum(),
                                  v2=C("v2").sum(),
                                  v3=C("v3").sum()).by("id3").execute()
            elif op == "groupby_q6":
                # Two-stage workaround — see run_groupby_q6 for rationale.
                agg = t.select(v1m=C("v1").max(),
                               v2m=C("v2").min()).by("id3").execute()
                result = agg.select("id3",
                                    range=C("v1m") - C("v2m")).execute()
            elif op == "groupby_q7":
                result = t.select(v3=C("v3").sum(),
                                  cnt=C("v1").count()
                                  ).by("id1", "id2", "id3",
                                       "id4", "id5", "id6").execute()
            elif op == "sort_single":
                result = t.order_by("id1").execute()
            elif op == "sort_multi":
                result = t.order_by("id1", "id2", "id3").execute()
            else:
                raise ValueError(f"unknown op: {op}")

        if result is None:
            return pl.DataFrame()
        # to_dict() can return wrapped scalar types (I64(3), F64(...));
        # polars treats those as Object dtype and refuses to write IPC.
        # Unwrap via .to_python() each rayforce scalar provides.
        d = result.to_dict()
        for col, vals in d.items():
            if vals and hasattr(vals[0], "to_python"):
                d[col] = [v.to_python() for v in vals]
        return pl.from_dict(d)

    def close(self) -> None:
        self._table_names.clear()
