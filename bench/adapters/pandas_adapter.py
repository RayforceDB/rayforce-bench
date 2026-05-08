"""Pandas adapter — slow baseline for context.

Pandas almost always loses by 5-50x to Polars/DuckDB. We include it not
because it's competitive, but because almost everyone reading the report
has an internal mental model calibrated against pandas — the "of course
pandas is slow" column makes the rest of the chart legible.
"""

from pathlib import Path

import pandas as pd

from .base import Adapter, BenchmarkResult


class PandasAdapter(Adapter):
    """Benchmark adapter for pandas DataFrames."""

    name = "pandas"

    def __init__(self):
        self.version = pd.__version__
        self._tables: dict[str, pd.DataFrame] = {}

    def load_data(self, path: Path, table_name: str = "data") -> None:
        # Materialize fully in memory; subsequent ops should not hit disk.
        self._tables[table_name] = pd.read_csv(path)

    def _get(self, name: str = "data") -> pd.DataFrame:
        if name not in self._tables:
            raise ValueError(f"Table '{name}' not loaded")
        return self._tables[name]

    def run_groupby_q1(self) -> BenchmarkResult:
        df = self._get()
        result, t = self._time_it(
            lambda: df.groupby("id1", as_index=False)["v1"].sum()
        )
        return BenchmarkResult("groupby_q1", t, len(result))

    def run_groupby_q2(self) -> BenchmarkResult:
        df = self._get()
        result, t = self._time_it(
            lambda: df.groupby(["id1", "id2"], as_index=False)["v1"].sum()
        )
        return BenchmarkResult("groupby_q2", t, len(result))

    def run_groupby_q3(self) -> BenchmarkResult:
        df = self._get()
        result, t = self._time_it(
            lambda: df.groupby("id3", as_index=False).agg(
                v1=("v1", "sum"), v3=("v3", "mean")
            )
        )
        return BenchmarkResult("groupby_q3", t, len(result))

    def run_groupby_q4(self) -> BenchmarkResult:
        df = self._get()
        result, t = self._time_it(
            lambda: df.groupby("id3", as_index=False).agg(
                v1=("v1", "mean"), v2=("v2", "mean"), v3=("v3", "mean")
            )
        )
        return BenchmarkResult("groupby_q4", t, len(result))

    def run_groupby_q5(self) -> BenchmarkResult:
        df = self._get()
        result, t = self._time_it(
            lambda: df.groupby("id3", as_index=False).agg(
                v1=("v1", "sum"), v2=("v2", "sum"), v3=("v3", "sum")
            )
        )
        return BenchmarkResult("groupby_q5", t, len(result))

    def run_groupby_q6(self) -> BenchmarkResult:
        df = self._get()

        def query():
            g = df.groupby("id3", as_index=False)
            return pd.DataFrame({
                "id3": g["v1"].max()["id3"],
                "range": g["v1"].max()["v1"].values - g["v2"].min()["v2"].values,
            })

        result, t = self._time_it(query)
        return BenchmarkResult("groupby_q6", t, len(result))

    def run_groupby_q7(self) -> BenchmarkResult:
        """Q7: sum(v3), count(v1) group by id1..id6 (canonical H2O)."""
        df = self._get()
        result, t = self._time_it(
            lambda: df.groupby(
                ["id1", "id2", "id3", "id4", "id5", "id6"], as_index=False
            ).agg(v3=("v3", "sum"), cnt=("v1", "count"))
        )
        return BenchmarkResult("groupby_q7", t, len(result))

    def run_join_inner(self, right_path: Path) -> BenchmarkResult:
        """Inner join on (id1, id2, id3) — canonical H2O J1."""
        left = self._get("left")
        right = pd.read_csv(right_path)
        result, t = self._time_it(
            lambda: left.merge(right, on=["id1", "id2", "id3"], how="inner")
        )
        return BenchmarkResult("join_inner", t, len(result))

    def run_join_left(self, right_path: Path) -> BenchmarkResult:
        """Left join on (id1, id2, id3) — canonical H2O J1."""
        left = self._get("left")
        right = pd.read_csv(right_path)
        result, t = self._time_it(
            lambda: left.merge(right, on=["id1", "id2", "id3"], how="left")
        )
        return BenchmarkResult("join_left", t, len(result))

    def run_sort_single(self) -> BenchmarkResult:
        df = self._get()
        result, t = self._time_it(lambda: df.sort_values("id1"))
        return BenchmarkResult("sort_single", t, len(result))

    def run_sort_multi(self) -> BenchmarkResult:
        df = self._get()
        result, t = self._time_it(lambda: df.sort_values(["id1", "id2", "id3"]))
        return BenchmarkResult("sort_multi", t, len(result))

    _PANDAS_DTYPES = {
        "u8": "uint8", "i16": "int16", "i32": "int32",
        "i64": "int64", "f64": "float64",
        "str8": "string", "str16": "string",
    }

    def run_sort_typed_full(self, csv_path: Path, dtype: str,
                             n_warmup: int, n_iter: int) -> list[BenchmarkResult]:
        pd_dtype = self._PANDAS_DTYPES[dtype]
        df = pd.read_csv(csv_path, dtype={"v": pd_dtype})
        rows = len(df)

        for _ in range(n_warmup):
            df.sort_values("v")

        results = []
        for _ in range(n_iter):
            _, t = self._time_it(lambda: df.sort_values("v"))
            results.append(BenchmarkResult(f"sort_{dtype}", t, rows))
        return results

    def close(self) -> None:
        self._tables.clear()
