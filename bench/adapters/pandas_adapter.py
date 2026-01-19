"""Pandas adapter for benchmarks."""

from pathlib import Path

import pandas as pd

from .base import Adapter, BenchmarkResult


class PandasAdapter(Adapter):
    """Benchmark adapter for pandas."""

    name = "pandas"

    def __init__(self):
        self.version = pd.__version__
        self._tables: dict[str, pd.DataFrame] = {}

    def load_data(self, path: Path, table_name: str = "data") -> None:
        self._tables[table_name] = pd.read_parquet(path)

    def _get_table(self, name: str = "data") -> pd.DataFrame:
        if name not in self._tables:
            raise ValueError(f"Table '{name}' not loaded")
        return self._tables[name]

    def run_groupby_q1(self) -> BenchmarkResult:
        """Q1: sum(v1) group by id1"""
        df = self._get_table()

        def query():
            return df.groupby("id1", as_index=False).agg({"v1": "sum"})

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q1", time_ns, len(result))

    def run_groupby_q2(self) -> BenchmarkResult:
        """Q2: sum(v1) group by id1, id2"""
        df = self._get_table()

        def query():
            return df.groupby(["id1", "id2"], as_index=False).agg({"v1": "sum"})

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q2", time_ns, len(result))

    def run_groupby_q3(self) -> BenchmarkResult:
        """Q3: sum(v1), mean(v3) group by id3"""
        df = self._get_table()

        def query():
            return df.groupby("id3", as_index=False).agg({"v1": "sum", "v3": "mean"})

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q3", time_ns, len(result))

    def run_groupby_q4(self) -> BenchmarkResult:
        """Q4: mean(v1), mean(v2), mean(v3) group by id3"""
        df = self._get_table()

        def query():
            return df.groupby("id3", as_index=False).agg(
                {"v1": "mean", "v2": "mean", "v3": "mean"}
            )

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q4", time_ns, len(result))

    def run_groupby_q5(self) -> BenchmarkResult:
        """Q5: sum(v1), sum(v2), sum(v3) group by id3"""
        df = self._get_table()

        def query():
            return df.groupby("id3", as_index=False).agg(
                {"v1": "sum", "v2": "sum", "v3": "sum"}
            )

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q5", time_ns, len(result))

    def run_join_inner(self, right_path: Path) -> BenchmarkResult:
        """Inner join on id1."""
        left = self._get_table("left")
        right = pd.read_parquet(right_path)

        def query():
            return left.merge(right, on="id1", how="inner")

        result, time_ns = self._time_it(query)
        return BenchmarkResult("join_inner", time_ns, len(result))

    def run_join_left(self, right_path: Path) -> BenchmarkResult:
        """Left join on id1."""
        left = self._get_table("left")
        right = pd.read_parquet(right_path)

        def query():
            return left.merge(right, on="id1", how="left")

        result, time_ns = self._time_it(query)
        return BenchmarkResult("join_left", time_ns, len(result))

    def run_sort_single(self) -> BenchmarkResult:
        """Sort by single column."""
        df = self._get_table()

        def query():
            return df.sort_values("id1")

        result, time_ns = self._time_it(query)
        return BenchmarkResult("sort_single", time_ns, len(result))

    def run_sort_multi(self) -> BenchmarkResult:
        """Sort by multiple columns."""
        df = self._get_table()

        def query():
            return df.sort_values(["id1", "id2", "id3"])

        result, time_ns = self._time_it(query)
        return BenchmarkResult("sort_multi", time_ns, len(result))

    def close(self) -> None:
        self._tables.clear()
