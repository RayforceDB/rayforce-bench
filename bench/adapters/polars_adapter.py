"""Polars adapter for benchmarks."""

from pathlib import Path

import polars as pl

from .base import Adapter, BenchmarkResult


class PolarsAdapter(Adapter):
    """Benchmark adapter for polars."""

    name = "polars"

    def __init__(self):
        self.version = pl.__version__
        self._tables: dict[str, pl.DataFrame] = {}

    def load_data(self, path: Path, table_name: str = "data") -> None:
        # Load CSV and materialize in memory
        self._tables[table_name] = pl.read_csv(path)

    def _get_table(self, name: str = "data") -> pl.DataFrame:
        if name not in self._tables:
            raise ValueError(f"Table '{name}' not loaded")
        return self._tables[name]

    def run_groupby_q1(self) -> BenchmarkResult:
        """Q1: sum(v1) group by id1"""
        df = self._get_table()

        def query():
            return df.group_by("id1").agg(pl.sum("v1"))

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q1", time_ns, len(result))

    def run_groupby_q2(self) -> BenchmarkResult:
        """Q2: sum(v1) group by id1, id2"""
        df = self._get_table()

        def query():
            return df.group_by("id1", "id2").agg(pl.sum("v1"))

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q2", time_ns, len(result))

    def run_groupby_q3(self) -> BenchmarkResult:
        """Q3: sum(v1), mean(v3) group by id3"""
        df = self._get_table()

        def query():
            return df.group_by("id3").agg(pl.sum("v1"), pl.mean("v3"))

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q3", time_ns, len(result))

    def run_groupby_q4(self) -> BenchmarkResult:
        """Q4: mean(v1), mean(v2), mean(v3) group by id3"""
        df = self._get_table()

        def query():
            return df.group_by("id3").agg(
                pl.mean("v1"), pl.mean("v2"), pl.mean("v3")
            )

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q4", time_ns, len(result))

    def run_groupby_q5(self) -> BenchmarkResult:
        """Q5: sum(v1), sum(v2), sum(v3) group by id3"""
        df = self._get_table()

        def query():
            return df.group_by("id3").agg(
                pl.sum("v1"), pl.sum("v2"), pl.sum("v3")
            )

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q5", time_ns, len(result))

    def run_groupby_q6(self) -> BenchmarkResult:
        """Q6: max(v1) - min(v2) group by id3"""
        df = self._get_table()

        def query():
            return df.group_by("id3").agg(
                (pl.max("v1") - pl.min("v2")).alias("range")
            )

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q6", time_ns, len(result))

    def run_groupby_q7(self) -> BenchmarkResult:
        """Q7: sum(v3), count(v1) group by id1..id6 (canonical H2O)."""
        df = self._get_table()

        def query():
            return df.group_by(["id1", "id2", "id3", "id4", "id5", "id6"]).agg(
                pl.sum("v3"), pl.col("v1").count().alias("cnt")
            )

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q7", time_ns, len(result))

    def run_join_inner(self, right_path: Path) -> BenchmarkResult:
        """Inner join on (id1, id2, id3) — canonical H2O J1."""
        left = self._get_table("left")
        right = pl.read_csv(right_path)
        result, time_ns = self._time_it(
            lambda: left.join(right, on=["id1", "id2", "id3"], how="inner")
        )
        return BenchmarkResult("join_inner", time_ns, len(result))

    def run_join_left(self, right_path: Path) -> BenchmarkResult:
        """Left join on (id1, id2, id3) — canonical H2O J1."""
        left = self._get_table("left")
        right = pl.read_csv(right_path)
        result, time_ns = self._time_it(
            lambda: left.join(right, on=["id1", "id2", "id3"], how="left")
        )
        return BenchmarkResult("join_left", time_ns, len(result))

    def run_sort_single(self) -> BenchmarkResult:
        """Sort by single column."""
        df = self._get_table()

        def query():
            return df.sort("id1")

        result, time_ns = self._time_it(query)
        return BenchmarkResult("sort_single", time_ns, len(result))

    def run_sort_multi(self) -> BenchmarkResult:
        """Sort by multiple columns."""
        df = self._get_table()

        def query():
            return df.sort("id1", "id2", "id3")

        result, time_ns = self._time_it(query)
        return BenchmarkResult("sort_multi", time_ns, len(result))

    _POLARS_DTYPES = {
        "u8": pl.UInt8, "i16": pl.Int16, "i32": pl.Int32,
        "i64": pl.Int64, "f64": pl.Float64,
        "str8": pl.Utf8, "str16": pl.Utf8,
    }

    def run_sort_typed_full(self, csv_path: Path, dtype: str,
                             n_warmup: int, n_iter: int) -> list[BenchmarkResult]:
        """Sort a single typed column for the extended sort grid."""
        pl_dtype = self._POLARS_DTYPES[dtype]
        df = pl.read_csv(csv_path, schema={"v": pl_dtype})
        rows = df.height

        for _ in range(n_warmup):
            df.sort("v")

        results = []
        for _ in range(n_iter):
            _, time_ns = self._time_it(lambda: df.sort("v"))
            results.append(BenchmarkResult(f"sort_{dtype}", time_ns, rows))
        return results

    def close(self) -> None:
        self._tables.clear()
