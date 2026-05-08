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
        """Q4: mean(v1), mean(v2), mean(v3) by id4 — canonical H2O."""
        df = self._get_table()

        def query():
            return df.group_by("id4").agg(
                pl.mean("v1"), pl.mean("v2"), pl.mean("v3")
            )

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q4", time_ns, len(result))

    def run_groupby_q5(self) -> BenchmarkResult:
        """Q5: sum(v1), sum(v2), sum(v3) by id6 — canonical H2O."""
        df = self._get_table()

        def query():
            return df.group_by("id6").agg(
                pl.sum("v1"), pl.sum("v2"), pl.sum("v3")
            )

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q5", time_ns, len(result))

    def run_groupby_q6(self) -> BenchmarkResult:
        """Q6: median(v3), sd(v3) by id4, id5 — canonical H2O."""
        df = self._get_table()

        def query():
            return df.group_by("id4", "id5").agg(
                pl.median("v3").alias("v3_median"),
                pl.std("v3").alias("v3_std"),
            )

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q6", time_ns, len(result))

    def run_groupby_q7(self) -> BenchmarkResult:
        """Q7: max(v1) - min(v2) by id3 — canonical H2O."""
        df = self._get_table()

        def query():
            return df.group_by("id3").agg(
                (pl.max("v1") - pl.min("v2")).alias("range_v1_v2")
            )

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q7", time_ns, len(result))

    def run_groupby_q8(self) -> BenchmarkResult:
        """Q8: largest two v3 by id6 — canonical H2O."""
        df = self._get_table()

        def query():
            return (
                df.drop_nulls("v3")
                  .sort("v3", descending=True)
                  .group_by("id6")
                  .agg(pl.col("v3").head(2).alias("largest2_v3"))
                  .explode("largest2_v3")
            )

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q8", time_ns, len(result))

    def run_groupby_q9(self) -> BenchmarkResult:
        """Q9: corr(v1, v2)^2 by id2, id4 — canonical H2O."""
        df = self._get_table()

        def query():
            return df.group_by("id2", "id4").agg(
                (pl.corr("v1", "v2") ** 2).alias("r2")
            )

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q9", time_ns, len(result))

    def run_groupby_q10(self) -> BenchmarkResult:
        """Q10: sum(v3), count(v1) by id1..id6 — canonical H2O."""
        df = self._get_table()

        def query():
            return df.group_by(["id1", "id2", "id3", "id4", "id5", "id6"]).agg(
                pl.sum("v3").alias("v3"),
                pl.col("v1").count().alias("cnt"),
            )

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q10", time_ns, len(result))

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

    def materialize(self, op: str, right_path: Path | None = None) -> pl.DataFrame:
        if op in ("join_inner", "join_left"):
            how = "inner" if op == "join_inner" else "left"
            joined = self._get_table("left").join(
                pl.read_csv(right_path), on=["id1", "id2", "id3"], how=how
            )
            keep = ["id1", "id2", "id3", "id4", "id5", "id6", "v1", "v2"]
            return joined.select([c for c in keep if c in joined.columns])
        df = self._get_table()
        if op == "groupby_q1":
            return df.group_by("id1").agg(pl.sum("v1"))
        if op == "groupby_q2":
            return df.group_by("id1", "id2").agg(pl.sum("v1"))
        if op == "groupby_q3":
            return df.group_by("id3").agg(pl.sum("v1"), pl.mean("v3"))
        if op == "groupby_q4":
            return df.group_by("id4").agg(pl.mean("v1"), pl.mean("v2"), pl.mean("v3"))
        if op == "groupby_q5":
            return df.group_by("id6").agg(pl.sum("v1"), pl.sum("v2"), pl.sum("v3"))
        if op == "groupby_q6":
            return df.group_by("id4", "id5").agg(
                pl.median("v3").alias("v3_median"),
                pl.std("v3").alias("v3_std"),
            )
        if op == "groupby_q7":
            return df.group_by("id3").agg(
                (pl.max("v1") - pl.min("v2")).alias("range_v1_v2")
            )
        if op == "groupby_q8":
            return (df.drop_nulls("v3")
                      .sort("v3", descending=True)
                      .group_by("id6")
                      .agg(pl.col("v3").head(2).alias("largest2_v3"))
                      .explode("largest2_v3"))
        if op == "groupby_q9":
            return df.group_by("id2", "id4").agg(
                (pl.corr("v1", "v2") ** 2).alias("r2")
            )
        if op == "groupby_q10":
            return df.group_by(["id1", "id2", "id3", "id4", "id5", "id6"]).agg(
                pl.sum("v3").alias("v3"),
                pl.col("v1").count().alias("cnt"),
            )
        if op == "sort_single":
            return df.sort("id1")
        if op == "sort_multi":
            return df.sort("id1", "id2", "id3")
        raise ValueError(f"unknown op: {op}")

    def close(self) -> None:
        self._tables.clear()
