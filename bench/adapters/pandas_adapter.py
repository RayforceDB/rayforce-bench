"""Pandas adapter — slow baseline for context.

Pandas almost always loses by 5-50x to Polars/DuckDB. We include it not
because it's competitive, but because almost everyone reading the report
has an internal mental model calibrated against pandas — the "of course
pandas is slow" column makes the rest of the chart legible.
"""

from pathlib import Path

import pandas as pd
import polars as pl

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
        """Q4: mean(v1), mean(v2), mean(v3) by id4 — canonical H2O."""
        df = self._get()
        result, t = self._time_it(
            lambda: df.groupby("id4", as_index=False).agg(
                v1=("v1", "mean"), v2=("v2", "mean"), v3=("v3", "mean")
            )
        )
        return BenchmarkResult("groupby_q4", t, len(result))

    def run_groupby_q5(self) -> BenchmarkResult:
        """Q5: sum(v1), sum(v2), sum(v3) by id6 — canonical H2O."""
        df = self._get()
        result, t = self._time_it(
            lambda: df.groupby("id6", as_index=False).agg(
                v1=("v1", "sum"), v2=("v2", "sum"), v3=("v3", "sum")
            )
        )
        return BenchmarkResult("groupby_q5", t, len(result))

    def run_groupby_q6(self) -> BenchmarkResult:
        """Q6: median(v3), sd(v3) by id4, id5 — canonical H2O."""
        df = self._get()
        result, t = self._time_it(
            lambda: df.groupby(["id4", "id5"], as_index=False).agg(
                v3_median=("v3", "median"), v3_std=("v3", "std")
            )
        )
        return BenchmarkResult("groupby_q6", t, len(result))

    def run_groupby_q7(self) -> BenchmarkResult:
        """Q7: max(v1) - min(v2) by id3 — canonical H2O."""
        df = self._get()

        def query():
            g = df.groupby("id3", as_index=False)
            return pd.DataFrame({
                "id3": g["v1"].max()["id3"],
                "range_v1_v2": g["v1"].max()["v1"].values
                               - g["v2"].min()["v2"].values,
            })

        result, t = self._time_it(query)
        return BenchmarkResult("groupby_q7", t, len(result))

    def run_groupby_q8(self) -> BenchmarkResult:
        """Q8: largest two v3 by id6 — canonical H2O."""
        df = self._get()

        def query():
            r = (df.dropna(subset=["v3"])
                   .sort_values("v3", ascending=False)
                   .groupby("id6", as_index=False)
                   .head(2)[["id6", "v3"]])
            return r.rename(columns={"v3": "largest2_v3"})

        result, t = self._time_it(query)
        return BenchmarkResult("groupby_q8", t, len(result))

    def run_groupby_q9(self) -> BenchmarkResult:
        """Q9: corr(v1, v2)^2 by id2, id4 — canonical H2O."""
        df = self._get()

        def query():
            r = (df.groupby(["id2", "id4"])
                   .apply(lambda g: g["v1"].corr(g["v2"]) ** 2)
                   .reset_index())
            r.columns = ["id2", "id4", "r2"]
            return r

        result, t = self._time_it(query)
        return BenchmarkResult("groupby_q9", t, len(result))

    def run_groupby_q10(self) -> BenchmarkResult:
        """Q10: sum(v3), count(v1) by id1..id6 — canonical H2O."""
        df = self._get()
        result, t = self._time_it(
            lambda: df.groupby(
                ["id1", "id2", "id3", "id4", "id5", "id6"], as_index=False
            ).agg(v3=("v3", "sum"), cnt=("v1", "count"))
        )
        return BenchmarkResult("groupby_q10", t, len(result))

    # Canonical H2O J1 — 5 single-key joins.

    def run_join_q1(self) -> BenchmarkResult:
        """Q1: x.merge(small, on=id1)."""
        x, r = self._get("x"), self._get("small")
        result, t = self._time_it(lambda: x.merge(r, on="id1", suffixes=("", "_right")))
        return BenchmarkResult("join_q1", t, len(result))

    def run_join_q2(self) -> BenchmarkResult:
        """Q2: x.merge(medium, on=id2)."""
        x, r = self._get("x"), self._get("medium")
        result, t = self._time_it(lambda: x.merge(r, on="id2", suffixes=("", "_right")))
        return BenchmarkResult("join_q2", t, len(result))

    def run_join_q3(self) -> BenchmarkResult:
        """Q3: x.merge(medium, on=id2, how=left)."""
        x, r = self._get("x"), self._get("medium")
        result, t = self._time_it(
            lambda: x.merge(r, on="id2", how="left", suffixes=("", "_right")))
        return BenchmarkResult("join_q3", t, len(result))

    def run_join_q4(self) -> BenchmarkResult:
        """Q4: x.merge(medium, on=id5) — string key."""
        x, r = self._get("x"), self._get("medium")
        result, t = self._time_it(lambda: x.merge(r, on="id5", suffixes=("", "_right")))
        return BenchmarkResult("join_q4", t, len(result))

    def run_join_q5(self) -> BenchmarkResult:
        """Q5: x.merge(big, on=id3)."""
        x, r = self._get("x"), self._get("big")
        result, t = self._time_it(lambda: x.merge(r, on="id3", suffixes=("", "_right")))
        return BenchmarkResult("join_q5", t, len(result))

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

    def materialize(self, op: str, right_path: Path | None = None) -> pl.DataFrame:
        # 'data' is loaded for groupby/sort ops. Joins (bonus or
        # canonical) load other tables and don't touch 'data'.
        is_canonical_join = (op.startswith("join_q")
                             and op[len("join_q"):].isdigit())
        is_bonus_join = op in ("join_inner", "join_left")
        df = None if (is_canonical_join or is_bonus_join) else self._get()
        if op == "groupby_q1":
            r = df.groupby("id1", as_index=False)["v1"].sum()
        elif op == "groupby_q2":
            r = df.groupby(["id1", "id2"], as_index=False)["v1"].sum()
        elif op == "groupby_q3":
            r = df.groupby("id3", as_index=False).agg(
                v1=("v1", "sum"), v3=("v3", "mean"))
        elif op == "groupby_q4":
            r = df.groupby("id4", as_index=False).agg(
                v1=("v1", "mean"), v2=("v2", "mean"), v3=("v3", "mean"))
        elif op == "groupby_q5":
            r = df.groupby("id6", as_index=False).agg(
                v1=("v1", "sum"), v2=("v2", "sum"), v3=("v3", "sum"))
        elif op == "groupby_q6":
            r = df.groupby(["id4", "id5"], as_index=False).agg(
                v3_median=("v3", "median"), v3_std=("v3", "std"))
        elif op == "groupby_q7":
            g = df.groupby("id3", as_index=False)
            r = pd.DataFrame({
                "id3": g["v1"].max()["id3"],
                "range_v1_v2": g["v1"].max()["v1"].values
                               - g["v2"].min()["v2"].values,
            })
        elif op == "groupby_q8":
            r = (df.dropna(subset=["v3"])
                   .sort_values("v3", ascending=False)
                   .groupby("id6", as_index=False)
                   .head(2)[["id6", "v3"]]
                   .rename(columns={"v3": "largest2_v3"}))
        elif op == "groupby_q9":
            r = (df.groupby(["id2", "id4"])
                   .apply(lambda g: g["v1"].corr(g["v2"]) ** 2)
                   .reset_index())
            r.columns = ["id2", "id4", "r2"]
        elif op == "groupby_q10":
            r = df.groupby(
                ["id1", "id2", "id3", "id4", "id5", "id6"], as_index=False
            ).agg(v3=("v3", "sum"), cnt=("v1", "count"))
        elif op == "join_q1":
            r = self._get("x").merge(self._get("small"), on="id1",
                                     suffixes=("", "_right"))
        elif op == "join_q2":
            r = self._get("x").merge(self._get("medium"), on="id2",
                                     suffixes=("", "_right"))
        elif op == "join_q3":
            r = self._get("x").merge(self._get("medium"), on="id2",
                                     how="left", suffixes=("", "_right"))
        elif op == "join_q4":
            r = self._get("x").merge(self._get("medium"), on="id5",
                                     suffixes=("", "_right"))
        elif op == "join_q5":
            r = self._get("x").merge(self._get("big"), on="id3",
                                     suffixes=("", "_right"))
        elif op in ("join_inner", "join_left"):
            how = "inner" if op == "join_inner" else "left"
            merged = self._get("left").merge(
                pd.read_csv(right_path), on=["id1", "id2", "id3"], how=how,
                suffixes=("", "_right"))
            # Canonical projection: keep keys + left.id4..id6 + left.v1 + right.v2.
            keep = ["id1", "id2", "id3", "id4", "id5", "id6", "v1", "v2"]
            r = merged[[c for c in keep if c in merged.columns]]
        elif op == "sort_single":
            r = df.sort_values("id1")
        elif op == "sort_multi":
            r = df.sort_values(["id1", "id2", "id3"])
        else:
            raise ValueError(f"unknown op: {op}")
        # nan_to_null=False so float NaN survives as NaN (not polars-null);
        # other engines preserve NaN (polars's pl.corr → NaN for degenerate
        # groups), and the cross-engine comparison must agree.
        return pl.from_pandas(r, nan_to_null=False)

    def close(self) -> None:
        self._tables.clear()
