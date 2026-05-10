"""DataFusion adapter — Apache Rust+Arrow query engine.

DataFusion is the foundation of several products (InfluxDB 3, GlareDB,
ROAPI, RisingLight, Sail). Including it lets us measure rayforce against
the common substrate of the Apache columnar ecosystem rather than just
one product.
"""

from pathlib import Path

from datafusion import SessionContext
import polars as pl

from .base import Adapter, BenchmarkResult, _dedupe_col_names


class DataFusionAdapter(Adapter):
    """Benchmark adapter for Apache DataFusion."""

    name = "datafusion"

    QUERY_STRINGS = {
        "groupby_q1":  "SELECT id1, sum(v1) FROM data GROUP BY id1",
        "groupby_q2":  "SELECT id1, id2, sum(v1) FROM data GROUP BY id1, id2",
        "groupby_q3":  "SELECT id3, sum(v1), avg(v3) FROM data GROUP BY id3",
        "groupby_q4":  "SELECT id4, avg(v1), avg(v2), avg(v3) FROM data GROUP BY id4",
        "groupby_q5":  "SELECT id6, sum(v1), sum(v2), sum(v3) FROM data GROUP BY id6",
        "groupby_q6":  "SELECT id4, id5, median(v3), stddev(v3) FROM data GROUP BY id4, id5",
        "groupby_q7":  "SELECT id3, max(v1) - min(v2) FROM data GROUP BY id3",
        "groupby_q8":  "SELECT id6, v3 FROM (SELECT id6, v3, ROW_NUMBER() OVER (PARTITION BY id6 ORDER BY v3 DESC) rn FROM data WHERE v3 IS NOT NULL) WHERE rn <= 2",
        "groupby_q9":  "SELECT id2, id4, POWER(corr(v1, v2), 2) FROM data GROUP BY id2, id4",
        "groupby_q10": "SELECT id1, id2, id3, id4, id5, id6, sum(v3), count(v1) FROM data GROUP BY id1, id2, id3, id4, id5, id6",
        "join_q1":     "SELECT * FROM x INNER JOIN small  USING (id1)",
        "join_q2":     "SELECT * FROM x INNER JOIN medium USING (id2)",
        "join_q3":     "SELECT * FROM x LEFT  JOIN medium USING (id2)",
        "join_q4":     "SELECT * FROM x INNER JOIN medium USING (id5)",
        "join_q5":     "SELECT * FROM x INNER JOIN big    USING (id3)",
        "join_inner":  "SELECT * FROM left INNER JOIN right USING (id1, id2, id3)",
        "join_left":   "SELECT * FROM left LEFT  JOIN right USING (id1, id2, id3)",
        "sort_single": "SELECT * FROM data ORDER BY id1",
        "sort_multi":  "SELECT * FROM data ORDER BY id1, id2, id3",
    }

    def __init__(self):
        import datafusion
        self.version = getattr(datafusion, "__version__", "unknown")
        self._ctx: SessionContext | None = None
        self._table_names: dict[str, str] = {}

    def _ensure_ctx(self) -> SessionContext:
        if self._ctx is None:
            self._ctx = SessionContext()
        return self._ctx

    def load_data(self, path: Path, table_name: str = "data") -> None:
        ctx = self._ensure_ctx()
        df_table = f"bench_{table_name}"
        # register_csv reads lazily; force materialization by collecting
        # into an in-memory table so subsequent timed queries don't pay
        # for disk reads.
        try:
            ctx.deregister_table(df_table)
        except Exception:
            pass
        ctx.register_csv(df_table, str(path))
        # Materialize: read all batches, register as memtable.
        batches = ctx.sql(f"SELECT * FROM {df_table}").collect()
        ctx.deregister_table(df_table)
        from datafusion import RecordBatchStream  # noqa: F401
        # The simplest robust path: keep CSV registered but warm OS page
        # cache by collecting once. Re-register to drop any stream state.
        ctx.register_csv(df_table, str(path))
        # Warm by collecting once (already done above).
        del batches
        self._table_names[table_name] = df_table

    def _get_table(self, name: str = "data") -> str:
        if name not in self._table_names:
            raise ValueError(f"Table '{name}' not loaded")
        return self._table_names[name]

    def _time(self, sql: str, name: str) -> BenchmarkResult:
        # Engine-only timing: drain `execute_stream()` so DataFusion
        # fully runs the plan without buffering all RecordBatches in
        # Python (as `.collect()` does) and without forcing a
        # unique-schema target (CREATE TABLE rejects USING-join dup
        # columns like duplicate `id4`). Each batch is dropped right
        # after counting its rows. Row count is computed inside the
        # drain — no extra query — and is *not* a count() rewrite the
        # optimiser can simplify away from the actual join/groupby.
        def query():
            # DataFusion's stream yields a wrapper whose only useful
            # method is `to_pyarrow()` — that gives a pyarrow
            # RecordBatch (zero-copy view, just a Python handle around
            # the Arrow buffer) on which we can read .num_rows for
            # display. The actual draining (engine compute) happens via
            # the iteration itself.
            stream = self._ctx.sql(sql).execute_stream()
            n = 0
            for batch in stream:
                n += batch.to_pyarrow().num_rows
            return n
        rows, t = self._time_it(query)
        return BenchmarkResult(name, t, rows)

    def run_groupby_q1(self) -> BenchmarkResult:
        """Q1: sum(v1) by id1 — canonical H2O."""
        t = self._get_table()
        return self._time(
            f"SELECT id1, sum(v1) AS v1 FROM {t} GROUP BY id1", "groupby_q1")

    def run_groupby_q2(self) -> BenchmarkResult:
        """Q2: sum(v1) by id1, id2 — canonical H2O."""
        t = self._get_table()
        return self._time(
            f"SELECT id1, id2, sum(v1) AS v1 FROM {t} GROUP BY id1, id2",
            "groupby_q2")

    def run_groupby_q3(self) -> BenchmarkResult:
        """Q3: sum(v1), mean(v3) by id3 — canonical H2O."""
        t = self._get_table()
        return self._time(
            f"SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM {t} GROUP BY id3",
            "groupby_q3")

    def run_groupby_q4(self) -> BenchmarkResult:
        """Q4: mean(v1), mean(v2), mean(v3) by id4 — canonical H2O."""
        t = self._get_table()
        return self._time(
            f"SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 "
            f"FROM {t} GROUP BY id4", "groupby_q4")

    def run_groupby_q5(self) -> BenchmarkResult:
        """Q5: sum(v1), sum(v2), sum(v3) by id6 — canonical H2O."""
        t = self._get_table()
        return self._time(
            f"SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 "
            f"FROM {t} GROUP BY id6", "groupby_q5")

    def run_groupby_q6(self) -> BenchmarkResult:
        """Q6: median(v3), sd(v3) by id4, id5 — canonical H2O."""
        t = self._get_table()
        return self._time(
            f"SELECT id4, id5, median(v3) AS v3_median, "
            f"stddev(v3) AS v3_std FROM {t} GROUP BY id4, id5",
            "groupby_q6")

    def run_groupby_q7(self) -> BenchmarkResult:
        """Q7: max(v1) - min(v2) by id3 — canonical H2O."""
        t = self._get_table()
        return self._time(
            f"SELECT id3, max(v1) - min(v2) AS range_v1_v2 "
            f"FROM {t} GROUP BY id3", "groupby_q7")

    def run_groupby_q8(self) -> BenchmarkResult:
        """Q8: largest two v3 by id6 — canonical H2O."""
        t = self._get_table()
        return self._time(
            f"SELECT id6, v3 AS largest2_v3 FROM ("
            f"  SELECT id6, v3, ROW_NUMBER() OVER "
            f"  (PARTITION BY id6 ORDER BY v3 DESC) AS rn "
            f"  FROM {t} WHERE v3 IS NOT NULL"
            f") sub WHERE rn <= 2", "groupby_q8")

    def run_groupby_q9(self) -> BenchmarkResult:
        """Q9: corr(v1, v2)^2 by id2, id4 — canonical H2O."""
        t = self._get_table()
        return self._time(
            f"SELECT id2, id4, POWER(corr(v1, v2), 2) AS r2 "
            f"FROM {t} GROUP BY id2, id4", "groupby_q9")

    def run_groupby_q10(self) -> BenchmarkResult:
        """Q10: sum(v3), count(v1) by id1..id6 — canonical H2O."""
        t = self._get_table()
        return self._time(
            f"SELECT id1, id2, id3, id4, id5, id6, "
            f"sum(v3) AS v3, count(v1) AS cnt FROM {t} "
            f"GROUP BY id1, id2, id3, id4, id5, id6", "groupby_q10")

    def _register_right(self, right_path: Path) -> str:
        ctx = self._ensure_ctx()
        try:
            ctx.deregister_table("bench_right_tmp")
        except Exception:
            pass
        ctx.register_csv("bench_right_tmp", str(right_path))
        return "bench_right_tmp"

    # Canonical H2O J1 — 5 single-key joins.

    def _canon_join(self, right_name: str, key: str, kind: str,
                    op_name: str) -> BenchmarkResult:
        x = self._get_table("x")
        r = self._get_table(right_name)
        return self._time(
            f"SELECT * FROM {x} {kind} JOIN {r} USING ({key})", op_name)

    def run_join_q1(self) -> BenchmarkResult:
        return self._canon_join("small", "id1", "INNER", "join_q1")

    def run_join_q2(self) -> BenchmarkResult:
        return self._canon_join("medium", "id2", "INNER", "join_q2")

    def run_join_q3(self) -> BenchmarkResult:
        return self._canon_join("medium", "id2", "LEFT", "join_q3")

    def run_join_q4(self) -> BenchmarkResult:
        return self._canon_join("medium", "id5", "INNER", "join_q4")

    def run_join_q5(self) -> BenchmarkResult:
        return self._canon_join("big", "id3", "INNER", "join_q5")

    def run_join_inner(self, right_path: Path) -> BenchmarkResult:
        """Inner join on (id1, id2, id3) — canonical H2O J1."""
        left = self._get_table("left")
        right = self._register_right(right_path)
        return self._time(
            f"SELECT * FROM {left} INNER JOIN {right} USING (id1, id2, id3)",
            "join_inner",
        )

    def run_join_left(self, right_path: Path) -> BenchmarkResult:
        """Left join on (id1, id2, id3) — canonical H2O J1."""
        left = self._get_table("left")
        right = self._register_right(right_path)
        return self._time(
            f"SELECT * FROM {left} LEFT JOIN {right} USING (id1, id2, id3)",
            "join_left",
        )

    def run_sort_single(self) -> BenchmarkResult:
        t = self._get_table()
        return self._time(f"SELECT * FROM {t} ORDER BY id1", "sort_single")

    def run_sort_multi(self) -> BenchmarkResult:
        t = self._get_table()
        return self._time(
            f"SELECT * FROM {t} ORDER BY id1, id2, id3",
            "sort_multi",
        )

    _DF_TYPES = {
        "u8": "TINYINT UNSIGNED", "i16": "SMALLINT", "i32": "INT",
        "i64": "BIGINT", "f64": "DOUBLE",
        "str8": "VARCHAR", "str16": "VARCHAR",
    }

    def run_sort_typed_full(self, csv_path: Path, dtype: str,
                             n_warmup: int, n_iter: int) -> list[BenchmarkResult]:
        cast = self._DF_TYPES[dtype]
        ctx = self._ensure_ctx()
        try:
            ctx.deregister_table("sort_data")
        except Exception:
            pass
        ctx.register_csv("sort_data_raw", str(csv_path))
        # Materialize as a typed in-memory view via SQL CAST; DataFusion's
        # planner will fold the cast into the scan.
        sql = f"SELECT CAST(v AS {cast}) AS v FROM sort_data_raw ORDER BY v"
        rows_batches = ctx.sql("SELECT count(*) FROM sort_data_raw").collect()
        rows = rows_batches[0].column(0)[0].as_py() if rows_batches else 0

        # Engine-only timing: route into a temp table so DF fully runs
        # the sort without materialising RecordBatches in Python.
        create_sql = f"CREATE TABLE _bench_out AS {sql}"
        drop_sql = "DROP TABLE IF EXISTS _bench_out"
        for _ in range(n_warmup):
            ctx.sql(drop_sql).collect()
            ctx.sql(create_sql).collect()
        ctx.sql(drop_sql).collect()

        results = []
        for _ in range(n_iter):
            _, t = self._time_it(lambda: ctx.sql(create_sql).collect())
            ctx.sql(drop_sql).collect()
            results.append(BenchmarkResult(f"sort_{dtype}", t, rows))

        try:
            ctx.deregister_table("sort_data_raw")
        except Exception:
            pass
        return results

    def materialize(self, op: str, right_path: Path | None = None) -> pl.DataFrame:
        t = self._get_table() if not op.startswith("join_") else None
        sql_map = {
            "groupby_q1": f"SELECT id1, sum(v1) AS v1 FROM {t} GROUP BY id1",
            "groupby_q2": f"SELECT id1, id2, sum(v1) AS v1 FROM {t} GROUP BY id1, id2",
            "groupby_q3": f"SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM {t} GROUP BY id3",
            "groupby_q4": f"SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM {t} GROUP BY id4",
            "groupby_q5": f"SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM {t} GROUP BY id6",
            "groupby_q6": (
                f"SELECT id4, id5, median(v3) AS v3_median, "
                f"stddev(v3) AS v3_std FROM {t} GROUP BY id4, id5"
            ),
            "groupby_q7": f"SELECT id3, max(v1) - min(v2) AS range_v1_v2 FROM {t} GROUP BY id3",
            "groupby_q8": (
                f"SELECT id6, v3 AS largest2_v3 FROM ("
                f"  SELECT id6, v3, ROW_NUMBER() OVER "
                f"  (PARTITION BY id6 ORDER BY v3 DESC) AS rn "
                f"  FROM {t} WHERE v3 IS NOT NULL"
                f") sub WHERE rn <= 2"
            ),
            "groupby_q9": (
                f"SELECT id2, id4, POWER(corr(v1, v2), 2) AS r2 "
                f"FROM {t} GROUP BY id2, id4"
            ),
            "groupby_q10": (
                f"SELECT id1, id2, id3, id4, id5, id6, "
                f"sum(v3) AS v3, count(v1) AS cnt FROM {t} "
                f"GROUP BY id1, id2, id3, id4, id5, id6"
            ),
            "sort_single": f"SELECT * FROM {t} ORDER BY id1",
            "sort_multi":  f"SELECT * FROM {t} ORDER BY id1, id2, id3",
        }
        if op in sql_map:
            sql = sql_map[op]
        elif op.startswith("join_q") and op[len("join_q"):].isdigit():
            x = self._get_table("x")
            joins = {
                "join_q1": (self._get_table("small"),  "INNER", "id1"),
                "join_q2": (self._get_table("medium"), "INNER", "id2"),
                "join_q3": (self._get_table("medium"), "LEFT",  "id2"),
                "join_q4": (self._get_table("medium"), "INNER", "id5"),
                "join_q5": (self._get_table("big"),    "INNER", "id3"),
            }
            r, kind, key = joins[op]
            sql = f"SELECT * FROM {x} {kind} JOIN {r} USING ({key})"
        elif op in ("join_inner", "join_left"):
            left = self._get_table("left")
            right = self._register_right(right_path)
            kind = "INNER" if op == "join_inner" else "LEFT"
            # Canonical projection — see duckdb_adapter.materialize for rationale.
            sql = (
                f"SELECT {left}.id1, {left}.id2, {left}.id3, "
                f"{left}.id4, {left}.id5, {left}.id6, {left}.v1, "
                f"{right}.v2 "
                f"FROM {left} {kind} JOIN {right} "
                f"ON {left}.id1 = {right}.id1 "
                f"AND {left}.id2 = {right}.id2 "
                f"AND {left}.id3 = {right}.id3"
            )
        else:
            raise ValueError(f"unknown op: {op}")
        # Collect Arrow batches → polars via pyarrow (zero-copy where possible).
        # For empty results, build an empty table with the right schema —
        # otherwise we lose the column list and `make check` reports a
        # schema mismatch (e.g. join with no overlapping keys).
        import pyarrow as pa
        df = self._ctx.sql(sql)
        schema = df.schema()
        batches = df.collect()
        if not batches:
            return pl.from_arrow(pa.Table.from_batches([], schema=schema))
        tbl = pa.Table.from_batches(batches)
        # USING(...) keeps duplicate non-key cols; rename to dedupe so
        # polars from_arrow doesn't reject the schema.
        new_names = _dedupe_col_names(tbl.column_names)
        if new_names != tbl.column_names:
            tbl = tbl.rename_columns(new_names)
        return pl.from_arrow(tbl)

    def close(self) -> None:
        if self._ctx is not None:
            for name in list(self._table_names.values()):
                try:
                    self._ctx.deregister_table(name)
                except Exception:
                    pass
            self._ctx = None
        self._table_names.clear()
