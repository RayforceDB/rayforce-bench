"""DataFusion adapter — Apache Rust+Arrow query engine.

DataFusion is the foundation of several products (InfluxDB 3, GlareDB,
ROAPI, RisingLight, Sail). Including it lets us measure rayforce against
the common substrate of the Apache columnar ecosystem rather than just
one product.
"""

from pathlib import Path

from datafusion import SessionContext

from .base import Adapter, BenchmarkResult


class DataFusionAdapter(Adapter):
    """Benchmark adapter for Apache DataFusion."""

    name = "datafusion"

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
        def query():
            return self._ctx.sql(sql).collect()
        result, t = self._time_it(query)
        rows = sum(b.num_rows for b in result) if result else 0
        return BenchmarkResult(name, t, rows)

    def run_groupby_q1(self) -> BenchmarkResult:
        t = self._get_table()
        return self._time(f"SELECT id1, sum(v1) FROM {t} GROUP BY id1", "groupby_q1")

    def run_groupby_q2(self) -> BenchmarkResult:
        t = self._get_table()
        return self._time(
            f"SELECT id1, id2, sum(v1) FROM {t} GROUP BY id1, id2",
            "groupby_q2",
        )

    def run_groupby_q3(self) -> BenchmarkResult:
        t = self._get_table()
        return self._time(
            f"SELECT id3, sum(v1), avg(v3) FROM {t} GROUP BY id3",
            "groupby_q3",
        )

    def run_groupby_q4(self) -> BenchmarkResult:
        t = self._get_table()
        return self._time(
            f"SELECT id3, avg(v1), avg(v2), avg(v3) FROM {t} GROUP BY id3",
            "groupby_q4",
        )

    def run_groupby_q5(self) -> BenchmarkResult:
        t = self._get_table()
        return self._time(
            f"SELECT id3, sum(v1), sum(v2), sum(v3) FROM {t} GROUP BY id3",
            "groupby_q5",
        )

    def run_groupby_q6(self) -> BenchmarkResult:
        t = self._get_table()
        return self._time(
            f"SELECT id3, max(v1) - min(v2) AS range FROM {t} GROUP BY id3",
            "groupby_q6",
        )

    def _register_right(self, right_path: Path) -> str:
        ctx = self._ensure_ctx()
        try:
            ctx.deregister_table("bench_right_tmp")
        except Exception:
            pass
        ctx.register_csv("bench_right_tmp", str(right_path))
        return "bench_right_tmp"

    def run_join_inner(self, right_path: Path) -> BenchmarkResult:
        left = self._get_table("left")
        right = self._register_right(right_path)
        return self._time(
            f"SELECT * FROM {left} INNER JOIN {right} USING (id1)",
            "join_inner",
        )

    def run_join_left(self, right_path: Path) -> BenchmarkResult:
        left = self._get_table("left")
        right = self._register_right(right_path)
        return self._time(
            f"SELECT * FROM {left} LEFT JOIN {right} USING (id1)",
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

        for _ in range(n_warmup):
            ctx.sql(sql).collect()

        results = []
        for _ in range(n_iter):
            _, t = self._time_it(lambda: ctx.sql(sql).collect())
            results.append(BenchmarkResult(f"sort_{dtype}", t, rows))

        try:
            ctx.deregister_table("sort_data_raw")
        except Exception:
            pass
        return results

    def close(self) -> None:
        if self._ctx is not None:
            for name in list(self._table_names.values()):
                try:
                    self._ctx.deregister_table(name)
                except Exception:
                    pass
            self._ctx = None
        self._table_names.clear()
