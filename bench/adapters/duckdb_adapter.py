"""DuckDB adapter for benchmarks."""

from pathlib import Path

import duckdb
import polars as pl

from .base import Adapter, BenchmarkResult


class DuckDBAdapter(Adapter):
    """Benchmark adapter for DuckDB."""

    name = "duckdb"

    def __init__(self):
        self.version = duckdb.__version__
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._table_names: dict[str, str] = {}

    def load_data(self, path: Path, table_name: str = "data") -> None:
        """Load CSV data into DuckDB in-memory table."""
        if self._conn is None:
            self._conn = duckdb.connect()

        # Create table from CSV file and materialize in memory
        sql_table_name = f"bench_{table_name}"
        self._conn.execute(f"CREATE TABLE {sql_table_name} AS SELECT * FROM read_csv('{path}')")
        self._table_names[table_name] = sql_table_name

    def _get_table(self, name: str = "data") -> str:
        """Get SQL table name."""
        if name not in self._table_names:
            raise ValueError(f"Table '{name}' not loaded")
        return self._table_names[name]

    def _time_sql(self, sql: str, name: str) -> BenchmarkResult:
        result, time_ns = self._time_it(
            lambda: self._conn.execute(sql).fetch_arrow_table())
        return BenchmarkResult(name, time_ns, len(result))

    def run_groupby_q1(self) -> BenchmarkResult:
        """Q1: sum(v1) by id1 — canonical H2O."""
        t = self._get_table()
        return self._time_sql(
            f"SELECT id1, SUM(v1) AS v1 FROM {t} GROUP BY id1", "groupby_q1")

    def run_groupby_q2(self) -> BenchmarkResult:
        """Q2: sum(v1) by id1, id2 — canonical H2O."""
        t = self._get_table()
        return self._time_sql(
            f"SELECT id1, id2, SUM(v1) AS v1 FROM {t} GROUP BY id1, id2",
            "groupby_q2")

    def run_groupby_q3(self) -> BenchmarkResult:
        """Q3: sum(v1), mean(v3) by id3 — canonical H2O."""
        t = self._get_table()
        return self._time_sql(
            f"SELECT id3, SUM(v1) AS v1, AVG(v3) AS v3 FROM {t} GROUP BY id3",
            "groupby_q3")

    def run_groupby_q4(self) -> BenchmarkResult:
        """Q4: mean(v1), mean(v2), mean(v3) by id4 — canonical H2O."""
        t = self._get_table()
        return self._time_sql(
            f"SELECT id4, AVG(v1) AS v1, AVG(v2) AS v2, AVG(v3) AS v3 "
            f"FROM {t} GROUP BY id4", "groupby_q4")

    def run_groupby_q5(self) -> BenchmarkResult:
        """Q5: sum(v1), sum(v2), sum(v3) by id6 — canonical H2O."""
        t = self._get_table()
        return self._time_sql(
            f"SELECT id6, SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 "
            f"FROM {t} GROUP BY id6", "groupby_q5")

    def run_groupby_q6(self) -> BenchmarkResult:
        """Q6: median(v3), sd(v3) by id4, id5 — canonical H2O."""
        t = self._get_table()
        return self._time_sql(
            f"SELECT id4, id5, MEDIAN(v3) AS v3_median, "
            f"STDDEV(v3) AS v3_std FROM {t} GROUP BY id4, id5",
            "groupby_q6")

    def run_groupby_q7(self) -> BenchmarkResult:
        """Q7: max(v1) - min(v2) by id3 — canonical H2O."""
        t = self._get_table()
        return self._time_sql(
            f"SELECT id3, MAX(v1) - MIN(v2) AS range_v1_v2 "
            f"FROM {t} GROUP BY id3", "groupby_q7")

    def run_groupby_q8(self) -> BenchmarkResult:
        """Q8: largest two v3 by id6 — canonical H2O."""
        t = self._get_table()
        return self._time_sql(
            f"SELECT id6, v3 AS largest2_v3 FROM ("
            f"  SELECT id6, v3, "
            f"  ROW_NUMBER() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS rn"
            f"  FROM {t} WHERE v3 IS NOT NULL"
            f") sub WHERE rn <= 2", "groupby_q8")

    def run_groupby_q9(self) -> BenchmarkResult:
        """Q9: corr(v1, v2)^2 by id2, id4 — canonical H2O."""
        t = self._get_table()
        return self._time_sql(
            f"SELECT id2, id4, POWER(CORR(v1, v2), 2) AS r2 "
            f"FROM {t} GROUP BY id2, id4", "groupby_q9")

    def run_groupby_q10(self) -> BenchmarkResult:
        """Q10: sum(v3), count(v1) by id1..id6 — canonical H2O."""
        t = self._get_table()
        return self._time_sql(
            f"SELECT id1, id2, id3, id4, id5, id6, "
            f"SUM(v3) AS v3, COUNT(v1) AS cnt FROM {t} "
            f"GROUP BY id1, id2, id3, id4, id5, id6", "groupby_q10")

    def run_join_inner(self, right_path: Path) -> BenchmarkResult:
        """Inner join on (id1, id2, id3) — canonical H2O J1."""
        left = self._get_table("left")
        self._conn.execute(f"CREATE TABLE bench_right_tmp AS SELECT * FROM read_csv('{right_path}')")

        def query():
            return self._conn.execute(
                f"SELECT * FROM {left} INNER JOIN bench_right_tmp "
                f"ON {left}.id1 = bench_right_tmp.id1 "
                f"AND {left}.id2 = bench_right_tmp.id2 "
                f"AND {left}.id3 = bench_right_tmp.id3"
            ).fetch_arrow_table()

        result, time_ns = self._time_it(query)
        self._conn.execute("DROP TABLE IF EXISTS bench_right_tmp")
        return BenchmarkResult("join_inner", time_ns, len(result))

    def run_join_left(self, right_path: Path) -> BenchmarkResult:
        """Left join on (id1, id2, id3) — canonical H2O J1."""
        left = self._get_table("left")
        self._conn.execute(f"CREATE TABLE bench_right_tmp AS SELECT * FROM read_csv('{right_path}')")

        def query():
            return self._conn.execute(
                f"SELECT * FROM {left} LEFT JOIN bench_right_tmp "
                f"ON {left}.id1 = bench_right_tmp.id1 "
                f"AND {left}.id2 = bench_right_tmp.id2 "
                f"AND {left}.id3 = bench_right_tmp.id3"
            ).fetch_arrow_table()

        result, time_ns = self._time_it(query)
        self._conn.execute("DROP TABLE IF EXISTS bench_right_tmp")
        return BenchmarkResult("join_left", time_ns, len(result))

    def run_sort_single(self) -> BenchmarkResult:
        """Sort by single column."""
        t = self._get_table()

        def query():
            return self._conn.execute(f"SELECT * FROM {t} ORDER BY id1").fetch_arrow_table()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("sort_single", time_ns, len(result))

    def run_sort_multi(self) -> BenchmarkResult:
        """Sort by multiple columns."""
        t = self._get_table()

        def query():
            return self._conn.execute(f"SELECT * FROM {t} ORDER BY id1, id2, id3").fetch_arrow_table()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("sort_multi", time_ns, len(result))

    _DUCKDB_TYPES = {
        "u8": "UTINYINT", "i16": "SMALLINT", "i32": "INTEGER",
        "i64": "BIGINT", "f64": "DOUBLE",
        "str8": "VARCHAR", "str16": "VARCHAR",
    }

    def run_sort_typed_full(self, csv_path: Path, dtype: str,
                             n_warmup: int, n_iter: int) -> list[BenchmarkResult]:
        """Sort a single typed column for the extended sort grid."""
        cast = self._DUCKDB_TYPES[dtype]
        if self._conn is None:
            self._conn = duckdb.connect()
        self._conn.execute("DROP TABLE IF EXISTS sort_data")
        self._conn.execute(
            f"CREATE TABLE sort_data AS "
            f"SELECT CAST(v AS {cast}) AS v FROM read_csv_auto('{csv_path}')"
        )
        rows = self._conn.execute("SELECT COUNT(*) FROM sort_data").fetchone()[0]
        sql = "CREATE OR REPLACE TABLE sort_result AS SELECT * FROM sort_data ORDER BY v"

        for _ in range(n_warmup):
            self._conn.execute(sql)

        results = []
        for _ in range(n_iter):
            self._conn.execute("DROP TABLE IF EXISTS sort_result")
            _, time_ns = self._time_it(lambda: self._conn.execute(sql))
            results.append(BenchmarkResult(f"sort_{dtype}", time_ns, rows))

        self._conn.execute("DROP TABLE IF EXISTS sort_result")
        self._conn.execute("DROP TABLE IF EXISTS sort_data")
        return results

    def materialize(self, op: str, right_path: Path | None = None) -> pl.DataFrame:
        # 'data' table is only loaded for non-join ops; joins use 'left'.
        # Defer _get_table() until we know which side we need.
        t = self._get_table() if not op.startswith("join_") else None
        sql_map = {
            "groupby_q1": f"SELECT id1, SUM(v1) AS v1 FROM {t} GROUP BY id1",
            "groupby_q2": f"SELECT id1, id2, SUM(v1) AS v1 FROM {t} GROUP BY id1, id2",
            "groupby_q3": f"SELECT id3, SUM(v1) AS v1, AVG(v3) AS v3 FROM {t} GROUP BY id3",
            "groupby_q4": f"SELECT id4, AVG(v1) AS v1, AVG(v2) AS v2, AVG(v3) AS v3 FROM {t} GROUP BY id4",
            "groupby_q5": f"SELECT id6, SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM {t} GROUP BY id6",
            "groupby_q6": (
                f"SELECT id4, id5, MEDIAN(v3) AS v3_median, "
                f"STDDEV(v3) AS v3_std FROM {t} GROUP BY id4, id5"
            ),
            "groupby_q7": f"SELECT id3, MAX(v1) - MIN(v2) AS range_v1_v2 FROM {t} GROUP BY id3",
            "groupby_q8": (
                f"SELECT id6, v3 AS largest2_v3 FROM ("
                f"  SELECT id6, v3, ROW_NUMBER() OVER "
                f"  (PARTITION BY id6 ORDER BY v3 DESC) AS rn "
                f"  FROM {t} WHERE v3 IS NOT NULL"
                f") sub WHERE rn <= 2"
            ),
            "groupby_q9": (
                f"SELECT id2, id4, POWER(CORR(v1, v2), 2) AS r2 "
                f"FROM {t} GROUP BY id2, id4"
            ),
            "groupby_q10": (
                f"SELECT id1, id2, id3, id4, id5, id6, "
                f"SUM(v3) AS v3, COUNT(v1) AS cnt FROM {t} "
                f"GROUP BY id1, id2, id3, id4, id5, id6"
            ),
            "sort_single": f"SELECT * FROM {t} ORDER BY id1",
            "sort_multi":  f"SELECT * FROM {t} ORDER BY id1, id2, id3",
        }
        if op in sql_map:
            return self._conn.execute(sql_map[op]).pl()
        if op in ("join_inner", "join_left"):
            left = self._get_table("left")
            self._conn.execute("DROP TABLE IF EXISTS bench_right_tmp")
            self._conn.execute(
                f"CREATE TABLE bench_right_tmp AS SELECT * FROM read_csv('{right_path}')"
            )
            kind = "INNER" if op == "join_inner" else "LEFT"
            # Explicit canonical projection: keys + left.id4..id6 + left.v1 + right.v2.
            # Avoids engine-specific duplicate-column naming in cross-engine compare.
            sql = (
                f"SELECT {left}.id1, {left}.id2, {left}.id3, "
                f"{left}.id4, {left}.id5, {left}.id6, {left}.v1, "
                f"bench_right_tmp.v2 "
                f"FROM {left} {kind} JOIN bench_right_tmp "
                f"ON {left}.id1 = bench_right_tmp.id1 "
                f"AND {left}.id2 = bench_right_tmp.id2 "
                f"AND {left}.id3 = bench_right_tmp.id3"
            )
            try:
                return self._conn.execute(sql).pl()
            finally:
                self._conn.execute("DROP TABLE IF EXISTS bench_right_tmp")
        raise ValueError(f"unknown op: {op}")

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        self._table_names.clear()
