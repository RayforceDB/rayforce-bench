"""DuckDB adapter for benchmarks."""

from pathlib import Path

import duckdb

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

    def run_groupby_q1(self) -> BenchmarkResult:
        """Q1: sum(v1) group by id1"""
        t = self._get_table()

        def query():
            return self._conn.execute(f"SELECT id1, SUM(v1) as v1_sum FROM {t} GROUP BY id1").fetchdf()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q1", time_ns, len(result))

    def run_groupby_q2(self) -> BenchmarkResult:
        """Q2: sum(v1) group by id1, id2"""
        t = self._get_table()

        def query():
            return self._conn.execute(
                f"SELECT id1, id2, SUM(v1) as v1_sum FROM {t} GROUP BY id1, id2"
            ).fetchdf()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q2", time_ns, len(result))

    def run_groupby_q3(self) -> BenchmarkResult:
        """Q3: sum(v1), mean(v3) group by id3"""
        t = self._get_table()

        def query():
            return self._conn.execute(
                f"SELECT id3, SUM(v1) as v1_sum, AVG(v3) as v3_avg FROM {t} GROUP BY id3"
            ).fetchdf()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q3", time_ns, len(result))

    def run_groupby_q4(self) -> BenchmarkResult:
        """Q4: mean(v1), mean(v2), mean(v3) group by id3"""
        t = self._get_table()

        def query():
            return self._conn.execute(
                f"SELECT id3, AVG(v1) as v1_avg, AVG(v2) as v2_avg, AVG(v3) as v3_avg FROM {t} GROUP BY id3"
            ).fetchdf()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q4", time_ns, len(result))

    def run_groupby_q5(self) -> BenchmarkResult:
        """Q5: sum(v1), sum(v2), sum(v3) group by id3"""
        t = self._get_table()

        def query():
            return self._conn.execute(
                f"SELECT id3, SUM(v1) as v1_sum, SUM(v2) as v2_sum, SUM(v3) as v3_sum FROM {t} GROUP BY id3"
            ).fetchdf()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q5", time_ns, len(result))

    def run_groupby_q6(self) -> BenchmarkResult:
        """Q6: max(v1) - min(v2) group by id3"""
        t = self._get_table()

        def query():
            return self._conn.execute(
                f"SELECT id3, MAX(v1) - MIN(v2) as range FROM {t} GROUP BY id3"
            ).fetchdf()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q6", time_ns, len(result))

    def run_join_inner(self, right_path: Path) -> BenchmarkResult:
        """Inner join on id1."""
        left = self._get_table("left")
        # Load right table and materialize in memory before timing
        self._conn.execute(f"CREATE TABLE bench_right_tmp AS SELECT * FROM read_csv('{right_path}')")

        def query():
            return self._conn.execute(
                f"SELECT * FROM {left} INNER JOIN bench_right_tmp ON {left}.id1 = bench_right_tmp.id1"
            ).fetchdf()

        result, time_ns = self._time_it(query)
        self._conn.execute("DROP TABLE IF EXISTS bench_right_tmp")
        return BenchmarkResult("join_inner", time_ns, len(result))

    def run_join_left(self, right_path: Path) -> BenchmarkResult:
        """Left join on id1."""
        left = self._get_table("left")
        # Load right table and materialize in memory before timing
        self._conn.execute(f"CREATE TABLE bench_right_tmp AS SELECT * FROM read_csv('{right_path}')")

        def query():
            return self._conn.execute(
                f"SELECT * FROM {left} LEFT JOIN bench_right_tmp ON {left}.id1 = bench_right_tmp.id1"
            ).fetchdf()

        result, time_ns = self._time_it(query)
        self._conn.execute("DROP TABLE IF EXISTS bench_right_tmp")
        return BenchmarkResult("join_left", time_ns, len(result))

    def run_sort_single(self) -> BenchmarkResult:
        """Sort by single column."""
        t = self._get_table()

        def query():
            return self._conn.execute(f"SELECT * FROM {t} ORDER BY id1").fetchdf()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("sort_single", time_ns, len(result))

    def run_sort_multi(self) -> BenchmarkResult:
        """Sort by multiple columns."""
        t = self._get_table()

        def query():
            return self._conn.execute(f"SELECT * FROM {t} ORDER BY id1, id2, id3").fetchdf()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("sort_multi", time_ns, len(result))

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        self._table_names.clear()
