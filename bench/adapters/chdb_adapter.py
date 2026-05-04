"""chDB adapter — embedded ClickHouse via the chdb Python package.

chdb runs the full ClickHouse engine in-process (no server, no Docker).
We use the session API so CREATE TABLE state persists across queries
within one adapter instance.
"""

from pathlib import Path

from chdb import session

from .base import Adapter, BenchmarkResult


class ChdbAdapter(Adapter):
    """Benchmark adapter for chDB (embedded ClickHouse)."""

    name = "chdb"

    def __init__(self):
        import chdb
        self.version = chdb.__version__
        self._sess: session.Session | None = None
        self._table_names: dict[str, str] = {}

    def _ensure_session(self) -> session.Session:
        if self._sess is None:
            self._sess = session.Session()
        return self._sess

    def load_data(self, path: Path, table_name: str = "data") -> None:
        sess = self._ensure_session()
        ch_table = f"bench_{table_name}"
        sess.query(f"DROP TABLE IF EXISTS {ch_table}")
        # MergeTree needs an ORDER BY; use tuple() to skip key requirement.
        sess.query(
            f"CREATE TABLE {ch_table} ENGINE=MergeTree ORDER BY tuple() AS "
            f"SELECT * FROM file('{path}', 'CSVWithNames')"
        )
        self._table_names[table_name] = ch_table

    def _get_table(self, name: str = "data") -> str:
        if name not in self._table_names:
            raise ValueError(f"Table '{name}' not loaded")
        return self._table_names[name]

    def _query_rows(self, sql: str) -> int:
        """Run a query and return its row count without measuring."""
        res = self._sess.query(sql, "CSV")
        if res is None:
            return 0
        # chdb's CSV output: count newlines. Empty result has empty body.
        text = str(res).strip()
        return text.count("\n") + 1 if text else 0

    def _time(self, sql: str, name: str) -> BenchmarkResult:
        result, time_ns = self._time_it(lambda: self._sess.query(sql, "CSV"))
        text = str(result).strip() if result is not None else ""
        rows = (text.count("\n") + 1) if text else 0
        return BenchmarkResult(name, time_ns, rows)

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
            f"SELECT id3, max(v1) - min(v2) FROM {t} GROUP BY id3",
            "groupby_q6",
        )

    def run_groupby_q7(self) -> BenchmarkResult:
        t = self._get_table()
        return self._time(
            f"SELECT id1, id2, id3, id4, id5, id6, sum(v3), count(v1) "
            f"FROM {t} GROUP BY id1, id2, id3, id4, id5, id6",
            "groupby_q7",
        )

    def _load_right(self, right_path: Path) -> str:
        sess = self._ensure_session()
        sess.query("DROP TABLE IF EXISTS bench_right_tmp")
        sess.query(
            f"CREATE TABLE bench_right_tmp ENGINE=MergeTree ORDER BY tuple() AS "
            f"SELECT * FROM file('{right_path}', 'CSVWithNames')"
        )
        return "bench_right_tmp"

    def run_join_inner(self, right_path: Path) -> BenchmarkResult:
        left = self._get_table("left")
        right = self._load_right(right_path)
        return self._time(
            f"SELECT * FROM {left} INNER JOIN {right} USING (id1)",
            "join_inner",
        )

    def run_join_left(self, right_path: Path) -> BenchmarkResult:
        left = self._get_table("left")
        right = self._load_right(right_path)
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

    _CHDB_TYPES = {
        "u8": "UInt8", "i16": "Int16", "i32": "Int32",
        "i64": "Int64", "f64": "Float64",
        "str8": "String", "str16": "String",
    }

    def run_sort_typed_full(self, csv_path: Path, dtype: str,
                             n_warmup: int, n_iter: int) -> list[BenchmarkResult]:
        cast = self._CHDB_TYPES[dtype]
        sess = self._ensure_session()
        sess.query("DROP TABLE IF EXISTS sort_data")
        sess.query(
            f"CREATE TABLE sort_data (v {cast}) ENGINE=MergeTree ORDER BY tuple() AS "
            f"SELECT CAST(v AS {cast}) AS v FROM file('{csv_path}', 'CSVWithNames')"
        )
        rows_text = str(sess.query("SELECT count() FROM sort_data", "CSV")).strip()
        rows = int(rows_text) if rows_text else 0
        sql = "SELECT * FROM sort_data ORDER BY v"

        for _ in range(n_warmup):
            sess.query(sql, "CSV")

        results = []
        for _ in range(n_iter):
            _, time_ns = self._time_it(lambda: sess.query(sql, "CSV"))
            results.append(BenchmarkResult(f"sort_{dtype}", time_ns, rows))

        sess.query("DROP TABLE IF EXISTS sort_data")
        return results

    def close(self) -> None:
        if self._sess is not None:
            try:
                self._sess.close()
            except Exception:
                pass
            self._sess = None
        self._table_names.clear()
