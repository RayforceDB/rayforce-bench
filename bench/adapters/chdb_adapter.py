"""chDB adapter — embedded ClickHouse via the chdb Python package.

chdb runs the full ClickHouse engine in-process (no server, no Docker).
We use the session API so CREATE TABLE state persists across queries
within one adapter instance.
"""

from pathlib import Path

from chdb import session
import polars as pl

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

    def _time(self, sql: str, name: str) -> BenchmarkResult:
        # ArrowStream is chdb's native binary materialization — much
        # cheaper than CSV string serialization. Stays inside the timer
        # so we measure engine + native materialization, on the same
        # footing as duckdb's .arrow() and polars's native pl.DataFrame.
        result, time_ns = self._time_it(
            lambda: self._sess.query(sql, "ArrowStream")
        )
        rows = self._count_rows_from_arrow_stream(result)
        return BenchmarkResult(name, time_ns, rows)

    @staticmethod
    def _count_rows_from_arrow_stream(res) -> int:
        if res is None:
            return 0
        raw = res.bytes() if hasattr(res, "bytes") else bytes(res)
        if not raw:
            return 0
        import io
        import pyarrow.ipc as ipc
        return ipc.open_stream(io.BytesIO(raw)).read_all().num_rows

    def run_groupby_q1(self) -> BenchmarkResult:
        """Q1: sum(v1) by id1 — canonical H2O."""
        t = self._get_table()
        return self._time(
            f"SELECT id1, sum(v1) AS v1 FROM {t} GROUP BY id1",
            "groupby_q1")

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
            f"FROM {t} GROUP BY id4",
            "groupby_q4")

    def run_groupby_q5(self) -> BenchmarkResult:
        """Q5: sum(v1), sum(v2), sum(v3) by id6 — canonical H2O."""
        t = self._get_table()
        return self._time(
            f"SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 "
            f"FROM {t} GROUP BY id6",
            "groupby_q5")

    def run_groupby_q6(self) -> BenchmarkResult:
        """Q6: median(v3), sd(v3) by id4, id5 — canonical H2O.

        ClickHouse: `median(x)` (alias of `quantile(0.5)`),
        `stddevSamp(x)` (sample std, ddof=1, matching polars).
        """
        t = self._get_table()
        return self._time(
            f"SELECT id4, id5, median(v3) AS v3_median, "
            f"stddevSamp(v3) AS v3_std FROM {t} GROUP BY id4, id5",
            "groupby_q6")

    def run_groupby_q7(self) -> BenchmarkResult:
        """Q7: max(v1) - min(v2) by id3 — canonical H2O."""
        t = self._get_table()
        return self._time(
            f"SELECT id3, max(v1) - min(v2) AS range_v1_v2 "
            f"FROM {t} GROUP BY id3",
            "groupby_q7")

    def run_groupby_q8(self) -> BenchmarkResult:
        """Q8: largest two v3 by id6 — canonical H2O."""
        t = self._get_table()
        return self._time(
            f"SELECT id6, v3 AS largest2_v3 FROM ("
            f"  SELECT id6, v3, "
            f"  row_number() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS rn "
            f"  FROM {t} WHERE v3 IS NOT NULL"
            f") sub WHERE rn <= 2",
            "groupby_q8")

    def run_groupby_q9(self) -> BenchmarkResult:
        """Q9: corr(v1, v2)^2 by id2, id4 — canonical H2O.

        ClickHouse uses `corr(x, y)` (population corr by default — same
        as polars's pl.corr); pow(corr, 2) gives r².
        """
        t = self._get_table()
        return self._time(
            f"SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 "
            f"FROM {t} GROUP BY id2, id4",
            "groupby_q9")

    def run_groupby_q10(self) -> BenchmarkResult:
        """Q10: sum(v3), count(v1) by id1..id6 — canonical H2O."""
        t = self._get_table()
        return self._time(
            f"SELECT id1, id2, id3, id4, id5, id6, "
            f"sum(v3) AS v3, count(v1) AS cnt "
            f"FROM {t} GROUP BY id1, id2, id3, id4, id5, id6",
            "groupby_q10")

    def _load_right(self, right_path: Path) -> str:
        sess = self._ensure_session()
        sess.query("DROP TABLE IF EXISTS bench_right_tmp")
        sess.query(
            f"CREATE TABLE bench_right_tmp ENGINE=MergeTree ORDER BY tuple() AS "
            f"SELECT * FROM file('{right_path}', 'CSVWithNames')"
        )
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
        right = self._load_right(right_path)
        return self._time(
            f"SELECT * FROM {left} INNER JOIN {right} USING (id1, id2, id3)",
            "join_inner",
        )

    def run_join_left(self, right_path: Path) -> BenchmarkResult:
        """Left join on (id1, id2, id3) — canonical H2O J1."""
        left = self._get_table("left")
        right = self._load_right(right_path)
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
                f"stddevSamp(v3) AS v3_std FROM {t} GROUP BY id4, id5"
            ),
            "groupby_q7": (
                f"SELECT id3, max(v1) - min(v2) AS range_v1_v2 "
                f"FROM {t} GROUP BY id3"
            ),
            "groupby_q8": (
                f"SELECT id6, v3 AS largest2_v3 FROM ("
                f"  SELECT id6, v3, row_number() OVER "
                f"  (PARTITION BY id6 ORDER BY v3 DESC) AS rn "
                f"  FROM {t} WHERE v3 IS NOT NULL"
                f") sub WHERE rn <= 2"
            ),
            "groupby_q9": (
                f"SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 "
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
            right = self._load_right(right_path)
            kind = "INNER" if op == "join_inner" else "LEFT"
            # Canonical projection — see duckdb_adapter.materialize for rationale.
            sql = (
                f"SELECT {left}.id1 AS id1, {left}.id2 AS id2, {left}.id3 AS id3, "
                f"{left}.id4 AS id4, {left}.id5 AS id5, {left}.id6 AS id6, "
                f"{left}.v1 AS v1, {right}.v2 AS v2 "
                f"FROM {left} {kind} JOIN {right} "
                f"ON {left}.id1 = {right}.id1 "
                f"AND {left}.id2 = {right}.id2 "
                f"AND {left}.id3 = {right}.id3"
            )
        else:
            raise ValueError(f"unknown op: {op}")
        # chdb returns Arrow when format='ArrowStream'. The query_result
        # object exposes .bytes() to get the raw IPC stream.
        import io
        import pyarrow.ipc as ipc
        res = self._sess.query(sql, "ArrowStream")
        if res is None:
            return pl.DataFrame()
        raw = res.bytes()
        if not raw:
            return pl.DataFrame()
        reader = ipc.open_stream(io.BytesIO(raw))
        return pl.from_arrow(reader.read_all())

    def close(self) -> None:
        if self._sess is not None:
            try:
                self._sess.close()
            except Exception:
                pass
            self._sess = None
        self._table_names.clear()
