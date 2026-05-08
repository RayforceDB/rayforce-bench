"""TimescaleDB adapter for benchmarks.

Requires TimescaleDB/PostgreSQL running locally on default port (5432).
Install client: pip install psycopg[binary]
"""

from pathlib import Path

from .base import Adapter, BenchmarkResult


class TimescaleAdapter(Adapter):
    """Benchmark adapter for TimescaleDB.

    Uses psycopg to connect to TimescaleDB/PostgreSQL.
    Requires TimescaleDB to be running locally.
    """

    name = "timescale"

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5433,  # Use 5433 to avoid conflict with local postgres
        user: str = "postgres",
        password: str = "postgres",
        database: str = "benchmark",
    ):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._conn = None
        self._table_names: dict[str, str] = {}

        try:
            import psycopg
            self._psycopg = psycopg
            self.version = psycopg.__version__
        except ImportError as e:
            raise ImportError(
                "psycopg not installed. Install with: pip install psycopg[binary]"
            ) from e

        self._connect()

    def _connect(self) -> None:
        """Connect to TimescaleDB."""
        self._conn = self._psycopg.connect(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            dbname=self._database,
            autocommit=True,
        )

    def load_data(self, path: Path, table_name: str = "data") -> None:
        """Load CSV data into TimescaleDB."""
        import io
        import polars as pl

        df = pl.read_csv(path)
        sql_table_name = f"bench_{table_name}"

        # Drop table if exists
        with self._conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {sql_table_name}")

        # Create table
        columns = []
        for name, dtype in df.schema.items():
            if dtype == pl.Int64:
                columns.append(f"{name} BIGINT")
            elif dtype == pl.Float64:
                columns.append(f"{name} DOUBLE PRECISION")
            else:
                columns.append(f"{name} TEXT")

        create_sql = f"CREATE TABLE {sql_table_name} ({', '.join(columns)})"
        with self._conn.cursor() as cur:
            cur.execute(create_sql)

        # Use COPY with CSV buffer for fast bulk insert
        buffer = io.StringIO()
        df.write_csv(buffer, include_header=False)
        buffer.seek(0)
        with self._conn.cursor() as cur:
            with cur.copy(f"COPY {sql_table_name} FROM STDIN WITH CSV") as copy:
                copy.write(buffer.read())

        self._table_names[table_name] = sql_table_name

    def _get_table(self, name: str = "data") -> str:
        """Get SQL table name."""
        if name not in self._table_names:
            raise ValueError(f"Table '{name}' not loaded")
        return self._table_names[name]

    def _time_pg(self, sql: str, name: str) -> BenchmarkResult:
        """Time `cur.execute(sql)` only — engine compute on the server.

        The PG protocol completes the SELECT server-side before
        returning control; `cur.rowcount` is populated immediately and
        does not require fetching rows over the wire. Skipping fetchall
        keeps the timer focused on engine work, not Python-side
        materialization.
        """
        cur = self._conn.cursor()
        try:
            _, time_ns = self._time_it(lambda: cur.execute(sql))
            rows = cur.rowcount
        finally:
            cur.close()
        return BenchmarkResult(name, time_ns, rows)

    def run_groupby_q1(self) -> BenchmarkResult:
        """Q1: sum(v1) by id1 — canonical H2O."""
        t = self._get_table()
        return self._time_pg(
            f"SELECT id1, SUM(v1) AS v1 FROM {t} GROUP BY id1",
            "groupby_q1")

    def run_groupby_q2(self) -> BenchmarkResult:
        """Q2: sum(v1) by id1, id2 — canonical H2O."""
        t = self._get_table()
        return self._time_pg(
            f"SELECT id1, id2, SUM(v1) AS v1 FROM {t} GROUP BY id1, id2",
            "groupby_q2")

    def run_groupby_q3(self) -> BenchmarkResult:
        """Q3: sum(v1), mean(v3) by id3 — canonical H2O."""
        t = self._get_table()
        return self._time_pg(
            f"SELECT id3, SUM(v1) AS v1, AVG(v3) AS v3 FROM {t} GROUP BY id3",
            "groupby_q3")

    def run_groupby_q4(self) -> BenchmarkResult:
        """Q4: mean(v1), mean(v2), mean(v3) by id4 — canonical H2O."""
        t = self._get_table()
        return self._time_pg(
            f"SELECT id4, AVG(v1) AS v1, AVG(v2) AS v2, AVG(v3) AS v3 "
            f"FROM {t} GROUP BY id4", "groupby_q4")

    def run_groupby_q5(self) -> BenchmarkResult:
        """Q5: sum(v1), sum(v2), sum(v3) by id6 — canonical H2O."""
        t = self._get_table()
        return self._time_pg(
            f"SELECT id6, SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 "
            f"FROM {t} GROUP BY id6", "groupby_q5")

    def run_groupby_q6(self) -> BenchmarkResult:
        """Q6: median(v3), sd(v3) by id4, id5 — canonical H2O.

        PostgreSQL: median via PERCENTILE_CONT(0.5) WITHIN GROUP, std via
        STDDEV_SAMP (sample std, ddof=1, matching polars).
        """
        t = self._get_table()
        return self._time_pg(
            f"SELECT id4, id5, "
            f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v3) AS v3_median, "
            f"STDDEV_SAMP(v3) AS v3_std "
            f"FROM {t} GROUP BY id4, id5", "groupby_q6")

    def run_groupby_q7(self) -> BenchmarkResult:
        """Q7: max(v1) - min(v2) by id3 — canonical H2O."""
        t = self._get_table()
        return self._time_pg(
            f"SELECT id3, MAX(v1) - MIN(v2) AS range_v1_v2 "
            f"FROM {t} GROUP BY id3", "groupby_q7")

    def run_groupby_q8(self) -> BenchmarkResult:
        """Q8: largest two v3 by id6 — canonical H2O."""
        t = self._get_table()
        return self._time_pg(
            f"SELECT id6, v3 AS largest2_v3 FROM ("
            f"  SELECT id6, v3, "
            f"  ROW_NUMBER() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS rn "
            f"  FROM {t} WHERE v3 IS NOT NULL"
            f") sub WHERE rn <= 2", "groupby_q8")

    def run_groupby_q9(self) -> BenchmarkResult:
        """Q9: corr(v1, v2)^2 by id2, id4 — canonical H2O."""
        t = self._get_table()
        return self._time_pg(
            f"SELECT id2, id4, POWER(CORR(v1, v2), 2) AS r2 "
            f"FROM {t} GROUP BY id2, id4", "groupby_q9")

    def run_groupby_q10(self) -> BenchmarkResult:
        """Q10: sum(v3), count(v1) by id1..id6 — canonical H2O."""
        t = self._get_table()
        return self._time_pg(
            f"SELECT id1, id2, id3, id4, id5, id6, "
            f"SUM(v3) AS v3, COUNT(v1) AS cnt "
            f"FROM {t} GROUP BY id1, id2, id3, id4, id5, id6",
            "groupby_q10")

    def _load_right(self, right_path: Path) -> str:
        """Bulk-COPY the right table; return its SQL name."""
        import io
        import polars as pl

        df = pl.read_csv(right_path)
        right_table = "bench_right_tmp"

        with self._conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {right_table}")

        columns = []
        for name, dtype in df.schema.items():
            if dtype == pl.Int64:
                columns.append(f"{name} BIGINT")
            elif dtype == pl.Float64:
                columns.append(f"{name} DOUBLE PRECISION")
            else:
                columns.append(f"{name} TEXT")

        buffer = io.StringIO()
        df.write_csv(buffer, include_header=False)
        buffer.seek(0)
        with self._conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {right_table} ({', '.join(columns)})")
            with cur.copy(f"COPY {right_table} FROM STDIN WITH CSV") as copy:
                copy.write(buffer.read())
        return right_table

    def run_join_inner(self, right_path: Path) -> BenchmarkResult:
        """Inner join on (id1, id2, id3) — canonical H2O J1."""
        left = self._get_table("left")
        right = self._load_right(right_path)
        sql = (
            f"SELECT * FROM {left} INNER JOIN {right} "
            f"ON {left}.id1 = {right}.id1 "
            f"AND {left}.id2 = {right}.id2 "
            f"AND {left}.id3 = {right}.id3"
        )
        result = self._time_pg(sql, "join_inner")
        with self._conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {right}")
        return result

    def run_join_left(self, right_path: Path) -> BenchmarkResult:
        """Left join on (id1, id2, id3) — canonical H2O J1."""
        left = self._get_table("left")
        right = self._load_right(right_path)
        sql = (
            f"SELECT * FROM {left} LEFT JOIN {right} "
            f"ON {left}.id1 = {right}.id1 "
            f"AND {left}.id2 = {right}.id2 "
            f"AND {left}.id3 = {right}.id3"
        )
        result = self._time_pg(sql, "join_left")
        with self._conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {right}")
        return result

    def run_sort_single(self) -> BenchmarkResult:
        """Sort by single column."""
        t = self._get_table()
        return self._time_pg(f"SELECT * FROM {t} ORDER BY id1", "sort_single")

    def run_sort_multi(self) -> BenchmarkResult:
        """Sort by multiple columns."""
        t = self._get_table()
        return self._time_pg(
            f"SELECT * FROM {t} ORDER BY id1, id2, id3", "sort_multi",
        )

    # PostgreSQL has no UINT8; SMALLINT covers 0..255 safely.
    _TIMESCALE_TYPES = {
        "u8": "SMALLINT", "i16": "SMALLINT", "i32": "INTEGER",
        "i64": "BIGINT", "f64": "DOUBLE PRECISION",
        "str8": "TEXT", "str16": "TEXT",
    }

    def run_sort_typed_full(self, csv_path: Path, dtype: str,
                             n_warmup: int, n_iter: int) -> list[BenchmarkResult]:
        """Sort a single typed column for the extended sort grid."""
        import io
        import polars as pl

        col_type = self._TIMESCALE_TYPES[dtype]
        sort_table = "bench_sort_tmp"

        with self._conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {sort_table}")
            cur.execute(f"CREATE TABLE {sort_table} (v {col_type})")

        # Stream CSV body (sans header) into the typed table via COPY.
        df = pl.read_csv(csv_path)
        buffer = io.StringIO()
        df.write_csv(buffer, include_header=False)
        buffer.seek(0)
        with self._conn.cursor() as cur:
            with cur.copy(f"COPY {sort_table} FROM STDIN WITH CSV") as copy:
                copy.write(buffer.read())

        sql = f"SELECT * FROM {sort_table} ORDER BY v"

        for _ in range(n_warmup):
            with self._conn.cursor() as cur:
                cur.execute(sql)
                cur.fetchall()

        results = []
        for _ in range(n_iter):
            def query():
                with self._conn.cursor() as cur:
                    cur.execute(sql)
                    return cur.fetchall()
            r, time_ns = self._time_it(query)
            results.append(BenchmarkResult(f"sort_{dtype}", time_ns, len(r)))

        with self._conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {sort_table}")
        return results

    def materialize(self, op: str, right_path: Path | None = None):
        import polars as pl
        t = self._get_table() if not op.startswith("join_") else None
        sql_map = {
            "groupby_q1": f"SELECT id1, SUM(v1) AS v1 FROM {t} GROUP BY id1",
            "groupby_q2": f"SELECT id1, id2, SUM(v1) AS v1 FROM {t} GROUP BY id1, id2",
            "groupby_q3": f"SELECT id3, SUM(v1) AS v1, AVG(v3) AS v3 FROM {t} GROUP BY id3",
            "groupby_q4": f"SELECT id4, AVG(v1) AS v1, AVG(v2) AS v2, AVG(v3) AS v3 FROM {t} GROUP BY id4",
            "groupby_q5": f"SELECT id6, SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM {t} GROUP BY id6",
            "groupby_q6": (
                f"SELECT id4, id5, "
                f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v3) AS v3_median, "
                f"STDDEV_SAMP(v3) AS v3_std "
                f"FROM {t} GROUP BY id4, id5"
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
            sql = sql_map[op]
            cleanup = None
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
            cleanup = right
        else:
            raise ValueError(f"unknown op: {op}")
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql)
                cols = [d.name for d in cur.description]
                rows = cur.fetchall()
            return pl.DataFrame(rows, schema=cols, orient="row")
        finally:
            if cleanup:
                with self._conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {cleanup}")

    def close(self) -> None:
        if self._conn is not None:
            # Drop benchmark tables
            for table_name in self._table_names.values():
                try:
                    with self._conn.cursor() as cur:
                        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                except Exception:
                    pass
            self._conn.close()
            self._conn = None
        self._table_names.clear()
