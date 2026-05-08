"""QuestDB adapter for benchmarks.

Requires QuestDB running locally on default port (8812 for PostgreSQL wire protocol).
Install client: pip install psycopg[binary]
"""

from pathlib import Path

from .base import Adapter, BenchmarkResult


class QuestDBAdapter(Adapter):
    """Benchmark adapter for QuestDB.

    Uses PostgreSQL wire protocol for queries and ILP for fast data ingestion.
    Requires QuestDB to be running locally.
    """

    name = "questdb"

    def __init__(self, host: str = "localhost", pg_port: int = 8812, ilp_port: int = 9009):
        self._host = host
        self._pg_port = pg_port
        self._ilp_port = ilp_port
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

        try:
            from questdb.ingress import Sender, ServerTimestamp
            self._Sender = Sender
            self._ServerTimestamp = ServerTimestamp
        except ImportError as e:
            raise ImportError(
                "questdb not installed. Install with: pip install questdb"
            ) from e

        self._connect()

    def _connect(self) -> None:
        """Connect to QuestDB via PostgreSQL wire protocol."""
        self._conn = self._psycopg.connect(
            host=self._host,
            port=self._pg_port,
            user="admin",
            password="quest",
            autocommit=True,
        )

    def load_data(self, path: Path, table_name: str = "data") -> None:
        """Load CSV data into QuestDB using ILP (InfluxDB Line Protocol).

        ILP is much faster than SQL INSERT (~100x speedup).
        """
        import polars as pl

        df = pl.read_csv(path)
        sql_table_name = f"bench_{table_name}"

        # Drop table if exists
        with self._conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {sql_table_name}")

        # Determine column types for ILP
        int_cols = [name for name, dtype in df.schema.items() if dtype == pl.Int64]
        float_cols = [name for name, dtype in df.schema.items() if dtype == pl.Float64]
        str_cols = [name for name, dtype in df.schema.items() if dtype in (pl.Utf8, pl.String)]

        # Use ILP for fast ingestion
        conf = f"tcp::addr={self._host}:{self._ilp_port};"
        with self._Sender.from_conf(conf) as sender:
            for row in df.iter_rows(named=True):
                sender.row(
                    sql_table_name,
                    symbols={col: row[col] for col in str_cols},
                    columns={
                        **{col: row[col] for col in int_cols},
                        **{col: row[col] for col in float_cols},
                    },
                    at=self._ServerTimestamp,
                )
            sender.flush()

        # ILP is async — the rows (and even the table itself, on a first
        # write) aren't queryable until QuestDB commits them (default
        # cadence ~1s). Block until count() matches the load, treating any
        # error from the SELECT as "not visible yet".
        import time as _time
        expected = df.height
        actual = 0
        deadline = _time.time() + 30
        while _time.time() < deadline:
            try:
                with self._conn.cursor() as cur:
                    cur.execute(f"SELECT count(*) FROM {sql_table_name}")
                    actual = cur.fetchone()[0]
                if actual >= expected:
                    break
            except Exception:
                actual = 0
            _time.sleep(0.1)
        else:
            raise RuntimeError(
                f"QuestDB ILP commit timeout: {actual}/{expected} rows visible"
            )

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
            with self._conn.cursor() as cur:
                cur.execute(f"SELECT id1, SUM(v1) as v1_sum FROM {t} GROUP BY id1")
                return cur.fetchall()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q1", time_ns, len(result))

    def run_groupby_q2(self) -> BenchmarkResult:
        """Q2: sum(v1) group by id1, id2"""
        t = self._get_table()

        def query():
            with self._conn.cursor() as cur:
                cur.execute(f"SELECT id1, id2, SUM(v1) as v1_sum FROM {t} GROUP BY id1, id2")
                return cur.fetchall()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q2", time_ns, len(result))

    def run_groupby_q3(self) -> BenchmarkResult:
        """Q3: sum(v1), mean(v3) group by id3"""
        t = self._get_table()

        def query():
            with self._conn.cursor() as cur:
                cur.execute(f"SELECT id3, SUM(v1) as v1_sum, AVG(v3) as v3_avg FROM {t} GROUP BY id3")
                return cur.fetchall()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q3", time_ns, len(result))

    def run_groupby_q4(self) -> BenchmarkResult:
        """Q4: mean(v1), mean(v2), mean(v3) group by id3"""
        t = self._get_table()

        def query():
            with self._conn.cursor() as cur:
                cur.execute(f"SELECT id3, AVG(v1) as v1_avg, AVG(v2) as v2_avg, AVG(v3) as v3_avg FROM {t} GROUP BY id3")
                return cur.fetchall()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q4", time_ns, len(result))

    def run_groupby_q5(self) -> BenchmarkResult:
        """Q5: sum(v1), sum(v2), sum(v3) group by id3"""
        t = self._get_table()

        def query():
            with self._conn.cursor() as cur:
                cur.execute(f"SELECT id3, SUM(v1) as v1_sum, SUM(v2) as v2_sum, SUM(v3) as v3_sum FROM {t} GROUP BY id3")
                return cur.fetchall()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q5", time_ns, len(result))

    def run_groupby_q6(self) -> BenchmarkResult:
        """Q6: max(v1) - min(v2) group by id3"""
        t = self._get_table()

        def query():
            with self._conn.cursor() as cur:
                cur.execute(f"SELECT id3, MAX(v1) - MIN(v2) as range FROM {t} GROUP BY id3")
                return cur.fetchall()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q6", time_ns, len(result))

    def run_groupby_q7(self) -> BenchmarkResult:
        """Q7: sum(v3), count(v1) group by id1..id6 (canonical H2O)."""
        t = self._get_table()

        def query():
            with self._conn.cursor() as cur:
                cur.execute(
                    f"SELECT id1, id2, id3, id4, id5, id6, "
                    f"SUM(v3) as v3, COUNT(v1) as cnt "
                    f"FROM {t} GROUP BY id1, id2, id3, id4, id5, id6"
                )
                return cur.fetchall()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q7", time_ns, len(result))

    def _load_right(self, path: Path) -> str:
        """Stream right table into QuestDB via ILP, wait for commit."""
        import polars as pl
        import time as _time

        df = pl.read_csv(path)
        right_table = "bench_right_tmp"

        with self._conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {right_table}")

        int_cols = [n for n, d in df.schema.items() if d == pl.Int64]
        float_cols = [n for n, d in df.schema.items() if d == pl.Float64]
        str_cols = [n for n, d in df.schema.items() if d in (pl.Utf8, pl.String)]

        conf = f"tcp::addr={self._host}:{self._ilp_port};"
        with self._Sender.from_conf(conf) as sender:
            for row in df.iter_rows(named=True):
                sender.row(
                    right_table,
                    symbols={c: row[c] for c in str_cols},
                    columns={
                        **{c: row[c] for c in int_cols},
                        **{c: row[c] for c in float_cols},
                    },
                    at=self._ServerTimestamp,
                )
            sender.flush()

        # Wait for ILP commit visibility (same dance as load_data).
        expected = df.height
        deadline = _time.time() + 30
        while _time.time() < deadline:
            try:
                with self._conn.cursor() as cur:
                    cur.execute(f"SELECT count(*) FROM {right_table}")
                    if cur.fetchone()[0] >= expected:
                        break
            except Exception:
                pass
            _time.sleep(0.1)
        return right_table

    def run_join_inner(self, right_path: Path) -> BenchmarkResult:
        """Inner join on (id1, id2, id3) — canonical H2O J1."""
        left = self._get_table("left")
        right = self._load_right(right_path)

        def query():
            with self._conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {left} INNER JOIN {right} "
                    f"ON {left}.id1 = {right}.id1 "
                    f"AND {left}.id2 = {right}.id2 "
                    f"AND {left}.id3 = {right}.id3"
                )
                return cur.fetchall()

        result, time_ns = self._time_it(query)
        with self._conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {right}")
        return BenchmarkResult("join_inner", time_ns, len(result))

    def run_join_left(self, right_path: Path) -> BenchmarkResult:
        """Left join on (id1, id2, id3) — canonical H2O J1."""
        left = self._get_table("left")
        right = self._load_right(right_path)

        def query():
            with self._conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {left} LEFT JOIN {right} "
                    f"ON {left}.id1 = {right}.id1 "
                    f"AND {left}.id2 = {right}.id2 "
                    f"AND {left}.id3 = {right}.id3"
                )
                return cur.fetchall()

        result, time_ns = self._time_it(query)
        with self._conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {right}")
        return BenchmarkResult("join_left", time_ns, len(result))

    def run_sort_single(self) -> BenchmarkResult:
        """Sort by single column."""
        t = self._get_table()

        def query():
            with self._conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {t} ORDER BY id1")
                return cur.fetchall()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("sort_single", time_ns, len(result))

    def run_sort_multi(self) -> BenchmarkResult:
        """Sort by multiple columns."""
        t = self._get_table()

        def query():
            with self._conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {t} ORDER BY id1, id2, id3")
                return cur.fetchall()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("sort_multi", time_ns, len(result))

    # u8 unsigned doesn't exist in QuestDB; SHORT covers 0..255 safely.
    _QUESTDB_TYPES = {
        "u8": "SHORT", "i16": "SHORT", "i32": "INT",
        "i64": "LONG", "f64": "DOUBLE",
        "str8": "SYMBOL", "str16": "SYMBOL",
    }

    def run_sort_typed_full(self, csv_path: Path, dtype: str,
                             n_warmup: int, n_iter: int) -> list[BenchmarkResult]:
        """Sort a single typed column for the extended sort grid."""
        import polars as pl
        import time as _time
        col_type = self._QUESTDB_TYPES[dtype]
        is_str = dtype.startswith("str")

        # Load the single-column CSV with the requested type.
        df = pl.read_csv(csv_path)
        sort_table = "bench_sort_tmp"

        with self._conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {sort_table}")

        # Random N-character strings are high-cardinality — push them as
        # STRING via ILP `columns`, not SYMBOL via `symbols` (Symbol is
        # a dictionary type and chokes on N=1M unique values).
        conf = f"tcp::addr={self._host}:{self._ilp_port};"
        with self._Sender.from_conf(conf) as sender:
            for row in df.iter_rows(named=True):
                v = row["v"]
                sender.row(sort_table, columns={"v": v},
                           at=self._ServerTimestamp)
            sender.flush()

        # Wait for ILP commit visibility. 1M-row ILP commits can take a
        # while; raise loudly on timeout so we don't sort an empty table
        # and report fake 0.36ms / 0 rows.
        expected = df.height
        actual = 0
        deadline = _time.time() + 120
        while _time.time() < deadline:
            try:
                with self._conn.cursor() as cur:
                    cur.execute(f"SELECT count(*) FROM {sort_table}")
                    actual = cur.fetchone()[0]
                if actual >= expected:
                    break
            except Exception:
                actual = 0
            _time.sleep(0.2)
        else:
            raise RuntimeError(
                f"QuestDB ILP commit timeout for sort_{dtype}: "
                f"{actual}/{expected} rows visible after 120s"
            )

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
