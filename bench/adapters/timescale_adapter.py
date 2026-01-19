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

    def run_join_inner(self, right_path: Path) -> BenchmarkResult:
        """Inner join on id1."""
        import io
        import polars as pl

        left = self._get_table("left")

        # Load right table and materialize in memory before timing
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

        def query():
            with self._conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {left} INNER JOIN {right_table} ON {left}.id1 = {right_table}.id1")
                return cur.fetchall()

        result, time_ns = self._time_it(query)

        with self._conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {right_table}")

        return BenchmarkResult("join_inner", time_ns, len(result))

    def run_join_left(self, right_path: Path) -> BenchmarkResult:
        """Left join on id1."""
        import io
        import polars as pl

        left = self._get_table("left")

        # Load right table and materialize in memory before timing
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

        def query():
            with self._conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {left} LEFT JOIN {right_table} ON {left}.id1 = {right_table}.id1")
                return cur.fetchall()

        result, time_ns = self._time_it(query)

        with self._conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {right_table}")

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
