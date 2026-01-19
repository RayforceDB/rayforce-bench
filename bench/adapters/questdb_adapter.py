"""QuestDB adapter for benchmarks.

Requires QuestDB running locally on default port (8812 for PostgreSQL wire protocol).
Install client: pip install psycopg[binary]
"""

from pathlib import Path

from .base import Adapter, BenchmarkResult


class QuestDBAdapter(Adapter):
    """Benchmark adapter for QuestDB.

    Uses PostgreSQL wire protocol to connect to QuestDB.
    Requires QuestDB to be running locally.
    """

    name = "questdb"

    def __init__(self, host: str = "localhost", port: int = 8812):
        self._host = host
        self._port = port
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
        """Connect to QuestDB."""
        self._conn = self._psycopg.connect(
            host=self._host,
            port=self._port,
            user="admin",
            password="quest",
            autocommit=True,
        )

    def load_data(self, path: Path, table_name: str = "data") -> None:
        """Load parquet data into QuestDB."""
        import pandas as pd

        df = pd.read_parquet(path)
        sql_table_name = f"bench_{table_name}"

        # Drop table if exists
        with self._conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {sql_table_name}")

        # Create table and insert data
        # QuestDB requires specific column types
        columns = []
        for col in df.columns:
            if df[col].dtype == "int64":
                columns.append(f"{col} LONG")
            elif df[col].dtype == "float64":
                columns.append(f"{col} DOUBLE")
            else:
                columns.append(f"{col} SYMBOL")

        create_sql = f"CREATE TABLE {sql_table_name} ({', '.join(columns)})"
        with self._conn.cursor() as cur:
            cur.execute(create_sql)

        # Insert data in batches
        batch_size = 10000
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            values = []
            for _, row in batch.iterrows():
                row_vals = []
                for col in df.columns:
                    val = row[col]
                    if isinstance(val, str):
                        row_vals.append(f"'{val}'")
                    else:
                        row_vals.append(str(val))
                values.append(f"({', '.join(row_vals)})")

            insert_sql = f"INSERT INTO {sql_table_name} VALUES {', '.join(values)}"
            with self._conn.cursor() as cur:
                cur.execute(insert_sql)

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
        raise NotImplementedError("QuestDB join benchmarks not implemented")

    def run_join_left(self, right_path: Path) -> BenchmarkResult:
        """Left join on id1."""
        raise NotImplementedError("QuestDB join benchmarks not implemented")

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
