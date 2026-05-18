"""QuestDB adapter for benchmarks.

Requires QuestDB running locally on default port (8812 for PostgreSQL wire protocol).
Install client: pip install psycopg[binary]
"""

from pathlib import Path

from .base import Adapter, BenchmarkResult, _dedupe_col_names


class QuestDBAdapter(Adapter):
    """Benchmark adapter for QuestDB.

    Uses PostgreSQL wire protocol for queries and ILP for fast data ingestion.
    Requires QuestDB to be running locally.
    """

    name = "questdb"

    QUERY_STRINGS = {
        "groupby_q1":  "SELECT id1, SUM(v1) FROM data GROUP BY id1",
        "groupby_q2":  "SELECT id1, id2, SUM(v1) FROM data GROUP BY id1, id2",
        "groupby_q3":  "SELECT id3, SUM(v1), AVG(v3) FROM data GROUP BY id3",
        "groupby_q4":  "SELECT id4, AVG(v1), AVG(v2), AVG(v3) FROM data GROUP BY id4",
        "groupby_q5":  "SELECT id6, SUM(v1), SUM(v2), SUM(v3) FROM data GROUP BY id6",
        "groupby_q6":  "-- NYI: QuestDB has no exact median(); only approx_median",
        "groupby_q7":  "SELECT id3, MAX(v1) - MIN(v2) FROM data GROUP BY id3",
        "groupby_q8":  "SELECT id6, v3 FROM (SELECT id6, v3, row_number() OVER (PARTITION BY id6 ORDER BY v3 DESC) rn FROM data WHERE v3 IS NOT NULL) WHERE rn <= 2",
        "groupby_q9":  "SELECT id2, id4, POWER(corr(v1, v2), 2) FROM data GROUP BY id2, id4",
        "groupby_q10": "SELECT id1, id2, id3, id4, id5, id6, SUM(v3), COUNT(v1) FROM data GROUP BY id1, id2, id3, id4, id5, id6",
        "join_q1":     "SELECT * FROM x INNER JOIN small  ON x.id1 = small.id1",
        "join_q2":     "SELECT * FROM x INNER JOIN medium ON x.id2 = medium.id2",
        "join_q3":     "SELECT * FROM x LEFT  JOIN medium ON x.id2 = medium.id2",
        "join_q4":     "SELECT * FROM x INNER JOIN medium ON x.id5 = medium.id5",
        "join_q5":     "SELECT * FROM x INNER JOIN big    ON x.id3 = big.id3",
        "join_inner":  "SELECT * FROM left INNER JOIN right ON left.id1=right.id1 AND left.id2=right.id2 AND left.id3=right.id3",
        "join_left":   "SELECT * FROM left LEFT  JOIN right ON left.id1=right.id1 AND left.id2=right.id2 AND left.id3=right.id3",
        "sort_single": "SELECT * FROM data ORDER BY id1",
        "sort_multi":  "SELECT * FROM data ORDER BY id1, id2, id3",
    }

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

        # Use ILP for ingestion.  Flush every FLUSH_EVERY rows so the
        # server can start committing before send finishes — without
        # periodic flush, all rows sit in the sender's local buffer until
        # the with-block exits, making first-commit visibility race
        # against the polling deadline below.
        FLUSH_EVERY = 100_000
        conf = f"tcp::addr={self._host}:{self._ilp_port};"
        with self._Sender.from_conf(conf) as sender:
            for i, row in enumerate(df.iter_rows(named=True)):
                sender.row(
                    sql_table_name,
                    symbols={col: row[col] for col in str_cols},
                    columns={
                        **{col: row[col] for col in int_cols},
                        **{col: row[col] for col in float_cols},
                    },
                    at=self._ServerTimestamp,
                )
                if (i + 1) % FLUSH_EVERY == 0:
                    sender.flush()
            sender.flush()

        # ILP is async — the rows (and even the table itself, on a first
        # write) aren't queryable until QuestDB commits them (default
        # cadence ~1s).  For multi-million-row loads the commit chases
        # the send tail by tens of seconds; 5-minute deadline covers the
        # canonical-join `big` (N=10M) without false ILP timeouts.
        import time as _time
        expected = df.height
        actual = 0
        deadline = _time.time() + 300
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

    def _time_pg(self, sql: str, name: str) -> BenchmarkResult:
        """Time `cur.execute(sql)` only — engine compute on the server.

        QuestDB completes the SELECT before returning control over the
        PG protocol; `cur.rowcount` is populated immediately and does
        not require fetching rows. Skipping fetchall keeps the timer
        focused on engine work, not Python-side row materialization.
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

        QuestDB has only `approx_median` / `approx_percentile`, no exact
        median. The canonical H2O bench compares exact median values, so
        we report NYI rather than ship an approximate result that would
        fail the equivalence check.
        """
        raise NotImplementedError(
            "QuestDB has no exact median (only approx_median); "
            "canonical H2O q6 needs exact median(v3)")

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
            f"  row_number() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS rn "
            f"  FROM {t} WHERE v3 IS NOT NULL"
            f") sub WHERE rn <= 2", "groupby_q8")

    def run_groupby_q9(self) -> BenchmarkResult:
        """Q9: corr(v1, v2)^2 by id2, id4 — canonical H2O."""
        t = self._get_table()
        return self._time_pg(
            f"SELECT id2, id4, POWER(corr(v1, v2), 2) AS r2 "
            f"FROM {t} GROUP BY id2, id4", "groupby_q9")

    def run_groupby_q10(self) -> BenchmarkResult:
        """Q10: sum(v3), count(v1) by id1..id6 — canonical H2O."""
        t = self._get_table()
        return self._time_pg(
            f"SELECT id1, id2, id3, id4, id5, id6, "
            f"SUM(v3) AS v3, COUNT(v1) AS cnt "
            f"FROM {t} GROUP BY id1, id2, id3, id4, id5, id6",
            "groupby_q10")

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

        # Periodic flush so server commits in parallel with send — same
        # rationale as load_data().
        FLUSH_EVERY = 100_000
        conf = f"tcp::addr={self._host}:{self._ilp_port};"
        with self._Sender.from_conf(conf) as sender:
            for i, row in enumerate(df.iter_rows(named=True)):
                sender.row(
                    right_table,
                    symbols={c: row[c] for c in str_cols},
                    columns={
                        **{c: row[c] for c in int_cols},
                        **{c: row[c] for c in float_cols},
                    },
                    at=self._ServerTimestamp,
                )
                if (i + 1) % FLUSH_EVERY == 0:
                    sender.flush()
            sender.flush()

        # Wait for ILP commit visibility (same dance as load_data).
        expected = df.height
        deadline = _time.time() + 300
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

    # Canonical H2O J1 — 5 single-key joins.

    def _canon_join(self, right_name: str, key: str, kind: str,
                    op_name: str) -> BenchmarkResult:
        x = self._get_table("x")
        r = self._get_table(right_name)
        # QuestDB's PG protocol doesn't support USING; use ON.
        return self._time_pg(
            f"SELECT * FROM {x} {kind} JOIN {r} ON {x}.{key} = {r}.{key}",
            op_name)

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
        # Periodic flush so server commits in parallel with send.
        FLUSH_EVERY = 100_000
        conf = f"tcp::addr={self._host}:{self._ilp_port};"
        with self._Sender.from_conf(conf) as sender:
            for i, row in enumerate(df.iter_rows(named=True)):
                v = row["v"]
                sender.row(sort_table, columns={"v": v},
                           at=self._ServerTimestamp)
                if (i + 1) % FLUSH_EVERY == 0:
                    sender.flush()
            sender.flush()

        # Wait for ILP commit visibility. 10M-row ILP commits can take
        # minutes; raise loudly on timeout so we don't sort an empty table
        # and report fake 0.36ms / 0 rows.
        expected = df.height
        actual = 0
        deadline = _time.time() + 300
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

        # Engine-only timing: only `cur.execute` is timed; `cur.rowcount`
        # outside the timer gives the row count without paying for PG
        # row materialization. Mirrors _time_pg used by canonical-suite
        # queries.
        for _ in range(n_warmup):
            with self._conn.cursor() as cur:
                cur.execute(sql)

        results = []
        for _ in range(n_iter):
            with self._conn.cursor() as cur:
                _, time_ns = self._time_it(lambda c=cur: c.execute(sql))
                rows = cur.rowcount
            results.append(BenchmarkResult(f"sort_{dtype}", time_ns, rows))

        with self._conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {sort_table}")
        return results

    def materialize(self, op: str, right_path: Path | None = None):
        import polars as pl
        if op == "groupby_q6":
            raise NotImplementedError(
                "QuestDB has no exact median; canonical H2O q6 needs "
                "exact median(v3), only approx_median is available.")
        t = self._get_table() if not op.startswith("join_") else None
        sql_map = {
            "groupby_q1": f"SELECT id1, SUM(v1) AS v1 FROM {t} GROUP BY id1",
            "groupby_q2": f"SELECT id1, id2, SUM(v1) AS v1 FROM {t} GROUP BY id1, id2",
            "groupby_q3": f"SELECT id3, SUM(v1) AS v1, AVG(v3) AS v3 FROM {t} GROUP BY id3",
            "groupby_q4": f"SELECT id4, AVG(v1) AS v1, AVG(v2) AS v2, AVG(v3) AS v3 FROM {t} GROUP BY id4",
            "groupby_q5": f"SELECT id6, SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM {t} GROUP BY id6",
            "groupby_q7": f"SELECT id3, MAX(v1) - MIN(v2) AS range_v1_v2 FROM {t} GROUP BY id3",
            "groupby_q8": (
                f"SELECT id6, v3 AS largest2_v3 FROM ("
                f"  SELECT id6, v3, row_number() OVER "
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
                f"SUM(v3) AS v3, COUNT(v1) AS cnt FROM {t} "
                f"GROUP BY id1, id2, id3, id4, id5, id6"
            ),
            "sort_single": f"SELECT * FROM {t} ORDER BY id1",
            "sort_multi":  f"SELECT * FROM {t} ORDER BY id1, id2, id3",
        }
        if op in sql_map:
            sql = sql_map[op]
            cleanup = None
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
            sql = (f"SELECT * FROM {x} {kind} JOIN {r} "
                   f"ON {x}.{key} = {r}.{key}")
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
            cols = _dedupe_col_names(cols)
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
