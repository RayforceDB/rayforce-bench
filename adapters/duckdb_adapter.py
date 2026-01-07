"""
DuckDB embedded adapter for benchmarking.

Uses DuckDB's Python API for in-process execution.
Measures only query execution time, not connection setup or data loading.
"""

import hashlib
import time
from pathlib import Path
from typing import Any

import duckdb

from benchmarks.adapter import Adapter, AdapterResult, SetupError, TaskError


# Type mapping from manifest types to DuckDB types
TYPE_MAP = {
    "I64": "BIGINT",
    "I32": "INTEGER",
    "I16": "SMALLINT",
    "F64": "DOUBLE",
    "F32": "FLOAT",
    "SYMBOL": "VARCHAR",
    "STRING": "VARCHAR",
    "DATE": "DATE",
    "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP",
    "BOOL": "BOOLEAN",
    "B8": "BOOLEAN",
}


class DuckDBAdapter(Adapter):
    """DuckDB in-process adapter.
    
    Uses DuckDB's embedded Python API for minimal overhead.
    All operations run in-process without IPC or network.
    """
    
    name = "duckdb"
    version = duckdb.__version__
    embedded = True
    
    def __init__(self, threads: int | None = None, memory_limit: str | None = None):
        """Initialize DuckDB adapter.
        
        Args:
            threads: Number of threads (None = auto).
            memory_limit: Memory limit (e.g., "4GB").
        """
        self.threads = threads
        self.memory_limit = memory_limit
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._table_name: str = ""
        self._tasks = self._build_task_registry()
    
    def _build_task_registry(self) -> dict[str, callable]:
        """Build registry of task handlers."""
        return {
            # H2OAI Group By queries (aligned with Rayforce benchmarks)
            "groupby_q1": self._task_groupby_q1,
            "groupby_q2": self._task_groupby_q2,
            "groupby_q3": self._task_groupby_q3,
            "groupby_q4": self._task_groupby_q4,
            "groupby_q5": self._task_groupby_q5,
            "groupby_q6": self._task_groupby_q6,
            "groupby_q7": self._task_groupby_q7,
            # Join queries
            "inner_join": self._task_inner_join,
            "left_join": self._task_left_join,
            # Generic SQL execution
            "sql": self._task_sql,
        }
    
    def setup(self, schema: dict[str, Any]) -> None:
        """Initialize DuckDB with in-memory database."""
        # Create in-memory connection
        self._conn = duckdb.connect(":memory:")
        
        # Configure settings
        if self.threads is not None:
            self._conn.execute(f"SET threads = {self.threads}")
        if self.memory_limit is not None:
            self._conn.execute(f"SET memory_limit = '{self.memory_limit}'")
        
        self._table_name = schema.get("table_name", "benchmark")
    
    def load_csv(self, csv_paths: list[Path], table_name: str) -> None:
        """Load CSV files into DuckDB table."""
        if not self._conn:
            raise SetupError("Adapter not initialized. Call setup() first.")
        
        if not csv_paths:
            raise SetupError("No CSV files provided")
        
        self._table_name = table_name
        
        # Use DuckDB's efficient read_csv for loading
        if len(csv_paths) == 1:
            csv_path = str(csv_paths[0])
            self._conn.execute(f"""
                CREATE TABLE {table_name} AS 
                SELECT * FROM read_csv('{csv_path}', auto_detect=true)
            """)
        else:
            # Multiple CSV files (partitioned dataset)
            csv_list = ", ".join(f"'{str(p)}'" for p in csv_paths)
            self._conn.execute(f"""
                CREATE TABLE {table_name} AS 
                SELECT * FROM read_csv([{csv_list}], auto_detect=true)
            """)
    
    def run(self, task: str, params: dict[str, Any]) -> AdapterResult:
        """Execute a benchmark task."""
        if not self._conn:
            raise TaskError("Adapter not initialized")
        
        handler = self._tasks.get(task)
        if handler is None:
            # Try as raw SQL
            if "query" in params:
                return self._execute_query(params["query"])
            raise TaskError(f"Unknown task: {task}")
        
        return handler(params)
    
    def close(self) -> None:
        """Close DuckDB connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
    
    def clear_cache(self) -> None:
        """DuckDB doesn't have explicit cache clearing."""
        # For cold runs, we'd need to recreate the connection
        # This is a no-op for now as DuckDB manages caching internally
        pass
    
    def get_info(self) -> dict[str, Any]:
        """Return DuckDB-specific metadata."""
        info = super().get_info()
        info.update({
            "duckdb_version": duckdb.__version__,
            "threads": self.threads,
            "memory_limit": self.memory_limit,
        })
        return info
    
    def _execute_query(self, query: str) -> AdapterResult:
        """Execute a query and return result metadata."""
        start_ns = time.perf_counter_ns()
        
        try:
            result = self._conn.execute(query)
            # Materialize result to get row count without returning data
            rows = result.fetchall()
            row_count = len(rows)
            
            end_ns = time.perf_counter_ns()
            
            # Compute checksum from first column values for validation
            checksum = None
            if rows and len(rows[0]) > 0:
                # Simple checksum from string representation of first column
                first_col_str = str([row[0] for row in rows[:1000]])  # Limit for performance
                checksum = int(hashlib.md5(first_col_str.encode()).hexdigest()[:8], 16)
            
            return AdapterResult(
                execution_time_ns=end_ns - start_ns,
                row_count=row_count,
                checksum=checksum,
            )
        except Exception as e:
            end_ns = time.perf_counter_ns()
            return AdapterResult(
                execution_time_ns=end_ns - start_ns,
                row_count=0,
                success=False,
                error_message=str(e),
            )
    
    # =========================================================================
    # H2OAI Group By Queries (matching Rayforce benchmark queries)
    # =========================================================================
    
    def _task_groupby_q1(self, params: dict[str, Any]) -> AdapterResult:
        """Q1: sum(v1) by id1"""
        table = params.get("table", self._table_name)
        query = f"SELECT id1, SUM(v1) AS v1 FROM {table} GROUP BY id1"
        return self._execute_query(query)
    
    def _task_groupby_q2(self, params: dict[str, Any]) -> AdapterResult:
        """Q2: sum(v1) by id1, id2"""
        table = params.get("table", self._table_name)
        query = f"SELECT id1, id2, SUM(v1) AS v1 FROM {table} GROUP BY id1, id2"
        return self._execute_query(query)
    
    def _task_groupby_q3(self, params: dict[str, Any]) -> AdapterResult:
        """Q3: sum(v1), avg(v3) by id3"""
        table = params.get("table", self._table_name)
        query = f"SELECT id3, SUM(v1) AS v1, AVG(v3) AS v3 FROM {table} GROUP BY id3"
        return self._execute_query(query)
    
    def _task_groupby_q4(self, params: dict[str, Any]) -> AdapterResult:
        """Q4: avg(v1), avg(v2), avg(v3) by id4"""
        table = params.get("table", self._table_name)
        query = f"SELECT id4, AVG(v1) AS v1, AVG(v2) AS v2, AVG(v3) AS v3 FROM {table} GROUP BY id4"
        return self._execute_query(query)
    
    def _task_groupby_q5(self, params: dict[str, Any]) -> AdapterResult:
        """Q5: sum(v1), sum(v2), sum(v3) by id6"""
        table = params.get("table", self._table_name)
        query = f"SELECT id6, SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM {table} GROUP BY id6"
        return self._execute_query(query)
    
    def _task_groupby_q6(self, params: dict[str, Any]) -> AdapterResult:
        """Q6: max(v1) - min(v2) by id3"""
        table = params.get("table", self._table_name)
        query = f"SELECT id3, MAX(v1) - MIN(v2) AS range_v1_v2 FROM {table} GROUP BY id3"
        return self._execute_query(query)
    
    def _task_groupby_q7(self, params: dict[str, Any]) -> AdapterResult:
        """Q7: sum(v3), count(*) by id1-id6"""
        table = params.get("table", self._table_name)
        query = f"""
            SELECT id1, id2, id3, id4, id5, id6, 
                   SUM(v3) AS v3, COUNT(*) AS count 
            FROM {table} 
            GROUP BY id1, id2, id3, id4, id5, id6
        """
        return self._execute_query(query)
    
    # =========================================================================
    # Join Queries (matching Rayforce benchmark queries)
    # =========================================================================
    
    def _task_inner_join(self, params: dict[str, Any]) -> AdapterResult:
        """Inner join on id1, id2"""
        left_table = params.get("left_table", "x")
        right_table = params.get("right_table", "y")
        query = f"""
            SELECT * FROM {left_table} 
            INNER JOIN {right_table} 
            ON {left_table}.id1 = {right_table}.id1 
            AND {left_table}.id2 = {right_table}.id2
        """
        return self._execute_query(query)
    
    def _task_left_join(self, params: dict[str, Any]) -> AdapterResult:
        """Left join on id1, id2"""
        left_table = params.get("left_table", "x")
        right_table = params.get("right_table", "y")
        query = f"""
            SELECT * FROM {left_table} 
            LEFT JOIN {right_table} 
            ON {left_table}.id1 = {right_table}.id1 
            AND {left_table}.id2 = {right_table}.id2
        """
        return self._execute_query(query)
    
    def _task_sql(self, params: dict[str, Any]) -> AdapterResult:
        """Execute arbitrary SQL query."""
        query = params.get("query")
        if not query:
            return AdapterResult(
                execution_time_ns=0,
                row_count=0,
                success=False,
                error_message="No query provided",
            )
        return self._execute_query(query)
