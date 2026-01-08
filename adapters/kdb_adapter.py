"""
KDB+/q adapter for benchmarking.

Uses q's built-in \t timer for accurate timing (similar to Rayforce's timeit).
Executes via subprocess with q binary.

Note: KDB+ is a commercial product. Ensure you have appropriate licensing.
"""

import hashlib
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

from benchmarks.adapter import Adapter, AdapterResult, SetupError, TaskError
from benchmarks.config import get_config


class KDBAdapter(Adapter):
    """KDB+/q adapter using subprocess execution.
    
    Uses q's \t command for timing, which measures only query execution
    (similar to Rayforce's timeit function).
    """
    
    name = "kdb"
    version = "4.0"
    embedded = False
    
    def __init__(
        self,
        binary_path: str | Path | None = None,
    ):
        """Initialize KDB adapter.
        
        Args:
            binary_path: Path to q binary (default: from config or 'q')
        """
        config = get_config()
        kdb_config = config.kdb
        
        self.binary_path = Path(binary_path or kdb_config.get("binary", "q"))
        
        self._schema: dict[str, Any] = {}
        self._table_name: str = ""
        self._csv_paths: list[Path] = []
        self._tables: dict[str, Path] = {}
        self._tasks = self._build_task_registry()
    
    def _build_task_registry(self) -> dict[str, callable]:
        """Build registry of task handlers."""
        return {
            # H2OAI Group By queries (q syntax)
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
            # Generic q execution
            "eval": self._task_eval,
        }
    
    def setup(self, schema: dict[str, Any]) -> None:
        """Initialize KDB environment."""
        self._schema = schema
        self._table_name = schema.get("table_name", "t")
        
        # Verify q binary exists (check PATH if not absolute)
        if not self.binary_path.is_absolute():
            found = shutil.which(str(self.binary_path))
            if found:
                self.binary_path = Path(found)
            else:
                raise SetupError(f"KDB q binary not found: {self.binary_path}")
        elif not self.binary_path.exists():
            raise SetupError(f"KDB q binary not found: {self.binary_path}")
        
        # Try to get version
        try:
            result = subprocess.run(
                [str(self.binary_path), "-q"],
                input="\\\\",  # Exit immediately
                capture_output=True,
                text=True,
                timeout=5,
            )
        except Exception:
            pass  # Version detection is optional
    
    def load_csv(self, csv_paths: list[Path], table_name: str) -> None:
        """Store CSV paths for loading in run()."""
        self._csv_paths = csv_paths
        self._table_name = table_name
        if csv_paths:
            self._tables[table_name] = csv_paths[0]
    
    def run(self, task: str, params: dict[str, Any]) -> AdapterResult:
        """Execute a benchmark task."""
        handler = self._tasks.get(task)
        if handler is None:
            if "expr" in params:
                return self._execute_q(params["expr"])
            raise TaskError(f"Unknown task: {task}")
        
        return handler(params)
    
    def close(self) -> None:
        """Clean up KDB resources."""
        pass
    
    def get_info(self) -> dict[str, Any]:
        """Return KDB-specific metadata."""
        info = super().get_info()
        info.update({
            "kdb_version": self.version,
            "mode": "subprocess",
            "binary_path": str(self.binary_path),
        })
        return info
    
    def _build_type_spec(self) -> str:
        """Build KDB type specification string.
        
        Example: "SSSJJJJJF" for G1 dataset
        """
        type_map = {
            "I64": "J",
            "I32": "I",
            "I16": "H",
            "F64": "F",
            "F32": "E",
            "SYMBOL": "S",
            "STRING": "*",
            "DATE": "D",
            "TIME": "T",
            "TIMESTAMP": "P",
            "BOOL": "B",
            "B8": "B",
        }
        columns = self._schema.get("columns", [])
        return "".join(type_map.get(col.get("type", "STRING"), "*") for col in columns)
    
    def _execute_q(self, expr: str) -> AdapterResult:
        """Execute a q expression and return result metadata.
        
        Uses \t for timing which measures only the query execution,
        similar to Rayforce's timeit function.
        
        Note: \t outputs timing directly to stdout (can't be assigned).
        Output format:
          <timing_ms>
          <row_count>
        """
        start_ns = time.perf_counter_ns()
        
        try:
            type_spec = self._build_type_spec()
            
            # Build load statements for all tables
            load_lines = []
            if self._tables:
                for tbl_name, tbl_path in self._tables.items():
                    load_lines.append(f'{tbl_name}:("{type_spec}";enlist",")0:`$":{tbl_path}"')
            elif self._csv_paths:
                csv_path = str(self._csv_paths[0])
                load_lines.append(f'{self._table_name}:("{type_spec}";enlist",")0:`$":{csv_path}"')
            
            load_script = "\n".join(load_lines)
            
            # Use \t for timing - outputs time in ms directly to stdout
            # Then output row count on next line
            q_script = f"""{load_script}
\\t r:{expr}
count r
\\\\
"""
            
            # Execute via subprocess
            result = subprocess.run(
                [str(self.binary_path), "-q"],  # -q for quiet mode
                input=q_script,
                capture_output=True,
                text=True,
                timeout=300,
            )
            
            end_ns = time.perf_counter_ns()
            
            # Parse output
            output = result.stdout.strip()
            stderr = result.stderr.strip()
            
            if result.returncode != 0 or "'error" in output.lower() or "'" in output:
                return AdapterResult(
                    execution_time_ns=end_ns - start_ns,
                    row_count=0,
                    success=False,
                    error_message=stderr or output,
                )
            
            # Parse output: first line is timing (ms), second is row count
            lines = [l.strip() for l in output.split('\n') if l.strip()]
            timing_ms = None
            row_count = 0
            
            if len(lines) >= 2:
                try:
                    timing_ms = int(lines[0])
                except ValueError:
                    pass
                try:
                    row_count = int(lines[1])
                except ValueError:
                    pass
            elif len(lines) == 1:
                # Might just have timing or count
                try:
                    timing_ms = int(lines[0])
                except ValueError:
                    pass
            
            # Use q's \t timing (in ms)
            if timing_ms is not None:
                execution_ns = timing_ms * 1_000_000
            else:
                execution_ns = end_ns - start_ns
            
            checksum = int(hashlib.md5(output.encode()).hexdigest()[:8], 16)
            
            return AdapterResult(
                execution_time_ns=execution_ns,
                row_count=row_count,
                checksum=checksum,
                query=expr,
            )
            
        except subprocess.TimeoutExpired:
            return AdapterResult(
                execution_time_ns=time.perf_counter_ns() - start_ns,
                row_count=0,
                success=False,
                error_message="Execution timeout",
                query=expr,
            )
        except Exception as e:
            return AdapterResult(
                execution_time_ns=time.perf_counter_ns() - start_ns,
                row_count=0,
                success=False,
                error_message=str(e),
                query=expr if 'expr' in locals() else None,
            )
    
    # =========================================================================
    # H2OAI Group By Queries (q syntax)
    # =========================================================================
    
    def _task_groupby_q1(self, params: dict[str, Any]) -> AdapterResult:
        """Q1: sum(v1) by id1"""
        table = params.get("table", self._table_name)
        expr = f"select v1:sum v1 by id1 from {table}"
        return self._execute_q(expr)
    
    def _task_groupby_q2(self, params: dict[str, Any]) -> AdapterResult:
        """Q2: sum(v1) by id1, id2"""
        table = params.get("table", self._table_name)
        expr = f"select v1:sum v1 by id1,id2 from {table}"
        return self._execute_q(expr)
    
    def _task_groupby_q3(self, params: dict[str, Any]) -> AdapterResult:
        """Q3: sum(v1), avg(v3) by id3"""
        table = params.get("table", self._table_name)
        expr = f"select v1:sum v1,v3:avg v3 by id3 from {table}"
        return self._execute_q(expr)
    
    def _task_groupby_q4(self, params: dict[str, Any]) -> AdapterResult:
        """Q4: avg(v1), avg(v2), avg(v3) by id4"""
        table = params.get("table", self._table_name)
        expr = f"select v1:avg v1,v2:avg v2,v3:avg v3 by id4 from {table}"
        return self._execute_q(expr)
    
    def _task_groupby_q5(self, params: dict[str, Any]) -> AdapterResult:
        """Q5: sum(v1), sum(v2), sum(v3) by id6"""
        table = params.get("table", self._table_name)
        expr = f"select v1:sum v1,v2:sum v2,v3:sum v3 by id6 from {table}"
        return self._execute_q(expr)
    
    def _task_groupby_q6(self, params: dict[str, Any]) -> AdapterResult:
        """Q6: max(v1) - min(v2) by id3"""
        table = params.get("table", self._table_name)
        expr = f"select range_v1_v2:(max v1)-(min v2) by id3 from {table}"
        return self._execute_q(expr)
    
    def _task_groupby_q7(self, params: dict[str, Any]) -> AdapterResult:
        """Q7: sum(v3), count by id1-id6"""
        table = params.get("table", self._table_name)
        expr = f"select v3:sum v3,cnt:count i by id1,id2,id3,id4,id5,id6 from {table}"
        return self._execute_q(expr)
    
    # =========================================================================
    # Join Queries (q syntax)
    # =========================================================================
    
    def _task_inner_join(self, params: dict[str, Any]) -> AdapterResult:
        """Inner join on id1, id2"""
        left_table = params.get("left_table", "x")
        right_table = params.get("right_table", "y")
        # ij = inner join, need to key the right table first
        expr = f"{left_table} ij `id1`id2 xkey {right_table}"
        return self._execute_q(expr)
    
    def _task_left_join(self, params: dict[str, Any]) -> AdapterResult:
        """Left join on id1, id2"""
        left_table = params.get("left_table", "x")
        right_table = params.get("right_table", "y")
        # lj = left join, need to key the right table first
        expr = f"{left_table} lj `id1`id2 xkey {right_table}"
        return self._execute_q(expr)
    
    def _task_eval(self, params: dict[str, Any]) -> AdapterResult:
        """Execute arbitrary q expression."""
        expr = params.get("expr")
        if not expr:
            return AdapterResult(
                execution_time_ns=0,
                row_count=0,
                success=False,
                error_message="No expression provided",
            )
        return self._execute_q(expr)
