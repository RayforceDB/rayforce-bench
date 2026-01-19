"""Rayforce native adapter for benchmarks.

Measures pure rayforce core execution time using eval_str("(timeit ...)").
No Python API overhead included in measurements.
"""

from pathlib import Path
import subprocess
import sys

from .base import Adapter, BenchmarkResult


class RayforceAdapter(Adapter):
    """Benchmark adapter for rayforce native core.

    Measures: Pure rayforce execution time (no Python overhead).

    Uses eval_str("(timeit ...)") to measure query execution time
    directly in the rayforce runtime.
    """

    name = "rayforce"

    def __init__(self, local_path: str | Path | None = None):
        """Initialize rayforce adapter.

        Args:
            local_path: Path to local rayforce-py repo for dev builds.
                       If None, uses installed package from PyPI.
        """
        self._local_path = Path(local_path) if local_path else None
        self._rayforce = None
        self._eval_str = None
        self._load_parquet = None
        self._table_names: dict[str, str] = {}  # Maps logical name to rayforce symbol

        self._setup_rayforce()

    def _setup_rayforce(self) -> None:
        """Setup rayforce module - either from PyPI or local build."""
        if self._local_path:
            self._setup_local_build()
        else:
            self._setup_pypi()

    def _setup_pypi(self) -> None:
        """Use rayforce from PyPI."""
        try:
            import rayforce
            from rayforce.plugins.parquet import load_parquet

            self._rayforce = rayforce
            self._eval_str = rayforce.eval_str
            self._load_parquet = load_parquet
            self.version = f"{rayforce.version} (pypi)"
        except ImportError as e:
            raise ImportError(
                "rayforce-py not installed. Install with: pip install rayforce-py"
            ) from e

    def _setup_local_build(self) -> None:
        """Build and use rayforce from local path."""
        if not self._local_path or not self._local_path.exists():
            raise ValueError(f"Local path does not exist: {self._local_path}")

        # Build the package using uv (faster) or pip
        print(f"Building rayforce-py from {self._local_path}...")

        result = subprocess.run(
            ["uv", "pip", "install", "-e", str(self._local_path), "--python", sys.executable],
            capture_output=True,
            text=True,
            cwd=self._local_path,
        )
        if result.returncode != 0:
            result = subprocess.run(
                [sys.executable, "-m", "pip", "install", "-e", str(self._local_path), "-q"],
                capture_output=True,
                text=True,
                cwd=self._local_path,
            )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to build rayforce-py: {result.stderr}")

        # Force reimport
        for mod in list(sys.modules.keys()):
            if mod.startswith("rayforce"):
                del sys.modules[mod]

        import rayforce
        from rayforce.plugins.parquet import load_parquet

        self._rayforce = rayforce
        self._eval_str = rayforce.eval_str
        self._load_parquet = load_parquet
        self.version = f"{rayforce.version} (local: {self._local_path})"
        print(f"Using rayforce {self.version}")

    def load_data(self, path: Path, table_name: str = "data") -> None:
        """Load data and save to rayforce runtime with a symbol name."""
        table = self._load_parquet(str(path))
        # Save table to rayforce runtime with a unique symbol name
        symbol_name = f"_bench_{table_name}"
        table.save(symbol_name)
        self._table_names[table_name] = symbol_name

    def _get_symbol(self, name: str = "data") -> str:
        """Get rayforce symbol name for a table."""
        if name not in self._table_names:
            raise ValueError(f"Table '{name}' not loaded")
        return self._table_names[name]

    def _run_timed_query(self, query: str, bench_name: str) -> BenchmarkResult:
        """Run a query with timeit and return result.

        Args:
            query: The rayforce query WITHOUT timeit wrapper
            bench_name: Name of the benchmark

        Returns:
            BenchmarkResult with timing in nanoseconds
        """
        timed_query = f"(timeit {query})"
        result = self._eval_str(timed_query)

        # timeit returns time in milliseconds, convert to nanoseconds
        if hasattr(result, "value"):
            time_ms = result.value
        elif hasattr(result, "to_python"):
            time_ms = result.to_python()
        else:
            time_ms = float(result)

        time_ns = int(time_ms * 1_000_000)  # milliseconds to nanoseconds

        # Get row count by running query without timeit
        result_table = self._eval_str(query)
        rows = len(result_table)

        return BenchmarkResult(bench_name, time_ns, rows)

    def run_groupby_q1(self) -> BenchmarkResult:
        """Q1: sum(v1) group by id1"""
        t = self._get_symbol()
        query = f"(select {{v1: (sum v1) by: id1 from: {t}}})"
        return self._run_timed_query(query, "groupby_q1")

    def run_groupby_q2(self) -> BenchmarkResult:
        """Q2: sum(v1) group by id1, id2"""
        t = self._get_symbol()
        query = f"(select {{v1: (sum v1) by: {{id1: id1 id2: id2}} from: {t}}})"
        return self._run_timed_query(query, "groupby_q2")

    def run_groupby_q3(self) -> BenchmarkResult:
        """Q3: sum(v1), mean(v3) group by id3"""
        t = self._get_symbol()
        query = f"(select {{v1: (sum v1) v3: (avg v3) by: id3 from: {t}}})"
        return self._run_timed_query(query, "groupby_q3")

    def run_groupby_q4(self) -> BenchmarkResult:
        """Q4: mean(v1), mean(v2), mean(v3) group by id3"""
        t = self._get_symbol()
        query = f"(select {{v1: (avg v1) v2: (avg v2) v3: (avg v3) by: id3 from: {t}}})"
        return self._run_timed_query(query, "groupby_q4")

    def run_groupby_q5(self) -> BenchmarkResult:
        """Q5: sum(v1), sum(v2), sum(v3) group by id3"""
        t = self._get_symbol()
        query = f"(select {{v1: (sum v1) v2: (sum v2) v3: (sum v3) by: id3 from: {t}}})"
        return self._run_timed_query(query, "groupby_q5")

    def run_join_inner(self, right_path: Path) -> BenchmarkResult:
        """Inner join on id1."""
        left_sym = self._get_symbol("left")
        # Load right table
        right_table = self._load_parquet(str(right_path))
        right_sym = "_bench_right_tmp"
        right_table.save(right_sym)

        query = f"(ij `id1 {left_sym} {right_sym})"
        return self._run_timed_query(query, "join_inner")

    def run_join_left(self, right_path: Path) -> BenchmarkResult:
        """Left join on id1."""
        left_sym = self._get_symbol("left")
        # Load right table
        right_table = self._load_parquet(str(right_path))
        right_sym = "_bench_right_tmp"
        right_table.save(right_sym)

        query = f"(lj `id1 {left_sym} {right_sym})"
        return self._run_timed_query(query, "join_left")

    def run_sort_single(self) -> BenchmarkResult:
        """Sort by single column."""
        t = self._get_symbol()
        query = f"(xasc {t} `id1)"
        return self._run_timed_query(query, "sort_single")

    def run_sort_multi(self) -> BenchmarkResult:
        """Sort by multiple columns."""
        t = self._get_symbol()
        query = f"(xasc {t} `id1`id2`id3)"
        return self._run_timed_query(query, "sort_multi")

    def close(self) -> None:
        self._table_names.clear()
