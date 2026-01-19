"""Rayforce-py (Python bindings) adapter for benchmarks.

Measures Python API overhead + rayforce core execution.
Supports both PyPI installation and local dev builds.
"""

from pathlib import Path
import subprocess
import sys

from .base import Adapter, BenchmarkResult


class RayforcePyAdapter(Adapter):
    """Benchmark adapter for rayforce-py Python bindings.

    Measures: Python API overhead + rayforce core execution time.

    Can use either:
    - PyPI installation (default)
    - Local dev build from ~/rayforce-py or custom path
    """

    name = "rayforce-py"

    def __init__(self, local_path: str | Path | None = None):
        """Initialize rayforce adapter.

        Args:
            local_path: Path to local rayforce-py repo for dev builds.
                       If None, uses installed package from PyPI.
        """
        self._local_path = Path(local_path) if local_path else None
        self._rayforce = None
        self._load_parquet = None
        self._tables: dict[str, object] = {}

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

        # Try uv first, fallback to pip
        result = subprocess.run(
            ["uv", "pip", "install", "-e", str(self._local_path), "--python", sys.executable],
            capture_output=True,
            text=True,
            cwd=self._local_path,
        )
        if result.returncode != 0:
            # Fallback to pip
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
        self._load_parquet = load_parquet
        self.version = f"{rayforce.version} (local: {self._local_path})"
        print(f"Using rayforce {self.version}")

    def load_data(self, path: Path, table_name: str = "data") -> None:
        self._tables[table_name] = self._load_parquet(str(path))

    def _get_table(self, name: str = "data"):
        if name not in self._tables:
            raise ValueError(f"Table '{name}' not loaded")
        return self._tables[name]

    def run_groupby_q1(self) -> BenchmarkResult:
        """Q1: sum(v1) group by id1"""
        rf = self._rayforce
        t = self._get_table()

        def query():
            return t.select(v1=rf.Column("v1").sum()).by("id1").execute()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q1", time_ns, len(result))

    def run_groupby_q2(self) -> BenchmarkResult:
        """Q2: sum(v1) group by id1, id2"""
        rf = self._rayforce
        t = self._get_table()

        def query():
            return t.select(v1=rf.Column("v1").sum()).by("id1", "id2").execute()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q2", time_ns, len(result))

    def run_groupby_q3(self) -> BenchmarkResult:
        """Q3: sum(v1), mean(v3) group by id3"""
        rf = self._rayforce
        t = self._get_table()

        def query():
            return t.select(
                v1=rf.Column("v1").sum(),
                v3=rf.Column("v3").mean()
            ).by("id3").execute()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q3", time_ns, len(result))

    def run_groupby_q4(self) -> BenchmarkResult:
        """Q4: mean(v1), mean(v2), mean(v3) group by id3"""
        rf = self._rayforce
        t = self._get_table()

        def query():
            return t.select(
                v1=rf.Column("v1").mean(),
                v2=rf.Column("v2").mean(),
                v3=rf.Column("v3").mean()
            ).by("id3").execute()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q4", time_ns, len(result))

    def run_groupby_q5(self) -> BenchmarkResult:
        """Q5: sum(v1), sum(v2), sum(v3) group by id3"""
        rf = self._rayforce
        t = self._get_table()

        def query():
            return t.select(
                v1=rf.Column("v1").sum(),
                v2=rf.Column("v2").sum(),
                v3=rf.Column("v3").sum()
            ).by("id3").execute()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("groupby_q5", time_ns, len(result))

    def run_join_inner(self, right_path: Path) -> BenchmarkResult:
        """Inner join on id1."""
        left = self._get_table("left")
        right = self._load_parquet(str(right_path))

        def query():
            return left.inner_join(right, on="id1").execute()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("join_inner", time_ns, len(result))

    def run_join_left(self, right_path: Path) -> BenchmarkResult:
        """Left join on id1."""
        left = self._get_table("left")
        right = self._load_parquet(str(right_path))

        def query():
            return left.left_join(right, on="id1").execute()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("join_left", time_ns, len(result))

    def run_sort_single(self) -> BenchmarkResult:
        """Sort by single column."""
        t = self._get_table()

        def query():
            return t.order_by("id1").execute()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("sort_single", time_ns, len(result))

    def run_sort_multi(self) -> BenchmarkResult:
        """Sort by multiple columns."""
        t = self._get_table()

        def query():
            return t.order_by("id1", "id2", "id3").execute()

        result, time_ns = self._time_it(query)
        return BenchmarkResult("sort_multi", time_ns, len(result))

    def close(self) -> None:
        self._tables.clear()
