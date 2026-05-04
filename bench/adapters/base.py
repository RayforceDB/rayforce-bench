"""Base adapter for benchmarks."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import time


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""
    name: str
    time_ns: int  # Nanoseconds
    rows: int
    error: str | None = None

    @property
    def time_ms(self) -> float:
        return self.time_ns / 1_000_000

    @property
    def time_s(self) -> float:
        return self.time_ns / 1_000_000_000


class Adapter(ABC):
    """Base class for benchmark adapters.

    Each adapter implements benchmarks for a specific engine
    (polars, duckdb, rayforce, etc.)
    """

    name: str = "base"
    version: str = "unknown"

    @abstractmethod
    def load_data(self, path: Path, table_name: str = "data") -> None:
        """Load data from CSV file.

        Data should be materialized in memory for accurate query timing.

        Args:
            path: Path to CSV file
            table_name: Name to assign to the loaded table
        """
        pass

    @abstractmethod
    def run_groupby_q1(self) -> BenchmarkResult:
        """Q1: sum(v1) group by id1"""
        pass

    @abstractmethod
    def run_groupby_q2(self) -> BenchmarkResult:
        """Q2: sum(v1) group by id1, id2"""
        pass

    @abstractmethod
    def run_groupby_q3(self) -> BenchmarkResult:
        """Q3: sum(v1), mean(v3) group by id3"""
        pass

    @abstractmethod
    def run_groupby_q4(self) -> BenchmarkResult:
        """Q4: mean(v1), mean(v2), mean(v3) group by id3"""
        pass

    @abstractmethod
    def run_groupby_q5(self) -> BenchmarkResult:
        """Q5: sum(v1), sum(v2), sum(v3) group by id3"""
        pass

    @abstractmethod
    def run_groupby_q7(self) -> BenchmarkResult:
        """Q7: sum(v3), count(v1) group by id1..id6 (canonical H2O 6-key)."""
        pass

    @abstractmethod
    def run_join_inner(self, right_path: Path) -> BenchmarkResult:
        """Inner join on id1."""
        pass

    @abstractmethod
    def run_join_left(self, right_path: Path) -> BenchmarkResult:
        """Left join on id1."""
        pass

    @abstractmethod
    def run_sort_single(self) -> BenchmarkResult:
        """Sort by single column."""
        pass

    @abstractmethod
    def run_sort_multi(self) -> BenchmarkResult:
        """Sort by multiple columns."""
        pass

    def get_info(self) -> dict[str, Any]:
        """Get adapter info."""
        return {
            "name": self.name,
            "version": self.version,
        }

    def close(self) -> None:
        """Clean up resources."""
        pass

    def _time_it(self, func) -> tuple[Any, int]:
        """Time a function execution.

        Returns:
            Tuple of (result, time_ns)
        """
        start = time.perf_counter_ns()
        result = func()
        end = time.perf_counter_ns()
        return result, end - start

    def run_sort_typed_full(self, csv_path: Path, dtype: str,
                             n_warmup: int, n_iter: int) -> list[BenchmarkResult]:
        """Sort a typed single-column CSV (extended sort grid).

        Optional — adapters that participate in the sort grid override this.
        Default raises NotImplementedError so questdb/timescale, which we
        don't include in the grid, fail loudly if accidentally selected.
        """
        raise NotImplementedError(
            f"{self.name} does not implement run_sort_typed_full; "
            f"exclude it from --adapters when running sort-ext"
        )

    def run_full(self, bench_name: str, n_warmup: int, n_iter: int,
                 right_path: Path | None = None) -> list[BenchmarkResult]:
        """Run warmup + measured iterations for a single benchmark.

        Default implementation invokes the bench method n_warmup + n_iter
        times. Adapters that need to perform warmup and iterations in a
        single external invocation (e.g. rayforce .rfl runner) override
        this method.
        """
        method = getattr(self, f"run_{bench_name}")
        if bench_name.startswith("join_"):
            if right_path is None:
                raise ValueError(f"{bench_name} requires right_path")
            invoke = lambda: method(right_path)
        else:
            invoke = method
        for _ in range(n_warmup):
            invoke()
        return [invoke() for _ in range(n_iter)]
