"""Base adapter for benchmarks."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import time


def _dedupe_col_names(cols: list[str]) -> list[str]:
    """Rename duplicate column names to `<name>_<n>` (n >= 1).

    Some engines' SQL `JOIN ... USING(key)` only deduplicates the key
    column; non-key cols with the same name on both sides remain dup.
    polars rejects DataFrames with dup col names, so adapter materialize
    methods that build a polars DataFrame from raw cursor descriptions
    must dedupe first. The `_<n>` suffix is recognised by check.py's
    drop-suffix normalizer, so cross-engine comparison still passes.
    """
    seen: dict[str, int] = {}
    out: list[str] = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c}_{seen[c]}")
    return out


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
        """Q4: mean(v1), mean(v2), mean(v3) by id4 — canonical H2O."""
        pass

    @abstractmethod
    def run_groupby_q5(self) -> BenchmarkResult:
        """Q5: sum(v1), sum(v2), sum(v3) by id6 — canonical H2O."""
        pass

    @abstractmethod
    def run_groupby_q6(self) -> BenchmarkResult:
        """Q6: median(v3), sd(v3) by id4, id5 — canonical H2O."""
        pass

    @abstractmethod
    def run_groupby_q7(self) -> BenchmarkResult:
        """Q7: max(v1) - min(v2) by id3 — canonical H2O."""
        pass

    @abstractmethod
    def run_groupby_q8(self) -> BenchmarkResult:
        """Q8: largest two v3 by id6 — canonical H2O."""
        pass

    @abstractmethod
    def run_groupby_q9(self) -> BenchmarkResult:
        """Q9: corr(v1, v2)^2 by id2, id4 — canonical H2O."""
        pass

    @abstractmethod
    def run_groupby_q10(self) -> BenchmarkResult:
        """Q10: sum(v3), count(v1) by id1..id6 — canonical H2O."""
        pass

    @abstractmethod
    def run_join_inner(self, right_path: Path) -> BenchmarkResult:
        """Inner join on (id1, id2, id3) — bonus 3-key stress test."""
        pass

    @abstractmethod
    def run_join_left(self, right_path: Path) -> BenchmarkResult:
        """Left join on (id1, id2, id3) — bonus 3-key stress test."""
        pass

    # Canonical H2O J1 join queries.
    # Adapter is expected to have all four tables — `x`, `small`,
    # `medium`, `big` — pre-loaded via load_canonical_join() before any
    # of these are called.

    @abstractmethod
    def run_join_q1(self) -> BenchmarkResult:
        """Q1: x.join(small, on=id1) — int key, small (1e3) right."""
        pass

    @abstractmethod
    def run_join_q2(self) -> BenchmarkResult:
        """Q2: x.join(medium, on=id2) — int key, medium (N/1e3) right."""
        pass

    @abstractmethod
    def run_join_q3(self) -> BenchmarkResult:
        """Q3: x.join(medium, on=id2, how=left) — left, int key."""
        pass

    @abstractmethod
    def run_join_q4(self) -> BenchmarkResult:
        """Q4: x.join(medium, on=id5) — string key, medium right."""
        pass

    @abstractmethod
    def run_join_q5(self) -> BenchmarkResult:
        """Q5: x.join(big, on=id3) — int key, big (N) right."""
        pass

    def load_canonical_join(self, data_dir: Path) -> None:
        """Load the 4 canonical H2O J1 tables (x, small, medium, big)
        from <data_dir>/{x,small,medium,big}.csv.

        Default implementation calls load_data() four times. Adapters
        with single-connection setup can override.
        """
        self.load_data(data_dir / "x.csv", "x")
        self.load_data(data_dir / "small.csv", "small")
        self.load_data(data_dir / "medium.csv", "medium")
        self.load_data(data_dir / "big.csv", "big")

    @abstractmethod
    def run_sort_single(self) -> BenchmarkResult:
        """Sort by single column — bonus."""
        pass

    @abstractmethod
    def run_sort_multi(self) -> BenchmarkResult:
        """Sort by multiple columns — bonus."""
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

    def materialize(self, op: str, right_path: Path | None = None):
        """Run an op and return the materialized result as a polars DataFrame.

        Used by `make check` to verify cross-adapter result equivalence.
        Bench timing path (`run_<op>`) does NOT call this — they exist
        side-by-side, so changes here cannot affect bench numbers.

        Default raises NotImplementedError; each adapter overrides with
        its own dispatch table covering the 11 H2O ops.
        """
        raise NotImplementedError(
            f"{self.name} does not implement materialize(); "
            f"cannot participate in `make check`"
        )

    def run_full(self, bench_name: str, n_warmup: int, n_iter: int,
                 right_path: Path | None = None) -> list[BenchmarkResult]:
        """Run warmup + measured iterations for a single benchmark.

        Default implementation invokes the bench method n_warmup + n_iter
        times. Adapters that need to perform warmup and iterations in a
        single external invocation override this method.
        """
        method = getattr(self, f"run_{bench_name}")
        # Canonical join_q1..q5 take no path arg — tables are pre-loaded
        # via load_canonical_join(). Bonus join_inner/join_left take a
        # right_path arg.
        is_canonical_join = (bench_name.startswith("join_q")
                             and bench_name[len("join_q"):].isdigit())
        if bench_name.startswith("join_") and not is_canonical_join:
            if right_path is None:
                raise ValueError(f"{bench_name} requires right_path")
            invoke = lambda: method(right_path)
        else:
            invoke = method
        for _ in range(n_warmup):
            invoke()
        return [invoke() for _ in range(n_iter)]
