"""Sort benchmark data generator."""

from dataclasses import dataclass

import numpy as np
import pyarrow as pa

from .base import DataGenerator, GeneratedDataset


@dataclass
class SortGenerator(DataGenerator):
    """Generate data for sort benchmarks.

    Creates a table with id1, id2, id3 columns matching the groupby
    schema for compatibility with sort benchmark queries.
    """

    n_rows: int = 10_000_000
    k: int = 100  # Cardinality for id columns
    null_pct: float = 0.0

    def generate(self) -> GeneratedDataset:
        n = self.n_rows
        k = self.k

        # Use integer id columns for sorting (same schema as groupby)
        columns = {
            "id1": pa.array(self.rng.integers(1, k + 1, size=n), type=pa.int64()),
            "id2": pa.array(self.rng.integers(1, k + 1, size=n), type=pa.int64()),
            "id3": pa.array(self.rng.integers(1, k + 1, size=n), type=pa.int64()),
            "v1": pa.array(self.rng.integers(1, 6, size=n), type=pa.int64()),
            "v2": pa.array(self.rng.integers(1, 16, size=n), type=pa.int64()),
            "v3": pa.array(self.rng.uniform(0, 100, size=n), type=pa.float64()),
        }

        table = pa.table(columns)

        name = f"sort_{self._format_size(n)}_k{k}"
        if self.null_pct > 0:
            name += f"_na{int(self.null_pct * 100)}"

        return GeneratedDataset(
            name=name,
            tables={"data": table},
            metadata={
                "generator": "sort",
                "n_rows": n,
                "k": k,
                "null_pct": self.null_pct,
                "seed": self.seed,
            }
        )

    def _format_size(self, n: int) -> str:
        """Format size for naming."""
        if n >= 1_000_000_000:
            return f"{n // 1_000_000_000}b"
        elif n >= 1_000_000:
            return f"{n // 1_000_000}m"
        elif n >= 1_000:
            return f"{n // 1_000}k"
        else:
            return str(n)
