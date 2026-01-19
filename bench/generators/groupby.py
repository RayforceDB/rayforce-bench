"""GroupBy benchmark data generator (H2OAI style)."""

from dataclasses import dataclass

import numpy as np
import pyarrow as pa

from .base import DataGenerator, GeneratedDataset


@dataclass
class GroupByGenerator(DataGenerator):
    """Generate H2OAI-style groupby benchmark data.

    Creates a table with:
    - id1-id3: low cardinality integer columns (K unique values)
    - v1: float values (normal distribution)
    - v2: float values (normal distribution)
    - v3: float values (normal distribution)

    Based on: https://h2oai.github.io/db-benchmark/
    """

    n_rows: int = 10_000_000
    k: int = 100  # Low cardinality (number of groups)
    null_pct: float = 0.0

    def generate(self) -> GeneratedDataset:
        n = self.n_rows
        k = self.k
        rng = np.random.default_rng(self.seed)

        # Integer id columns (K unique values each, range [1, k])
        id1 = pa.array(rng.integers(1, k + 1, n), type=pa.int64())
        id2 = pa.array(rng.integers(1, k + 1, n), type=pa.int64())
        id3 = pa.array(rng.integers(1, k + 1, n), type=pa.int64())

        # Float value columns (standard normal distribution)
        v1 = pa.array(rng.standard_normal(n), type=pa.float64())
        v2 = pa.array(rng.standard_normal(n), type=pa.float64())
        v3 = pa.array(rng.standard_normal(n), type=pa.float64())

        # Build table
        table = pa.table({
            "id1": id1,
            "id2": id2,
            "id3": id3,
            "v1": v1,
            "v2": v2,
            "v3": v3,
        })

        name = f"groupby_{self._format_size(n)}_k{k}"

        return GeneratedDataset(
            name=name,
            tables={"data": table},
            metadata={
                "generator": "groupby",
                "n_rows": n,
                "k": k,
                "seed": self.seed,
                "schema": {
                    "id1": f"int64 (K={k} unique)",
                    "id2": f"int64 (K={k} unique)",
                    "id3": f"int64 (K={k} unique)",
                    "v1": "float64 (normal dist)",
                    "v2": "float64 (normal dist)",
                    "v3": "float64 (normal dist)",
                }
            }
        )

    def _format_size(self, n: int) -> str:
        """Format size for naming: 1000000 -> 1m, 1000000000 -> 1b."""
        if n >= 1_000_000_000:
            return f"{n // 1_000_000_000}b"
        elif n >= 1_000_000:
            return f"{n // 1_000_000}m"
        elif n >= 1_000:
            return f"{n // 1_000}k"
        else:
            return str(n)
