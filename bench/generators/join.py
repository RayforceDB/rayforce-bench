"""Join benchmark data generator (H2OAI style)."""

from dataclasses import dataclass

import numpy as np
import pyarrow as pa

from .base import DataGenerator, GeneratedDataset


@dataclass
class JoinGenerator(DataGenerator):
    """Generate H2OAI-style join benchmark data.

    Creates two tables:
    - left: Main table with id columns and values
    - right: Lookup table with smaller cardinality

    Based on: https://h2oai.github.io/db-benchmark/
    """

    n_rows_left: int = 10_000_000
    n_rows_right: int = 1_000_000
    null_pct: float = 0.0

    def generate(self) -> GeneratedDataset:
        n_left = self.n_rows_left
        n_right = self.n_rows_right

        # Left table: fact table
        # id1: matches right table keys
        # id2: some overlap with right table
        # id3: no overlap (for testing outer joins)
        left_id1 = self._random_integers(n_left, 1, n_right + 1)
        left_id2 = self._random_integers(n_left, 1, n_right + 1)
        left_id3 = self._random_integers(n_left, n_right + 1, n_right * 2 + 1)
        left_v1 = self._random_floats(n_left, 0.0, 100.0)

        # Right table: dimension table
        right_id1 = np.arange(1, n_right + 1)  # Unique keys
        right_id2 = self._random_integers(n_right, 1, n_right + 1)  # Some duplicates
        right_id3 = self._random_integers(n_right, 1, n_right + 1)
        right_v2 = self._random_floats(n_right, 0.0, 100.0)

        # Build tables
        if self.null_pct > 0:
            left_table = pa.table({
                "id1": self._with_nulls(left_id1, self.null_pct),
                "id2": self._with_nulls(left_id2, self.null_pct),
                "id3": self._with_nulls(left_id3, self.null_pct),
                "v1": self._with_nulls(left_v1, self.null_pct),
            })
            right_table = pa.table({
                "id1": right_id1,  # Keep keys non-null
                "id2": self._with_nulls(right_id2, self.null_pct),
                "id3": self._with_nulls(right_id3, self.null_pct),
                "v2": self._with_nulls(right_v2, self.null_pct),
            })
        else:
            left_table = pa.table({
                "id1": left_id1,
                "id2": left_id2,
                "id3": left_id3,
                "v1": left_v1,
            })
            right_table = pa.table({
                "id1": right_id1,
                "id2": right_id2,
                "id3": right_id3,
                "v2": right_v2,
            })

        name = f"join_{self._format_size(n_left)}x{self._format_size(n_right)}"
        if self.null_pct > 0:
            name += f"_na{int(self.null_pct * 100)}"

        return GeneratedDataset(
            name=name,
            tables={
                "left": left_table,
                "right": right_table,
            },
            metadata={
                "generator": "join",
                "n_rows_left": n_left,
                "n_rows_right": n_right,
                "null_pct": self.null_pct,
                "seed": self.seed,
                "tables": {
                    "left": {
                        "description": "Fact table",
                        "schema": {
                            "id1": f"int64 [1, {n_right}] - matches right.id1",
                            "id2": f"int64 [1, {n_right}] - partial overlap",
                            "id3": f"int64 [{n_right + 1}, {n_right * 2}] - no overlap",
                            "v1": "float64 [0, 100)",
                        }
                    },
                    "right": {
                        "description": "Dimension table",
                        "schema": {
                            "id1": f"int64 [1, {n_right}] - unique keys",
                            "id2": f"int64 [1, {n_right}] - some duplicates",
                            "id3": f"int64 [1, {n_right}]",
                            "v2": "float64 [0, 100)",
                        }
                    }
                }
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
