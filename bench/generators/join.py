"""Join benchmark data generator (canonical H2O.ai schema).

Two tables, both 7 columns. Schema mirrors the canonical H2O J1 dataset:

  id1, id2  int64   range [1, K]              (cardinality K, default 100)
  id3       int64   range [1, max(n//K, K)]   (high cardinality)
  id4, id5  string  cardinality K
  id6       string  cardinality max(n//K, K)
  v1 (left) / v2 (right)  float64  [0, 100)

Note the spread is deliberately mirrored vs. the groupby table: integer
keys + string side columns. This stresses different join paths than
the groupby workload.

Determinism: PCG64 + SHA256 manifest, same as groupby.
"""

from dataclasses import dataclass

import numpy as np
import pyarrow as pa

from .base import DataGenerator, GeneratedDataset


@dataclass
class JoinGenerator(DataGenerator):
    n_rows_left: int = 10_000_000
    n_rows_right: int = 10_000_000
    k: int = 100
    null_pct: float = 0.0

    def generate(self) -> GeneratedDataset:
        left = self._build_side(self.n_rows_left, "v1", self.seed)
        right = self._build_side(self.n_rows_right, "v2", self.seed + 1)

        name = (
            f"join_{self._format_size(self.n_rows_left)}"
            f"x{self._format_size(self.n_rows_right)}"
        )
        if self.null_pct > 0:
            name += f"_na{int(self.null_pct * 100)}"

        return GeneratedDataset(
            name=name,
            tables={"left": left, "right": right},
            metadata={
                "generator": "join",
                "schema_version": "h2o-canonical-v1",
                "n_rows_left": self.n_rows_left,
                "n_rows_right": self.n_rows_right,
                "k": self.k,
                "null_pct": self.null_pct,
                "seed": self.seed,
                "schema": {
                    "id1": f"int64 [1, {self.k}]",
                    "id2": f"int64 [1, {self.k}]",
                    "id3": "int64 high-cardinality",
                    "id4": f"string cardinality {self.k}",
                    "id5": f"string cardinality {self.k}",
                    "id6": "string high-cardinality",
                    "v":  "float64 [0, 100)",
                },
            },
        )

    def _build_side(self, n: int, vcol: str, seed: int) -> pa.Table:
        k = self.k
        n_high = max(n // k, k)
        rng = np.random.Generator(np.random.PCG64(seed))

        id1 = rng.integers(1, k + 1, n, dtype=np.int64)
        id2 = rng.integers(1, k + 1, n, dtype=np.int64)
        id3 = rng.integers(1, n_high + 1, n, dtype=np.int64)

        k_width = len(str(k))
        nh_width = len(str(n_high))
        id_low_pool = np.array([f"id{i:0{k_width}d}" for i in range(1, k + 1)])
        id_high_pool = np.array([f"id{i:0{nh_width}d}" for i in range(1, n_high + 1)])

        id4 = id_low_pool[rng.integers(0, k, n)]
        id5 = id_low_pool[rng.integers(0, k, n)]
        id6 = id_high_pool[rng.integers(0, n_high, n)]

        v = np.round(rng.uniform(0.0, 100.0, n), 6)

        if self.null_pct > 0:
            cols = {
                "id1": self._with_nulls(id1, self.null_pct),
                "id2": self._with_nulls(id2, self.null_pct),
                "id3": self._with_nulls(id3, self.null_pct),
                "id4": self._with_nulls(id4, self.null_pct),
                "id5": self._with_nulls(id5, self.null_pct),
                "id6": self._with_nulls(id6, self.null_pct),
                vcol:  self._with_nulls(v,   self.null_pct),
            }
        else:
            cols = {
                "id1": pa.array(id1, type=pa.int64()),
                "id2": pa.array(id2, type=pa.int64()),
                "id3": pa.array(id3, type=pa.int64()),
                "id4": pa.array(id4, type=pa.string()),
                "id5": pa.array(id5, type=pa.string()),
                "id6": pa.array(id6, type=pa.string()),
                vcol:  pa.array(v,   type=pa.float64()),
            }
        return pa.table(cols)

    @staticmethod
    def _format_size(n: int) -> str:
        if n >= 1_000_000_000:
            return f"{n // 1_000_000_000}b"
        if n >= 1_000_000:
            return f"{n // 1_000_000}m"
        if n >= 1_000:
            return f"{n // 1_000}k"
        return str(n)
