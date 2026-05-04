"""GroupBy benchmark data generator (canonical H2O.ai db-benchmark schema).

Schema (9 columns) matches https://h2oai.github.io/db-benchmark/ exactly,
so any external H2O dataset is consumable, and so the same .rfl scripts
that ship in ~/rayforce/bench/h2o/*.rfl can be pointed at our files.

  id1, id2  string  cardinality K            (default K=100, e.g. "id001"..)
  id3       string  cardinality max(n//K, K) (e.g. "id000000001"..)
  id4, id5  int64   range [1, K]
  id6       int64   range [1, max(n//K, K)]
  v1        int64   range [1, 5]
  v2        int64   range [1, 15]
  v3        float64 range [0, 100), 6 decimals

Determinism: PCG64 with explicit seed (stable across numpy versions ≥1.17),
plus SHA256 of the CSV in manifest.json so users on different machines
can verify byte-identical inputs.
"""

from dataclasses import dataclass

import numpy as np
import pyarrow as pa

from .base import DataGenerator, GeneratedDataset


@dataclass
class GroupByGenerator(DataGenerator):
    n_rows: int = 10_000_000
    k: int = 100
    null_pct: float = 0.0

    def generate(self) -> GeneratedDataset:
        n = self.n_rows
        k = self.k
        n_high = max(n // k, k)

        # PCG64 produces the same sequence on every machine for a given
        # seed. default_rng() also uses PCG64 since numpy 1.17, but we
        # construct it explicitly so the choice survives any future
        # default change.
        rng = np.random.Generator(np.random.PCG64(self.seed))

        # ── id1, id2: low-cardinality strings "id001".."id<K>" ──────────
        k_width = len(str(k))
        id_low_pool = np.array([f"id{i:0{k_width}d}" for i in range(1, k + 1)])
        id1 = id_low_pool[rng.integers(0, k, n)]
        id2 = id_low_pool[rng.integers(0, k, n)]

        # ── id3: high-cardinality strings "id00000000001".."id<n_high>" ─
        nh_width = len(str(n_high))
        id_high_pool = np.array(
            [f"id{i:0{nh_width}d}" for i in range(1, n_high + 1)]
        )
        id3 = id_high_pool[rng.integers(0, n_high, n)]

        # ── id4..id6: integers ───────────────────────────────────────────
        id4 = rng.integers(1, k + 1, n, dtype=np.int64)
        id5 = rng.integers(1, k + 1, n, dtype=np.int64)
        id6 = rng.integers(1, n_high + 1, n, dtype=np.int64)

        # ── v1, v2: small integers; v3: float in [0,100) rounded to 6dp ─
        v1 = rng.integers(1, 6, n, dtype=np.int64)
        v2 = rng.integers(1, 16, n, dtype=np.int64)
        v3 = np.round(rng.uniform(0.0, 100.0, n), 6)

        if self.null_pct > 0:
            cols = {
                "id1": self._with_nulls(id1, self.null_pct),
                "id2": self._with_nulls(id2, self.null_pct),
                "id3": self._with_nulls(id3, self.null_pct),
                "id4": self._with_nulls(id4, self.null_pct),
                "id5": self._with_nulls(id5, self.null_pct),
                "id6": self._with_nulls(id6, self.null_pct),
                "v1":  self._with_nulls(v1,  self.null_pct),
                "v2":  self._with_nulls(v2,  self.null_pct),
                "v3":  self._with_nulls(v3,  self.null_pct),
            }
        else:
            cols = {
                "id1": pa.array(id1, type=pa.string()),
                "id2": pa.array(id2, type=pa.string()),
                "id3": pa.array(id3, type=pa.string()),
                "id4": pa.array(id4, type=pa.int64()),
                "id5": pa.array(id5, type=pa.int64()),
                "id6": pa.array(id6, type=pa.int64()),
                "v1":  pa.array(v1,  type=pa.int64()),
                "v2":  pa.array(v2,  type=pa.int64()),
                "v3":  pa.array(v3,  type=pa.float64()),
            }

        table = pa.table(cols)

        name = f"groupby_{self._format_size(n)}_k{k}"
        if self.null_pct > 0:
            name += f"_na{int(self.null_pct * 100)}"

        return GeneratedDataset(
            name=name,
            tables={"data": table},
            metadata={
                "generator": "groupby",
                "schema_version": "h2o-canonical-v1",
                "n_rows": n,
                "k": k,
                "n_high": n_high,
                "null_pct": self.null_pct,
                "seed": self.seed,
                "schema": {
                    "id1": f"string (cardinality {k})",
                    "id2": f"string (cardinality {k})",
                    "id3": f"string (cardinality {n_high})",
                    "id4": f"int64 [1, {k}]",
                    "id5": f"int64 [1, {k}]",
                    "id6": f"int64 [1, {n_high}]",
                    "v1":  "int64 [1, 5]",
                    "v2":  "int64 [1, 15]",
                    "v3":  "float64 [0, 100), 6 decimals",
                },
            },
        )

    @staticmethod
    def _format_size(n: int) -> str:
        if n >= 1_000_000_000:
            return f"{n // 1_000_000_000}b"
        if n >= 1_000_000:
            return f"{n // 1_000_000}m"
        if n >= 1_000:
            return f"{n // 1_000}k"
        return str(n)
