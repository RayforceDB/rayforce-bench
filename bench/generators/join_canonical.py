"""Canonical H2O J1 join data generator.

Reproduces the schema and key-distribution conventions of
h2oai/db-benchmark J1 datasets — i.e. one main table `x` plus three
right-side tables of progressively richer schema and varying row count
(`small`, `medium`, `big`). The 5 canonical join queries each pick a
specific right table and key column:

  q1: x.join(small,  on="id1")          — int key, 1e3 rows
  q2: x.join(medium, on="id2")          — int key, N/1e3 rows
  q3: x.join(medium, on="id2", left)    — int key, left join
  q4: x.join(medium, on="id5")          — string key
  q5: x.join(big,    on="id3")          — int key, N rows

Schema (canonical H2O J1):
  x      : id1, id2, id3 (i64), id4, id5, id6 (str), v1 (f64)
  small  : id1            (i64), id4           (str), v2 (f64)
  medium : id1, id2       (i64), id4, id5      (str), v2 (f64)
  big    : id1, id2, id3  (i64), id4, id5, id6 (str), v2 (f64)

Determinism: PCG64 with documented seeds. Right-side row indices are
drawn from the same key universe as `x` so non-zero match rate is
guaranteed without saturation.
"""

from dataclasses import dataclass

import numpy as np
import pyarrow as pa

from .base import DataGenerator, GeneratedDataset


@dataclass
class CanonicalJoinGenerator(DataGenerator):
    """Generates 4 tables for canonical H2O J1 benchmark.

    Args:
        n_rows: size of main `x` table (e.g. 1e7). Right-side sizes
            follow H2O convention: small=1e3, medium=N/1e3, big=N.
        k: low-cardinality key range [1, k] for id1, id2 and string
            cardinality for id4, id5.
    """
    n_rows: int = 10_000_000
    k: int = 100

    def generate(self) -> GeneratedDataset:
        N = self.n_rows
        n_small = min(1_000, N) if N >= 1_000 else max(1, N // 10)
        n_medium = max(N // 1_000, n_small) if N >= 1_000 else max(1, N // 10)
        n_big = N

        # x:      id1, id2, id3 (i64), id4, id5, id6 (str), v1 (f64)
        x      = self._build_side(N,        "v1", self.seed,     have=("id1","id2","id3","id4","id5","id6"))
        # right tables progressively richer.
        small  = self._build_side(n_small,  "v2", self.seed + 1, have=("id1","id4"))
        medium = self._build_side(n_medium, "v2", self.seed + 2, have=("id1","id2","id4","id5"))
        big    = self._build_side(n_big,    "v2", self.seed + 3, have=("id1","id2","id3","id4","id5","id6"))

        name = f"join_canonical_{self._format_size(N)}_k{self.k}"
        return GeneratedDataset(
            name=name,
            tables={"x": x, "small": small, "medium": medium, "big": big},
            metadata={
                "generator": "join_canonical_h2o",
                "schema_version": "h2o-canonical-j1-v1",
                "n_rows_x": N,
                "n_rows_small": n_small,
                "n_rows_medium": n_medium,
                "n_rows_big": n_big,
                "k": self.k,
                "seed": self.seed,
            },
        )

    def _build_side(self, n: int, vcol: str, seed: int,
                    have: tuple[str, ...]) -> pa.Table:
        """Build one table containing the requested non-key columns.

        `have` lists which of id1..id6 to include. v-column always
        included with name `vcol`.
        """
        k = self.k
        n_high = max(n // k, k) if k else n
        rng = np.random.Generator(np.random.PCG64(seed))

        # int keys are uniform over [1, k] (low-card) or [1, n_high]
        id1_v = rng.integers(1, k + 1, n, dtype=np.int64) if "id1" in have else None
        id2_v = rng.integers(1, k + 1, n, dtype=np.int64) if "id2" in have else None
        id3_v = rng.integers(1, n_high + 1, n, dtype=np.int64) if "id3" in have else None

        k_width = len(str(k))
        nh_width = len(str(n_high))
        id_low_pool = np.array([f"id{i:0{k_width}d}" for i in range(1, k + 1)])
        id_high_pool = np.array([f"id{i:0{nh_width}d}" for i in range(1, n_high + 1)])

        id4_v = id_low_pool[rng.integers(0, k, n)] if "id4" in have else None
        id5_v = id_low_pool[rng.integers(0, k, n)] if "id5" in have else None
        id6_v = id_high_pool[rng.integers(0, n_high, n)] if "id6" in have else None

        v_v = np.round(rng.uniform(0.0, 100.0, n), 6)

        cols: dict[str, pa.Array] = {}
        if id1_v is not None: cols["id1"] = pa.array(id1_v, type=pa.int64())
        if id2_v is not None: cols["id2"] = pa.array(id2_v, type=pa.int64())
        if id3_v is not None: cols["id3"] = pa.array(id3_v, type=pa.int64())
        if id4_v is not None: cols["id4"] = pa.array(id4_v, type=pa.string())
        if id5_v is not None: cols["id5"] = pa.array(id5_v, type=pa.string())
        if id6_v is not None: cols["id6"] = pa.array(id6_v, type=pa.string())
        cols[vcol] = pa.array(v_v, type=pa.float64())
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
