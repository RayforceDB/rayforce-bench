"""Canonical H2O J1 join data generator.

Reproduces the schema and key-distribution conventions of
h2oai/db-benchmark J1 datasets — i.e. one main table `x` plus three
right-side tables of progressively richer schema and varying row count
(`small`, `medium`, `big`). The 5 canonical join queries each pick a
specific right table and key column:

  q1: x.join(small,  on="id1")          — int key, n_small rows
  q2: x.join(medium, on="id2")          — int key, N/1000 rows
  q3: x.join(medium, on="id2", left)    — int key, left join
  q4: x.join(medium, on="id5")          — string key
  q5: x.join(big,    on="id3")          — int key, N rows

Schema (canonical H2O J1):
  x      : id1, id2, id3 (i64), id4, id5, id6 (str), v1 (f64)
  small  : id1            (i64), id4           (str), v2 (f64)
  medium : id1, id2       (i64), id4, id5      (str), v2 (f64)
  big    : id1, id2, id3  (i64), id4, id5, id6 (str), v2 (f64)

Key-distribution invariant (matches H2O J1):
  - right-side join columns are UNIQUE permutations of [1..n] —
    `small.id1`, `medium.id2`, `medium.id5`, `big.id3`, `big.id6`.
  - x's join columns are sampled (with replacement) from the same
    universe as their target right table.
  - therefore every row in `x` matches AT MOST one row on the right,
    so q1..q5 produce ≤ |x| rows. (Without uniqueness the result
    explodes by the duplication factor and OOMs at moderate N.)

Determinism: PCG64 with documented seeds.
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
        k: kept for path-naming compatibility (`..._k100`); does not
            affect distributions — those are derived from n_small /
            n_medium / n_big per H2O J1.
    """
    n_rows: int = 10_000_000
    k: int = 100

    def generate(self) -> GeneratedDataset:
        N = self.n_rows
        n_small  = 1_000 if N >= 1_000 else max(1, N // 10)
        n_medium = max(N // 1_000, n_small)
        n_big    = N

        x      = self._build_x(N, n_small, n_medium, n_big, self.seed)
        small  = self._build_small(n_small,            self.seed + 1)
        medium = self._build_medium(n_medium, n_small, self.seed + 2)
        big    = self._build_big(n_big, n_small, n_medium, self.seed + 3)

        name = f"join_canonical_{self._format_size(N)}_k{self.k}"
        return GeneratedDataset(
            name=name,
            tables={"x": x, "small": small, "medium": medium, "big": big},
            metadata={
                "generator": "join_canonical_h2o",
                "schema_version": "h2o-canonical-j1-v2",
                "n_rows_x": N,
                "n_rows_small": n_small,
                "n_rows_medium": n_medium,
                "n_rows_big": n_big,
                "k": self.k,
                "seed": self.seed,
            },
        )

    @staticmethod
    def _str_pool(n: int) -> np.ndarray:
        """Pre-format n zero-padded id strings: id001, id002, ..."""
        width = max(3, len(str(n)))
        return np.array([f"id{i:0{width}d}" for i in range(1, n + 1)])

    def _build_x(self, n: int, n_small: int, n_medium: int, n_big: int,
                 seed: int) -> pa.Table:
        rng = np.random.Generator(np.random.PCG64(seed))

        # int keys — sampled (with replacement) from each right-side
        # universe so every x row has at most one match.
        id1 = rng.integers(1, n_small  + 1, n, dtype=np.int64)
        id2 = rng.integers(1, n_medium + 1, n, dtype=np.int64)
        id3 = rng.integers(1, n_big    + 1, n, dtype=np.int64)

        # str keys — sampled from string universes of the same size.
        id4 = self._str_pool(n_small) [rng.integers(0, n_small,  n)]
        id5 = self._str_pool(n_medium)[rng.integers(0, n_medium, n)]
        id6 = self._str_pool(n_big)   [rng.integers(0, n_big,    n)]

        v1 = np.round(rng.uniform(0.0, 100.0, n), 6)

        return pa.table({
            "id1": pa.array(id1, type=pa.int64()),
            "id2": pa.array(id2, type=pa.int64()),
            "id3": pa.array(id3, type=pa.int64()),
            "id4": pa.array(id4, type=pa.string()),
            "id5": pa.array(id5, type=pa.string()),
            "id6": pa.array(id6, type=pa.string()),
            "v1":  pa.array(v1,  type=pa.float64()),
        })

    def _build_small(self, n: int, seed: int) -> pa.Table:
        """small: unique id1 (perm of [1..n]) + str id4 + v2."""
        rng = np.random.Generator(np.random.PCG64(seed))
        id1 = rng.permutation(n).astype(np.int64) + 1
        id4 = self._str_pool(n)[rng.permutation(n)]
        v2  = np.round(rng.uniform(0.0, 100.0, n), 6)
        return pa.table({
            "id1": pa.array(id1, type=pa.int64()),
            "id4": pa.array(id4, type=pa.string()),
            "v2":  pa.array(v2,  type=pa.float64()),
        })

    def _build_medium(self, n: int, n_small: int, seed: int) -> pa.Table:
        """medium: unique id2 + unique id5; id1, id4 sampled from small universe."""
        rng = np.random.Generator(np.random.PCG64(seed))
        id1 = rng.integers(1, n_small + 1, n, dtype=np.int64)
        id2 = rng.permutation(n).astype(np.int64) + 1
        id4 = self._str_pool(n_small)[rng.integers(0, n_small, n)]
        id5 = self._str_pool(n)[rng.permutation(n)]
        v2  = np.round(rng.uniform(0.0, 100.0, n), 6)
        return pa.table({
            "id1": pa.array(id1, type=pa.int64()),
            "id2": pa.array(id2, type=pa.int64()),
            "id4": pa.array(id4, type=pa.string()),
            "id5": pa.array(id5, type=pa.string()),
            "v2":  pa.array(v2,  type=pa.float64()),
        })

    def _build_big(self, n: int, n_small: int, n_medium: int,
                   seed: int) -> pa.Table:
        """big: unique id3 + unique id6; id1/id2/id4/id5 sampled from
        small/medium universes."""
        rng = np.random.Generator(np.random.PCG64(seed))
        id1 = rng.integers(1, n_small  + 1, n, dtype=np.int64)
        id2 = rng.integers(1, n_medium + 1, n, dtype=np.int64)
        id3 = rng.permutation(n).astype(np.int64) + 1
        id4 = self._str_pool(n_small) [rng.integers(0, n_small,  n)]
        id5 = self._str_pool(n_medium)[rng.integers(0, n_medium, n)]
        id6 = self._str_pool(n)[rng.permutation(n)]
        v2  = np.round(rng.uniform(0.0, 100.0, n), 6)
        return pa.table({
            "id1": pa.array(id1, type=pa.int64()),
            "id2": pa.array(id2, type=pa.int64()),
            "id3": pa.array(id3, type=pa.int64()),
            "id4": pa.array(id4, type=pa.string()),
            "id5": pa.array(id5, type=pa.string()),
            "id6": pa.array(id6, type=pa.string()),
            "v2":  pa.array(v2,  type=pa.float64()),
        })

    @staticmethod
    def _format_size(n: int) -> str:
        if n >= 1_000_000_000:
            return f"{n // 1_000_000_000}b"
        if n >= 1_000_000:
            return f"{n // 1_000_000}m"
        if n >= 1_000:
            return f"{n // 1_000}k"
        return str(n)
