"""Base data generator for benchmarks."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import json
import hashlib

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv


@dataclass
class GeneratedDataset:
    """Result of data generation."""
    name: str
    tables: dict[str, pa.Table]
    metadata: dict[str, Any]

    def write(self, output_dir: Path, formats: list[str] | None = None) -> dict[str, Path]:
        """Write dataset to disk and emit a manifest with SHA256 sums.

        The SHA256 in manifest.json lets a user on a different machine
        verify byte-identical inputs. Two runs with the same seed + n_rows
        + k must produce the same hash; if they don't, something has
        changed in the generator (numpy bit-generator, formatting,
        ordering) and benchmark numbers are no longer cross-comparable.
        """
        formats = formats or ["parquet"]
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        paths = {}
        file_info = {}

        for table_name, table in self.tables.items():
            table_paths = {}
            file_hashes = {}

            for fmt in formats:
                if fmt == "parquet":
                    path = output_dir / f"{table_name}.parquet"
                    pq.write_table(table, path, compression="zstd")
                elif fmt == "csv":
                    path = output_dir / f"{table_name}.csv"
                    csv.write_csv(table, path)
                else:
                    raise ValueError(f"Unknown format: {fmt}")

                table_paths[fmt] = str(path)
                file_hashes[fmt] = _sha256_file(path)

            paths[table_name] = table_paths
            file_info[table_name] = {
                "rows": table.num_rows,
                "columns": table.num_columns,
                "schema": {col.name: str(col.type) for col in table.schema},
                "files": table_paths,
                "sha256": file_hashes,
            }

        manifest = {
            "name": self.name,
            "tables": file_info,
            "metadata": self.metadata,
        }

        manifest_path = output_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        return paths


def _sha256_file(path: Path, chunk_size: int = 1 << 20) -> str:
    """SHA256 of a file, streamed so we don't blow up on multi-GB CSVs."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()


@dataclass
class DataGenerator(ABC):
    """Base class for data generators."""

    seed: int = 42

    def __post_init__(self):
        self.rng = np.random.default_rng(self.seed)

    @abstractmethod
    def generate(self) -> GeneratedDataset:
        """Generate the dataset.

        Returns:
            GeneratedDataset with tables and metadata
        """
        pass

    def _random_strings(self, prefix: str, n: int, cardinality: int) -> np.ndarray:
        """Generate random categorical strings.

        Args:
            prefix: Prefix for string values (e.g., 'id')
            n: Number of values to generate
            cardinality: Number of unique values

        Returns:
            Array of strings like 'id000001', 'id000042', etc.
        """
        values = self.rng.integers(1, cardinality + 1, size=n)
        width = len(str(cardinality))
        return np.array([f"{prefix}{v:0{width}d}" for v in values])

    def _random_integers(self, n: int, low: int, high: int) -> np.ndarray:
        """Generate random integers in [low, high)."""
        return self.rng.integers(low, high, size=n)

    def _random_floats(self, n: int, low: float = 0.0, high: float = 100.0) -> np.ndarray:
        """Generate random floats in [low, high)."""
        return self.rng.uniform(low, high, size=n)

    def _with_nulls(self, arr: np.ndarray, null_pct: float) -> pa.Array:
        """Add nulls to an array.

        Args:
            arr: Input numpy array
            null_pct: Percentage of nulls (0.0 to 1.0)

        Returns:
            PyArrow array with nulls
        """
        if null_pct <= 0:
            return pa.array(arr)

        mask = self.rng.random(len(arr)) < null_pct
        arr = arr.copy()
        if arr.dtype.kind in ('i', 'u', 'f'):
            # Numeric - convert to float for NaN support, then to PyArrow
            arr = arr.astype(np.float64)
            arr[mask] = np.nan
            return pa.array(arr)
        else:
            # String - use None
            arr = arr.astype(object)
            arr[mask] = None
            return pa.array(arr)
