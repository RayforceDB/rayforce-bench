"""
Database adapters for the benchmark framework.

Each adapter implements the Adapter interface and provides
database-specific implementation for benchmark tasks.
"""

from .duckdb_adapter import DuckDBAdapter
from .rayforce_adapter import RayforceAdapter

__all__ = [
    "DuckDBAdapter",
    "RayforceAdapter",
]
