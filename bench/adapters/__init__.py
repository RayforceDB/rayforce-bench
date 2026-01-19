from .base import Adapter, BenchmarkResult
from .duckdb_adapter import DuckDBAdapter
from .pandas_adapter import PandasAdapter
from .polars_adapter import PolarsAdapter
from .questdb_adapter import QuestDBAdapter
from .rayforce_adapter import RayforceAdapter
from .rayforce_py_adapter import RayforcePyAdapter
from .timescale_adapter import TimescaleAdapter


def check_dependencies() -> dict[str, str | None]:
    """Check all adapter dependencies and return status.

    Returns dict of {package: version or None if missing}.
    """
    deps = {}

    # pandas
    try:
        import pandas
        deps["pandas"] = pandas.__version__
    except ImportError:
        deps["pandas"] = None

    # polars
    try:
        import polars
        deps["polars"] = polars.__version__
    except ImportError:
        deps["polars"] = None

    # duckdb
    try:
        import duckdb
        deps["duckdb"] = duckdb.__version__
    except ImportError:
        deps["duckdb"] = None

    # psycopg (for questdb and timescale)
    try:
        import psycopg
        deps["psycopg"] = psycopg.__version__
    except ImportError:
        deps["psycopg"] = None

    # rayforce
    try:
        import rayforce
        deps["rayforce"] = rayforce.version
    except ImportError:
        deps["rayforce"] = None

    # pyarrow
    try:
        import pyarrow
        deps["pyarrow"] = pyarrow.__version__
    except ImportError:
        deps["pyarrow"] = None

    return deps


def print_dependency_status():
    """Print dependency status table."""
    deps = check_dependencies()

    print("\n=== Dependency Status ===")
    missing = []
    for pkg, version in deps.items():
        if version:
            print(f"  ✓ {pkg}: {version}")
        else:
            print(f"  ✗ {pkg}: NOT INSTALLED")
            missing.append(pkg)

    if missing:
        print(f"\nMissing packages: pip install {' '.join(missing)}")
        return False
    print()
    return True


__all__ = [
    "Adapter",
    "BenchmarkResult",
    "DuckDBAdapter",
    "PandasAdapter",
    "PolarsAdapter",
    "QuestDBAdapter",
    "RayforceAdapter",
    "RayforcePyAdapter",
    "TimescaleAdapter",
    "check_dependencies",
    "print_dependency_status",
]
