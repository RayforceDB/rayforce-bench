from .base import Adapter, BenchmarkResult
from .duckdb_adapter import DuckDBAdapter
from .polars_adapter import PolarsAdapter
from .questdb_adapter import QuestDBAdapter
from .rayforce_adapter import RayforceAdapter
from .timescale_adapter import TimescaleAdapter


def check_dependencies() -> dict[str, str | None]:
    """Check all adapter dependencies and return status.

    Returns dict of {package: version or None if missing}.
    """
    deps = {}

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


def print_dependency_status(quiet: bool = False):
    """Print dependency status table."""
    deps = check_dependencies()

    missing = []
    for pkg, version in deps.items():
        if version:
            if not quiet:
                print(f"  ✓ {pkg}: {version}")
        else:
            print(f"  ✗ {pkg}: NOT INSTALLED")
            missing.append(pkg)

    if missing:
        print(f"\nMissing: pip install {' '.join(missing)}")
        return False
    return True


__all__ = [
    "Adapter",
    "BenchmarkResult",
    "DuckDBAdapter",
    "PolarsAdapter",
    "QuestDBAdapter",
    "RayforceAdapter",
    "TimescaleAdapter",
    "check_dependencies",
    "print_dependency_status",
]
