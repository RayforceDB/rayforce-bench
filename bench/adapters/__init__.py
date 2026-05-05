from .base import Adapter, BenchmarkResult


def _maybe_import(modname, classname):
    """Import an adapter module lazily; return None if dependency missing."""
    try:
        mod = __import__(f"bench.adapters.{modname}", fromlist=[classname])
        return getattr(mod, classname)
    except ImportError:
        return None


# Public adapter classes — None entries mean the engine isn't installed
# in this venv; the worker will surface a clean error if the user picks one.
DuckDBAdapter      = _maybe_import("duckdb_adapter",      "DuckDBAdapter")
PolarsAdapter      = _maybe_import("polars_adapter",      "PolarsAdapter")
PandasAdapter      = _maybe_import("pandas_adapter",      "PandasAdapter")
ChdbAdapter        = _maybe_import("chdb_adapter",        "ChdbAdapter")
DataFusionAdapter  = _maybe_import("datafusion_adapter",  "DataFusionAdapter")
RayforceAdapter    = _maybe_import("rayforce_adapter",    "RayforceAdapter")
# rayforce_rfl_adapter removed — see commit log for rationale.
QuestDBAdapter     = _maybe_import("questdb_adapter",     "QuestDBAdapter")
TimescaleAdapter   = _maybe_import("timescale_adapter",   "TimescaleAdapter")


def check_dependencies() -> dict[str, str | None]:
    """Check all adapter dependencies and return status."""
    deps = {}

    def _try(pkg, version_attr="__version__"):
        try:
            mod = __import__(pkg)
            return getattr(mod, version_attr, "?")
        except ImportError:
            return None

    deps["polars"]     = _try("polars")
    deps["duckdb"]     = _try("duckdb")
    deps["pandas"]     = _try("pandas")
    deps["chdb"]       = _try("chdb")
    deps["datafusion"] = _try("datafusion")
    deps["psycopg"]    = _try("psycopg")
    deps["rayforce"]   = _try("rayforce", "version")
    deps["pyarrow"]    = _try("pyarrow")
    return deps


def print_dependency_status(quiet: bool = False) -> bool:
    deps = check_dependencies()
    missing = []
    for pkg, version in deps.items():
        if version:
            if not quiet:
                print(f"  ✓ {pkg}: {version}")
        else:
            if not quiet:
                print(f"  ✗ {pkg}: NOT INSTALLED")
            missing.append(pkg)
    if missing and not quiet:
        print(f"\nMissing optional deps: pip install {' '.join(missing)}")
    return len(missing) == 0


__all__ = [
    "Adapter",
    "BenchmarkResult",
    "DuckDBAdapter",
    "PolarsAdapter",
    "PandasAdapter",
    "ChdbAdapter",
    "DataFusionAdapter",
    "RayforceAdapter",
    "QuestDBAdapter",
    "TimescaleAdapter",
    "check_dependencies",
    "print_dependency_status",
]
