"""Swap-usage monitoring around benchmark runs.

When the OS starts paging, results stop reflecting engine performance and
start reflecting disk I/O. This module records swap usage before and after
each operation and warns when growth crosses a threshold.
"""

from dataclasses import dataclass


SWAP_THRESHOLD_BYTES = 100 * 1024 * 1024  # 100 MB


@dataclass
class SwapSample:
    used: int
    total: int

    @classmethod
    def now(cls) -> "SwapSample":
        try:
            import psutil
            s = psutil.swap_memory()
            return cls(used=s.used, total=s.total)
        except ImportError:
            return cls(used=0, total=0)


def fmt_mb(n: int) -> str:
    return f"{n / 1024 / 1024:.0f} MB"


def warn_if_already_used(sample: SwapSample) -> bool:
    """Print a warning if swap is in use before benchmarks start.

    Returns True if a warning was printed.
    """
    if sample.total == 0:
        return False
    if sample.used > SWAP_THRESHOLD_BYTES:
        print(f"WARNING: swap already in use ({fmt_mb(sample.used)} of "
              f"{fmt_mb(sample.total)}). Free memory before running benchmarks "
              f"to avoid skewed results.")
        return True
    return False


def warn_if_grew(before: SwapSample, after: SwapSample, label: str) -> bool:
    """Print a warning if swap usage grew during a benchmark run.

    Returns True if a warning was printed.
    """
    if after.total == 0:
        return False
    delta = after.used - before.used
    if delta > SWAP_THRESHOLD_BYTES:
        print(f"  WARNING [{label}]: swap grew by {fmt_mb(delta)} during run. "
              f"Result is unreliable — reduce dataset size.")
        return True
    return False
