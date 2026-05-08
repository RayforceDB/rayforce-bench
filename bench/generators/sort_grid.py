"""Extended sort benchmark — typed columns × scaling lengths.

Random pattern only (other patterns deliberately omitted — focus is
absolute throughput per type, not stability under partially-sorted input).
Lengths follow a 9-points-per-decade scaling curve so the resulting plot
is a smooth log-log curve.

Output layout: <out_root>/<dtype>/<n>.csv with a single 'v' column.
"""

import math
from pathlib import Path

import numpy as np


DTYPES = ["u8", "i16", "i32", "i64", "f64", "str8", "str16"]


def scaling_lengths(max_n: int) -> list[int]:
    """Return [1, 2, ..., 9, 10, 20, ..., 90, ..., max_n] up to max_n."""
    pts: set[int] = set()
    if max_n < 1:
        return []
    decades = int(math.log10(max_n)) + 1
    for exp in range(decades):
        base = 10 ** exp
        for m in range(1, 10):
            n = base * m
            if n <= max_n:
                pts.add(n)
    pts.add(max_n)
    return sorted(pts)


def _gen_column(dtype: str, n: int, rng: np.random.Generator):
    """Return either np.ndarray (numeric) or list[str] (strings)."""
    if dtype == "u8":
        return rng.integers(0, 256, n, dtype=np.uint8)
    if dtype == "i16":
        return rng.integers(-(2**15), 2**15, n, dtype=np.int16)
    if dtype == "i32":
        return rng.integers(-(2**31), 2**31, n, dtype=np.int32)
    if dtype == "i64":
        # Stay within ±2^62 to avoid edge-cases on signed-vs-unsigned readers.
        return rng.integers(-(2**62), 2**62, n, dtype=np.int64)
    if dtype == "f64":
        return rng.standard_normal(n).astype(np.float64)
    if dtype.startswith("str"):
        length = int(dtype[3:])
        # Build a single (n, length) byte matrix, then view each row as fixed-len bytes.
        bytes_arr = rng.integers(ord("a"), ord("z") + 1, size=(n, length),
                                 dtype=np.uint8)
        return bytes_arr.view(f"S{length}").reshape(-1)
    raise ValueError(f"Unknown dtype: {dtype}")


def _write_csv(out_path: Path, col, dtype: str) -> None:
    """Write a single-column CSV. Strings are written raw (no quoting) since
    we control the alphabet (a..z) and there are no separators or newlines."""
    with open(out_path, "w") as f:
        f.write("v\n")
        if dtype.startswith("str"):
            for s in col:
                f.write(s.decode("ascii"))
                f.write("\n")
        elif dtype == "f64":
            np.savetxt(f, col, fmt="%.17g")
        else:
            np.savetxt(f, col, fmt="%d")


def gen_one(out_root: Path, dtype: str, n: int, seed: int = 0,
            overwrite: bool = False) -> Path:
    """Generate <out_root>/<dtype>/<n>.csv. Returns the path."""
    out_dir = out_root / dtype
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{n}.csv"
    if out_path.exists() and not overwrite:
        return out_path

    rng = np.random.default_rng(seed + hash((dtype, n)) % (2**31))
    col = _gen_column(dtype, n, rng)
    _write_csv(out_path, col, dtype)
    return out_path


def gen_grid(out_root: Path, dtypes: list[str], lengths: list[int],
             seed: int = 0, verbose: bool = True) -> list[tuple[str, int, Path]]:
    """Generate the full grid. Returns [(dtype, n, path), ...]."""
    paths = []
    total = len(dtypes) * len(lengths)
    done = 0
    for dtype in dtypes:
        for n in lengths:
            p = gen_one(out_root, dtype, n, seed=seed)
            paths.append((dtype, n, p))
            done += 1
            if verbose:
                print(f"  [{done}/{total}] {dtype}/{n}: {p}")
    return paths
