# Benchmark Fairness & Methodology

This document explains every choice that affects how engines are compared, so a reader can decide whether the numbers represent the workload they actually care about. Pointers to the relevant source files are inline so claims are verifiable.

## Core principles

1. **All engines are measured the same way** — Python-level `time.perf_counter_ns` around the call into the engine. No engine-internal `(timeit ...)` shortcuts.
2. **Each (adapter, op) runs in its own subprocess** — so memory pressure or a crash from one engine can't affect another, and Python module state can't leak between engines.
3. **All engines see byte-identical inputs** — datasets are deterministic (PCG64) and `manifest.json` carries SHA256 of every CSV. If your hash differs from someone else's, you don't have the same data.
4. **The OS isn't lying to us** — swap usage is sampled before and after each operation; the orchestrator warns when the disk started paging, because at that point the timing reflects I/O, not the engine.

## Timing methodology

### Where the clock starts and stops

Every adapter wraps the call into the engine in `_time_it()` from `bench/adapters/base.py`:

```python
def _time_it(self, func) -> tuple[Any, int]:
    start = time.perf_counter_ns()
    result = func()
    end = time.perf_counter_ns()
    return result, end - start
```

Examples:
- DuckDB: clock wraps `conn.execute(SQL).fetchdf()`.
- Polars: clock wraps `df.group_by(...).agg(...)`.
- chDB: clock wraps `session.query(SQL, "CSV")`.
- Rayforce (py): clock wraps `eval_str(query)`.

**What we measure:** SQL/expression parsing + planning + execution + result materialization in engine memory.

**What we exclude:** dataset loading from disk (done before timing), Python ↔ engine binding overhead beyond the call boundary, network round-trips except for server-based engines (where they're unavoidable).

### Why no engine `timeit`

Earlier prototypes used rayforce's internal `(timeit ...)` for the rayforce adapter and `time.perf_counter_ns` for everything else. That asymmetry tilted comparisons in rayforce's favour by hiding Python-binding overhead. We removed it in commit `63fc31d`. Trade-off: rayforce-py now pays the same Python overhead in the timer as the other adapters; that overhead is constant (~1µs) and shrinks as a fraction of total time on larger inputs.

### Iterations and warmup

Default for `make bench`: **5 measured iterations + 2 warmup**. Reported metrics:

- **Median** (primary) — robust to outliers from GC pauses, CPU frequency scaling, etc.
- **Min** — best-case, useful as a lower bound on engine speed.
- **Max** — worst-case, a sanity check that no single iteration dominated.

For `make bench-scaling` (sweep across orders of magnitude), iterations adapt to the input size, mirroring teide-bench's `sort_bench_multi.iter_counts`:

| Rows               | Iterations | Warmup |
|--------------------|------------|--------|
| ≤ 100              | 21         | 5      |
| 101 – 100,000      | 7          | 3      |
| 100,001 – 10,000,000 | 5        | 2      |
| > 10,000,000       | 3          | 1      |

Reasoning: tiny inputs run in microseconds, dominated by `perf_counter` noise (~50ns floor); we need many samples to get a stable median. Huge inputs cost seconds per iteration — three samples and a warmup is enough.

## Process isolation

Each `(adapter, op)` pair runs in its own child via `bench.worker` (or `bench.sort_grid_worker`):

```
parent (orchestrator)
  ├── subprocess.run([python, -m, bench.worker, --adapter, rayforce, --benchmark, groupby_q1, ...])
  │     └── child loads rayforce, runs warmup + iterations, writes JSON
  ├── subprocess.run([python, -m, bench.worker, --adapter, duckdb,   ...])
  └── ...
```

Why: chdb / polars / pandas / duckdb / rayforce all hold native memory and global state. Running them sequentially in one process lets memory pressure from an earlier engine stay resident and degrade the next one's cache. Subprocess isolation gives every engine the same starting conditions.

Source: `bench/runner.py` (`_run_worker`), `bench/scaling_runner.py` (`_spawn_h2o`, `_spawn_sort_grid`), `bench/worker.py`, `bench/sort_grid_worker.py`.

## Reproducibility — same data on every machine

### Deterministic generation

Random data is generated via `numpy.random.Generator(PCG64(seed))`. PCG64 has been numpy's stable bit-generator since 1.17, so a given seed produces the same byte stream regardless of numpy version (we don't rely on `default_rng`'s default since that's allowed to change).

Source: `bench/generators/groupby.py:46`, `bench/generators/join.py:73`.

### SHA256 contract

Every CSV emitted has its SHA256 written into `manifest.json`:

```json
{
  "name": "groupby_10k_k100",
  "tables": {
    "data": {
      "rows": 10000,
      "schema": {"id1": "string", "id2": "string", ...},
      "files": {"csv": "data/groupby_10k_k100/data.csv"},
      "sha256": {"csv": "90d5803731e4b4f251dcdcfeb4a548cf519639e61a0ad4340100ec080ac2c988"}
    }
  }
}
```

If two users on different machines run `make data SIZE=10m` and see different hashes, the generator changed and benchmark numbers are no longer cross-comparable. This is the contract that lets external observers verify "everyone is benching the same data".

Source: `bench/generators/base.py:_sha256_file`.

## Schema — canonical H2O.ai

Schemas match [H2O.ai db-benchmark](https://h2oai.github.io/db-benchmark/) exactly, so canonical H2O datasets are interchangeable with ours.

### GroupBy (9 columns)

| Column | Type    | Cardinality / Range          |
|--------|---------|------------------------------|
| `id1`  | string  | K (e.g. `id001`..`id100`)    |
| `id2`  | string  | K                            |
| `id3`  | string  | max(n//K, K) = `n_high`      |
| `id4`  | int64   | [1, K]                       |
| `id5`  | int64   | [1, K]                       |
| `id6`  | int64   | [1, n_high]                  |
| `v1`   | int64   | [1, 5]                       |
| `v2`   | int64   | [1, 15]                      |
| `v3`   | float64 | [0, 100), 6 decimals         |

### Join (7 columns, two equal-sized tables)

| Column | Type    | Notes                                         |
|--------|---------|-----------------------------------------------|
| `id1`  | int64   | [1, K]                                        |
| `id2`  | int64   | [1, K]                                        |
| `id3`  | int64   | [1, n_high]                                   |
| `id4`  | string  | cardinality K                                 |
| `id5`  | string  | cardinality K                                 |
| `id6`  | string  | cardinality n_high                            |
| `v1`   | float64 | left table; `v2` on right table               |

The deliberately-inverted spread (int keys + string sides) stresses different join paths than the groupby workload, which has the opposite layout. Identical schema choice as canonical H2O J1.

### Why string IDs for groupby?

Earlier versions used `int64` for `id1..id3`, on the (well-intentioned but misguided) reasoning that "string ids would penalize hash-based systems unfairly". That reasoning was wrong: every modern analytical engine has dedicated string-hashing fast paths, and refusing to test them just means we're benchmarking a workload that doesn't match what real users run. Canonical H2O uses strings for `id1..id3`, and so do we.

## Per-engine schema mapping

Each adapter loads CSV into its native string type. No re-encoding happens on the timed path.

| Adapter      | id1..id3 (string) | id4..id6 (int64) | v1, v2 (int) | v3 (float)        |
|--------------|-------------------|------------------|--------------|-------------------|
| DuckDB       | VARCHAR           | BIGINT           | BIGINT       | DOUBLE            |
| Polars       | Utf8              | Int64            | Int64        | Float64           |
| Pandas       | object/string     | int64            | int64        | float64           |
| chDB         | String            | Int64            | Int64        | Float64           |
| DataFusion   | Utf8              | Int64            | Int64        | Float64           |
| Rayforce     | Symbol            | I64              | I64          | F64               |
| QuestDB      | SYMBOL (ILP)      | LONG             | LONG         | DOUBLE            |
| TimescaleDB  | TEXT              | BIGINT           | BIGINT       | DOUBLE PRECISION  |

`SYMBOL` for rayforce maps to `RAY_SYM` (dictionary-encoded, adaptive width W8/W16/W32/W64) — the same type the engine would pick for low-cardinality string columns in any production workload.

For the **typed-sort grid** (`make bench-sort-ext` and within `make bench-scaling`), `str8`/`str16` map to rayforce's `RAY_STR` (variable-length string with SSO at 12 bytes) — a real string type, not a dictionary. This is the apples-to-apples test against DuckDB VARCHAR (12-byte inline) and Polars Utf8/Arrow StringView (~12 byte SSO).

## Threading

| Adapter      | Threading           |
|--------------|---------------------|
| Rayforce     | Multi-threaded      |
| DuckDB       | Multi-threaded      |
| Polars       | Multi-threaded (Rayon, all cores) |
| chDB         | Multi-threaded (ClickHouse default) |
| DataFusion   | Multi-threaded (Tokio, all cores) |
| Pandas       | Single-threaded (GIL) |
| QuestDB      | Server-managed      |
| TimescaleDB  | Server-managed      |

We do not artificially cap threading — that wouldn't reflect real usage. Pandas's single-threaded result is intrinsic to its design and is what users actually see in production.

## Swap monitor

Before each operation we sample `psutil.swap_memory().used`; after, we sample again. If growth exceeds 100 MB, the orchestrator prints:

```
WARNING [duckdb/groupby_q7]: swap grew by 245 MB during run.
Result is unreliable — reduce dataset size.
```

Once the OS pages out, timing reflects disk speed, not engine performance. The warning is non-fatal — the result still gets recorded — but it tells you that this row in the table is not a fair number to cite.

Source: `bench/swapcheck.py`.

## What's deliberately excluded

- **`make bench-sort-ext`** runs only the **embedded** engines (rayforce, duckdb, polars, chdb, datafusion, pandas). QuestDB and TimescaleDB are excluded because Docker round-trip + SQL parse cost dwarfs the actual sort.
- **Random pattern only** for the typed-sort grid. We don't currently test partially-sorted or reverse-sorted inputs because most engines have similar performance there and the grid is already large.
- **No nulls in default datasets.** `--null-pct` is supported by the generator but defaults to 0. Nullable benchmarks would be a separate suite.

## Validation

Each adapter returns the row count of its query result. The orchestrator records this in JSON. For groupby queries, all adapters with the same data should return the same row count — a quick sanity check. Mismatches are visible in the output and indicate either a SQL semantics difference (e.g. one engine treats `COUNT(NULL)` differently) or a bug.

We do **not** currently compare actual values across engines — for that we'd need a canonical reference output. This is a known gap, not a hidden one.

## Reproducing results

```bash
# Generate a known dataset
make data SIZE=10m

# Verify the SHA256 matches what's on the public report
cat data/groupby_10m_k100/manifest.json | grep sha256

# Run with the same iteration counts as the reference
make bench SIZE=10m
make bench-scaling
```

Variance you can't eliminate: CPU model, core count, RAM, OS / kernel, background load, thermal throttling. Run on an idle system with the OS settled — close browsers, stop file indexers, disable Spotlight on macOS.
