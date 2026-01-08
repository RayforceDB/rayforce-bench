# Benchmark Fairness Methodology

This document explains how we ensure fair comparisons between databases.

## Core Principle

**We measure query execution time, not data transfer or serialization.**

Each database should be measured on its ability to execute queries against data 
that is already loaded into its memory. We explicitly exclude:
- Data loading time (CSV parsing)
- Result serialization to Python
- Network/IPC overhead

## Per-Adapter Implementation

### DuckDB (Embedded Python)

```
Timing includes:
✓ Query parsing and planning
✓ Query execution
✓ Result materialization (in DuckDB memory)

Timing excludes:
✗ CSV loading (done once before benchmarks)
✗ Result transfer to Python (fetchall() called AFTER timing)
✗ Checksum computation
```

Implementation:
```python
# DuckDB execute() is NOT lazy - full query runs immediately
start = time.perf_counter_ns()
result = conn.execute(query)
end = time.perf_counter_ns()

# Fetch results AFTER timing (for validation only)
rows = result.fetchall()
row_count = len(rows)
```

### Polars (Embedded Python with Lazy Evaluation)

```
Timing includes:
✓ Query plan construction (group_by, agg, join calls)
✓ Query optimization (predicate pushdown, projection)
✓ Query execution via collect() (native Rust execution)
✓ Parallel processing across all CPU cores
✓ Result materialization (DataFrame creation)

Timing excludes:
✗ CSV loading (done once before benchmarks)
✗ Result inspection (head(), to_list() for checksum)
```

Implementation:
```python
# Data loaded before timing, lazy view created
df = pl.read_csv(...)
lf = df.lazy()  # Create lazy view for query optimization

# Time BOTH query plan construction AND execution
# This matches DuckDB/Rayforce/KDB+ which include parsing+planning
start = time.perf_counter_ns()
query = lf.group_by("id1").agg(pl.sum("v1"))  # Plan construction
result = query.collect()  # Execution
end = time.perf_counter_ns()

# Validation AFTER timing
row_count = len(result)
```

**Why include query plan construction?**
- DuckDB's `execute()` includes parsing + planning
- Rayforce's `timeit` includes parsing + planning  
- KDB+'s `\t` includes parsing + planning
- For fair comparison, Polars must include plan construction too

**Why Lazy Evaluation?**
- Query plan is optimized (predicate pushdown, projection)
- `collect()` triggers native Rust parallel execution
- Polars uses all available CPU cores via Rayon thread pool
- Environment variable `POLARS_MAX_THREADS` controls parallelism

### Rayforce (Subprocess with `timeit`)

```
Timing includes:
✓ Query parsing and planning
✓ Query execution  
✓ Result materialization (in Rayforce memory)
✓ Memory cleanup (implicit when result goes out of scope in timeit)

Timing excludes:
✗ CSV loading (done before timeit in script)
✗ Subprocess startup (happens before timing)
✗ Result counting (done after timeit, re-runs query)
```

Implementation:
```lisp
;; Data loading - NOT timed
(set t (read-csv [...] "data.csv"))

;; Query execution - timed by Rayforce's internal timeit
;; timeit returns execution time in milliseconds
(set _timing (timeit (select {...})))

;; Count retrieval - NOT timed (re-runs query)
(set _result (select {...}))
(set _count (count _result))
```

### KDB+/q (Subprocess with `\t`)

```
Timing includes:
✓ Query parsing
✓ Query execution
✓ Result materialization (in q memory)

Timing excludes:
✗ CSV loading (done before \t)
✗ Subprocess startup
✗ Result counting (done after \t)
```

Implementation:
```q
/ Data loading - NOT timed
t:("SSSJJJJJF";enlist",")0:`:data.csv

/ Query execution - timed by q's \t command
/ \t returns time in milliseconds
t:\t r:select v1:sum v1 by id1 from t

/ Output timing and count
-1"TIMING_MS:",string t;
-1"ROW_COUNT:",string count r;
```

**Note:** KDB+'s `\t` timer is similar to Rayforce's `timeit` - it measures
only the query execution, not data loading or result inspection.

## Process Model Comparison

| Adapter   | Process Model         | Data Persistence | Timing Method        |
|-----------|----------------------|------------------|----------------------|
| DuckDB    | Same process         | Warm for all     | `time.perf_counter_ns()` |
| Polars    | Same process         | Warm for all     | `time.perf_counter_ns()` |
| Rayforce  | New subprocess/iter  | OS-cached CSV    | `timeit` (internal)  |
| KDB+      | New subprocess/iter  | OS-cached CSV    | `\t` (internal)      |

### Why Subprocess Adapters Are Still Fair

For subprocess-based adapters (Rayforce, KDB+):
1. CSV loading happens BEFORE timing starts
2. The database's internal timer (`timeit`, `\t`) measures only query execution
3. Data is effectively "warm" after first load (OS file cache)
4. Subprocess overhead is not counted in timing

## Threading Configuration

| Adapter   | Default Threading     | Override Option           |
|-----------|----------------------|---------------------------|
| DuckDB    | Multi-threaded (auto)| `--duckdb-threads N`      |
| Polars    | Multi-threaded (auto)| `--polars-threads N`      |
| Rayforce  | Configurable         | Via rayforce `-c` flag    |
| KDB+      | Single-threaded      | N/A (q is single-threaded)|

For fair comparison, consider matching thread counts:
```bash
# Single-threaded comparison
python run_bench.py --duckdb-threads 1 --polars-threads 1 ...
```

## Validation

Each benchmark validates:
1. **Row counts match** - Same number of result rows
2. **Checksums match** (optional) - Result data is identical

This prevents "fast but wrong" optimizations.

## What We're Measuring

✓ **Database engine performance** - Core query execution speed
✓ **Algorithm efficiency** - Hash joins, group-by aggregations
✓ **Memory management** - Result materialization

✗ **Python binding overhead** - Different for each adapter
✗ **IPC/serialization** - Not relevant for embedded use
✗ **Disk I/O** - Data should be OS-cached during benchmarks
