# Benchmark Fairness Methodology

This document explains how we ensure fair comparisons between databases and DataFrame libraries.

## Core Principle

**We measure query execution time, not data transfer or serialization.**

Each system is measured on its ability to execute queries against data already loaded into memory. We explicitly exclude:
- Data loading time (Parquet parsing)
- Result serialization to Python
- Network/IPC overhead

## Timing Methodology

### Warmup Phase

Before measuring, each benchmark runs warmup iterations to ensure:
- JIT compilation is complete (where applicable)
- CPU caches are warm
- Memory is allocated

Default: 2 warmup iterations.

### Measurement Phase

Multiple iterations are run and we report:
- **Median**: Primary metric (robust to outliers)
- **Min**: Best-case performance
- **Max**: Worst-case performance

Default: 5 measured iterations.

## Per-Adapter Implementation

### Rayforce (Native `timeit`)

```
Timing includes:
✓ Query parsing
✓ Query execution
✓ Result materialization (in rayforce memory)

Timing excludes:
✗ Parquet loading (done before benchmarks)
✗ Result transfer to Python
✗ Row count retrieval (separate query)
```

Implementation:
```python
# Data loading - NOT timed
table = load_parquet(path)
table.save("_bench_data")

# Query execution - timed by rayforce's internal timeit
result = eval_str("(timeit (select {...}))")
time_ms = result.value  # timeit returns milliseconds

# Row count - NOT timed (separate query)
rows = len(eval_str("(select {...})"))
```

**Why use `timeit`?**
- Measures pure rayforce execution without Python overhead
- Same timing mechanism used by rayforce developers
- Most accurate representation of engine performance

### Polars (Python timing)

```
Timing includes:
✓ Query construction (group_by, agg calls)
✓ Query execution (native Rust)
✓ Result materialization (DataFrame creation)

Timing excludes:
✗ Parquet loading (done before benchmarks)
✗ Result inspection (len() for row count)
```

Implementation:
```python
# Data loading - NOT timed
df = pl.read_parquet(path)

# Query execution - timed
start = time.perf_counter_ns()
result = df.group_by("id1").agg(pl.sum("v1"))
end = time.perf_counter_ns()

# Row count - NOT included in timing
rows = len(result)
```

### DuckDB (Python timing)

```
Timing includes:
✓ SQL parsing
✓ Query planning
✓ Query execution
✓ Result materialization (in DuckDB memory)

Timing excludes:
✗ Parquet loading (done before benchmarks)
✗ Result fetch to Python (fetchall() after timing)
```

Implementation:
```python
# Data loading - NOT timed
conn.execute("CREATE TABLE data AS SELECT * FROM 'data.parquet'")

# Query execution - timed
start = time.perf_counter_ns()
result = conn.execute("SELECT id1, SUM(v1) FROM data GROUP BY id1")
end = time.perf_counter_ns()

# Fetch results - NOT timed
rows = result.fetchall()
```

### Pandas (Python timing)

```
Timing includes:
✓ GroupBy/merge operations
✓ Aggregation computation
✓ Result DataFrame creation

Timing excludes:
✗ Parquet loading (done before benchmarks)
✗ Result inspection
```

Implementation:
```python
# Data loading - NOT timed
df = pd.read_parquet(path)

# Query execution - timed
start = time.perf_counter_ns()
result = df.groupby("id1")["v1"].sum().reset_index()
end = time.perf_counter_ns()
```

### QuestDB / TimescaleDB (Server-based)

```
Timing includes:
✓ SQL parsing (server-side)
✓ Query execution (server-side)
✓ Result transfer (network)
✓ Result materialization

Timing excludes:
✗ Data loading (done before benchmarks)
✗ Container startup (done before benchmarks)
```

**Note**: Server-based adapters include network overhead, which is unavoidable for their architecture. This is a known trade-off vs embedded databases.

## Data Schema

All benchmarks use consistent data schemas:

### GroupBy Data
```
id1: int64  (K unique values, e.g., 1-100)
id2: int64  (K unique values)
id3: int64  (K unique values)
v1:  float64 (normal distribution)
v2:  float64 (normal distribution)
v3:  float64 (normal distribution)
```

**Important**: Using `int64` for id columns is critical for performance. String-based ids would penalize hash-based systems unfairly.

### Join Data
```
Left table:  id1 (int64), id2 (int64), v1 (float64)
Right table: id1 (int64), id3 (int64), v2 (float64)
```

## Threading Configuration

Each adapter uses its default/optimal threading model:

| Adapter | Threading |
|---------|-----------|
| Rayforce | Multi-threaded (automatic) |
| Polars | Multi-threaded (Rayon, all cores) |
| DuckDB | Multi-threaded (automatic) |
| Pandas | Single-threaded (GIL-bound) |
| QuestDB | Server-managed |
| TimescaleDB | Server-managed |

We do not artificially limit threading as this would not reflect real-world usage.

## Validation

Each benchmark validates results:
1. **Row counts** - All adapters must return the same number of rows
2. **Data correctness** - Spot-checked to ensure correct aggregations

## What We're Measuring

**Included:**
- Database/engine execution speed
- Algorithm efficiency (hash joins, aggregations)
- Memory management
- Parallelization effectiveness

**Excluded:**
- Python binding overhead
- Data loading/parsing
- Result serialization
- Network latency (except server-based)

## Reproducibility

To reproduce results:

```bash
# Use fixed random seed for data generation
python -m bench.generate -o data groupby -n 1m -k 100 --seed 42

# Run with sufficient iterations
python -m bench.runner groupby -d data/groupby_1m_k100 -i 10 -w 3
```

Results may vary based on:
- CPU model and core count
- Available RAM
- OS and kernel version
- Background processes

For best results, run benchmarks on an idle system.
