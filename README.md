# RayforceDB Benchmark Suite

Benchmarking framework for comparing RayforceDB against popular DataFrame libraries and databases.

> **Branch `prototype`**: subprocess-isolated workers, swap-usage monitor,
> uniform Python-level timing, dual-mode rayforce adapter (rayforce-py or
> native binary + .rfl), and an extended typed sort grid. See **Roadmap**
> below for what's next.

## Live Results

**[View Benchmark Results](https://anthropics.github.io/rayforce-bench/)**

## Quick Start

```bash
git clone https://github.com/anthropics/rayforce-bench.git
cd rayforce-bench

make setup            # Install dependencies
make data             # Generate H2O datasets (10M rows by default)
make bench            # Run H2O groupby benchmarks (q1..q7)
make bench-scaling    # Run scaling sweep across sizes 10..1m → docs/scaling.html
make bench-sort-ext   # Run extended typed-sort scaling grid (optional)
```

Outputs:
- `docs/index.html` — boxplot + comparison view (per-iteration distribution)
- `docs/histogram.html` — Plotly bar chart (single-size snapshot)
- `docs/scaling.html` — interactive log-log scaling curve with engine + op filters
- `docs/sort.html` — log-log scaling curve for the extended sort grid

## Reproducibility

All datasets use canonical [H2O.ai db-benchmark](https://h2oai.github.io/db-benchmark/)
schemas and are generated deterministically (PCG64 random, stable since
numpy 1.17). Every CSV emitted carries a `sha256` field in
`manifest.json`, so two runs of `make data SIZE=10m` on different
machines must produce byte-identical files. If they don't — the
generator changed and benchmark numbers are no longer comparable across
machines.

## Adapters

Default `make bench` runs the embedded engines (no Docker required):

| Adapter | Type | Why included |
|---------|------|--------------|
| `rayforce`   | Embedded columnar | The engine being benchmarked. |
| `duckdb`     | Embedded SQL OLAP | De-facto leader for embedded analytics. |
| `polars`     | Embedded DataFrame (Rust + Arrow) | Fastest mainstream DataFrame library. |
| `chdb`       | Embedded ClickHouse | Lets us measure against ClickHouse without running a server. |
| `datafusion` | Embedded SQL (Rust + Arrow) | Substrate for InfluxDB 3, GlareDB, ROAPI — measuring against it covers the Apache columnar ecosystem. |
| `pandas`     | DataFrame (Python) | Slow baseline. Included so readers calibrated against pandas can map the rest of the chart. |

`ALL=1` adds two server-based engines (require Docker):

| Adapter | Type | Why included |
|---------|------|--------------|
| `questdb`   | Time-series | Specialized TSDB with SQL — relevant for the financial / market-data segment that's natural rayforce territory. |
| `timescale` | Postgres extension | TSDB baseline. Not a true OLAP competitor; included for context only. |

### Rayforce execution modes

- **`py` (default)** — uses the `rayforce-py` PyPI package. Requires
  `pip install rayforce-py` (pending public release).
- **`rfl`** — drives the native `rayforce` binary by generating temporary
  `.rfl` scripts and parsing `(timeit ...)` output. Set via
  `RAYFORCE_MODE=rfl` (Makefile) or `--rayforce-mode rfl` (CLI). Useful
  while `rayforce-py` is not yet on PyPI, or to benchmark the C core
  directly. The default binary path is `~/rayforce/rayforce`.

## Benchmarks

Based on [H2O.ai db-benchmark](https://h2oai.github.io/db-benchmark/):

### GroupBy Queries (canonical H2O, on a 9-column dataset)
- **Q1**: `sum(v1) group by id1`
- **Q2**: `sum(v1) group by id1, id2`
- **Q3**: `sum(v1), mean(v3) group by id3`
- **Q4**: `mean(v1), mean(v2), mean(v3) group by id3`
- **Q5**: `sum(v1), sum(v2), sum(v3) group by id3`
- **Q6**: `max(v1) - min(v2) group by id3`
- **Q7**: `sum(v3), count(v1) group by id1, id2, id3, id4, id5, id6` (6-key)

Schema: `id1..id3` are strings (cardinality K), `id4..id6` are int64
(cardinality K, K, n_high), `v1`/`v2` int, `v3` float — same as
`~/rayforce/bench/h2o/q*.rfl` and teide-bench.

### Join Queries
- **Inner Join**: Join on `id1`
- **Left Join**: Join on `id1`

Schema: integer `id1..id3` keys + string `id4..id6` side columns + float
value (`v1` left, `v2` right). Two equal-size tables.

### Scaling sweep (`make bench-scaling`)

Runs every adapter through every H2O op + the typed sort grid at every
size in `SIZES` (default `10,100,1k,10k,100k,1m`). Adaptive iteration
counts: tiny inputs run more iterations to beat the timer noise floor;
huge inputs run fewer because each iteration is already slow.

Output: `docs/scaling.html` with two checkbox groups (engines, ops) and
preset buttons for groupby / join / sort H2O / sort typed. Toggle
anything on/off to see the comparison you care about.

### Sort Queries (H2O standard, on the groupby dataset)
- **Single Column** (`s1`): Sort by `id1`
- **Multi Column** (`s6`): Sort by `id1, id2, id3`

### Extended Sort Grid (optional, `make bench-sort-ext`)

A separate scaling-curve benchmark — random data only, but swept across
multiple types and sizes:

- **Patterns**: `random` (only)
- **Dtypes**: `u8`, `i16`, `i32`, `i64`, `f64`, `str8`, `str16`
- **Lengths**: 9 points per decade up to `SORT_MAX` (default `1m`,
  configurable up to `100m` if you have the RAM)
- **Iterations**: 3 measured + 1 warmup per point

The `str8` / `str16` split deliberately straddles the `RAY_STR` SSO
boundary at 12 bytes — `str8` stays inline in the column cell, `str16`
spills to the string pool. The same effect applies to DuckDB VARCHAR
(also 12-byte inline) and Polars Utf8 / Arrow StringView.

QuestDB and TimescaleDB are excluded from the extended sort grid by
default — Docker overhead and SQL setup cost dwarf the actual sort.

## Usage

### Basic Commands

```bash
# Check dependencies
make check

# Generate data
make data           # 1M rows (default)
make data-small     # 100K rows (quick tests)
make data-large     # 10M rows (production benchmarks)

# Run benchmarks
make bench          # Default adapters (pandas, polars, duckdb, rayforce)
make bench-all      # All adapters (requires Docker for QuestDB/TimescaleDB)
```

### Running Individual Benchmark Suites

```bash
# GroupBy only
python -m bench.runner groupby -d data/groupby_1m_k100 -a pandas polars duckdb rayforce

# Join only
python -m bench.runner join -d data/join_1m_100k -a pandas polars duckdb rayforce

# Sort only
python -m bench.runner sort -d data/sort_1m_k100 -a pandas polars duckdb rayforce

# All suites
python -m bench.runner all -d data/groupby_1m_k100 -a pandas polars duckdb rayforce
```

### CLI Options

```
python -m bench.runner <benchmark> [options]

Arguments:
  benchmark              groupby, join, sort, or all

Options:
  -d, --data PATH        Path to dataset directory (required)
  -a, --adapters LIST    Adapters to benchmark (default: rayforce polars duckdb)
  -i, --iterations N     Number of measured iterations (default: 5)
  -w, --warmup N         Number of warmup iterations (default: 2)
  --rayforce-local PATH  Path to local rayforce-py repo for dev builds
  --rayforce-branch X    Clone rayforce-py from this git branch and build it
  --rayforce-mode py|rfl Rayforce execution mode (default: py)
  --rayforce-bin PATH    Path to rayforce binary (rfl mode, default ~/rayforce/rayforce)
  --html PATH            Output HTML report path (default: docs/index.html)
  --no-html              Skip HTML report generation
  --no-docker            Don't auto-start Docker containers
  --stop-infra           Stop Docker containers after benchmarks
  --check-deps           Check dependencies and exit

Each (adapter, op) runs in its own subprocess for hard memory isolation
(borrowed from teide-bench). Swap usage is sampled before and after each
operation; the orchestrator warns when growth crosses 100 MB so you can
trust whether a result reflects engine performance or disk paging.
```

### Extended Sort Grid CLI

```
python -m bench.sort_grid_runner [options]

Options:
  -a, --adapters LIST    Adapters (default: rayforce duckdb polars)
  --dtypes LIST          Comma-separated dtypes (default: u8,i16,i32,i64,f64,str8,str16)
  --max SIZE             Max length on the scaling curve (default: 1m)
  --data-dir PATH        Where to read/generate per-dtype CSVs (default: data/sort_grid)
  -i, --iterations N     Measured iterations per point (default: 3)
  -w, --warmup N         Warmup iterations (default: 1)
  -o, --output PATH      Output JSON (default: docs/sort_data.json)
  --gen-only             Generate CSVs and exit
  --rayforce-mode py|rfl Same as runner
  --rayforce-bin PATH    Same as runner
```

## Benchmarking a development rayforce

The `--rayforce-mode` flag picks how the rayforce adapter executes queries. Three orthogonal ways to point it at a dev build:

### 1. Local rayforce-py checkout (`py` mode)

```bash
make bench LOCAL=1 RAYFORCE_LOCAL=~/rayforce-py
# or directly:
python -m bench.runner groupby -d data/groupby_10m_k100 \
    -a rayforce duckdb polars \
    --rayforce-local ~/rayforce-py
```

Builds the wrapper from the path, installs it into the venv, runs against it. Version label in reports becomes `rayforce@<branch> (<commit>) [dirty]`.

### 2. rayforce-py git branch

```bash
python -m bench.runner groupby -d data/groupby_10m_k100 \
    -a rayforce duckdb polars \
    --rayforce-branch feature/sort
```

Clones `RayforceDB/rayforce-py.git` at that branch into `.deps/rayforce-py-branch-<name>/`, builds, and uses it. Re-running pulls fresh — useful for tracking a colleague's branch over time.

### 3. Native binary + `.rfl` (`rfl` mode)

```bash
make bench RAYFORCE_MODE=rfl                                      # default ~/rayforce/rayforce
make bench RAYFORCE_MODE=rfl RAYFORCE_BIN=/custom/path/rayforce
```

Generates a temporary `.rfl` script per (op, iteration set), invokes the binary, parses `(timeit ...)` output from stdout. Doesn't need rayforce-py installed at all — useful while the wrapper is pre-PyPI, or when you want to benchmark a C-core branch directly.

## Server-Based Adapters (Docker)

QuestDB and TimescaleDB are off by default. Add `ALL=1` to opt in — the runner auto-starts containers via `bench/infra.py`:

```bash
make bench ALL=1                # auto-start containers, run, leave them up
make bench ALL=1 STOP_INFRA=1   # auto-start, run, stop on exit (in CI)
```

Manual control:

```bash
python -m bench.infra start     # bring up rayforce-bench-{questdb,timescale}
python -m bench.infra status    # show running / stopped / not-created
python -m bench.infra stop      # stop containers (preserves state)
python -m bench.infra cleanup   # stop and remove
```

Ports:
- **QuestDB**: 8812 (PostgreSQL wire protocol), 9009 (ILP), 9000 (web UI).
- **TimescaleDB**: 5433 host → 5432 container (avoids conflict with a local Postgres).

## Project Structure

```
rayforce-bench/
├── bench/
│   ├── adapters/                # One file per engine
│   │   ├── base.py              # Abstract Adapter + _time_it + run_full
│   │   ├── duckdb_adapter.py
│   │   ├── polars_adapter.py
│   │   ├── pandas_adapter.py
│   │   ├── chdb_adapter.py      # embedded ClickHouse
│   │   ├── datafusion_adapter.py
│   │   ├── rayforce_adapter.py      # rayforce-py (PyPI / --rayforce-local / --rayforce-branch)
│   │   ├── rayforce_rfl_adapter.py  # native binary + generated .rfl scripts
│   │   ├── questdb_adapter.py
│   │   └── timescale_adapter.py
│   ├── generators/              # Canonical H2O.ai data
│   │   ├── base.py              # GeneratedDataset, manifest with SHA256
│   │   ├── groupby.py           # 9-col groupby (id1..3 string, id4..6 int)
│   │   ├── join.py              # 7-col join (int keys + string sides)
│   │   └── sort_grid.py         # typed sort columns × scaling lengths
│   ├── runner.py                # H2O orchestrator (one --data path)
│   ├── worker.py                # H2O child process (single op)
│   ├── scaling_runner.py        # Sweep across sizes 10..N
│   ├── sort_grid_runner.py      # Extended typed-sort grid
│   ├── sort_grid_worker.py      # Sort-grid child process
│   ├── report.py                # Boxplot + histogram + scaling/sort HTML
│   ├── engine_source.py         # --rayforce-branch resolution + git labels
│   ├── infra.py                 # Docker management for QuestDB / Timescale
│   ├── swapcheck.py             # psutil.swap_memory monitor + warnings
│   └── generate.py              # Data generation CLI
├── data/                        # Generated datasets (git-ignored)
├── docs/                        # GitHub Pages output
│   ├── index.html               # Boxplot + comparison (single-size H2O)
│   ├── histogram.html           # Plotly bar chart (single-size H2O)
│   ├── scaling.html             # Interactive scaling chart (engine + op filters)
│   ├── sort.html                # Extended sort grid scaling
│   ├── data.json                # H2O run JSON
│   ├── scaling_data.json        # Scaling sweep JSON
│   └── sort_data.json           # Sort grid JSON
├── Makefile
├── requirements.txt
├── README.md
└── FAIRNESS.md                  # Methodology + per-engine timing details
```

## Data Format

Canonical H2O.ai schemas — same as `~/rayforce/bench/h2o/*.rfl` and the canonical [H2O db-benchmark](https://h2oai.github.io/db-benchmark/). Files are CSV with a header row and a `manifest.json` carrying SHA256 of every emitted file.

### GroupBy Dataset (9 columns)

| Column | Type    | Cardinality / Range          | Example         |
|--------|---------|------------------------------|-----------------|
| id1    | string  | K (e.g. 100)                 | `"id042"`       |
| id2    | string  | K                            | `"id087"`       |
| id3    | string  | max(n // K, K) = `n_high`    | `"id00012345"`  |
| id4    | int64   | [1, K]                       | `73`            |
| id5    | int64   | [1, K]                       | `12`            |
| id6    | int64   | [1, n_high]                  | `45678`         |
| v1     | int64   | [1, 5]                       | `3`             |
| v2     | int64   | [1, 15]                      | `9`             |
| v3     | float64 | [0, 100), 6 decimals         | `42.157394`     |

### Join Dataset (7 columns × 2 tables)

Two tables (`left.csv`, `right.csv`) of equal size. Schema is deliberately mirrored against the groupby table — int keys, string side columns — to stress different join paths.

| Column | Type    | Example       |
|--------|---------|---------------|
| id1    | int64   | `42`          |
| id2    | int64   | `87`          |
| id3    | int64   | `12345`       |
| id4    | string  | `"id042"`     |
| id5    | string  | `"id087"`     |
| id6    | string  | `"id12345"`   |
| `v1` / `v2` | float64 | `42.157394` (`v1` on left, `v2` on right) |

### Cross-machine verification

```bash
make data SIZE=10m
cat data/groupby_10m_k100/manifest.json | jq '.tables.data.sha256.csv'
```

Two users with the same `(seed, n_rows, k)` must see the same hash. If they don't, the generator changed and benchmark numbers are no longer comparable.

## Fairness

See [FAIRNESS.md](FAIRNESS.md) for detailed methodology on how we ensure fair comparisons.

Key principles:
- All adapters timed externally with `time.perf_counter_ns` around the
  query call — no engine-internal `(timeit ...)` shortcuts
- Each (adapter, op) runs in its own subprocess so memory pressure from
  one engine can't contaminate another
- Data is pre-loaded into memory (or pre-read in the `.rfl` script before
  the timed block) so the timing reflects query execution, not CSV parse
- Warmup iterations ensure JIT compilation and cache warming
- Swap-usage monitor flags any run where the OS started paging — those
  results are not reliable

## Roadmap

- **prototype branch** (current): H2O suite + extended sort grid + SSO
  visibility + dual-mode rayforce + swap monitor.
- **next**: ClickBench adapter — 43 queries on Yandex Metrica's
  100M-row `hits.parquet`, the de-facto industrial benchmark for
  analytical engines (https://benchmark.clickhouse.com/).
- **after that**: TPC-H SF1/SF10 (DuckDB has dbgen built in), then JOB
  (Join Order Benchmark on IMDB) for query-optimizer comparison.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add your adapter in `bench/adapters/`
4. Update this README
5. Submit a pull request

## License

MIT
