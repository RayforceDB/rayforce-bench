# RayforceDB Benchmark Suite

Benchmarking framework for comparing RayforceDB against popular DataFrame libraries and databases.

## Live Results

**[View Benchmark Results](https://anthropics.github.io/rayforce-bench/)**

## Quick Start

```bash
# Clone the repository
git clone https://github.com/anthropics/rayforce-bench.git
cd rayforce-bench

# Install dependencies
make setup

# Generate benchmark data (1M rows)
make data

# Run benchmarks
make bench
```

Results are generated in `docs/index.html`.

## Adapters

| Adapter | Type | Description |
|---------|------|-------------|
| `rayforce` | Embedded | RayforceDB native execution via `timeit` |
| `polars` | Embedded | Polars DataFrame (Rust-based) |
| `duckdb` | Embedded | DuckDB embedded SQL |
| `pandas` | Embedded | Pandas DataFrame |
| `questdb` | Server | QuestDB via PostgreSQL protocol |
| `timescale` | Server | TimescaleDB (PostgreSQL) |

## Benchmarks

Based on [H2O.ai db-benchmark](https://h2oai.github.io/db-benchmark/):

### GroupBy Queries
- **Q1**: `sum(v1) group by id1`
- **Q2**: `sum(v1) group by id1, id2`
- **Q3**: `sum(v1), mean(v3) group by id3`
- **Q4**: `mean(v1), mean(v2), mean(v3) group by id3`
- **Q5**: `sum(v1), sum(v2), sum(v3) group by id3`

### Join Queries
- **Inner Join**: Join on `id1`
- **Left Join**: Join on `id1`

### Sort Queries
- **Single Column**: Sort by `id1`
- **Multi Column**: Sort by `id1, id2, id3`

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
  benchmark             groupby, join, sort, or all

Options:
  -d, --data PATH       Path to dataset directory (required)
  -a, --adapters LIST   Adapters to benchmark (default: pandas polars duckdb rayforce)
  -i, --iterations N    Number of measured iterations (default: 5)
  -w, --warmup N        Number of warmup iterations (default: 2)
  --rayforce-local PATH Path to local rayforce-py repo for dev builds
  --html PATH           Output HTML report path (default: docs/index.html)
  --no-html             Skip HTML report generation
  --no-docker           Don't auto-start Docker containers
  --stop-infra          Stop Docker containers after benchmarks
  --check-deps          Check dependencies and exit
```

## Benchmarking with Local Rayforce Build

To benchmark a development build of rayforce-py:

```bash
# Method 1: Using make
make bench-local RAYFORCE_LOCAL=~/rayforce-py

# Method 2: Direct command
python -m bench.runner groupby \
    -d data/groupby_1m_k100 \
    -a pandas polars duckdb rayforce \
    --rayforce-local ~/rayforce-py

# Method 3: Install locally first, then benchmark
cd ~/rayforce-py
pip install -e .
cd ~/rayforce-bench
make bench
```

The `--rayforce-local` option will:
1. Build rayforce-py from the specified path
2. Use the local build for benchmarks
3. Show version as `X.Y.Z (local: /path/to/rayforce-py)`

## Server-Based Adapters (Docker)

QuestDB and TimescaleDB require Docker:

```bash
# Start containers
make infra-start

# Run benchmarks with all adapters
make bench-all

# Stop containers
make infra-stop

# Check container status
make infra-status

# Remove containers completely
make infra-cleanup
```

Container configuration:
- **QuestDB**: Port 8812 (PostgreSQL wire protocol)
- **TimescaleDB**: Port 5433 (to avoid conflict with local PostgreSQL)

## Project Structure

```
rayforce-bench/
├── bench/
│   ├── adapters/           # Database adapters
│   │   ├── base.py         # Abstract Adapter interface
│   │   ├── pandas_adapter.py
│   │   ├── polars_adapter.py
│   │   ├── duckdb_adapter.py
│   │   ├── rayforce_adapter.py
│   │   ├── questdb_adapter.py
│   │   └── timescale_adapter.py
│   ├── generators/         # Data generators
│   │   ├── groupby.py      # H2O-style groupby data
│   │   ├── join.py         # Join benchmark data
│   │   └── sort.py         # Sort benchmark data
│   ├── runner.py           # Benchmark runner CLI
│   ├── report.py           # HTML report generator
│   ├── infra.py            # Docker infrastructure management
│   └── generate.py         # Data generation CLI
├── data/                   # Generated datasets (git-ignored)
├── docs/                   # Generated reports (GitHub Pages)
│   ├── index.html          # Interactive benchmark report
│   └── data.json           # Raw benchmark data
├── Makefile
├── requirements.txt
├── README.md
└── FAIRNESS.md             # Benchmark methodology
```

## Data Format

Datasets are stored as Parquet files with the following schemas:

### GroupBy Dataset
| Column | Type | Description |
|--------|------|-------------|
| id1 | int64 | Low cardinality key (K unique values) |
| id2 | int64 | Low cardinality key (K unique values) |
| id3 | int64 | Low cardinality key (K unique values) |
| v1 | float64 | Value column (normal distribution) |
| v2 | float64 | Value column (normal distribution) |
| v3 | float64 | Value column (normal distribution) |

### Join Dataset
| Table | Columns | Description |
|-------|---------|-------------|
| left | id1, id2, v1 | Left table (larger) |
| right | id1, id3, v2 | Right table (smaller) |

## Fairness

See [FAIRNESS.md](FAIRNESS.md) for detailed methodology on how we ensure fair comparisons.

Key principles:
- Measure query execution time only (not data loading or result serialization)
- Each adapter uses its native timing mechanism where possible
- Data is pre-loaded into memory before timing starts
- Warmup iterations ensure JIT compilation and cache warming

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add your adapter in `bench/adapters/`
4. Update this README
5. Submit a pull request

## License

MIT
