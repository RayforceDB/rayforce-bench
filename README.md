# Rayforce Benchmark Framework

A vendor-neutral benchmarking framework to compare RayforceDB against other databases (SQL and non-SQL).

## Goals

- Measure execution engines honestly and reproducibly
- Minimize IPC, network overhead, and serialization costs
- Support both embedded and server-based databases
- Generate reproducible, verifiable results

## Installation

```bash
pip install -r requirements.txt
```

## Project Structure

```
rayforce-bench/
├── benchmarks/           # Benchmark runners and utilities
│   ├── __init__.py
│   ├── adapter.py        # Base adapter interface
│   ├── runner.py         # Benchmark execution engine
│   ├── stats.py          # Statistics computation
│   └── report.py         # HTML report generation
├── adapters/             # Database adapters
│   ├── __init__.py
│   ├── duckdb_adapter.py # DuckDB embedded adapter
│   └── rayforce_adapter.py # RayforceDB adapter (stub)
├── datasets/             # Generated datasets (CSV + manifest)
│   └── h2oai_groupby_1e7/
│       ├── manifest.json
│       └── G1_1e7_1e2_0_0.csv
├── suites/               # Benchmark suite definitions
│   └── groupby.yaml
├── reports/              # Generated HTML reports
├── requirements.txt
└── run_bench.py          # CLI entry point
```

## Quick Start

1. Generate or obtain a dataset (CSV files + manifest.json)
2. Define a benchmark suite (YAML)
3. Run the benchmark:

```bash
python run_bench.py --suite suites/groupby.yaml --adapters duckdb rayforce
```

4. View the report in `reports/`

## Concepts

### Dataset

A dataset is a collection of CSV files with a manifest describing schema and metadata.
The framework treats datasets as immutable inputs.

### Adapter

Each database implements an adapter with a strict interface:
- `setup(schema)` - Initialize database with schema
- `load_csv(csv_paths)` - Load CSV data
- `run(task, params)` - Execute a benchmark task
- `close()` - Cleanup

### Suite

A benchmark suite defines:
- Which dataset to use
- Tasks (queries/operations) to run
- Validation rules
- Warmup and measured iterations
- Cold vs warm execution modes

## Compatibility with Rayforce Benchmarks

This framework aligns with existing Rayforce benchmark conventions:
- Uses H2OAI Group By Benchmark naming (G1_*, J1_*)
- Supports the same benchmark queries (Q1-Q7 group-by, etc.)
- Results are comparable to previously published numbers

## License

MIT
