# Rayforce Benchmark Framework

A vendor-neutral benchmarking framework to compare RayforceDB against other databases.

## Live Results

📊 **[View Benchmark Results](https://your-github-username.github.io/rayforce-bench/)**

## Goals

- Measure execution engines honestly and reproducibly
- Minimize IPC, network overhead, and serialization costs
- Support both embedded and server-based databases
- Generate reproducible, verifiable results

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run benchmarks
python run_bench.py --suite suites/example_full.yaml \
    --dataset datasets/example_groupby \
    --adapters duckdb rayforce \
    --rayforce-binary /path/to/rayforce

# Results are generated in docs/ (for GitHub Pages)
```

## Project Structure

```
rayforce-bench/
├── benchmarks/           # Benchmark runners and utilities
│   ├── adapter.py        # Base adapter interface
│   ├── runner.py         # Benchmark execution engine
│   ├── stats.py          # Statistics computation
│   └── report.py         # HTML report generation
├── adapters/             # Database adapters
│   ├── duckdb_adapter.py # DuckDB embedded adapter
│   ├── rayforce_adapter.py # RayforceDB adapter
│   └── kdb_adapter.py    # KDB+/q adapter (stub)
├── datasets/             # Test datasets (CSV + manifest)
│   ├── example_groupby/
│   └── example_join/
├── suites/               # Benchmark suite definitions
│   ├── example.yaml
│   ├── example_full.yaml
│   ├── example_join.yaml
│   ├── groupby.yaml      # H2OAI Group By benchmark
│   └── join.yaml         # H2OAI Join benchmark
├── docs/                 # Generated HTML (GitHub Pages)
├── requirements.txt
└── run_bench.py          # CLI entry point
```

## Configuration

Create `config.local.yaml` to specify local paths:

```yaml
rayforce:
  binary_path: /path/to/rayforce

kdb:
  binary_path: /path/to/q
```

Or use command-line options:

```bash
python run_bench.py --rayforce-binary /path/to/rayforce ...
```

## GitHub Pages Deployment

The benchmark results are automatically generated as a static website in `docs/`:

1. Run benchmarks to generate `docs/index.html`
2. Commit and push to GitHub
3. Enable GitHub Pages from repository Settings → Pages → Source: `docs/`

The report includes:
- Interactive charts with ECharts
- Dark/light mode toggle
- Performance comparison cards
- Detailed results table
- Per-query breakdown

## Adding a New Adapter

1. Create `adapters/your_adapter.py` implementing `Adapter` interface
2. Register in `run_bench.py`
3. Run benchmarks

## Fairness

See [FAIRNESS.md](FAIRNESS.md) for methodology ensuring fair comparisons between databases.

## License

MIT
