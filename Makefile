# Rayforce Benchmark Framework
# =============================
#
# Usage:
#   make setup          - Install Python dependencies
#   make data           - Generate benchmark datasets (1M rows)
#   make data-small     - Generate small datasets for testing (100K rows)
#   make data-large     - Generate large datasets (10M rows)
#   make bench          - Run all benchmarks with default adapters
#   make bench-all      - Run benchmarks with all adapters (requires Docker)
#   make bench-local    - Run benchmarks with local rayforce build
#   make infra-start    - Start Docker containers (QuestDB, TimescaleDB)
#   make infra-stop     - Stop Docker containers
#   make clean          - Remove generated data and cache
#   make check          - Check dependencies and environment

.PHONY: setup data data-small data-large bench bench-all bench-local \
        infra-start infra-stop clean check help

# Configuration
PYTHON ?= python
DATA_DIR ?= data
RAYFORCE_LOCAL ?= ~/rayforce-py
ADAPTERS_DEFAULT ?= pandas polars duckdb rayforce
ADAPTERS_ALL ?= pandas polars duckdb questdb timescale rayforce
ITERATIONS ?= 5
WARMUP ?= 2

# Default target
help:
	@echo "Rayforce Benchmark Framework"
	@echo "============================"
	@echo ""
	@echo "Quick Start:"
	@echo "  make setup && make data && make bench"
	@echo ""
	@echo "Targets:"
	@echo "  setup          Install Python dependencies"
	@echo "  data           Generate benchmark data (1M rows)"
	@echo "  data-small     Generate small data (100K rows)"
	@echo "  data-large     Generate large data (10M rows)"
	@echo "  bench          Run benchmarks (pandas, polars, duckdb, rayforce)"
	@echo "  bench-all      Run benchmarks with all adapters (requires Docker)"
	@echo "  bench-local    Run benchmarks with local rayforce build"
	@echo "  infra-start    Start QuestDB and TimescaleDB containers"
	@echo "  infra-stop     Stop Docker containers"
	@echo "  check          Check dependencies"
	@echo "  clean          Remove generated data"
	@echo ""
	@echo "Options:"
	@echo "  RAYFORCE_LOCAL=~/rayforce-py  Path to local rayforce-py repo"
	@echo "  ITERATIONS=5                   Number of benchmark iterations"
	@echo "  WARMUP=2                       Number of warmup iterations"

# Install dependencies
setup:
	@echo "=== Installing dependencies ==="
	pip install -r requirements.txt
	@echo ""
	@echo "=== Checking installation ==="
	$(PYTHON) -m bench.runner --check-deps

# Check dependencies
check:
	$(PYTHON) -m bench.runner --check-deps

# Generate standard datasets (1M rows)
data:
	@echo "=== Generating groupby data (1M rows) ==="
	$(PYTHON) -m bench.generate -o $(DATA_DIR) groupby -n 1m -k 100
	@echo ""
	@echo "=== Generating join data (1M x 100K rows) ==="
	$(PYTHON) -m bench.generate -o $(DATA_DIR) join --left-rows 1m --right-rows 100k
	@echo ""
	@echo "=== Generating sort data (1M rows) ==="
	$(PYTHON) -m bench.generate -o $(DATA_DIR) sort -n 1m -k 100
	@echo ""
	@echo "Data generated in $(DATA_DIR)/"

# Generate small datasets for testing
data-small:
	@echo "=== Generating small datasets (100K rows) ==="
	$(PYTHON) -m bench.generate -o $(DATA_DIR) groupby -n 100k -k 100
	$(PYTHON) -m bench.generate -o $(DATA_DIR) join --left-rows 100k --right-rows 10k
	$(PYTHON) -m bench.generate -o $(DATA_DIR) sort -n 100k -k 100

# Generate large datasets
data-large:
	@echo "=== Generating large datasets (10M rows) ==="
	$(PYTHON) -m bench.generate -o $(DATA_DIR) groupby -n 10m -k 100
	$(PYTHON) -m bench.generate -o $(DATA_DIR) join --left-rows 10m --right-rows 1m
	$(PYTHON) -m bench.generate -o $(DATA_DIR) sort -n 10m -k 100

# Run benchmarks with default adapters
bench: bench-groupby bench-join bench-sort
	@echo ""
	@echo "=== All benchmarks complete ==="
	@echo "Results: docs/data.json"
	@echo "Report:  docs/index.html"

bench-groupby:
	@echo "=== Running groupby benchmarks ==="
	$(PYTHON) -m bench.runner groupby \
		-d $(DATA_DIR)/groupby_1m_k100 \
		-a $(ADAPTERS_DEFAULT) \
		-i $(ITERATIONS) -w $(WARMUP)

bench-join:
	@echo "=== Running join benchmarks ==="
	$(PYTHON) -m bench.runner join \
		-d $(DATA_DIR)/join_1m_100k \
		-a $(ADAPTERS_DEFAULT) \
		-i $(ITERATIONS) -w $(WARMUP)

bench-sort:
	@echo "=== Running sort benchmarks ==="
	$(PYTHON) -m bench.runner sort \
		-d $(DATA_DIR)/sort_1m_k100 \
		-a $(ADAPTERS_DEFAULT) \
		-i $(ITERATIONS) -w $(WARMUP)

# Run benchmarks with all adapters (requires Docker)
bench-all:
	@echo "=== Running benchmarks with all adapters ==="
	$(PYTHON) -m bench.runner all \
		-d $(DATA_DIR)/groupby_1m_k100 \
		-a $(ADAPTERS_ALL) \
		-i $(ITERATIONS) -w $(WARMUP) \
		--stop-infra

# Run benchmarks with local rayforce build
bench-local:
	@echo "=== Running benchmarks with local rayforce build ==="
	@if [ ! -d "$(RAYFORCE_LOCAL)" ]; then \
		echo "Error: RAYFORCE_LOCAL=$(RAYFORCE_LOCAL) not found"; \
		echo "Set RAYFORCE_LOCAL to your rayforce-py repo path"; \
		exit 1; \
	fi
	$(PYTHON) -m bench.runner groupby \
		-d $(DATA_DIR)/groupby_1m_k100 \
		-a $(ADAPTERS_DEFAULT) \
		--rayforce-local $(RAYFORCE_LOCAL) \
		-i $(ITERATIONS) -w $(WARMUP)

# Infrastructure management
infra-start:
	@echo "=== Starting Docker containers ==="
	$(PYTHON) -m bench.infra start

infra-stop:
	@echo "=== Stopping Docker containers ==="
	$(PYTHON) -m bench.infra stop

infra-status:
	$(PYTHON) -m bench.infra status

infra-cleanup:
	@echo "=== Removing Docker containers ==="
	$(PYTHON) -m bench.infra cleanup

# Clean up
clean:
	@echo "=== Cleaning up ==="
	rm -rf $(DATA_DIR)/groupby_* $(DATA_DIR)/join_* $(DATA_DIR)/sort_*
	rm -rf bench/__pycache__ bench/**/__pycache__
	rm -f docs/data.json
	@echo "Cleaned."

clean-all: clean
	rm -rf $(DATA_DIR)

# Development helpers
lint:
	ruff check bench/

format:
	ruff format bench/
