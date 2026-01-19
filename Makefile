# Rayforce Benchmark
#
# Usage:
#   make setup        Install dependencies
#   make data         Generate data (SIZE=1m)
#   make bench        Run benchmarks
#   make clean        Clean up
#
# Options:
#   SIZE=1m|100k|10m  Data size (default: 1m)
#   LOCAL=1           Use local rayforce build from ~/rayforce-py
#   ALL=1             Include QuestDB & TimescaleDB (requires Docker)

.PHONY: setup data bench clean help

PYTHON ?= python
DATA_DIR ?= data
SIZE ?= 1m
RAYFORCE_LOCAL ?= ~/rayforce-py
ITERATIONS ?= 5
WARMUP ?= 2

# Adapters
ifdef ALL
ADAPTERS := rayforce polars duckdb questdb timescale
STOP_INFRA := --stop-infra
else
ADAPTERS := rayforce polars duckdb
STOP_INFRA :=
endif

# Local build flag
ifdef LOCAL
LOCAL_FLAG := --rayforce-local $(RAYFORCE_LOCAL)
else
LOCAL_FLAG :=
endif

# Data paths based on size
ifeq ($(SIZE),100k)
GROUPBY_DATA := $(DATA_DIR)/groupby_100k_k100
JOIN_DATA := $(DATA_DIR)/join_100k_10k
SORT_DATA := $(DATA_DIR)/sort_100k_k100
JOIN_RIGHT := 10k
else ifeq ($(SIZE),10m)
GROUPBY_DATA := $(DATA_DIR)/groupby_10m_k100
JOIN_DATA := $(DATA_DIR)/join_10m_1m
SORT_DATA := $(DATA_DIR)/sort_10m_k100
JOIN_RIGHT := 1m
else
GROUPBY_DATA := $(DATA_DIR)/groupby_1m_k100
JOIN_DATA := $(DATA_DIR)/join_1m_100k
SORT_DATA := $(DATA_DIR)/sort_1m_k100
JOIN_RIGHT := 100k
endif

help:
	@echo "make setup       Install dependencies"
	@echo "make data        Generate data (SIZE=1m|100k|10m)"
	@echo "make bench       Run benchmarks"
	@echo "make clean       Clean generated data"
	@echo ""
	@echo "Options:"
	@echo "  SIZE=1m        Data size: 100k, 1m, 10m"
	@echo "  LOCAL=1        Use local rayforce from ~/rayforce-py"
	@echo "  ALL=1          Include QuestDB & TimescaleDB"

setup:
	@pip install -q -r requirements.txt
	@$(PYTHON) -m bench.runner --check-deps

data:
	@$(PYTHON) -m bench.generate -o $(DATA_DIR) groupby -n $(SIZE) -k 100
	@$(PYTHON) -m bench.generate -o $(DATA_DIR) join --left-rows $(SIZE) --right-rows $(JOIN_RIGHT)
	@$(PYTHON) -m bench.generate -o $(DATA_DIR) sort -n $(SIZE) -k 100

bench: _clean-cache
	@$(PYTHON) -m bench.runner groupby -d $(GROUPBY_DATA) -a $(ADAPTERS) $(LOCAL_FLAG) -i $(ITERATIONS) -w $(WARMUP) $(STOP_INFRA)

bench-join: _clean-cache
	@$(PYTHON) -m bench.runner join -d $(JOIN_DATA) -a $(ADAPTERS) $(LOCAL_FLAG) -i $(ITERATIONS) -w $(WARMUP) $(STOP_INFRA)

bench-sort: _clean-cache
	@$(PYTHON) -m bench.runner sort -d $(SORT_DATA) -a $(ADAPTERS) $(LOCAL_FLAG) -i $(ITERATIONS) -w $(WARMUP) $(STOP_INFRA)

bench-all: _clean-cache
	@$(PYTHON) -m bench.runner all -d $(GROUPBY_DATA) -a $(ADAPTERS) $(LOCAL_FLAG) -i $(ITERATIONS) -w $(WARMUP) $(STOP_INFRA)

_clean-cache:
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

clean:
	@rm -rf $(DATA_DIR)/groupby_* $(DATA_DIR)/join_* $(DATA_DIR)/sort_*
	@rm -rf bench/__pycache__ bench/**/__pycache__
	@rm -f docs/data.json
