# Rayforce Benchmark
#
# Usage:
#   make setup            Install dependencies
#   make data             Generate canonical H2O datasets (SIZE=10m default)
#   make bench            Run H2O groupby benchmarks (q1..q7)
#   make bench-join       Run H2O join benchmarks
#   make bench-sort       Run H2O sort benchmarks (s1, s6 on groupby data)
#   make bench-all        Run full H2O suite (groupby + join + sort)
#   make bench-scaling    Run scaling sweep (10..1m by default) → docs/scaling.html
#   make bench-sort-ext   Run extended sort grid (typed × scaling)
#   make clean            Clean generated data
#
# Options:
#   SIZE=10m|1m|100k|10k   H2O dataset size (default: 10m)
#   LOCAL=1                Use rayforce-py from $(RAYFORCE_LOCAL) (~/rayforce-py)
#   ALL=1                  Include QuestDB & TimescaleDB (requires Docker)
#   SIZES=10,100,1k,...    Sizes for bench-scaling sweep
#   SORT_MAX=1m            Max length on the extended sort scaling curve
#   SORT_DTYPES=u8,...     Comma-separated dtypes for the sort grid

.PHONY: setup data bench bench-join bench-sort bench-all bench-scaling \
        bench-sort-ext sort-grid-data clean help

PYTHON ?= python
DATA_DIR ?= data
SIZE ?= 10m
RAYFORCE_LOCAL ?= ~/rayforce-py
ITERATIONS ?= 5
WARMUP ?= 2

# bench-scaling defaults
SIZES ?= 10,100,1k,10k,100k,1m

# Sort grid defaults
SORT_MAX ?= 1m
SORT_ITER ?= 3
SORT_WARMUP ?= 1
SORT_DTYPES ?= u8,i16,i32,i64,f64,str8,str16

# Adapters
ifdef ALL
ADAPTERS := rayforce polars duckdb chdb datafusion pandas questdb timescale
STOP_INFRA := --stop-infra
else
ADAPTERS := rayforce polars duckdb chdb datafusion pandas
STOP_INFRA :=
endif

# Local build flag
ifdef LOCAL
LOCAL_FLAG := --rayforce-local $(RAYFORCE_LOCAL)
else
LOCAL_FLAG :=
endif

RAYFORCE_FLAGS := $(LOCAL_FLAG)

# H2O data paths. Canonical H2O J1 has equal-sized left and right tables.
ifeq ($(SIZE),10k)
GROUPBY_DATA := $(DATA_DIR)/groupby_10k_k100
JOIN_DATA := $(DATA_DIR)/join_10kx10k
JOIN_RIGHT := 10k
else ifeq ($(SIZE),100k)
GROUPBY_DATA := $(DATA_DIR)/groupby_100k_k100
JOIN_DATA := $(DATA_DIR)/join_100kx100k
JOIN_RIGHT := 100k
else ifeq ($(SIZE),1m)
GROUPBY_DATA := $(DATA_DIR)/groupby_1m_k100
JOIN_DATA := $(DATA_DIR)/join_1mx1m
JOIN_RIGHT := 1m
else
GROUPBY_DATA := $(DATA_DIR)/groupby_10m_k100
JOIN_DATA := $(DATA_DIR)/join_10mx10m
JOIN_RIGHT := 10m
endif

# H2O sort uses the groupby dataset (s1=id1, s6=id1+id2+id3) — same convention
# as ~/rayforce/bench/h2o/{s1,s6}.rfl.
SORT_DATA := $(GROUPBY_DATA)

help:
	@echo "make setup            Install dependencies"
	@echo "make data             Generate H2O datasets (SIZE=10m|1m|100k|10k)"
	@echo "make bench            Run H2O groupby benchmarks (q1..q7)"
	@echo "make bench-join       Run H2O join benchmarks"
	@echo "make bench-sort       Run H2O sort (s1, s6 on groupby data)"
	@echo "make bench-all        Run full H2O suite"
	@echo "make bench-scaling    Scaling sweep across sizes → docs/scaling.html"
	@echo "make bench-sort-ext   Extended sort grid (typed scaling)"
	@echo "make clean            Clean generated data"
	@echo ""
	@echo "Options:"
	@echo "  SIZE=10m             Data size: 10k, 100k, 1m, 10m"
	@echo "  LOCAL=1              Use rayforce-py from $(RAYFORCE_LOCAL)"
	@echo "  ALL=1                Include QuestDB & TimescaleDB"
	@echo "  SIZES=10,100,1k,...  Sizes for bench-scaling"
	@echo "  SORT_MAX=1m          Max length on extended sort grid (1k..100m)"

setup:
	@pip install -q -r requirements.txt
	@$(PYTHON) -m bench.runner --check-deps

data:
	@$(PYTHON) -m bench.generate -o $(DATA_DIR) groupby -n $(SIZE) -k 100
	@$(PYTHON) -m bench.generate -o $(DATA_DIR) join --left-rows $(SIZE) --right-rows $(JOIN_RIGHT)

bench: _clean-cache
	@$(PYTHON) -m bench.runner groupby -d $(GROUPBY_DATA) -a $(ADAPTERS) $(RAYFORCE_FLAGS) -i $(ITERATIONS) -w $(WARMUP) $(STOP_INFRA)

bench-join: _clean-cache
	@$(PYTHON) -m bench.runner join -d $(JOIN_DATA) -a $(ADAPTERS) $(RAYFORCE_FLAGS) -i $(ITERATIONS) -w $(WARMUP) $(STOP_INFRA)

bench-sort: _clean-cache
	@$(PYTHON) -m bench.runner sort -d $(SORT_DATA) -a $(ADAPTERS) $(RAYFORCE_FLAGS) -i $(ITERATIONS) -w $(WARMUP) $(STOP_INFRA)

bench-all: _clean-cache
	@$(PYTHON) -m bench.runner all -d $(GROUPBY_DATA) --join-data $(JOIN_DATA) -a $(ADAPTERS) $(RAYFORCE_FLAGS) -i $(ITERATIONS) -w $(WARMUP) $(STOP_INFRA)

# Scaling sweep: every adapter × every op × every size in $(SIZES).
# Generates the interactive scaling.html with engine + op filters.
bench-scaling: _clean-cache
	@$(PYTHON) -u -m bench.scaling_runner \
		--sizes $(SIZES) -a $(ADAPTERS) \
		--data-dir $(DATA_DIR) \
		-i $(ITERATIONS) -w $(WARMUP) \
		$(RAYFORCE_FLAGS) $(STOP_INFRA)

# Extended sort grid: typed columns × scaling lengths (random pattern only).
# QuestDB / Timescale excluded — Docker overhead dwarfs the actual sort.
bench-sort-ext: _clean-cache
	@$(PYTHON) -m bench.sort_grid_runner \
		--max $(SORT_MAX) \
		--dtypes $(SORT_DTYPES) \
		--data-dir $(DATA_DIR)/sort_grid \
		-i $(SORT_ITER) -w $(SORT_WARMUP) \
		$(RAYFORCE_FLAGS)

# Generate sort-grid CSVs without running the bench (useful for sharing data).
sort-grid-data:
	@$(PYTHON) -m bench.sort_grid_runner --gen-only \
		--max $(SORT_MAX) --dtypes $(SORT_DTYPES) \
		--data-dir $(DATA_DIR)/sort_grid

_clean-cache:
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

clean:
	@rm -rf $(DATA_DIR)/groupby_* $(DATA_DIR)/join_* $(DATA_DIR)/sort_grid
	@rm -rf bench/__pycache__ bench/**/__pycache__
	@rm -f docs/data.json docs/sort_data.json docs/scaling_data.json
