# Dataset Manifest Format

Each dataset directory MUST contain a `manifest.json` file describing the dataset.

## Schema

```json
{
  "name": "h2oai_groupby_1e7",
  "description": "H2OAI Group By Benchmark - 10M rows",
  "version": "1.0.0",
  
  "schema": [
    {"name": "id1", "type": "SYMBOL", "nullable": false},
    {"name": "id2", "type": "SYMBOL", "nullable": false},
    {"name": "id3", "type": "SYMBOL", "nullable": false},
    {"name": "id4", "type": "I64", "nullable": false},
    {"name": "id5", "type": "I64", "nullable": false},
    {"name": "id6", "type": "I64", "nullable": false},
    {"name": "v1", "type": "I64", "nullable": false},
    {"name": "v2", "type": "I64", "nullable": false},
    {"name": "v3", "type": "F64", "nullable": false}
  ],
  
  "table_name": "benchmark",
  
  "row_count": 10000000,
  "seed": 42,
  
  "files": [
    "G1_1e7_1e2_0_0.csv"
  ],
  
  "checksums": {
    "G1_1e7_1e2_0_0.csv": "sha256:abc123..."
  },
  
  "generation": {
    "tool": "h2oai/db-benchmark",
    "command": "Rscript _data/groupby-datagen.R 1e7 1e2 0 0",
    "timestamp": "2024-01-15T10:30:00Z"
  }
}
```

## Field Descriptions

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Unique dataset identifier |
| `schema` | array | Column definitions |
| `files` | array | List of CSV filenames (relative to manifest) |
| `row_count` | integer | Total row count across all files |

### Schema Column Types

| Type | Description | Example |
|------|-------------|---------|
| `I64` | 64-bit signed integer | 42 |
| `I32` | 32-bit signed integer | 42 |
| `I16` | 16-bit signed integer | 42 |
| `F64` | 64-bit float | 3.14159 |
| `F32` | 32-bit float | 3.14 |
| `SYMBOL` | Interned string (categorical) | "AAPL" |
| `STRING` | Variable-length string | "Hello" |
| `DATE` | Date (YYYY.MM.DD) | 2024.01.15 |
| `TIME` | Time (HH:MM:SS.mmm) | 09:30:00.000 |
| `TIMESTAMP` | Date + Time | 2024.01.15D09:30:00.000 |
| `BOOL` / `B8` | Boolean | true/false |
| `GUID` | 128-bit UUID | 550e8400-e29b-41d4-a716-446655440000 |

### Optional Fields

| Field | Type | Description |
|-------|------|-------------|
| `description` | string | Human-readable description |
| `version` | string | Dataset version |
| `table_name` | string | Default table name for loading |
| `seed` | integer | Random seed used for generation |
| `checksums` | object | File checksums for verification |
| `generation` | object | How the dataset was generated |

## Example Datasets

### H2OAI Group By (G1)

```
datasets/h2oai_groupby_1e7/
├── manifest.json
└── G1_1e7_1e2_0_0.csv
```

### H2OAI Join (J1)

```
datasets/h2oai_join_1e7/
├── manifest.json
├── J1_1e7_NA_0_0.csv    # Left table
└── J1_1e7_1e7_0_0.csv   # Right table
```

### Partitioned Dataset

```
datasets/timeseries_partitioned/
├── manifest.json
├── data_2024_01.csv
├── data_2024_02.csv
└── data_2024_03.csv
```

## Verification

The benchmark runner verifies:
1. All files listed in `files` exist
2. Checksums match (if provided)
3. Schema is valid
4. Row count matches actual data (optional)
