#!/usr/bin/env python3
"""
Generate example dataset for testing.

Creates a small H2OAI-compatible dataset for testing the benchmark framework.
For production benchmarks, use the official H2OAI data generator.
"""

import csv
import random
import sys
from pathlib import Path


def generate_h2oai_groupby(
    output_path: Path,
    n_rows: int = 1000,
    n_groups: int = 100,
    seed: int = 42,
) -> None:
    """Generate H2OAI Group By compatible dataset.
    
    Schema:
        id1: SYMBOL (n_groups groups)
        id2: SYMBOL (n_groups groups)
        id3: SYMBOL (n_groups groups)
        id4: I64 (n_groups unique values)
        id5: I64 (n_groups unique values)
        id6: I64 (n_groups unique values)
        v1: I64 (random 1-5)
        v2: I64 (random 1-15)
        v3: F64 (random 0-100)
    """
    random.seed(seed)
    
    # Generate group values
    id1_values = [f"id{i:03d}" for i in range(1, n_groups + 1)]
    id2_values = [f"id{i:03d}" for i in range(1, n_groups + 1)]
    id3_values = [f"id{i:03d}" for i in range(1, n_groups + 1)]
    id4_values = list(range(1, n_groups + 1))
    id5_values = list(range(1, n_groups + 1))
    id6_values = list(range(1, n_groups + 1))
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id1", "id2", "id3", "id4", "id5", "id6", "v1", "v2", "v3"])
        
        for _ in range(n_rows):
            row = [
                random.choice(id1_values),
                random.choice(id2_values),
                random.choice(id3_values),
                random.choice(id4_values),
                random.choice(id5_values),
                random.choice(id6_values),
                random.randint(1, 5),
                random.randint(1, 15),
                round(random.uniform(0, 100), 6),
            ]
            writer.writerow(row)
    
    print(f"Generated {n_rows} rows to {output_path}")


def generate_h2oai_join(
    output_dir: Path,
    n_rows: int = 1000,
    seed: int = 42,
) -> None:
    """Generate H2OAI Join compatible dataset (two tables).
    
    Creates:
        - x table (left): J1_{n_rows}_NA_0_0.csv
        - y table (right): J1_{n_rows}_{n_rows}_0_0.csv
    """
    random.seed(seed)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate x table (left)
    x_path = output_dir / f"J1_{n_rows}_NA_0_0.csv"
    with open(x_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id1", "id2", "id3", "id4", "id5", "id6", "v1"])
        
        for i in range(n_rows):
            row = [
                i % 100 + 1,  # id1
                i % 100 + 1,  # id2
                i % 100 + 1,  # id3
                f"id{i % 100 + 1:03d}",  # id4
                f"id{i % 100 + 1:03d}",  # id5
                f"id{i % 100 + 1:03d}",  # id6
                round(random.uniform(0, 100), 6),  # v1
            ]
            writer.writerow(row)
    
    print(f"Generated {n_rows} rows to {x_path}")
    
    # Generate y table (right)
    y_path = output_dir / f"J1_{n_rows}_{n_rows}_0_0.csv"
    with open(y_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id1", "id2", "id3", "id4", "id5", "id6", "v2"])
        
        for i in range(n_rows):
            row = [
                i % 100 + 1,  # id1
                i % 100 + 1,  # id2
                i % 100 + 1,  # id3
                f"id{i % 100 + 1:03d}",  # id4
                f"id{i % 100 + 1:03d}",  # id5
                f"id{i % 100 + 1:03d}",  # id6
                round(random.uniform(0, 100), 6),  # v2
            ]
            writer.writerow(row)
    
    print(f"Generated {n_rows} rows to {y_path}")


def main():
    project_root = Path(__file__).parent.parent
    datasets_dir = project_root / "datasets"
    
    # Generate example group-by dataset
    print("\nGenerating example group-by dataset...")
    generate_h2oai_groupby(
        datasets_dir / "example_groupby" / "data.csv",
        n_rows=1000,
        n_groups=100,
        seed=42,
    )
    
    # Generate example join dataset
    print("\nGenerating example join dataset...")
    generate_h2oai_join(
        datasets_dir / "example_join",
        n_rows=1000,
        seed=42,
    )
    
    print("\n✓ Example datasets generated successfully")
    print(f"  Location: {datasets_dir}")


if __name__ == "__main__":
    main()
