#!/usr/bin/env python3
"""CLI for generating benchmark data."""

import argparse
from pathlib import Path
import sys

from .generators import GroupByGenerator, JoinGenerator, SortGenerator


def parse_size(s: str) -> int:
    """Parse size string like '10m', '1b', '100k' into integer."""
    s = s.lower().strip()
    multipliers = {
        'k': 1_000,
        'm': 1_000_000,
        'b': 1_000_000_000,
    }
    if s[-1] in multipliers:
        return int(float(s[:-1]) * multipliers[s[-1]])
    return int(s)


def cmd_groupby(args):
    """Generate groupby benchmark data."""
    gen = GroupByGenerator(
        n_rows=parse_size(args.rows),
        k=args.k,
        null_pct=args.null_pct,
        seed=args.seed,
    )
    dataset = gen.generate()
    output_dir = Path(args.output) / dataset.name
    dataset.write(output_dir, formats=args.format.split(','))
    print(f"groupby: {output_dir}")


def cmd_join(args):
    """Generate join benchmark data."""
    gen = JoinGenerator(
        n_rows_left=parse_size(args.left_rows),
        n_rows_right=parse_size(args.right_rows),
        null_pct=args.null_pct,
        seed=args.seed,
    )
    dataset = gen.generate()
    output_dir = Path(args.output) / dataset.name
    dataset.write(output_dir, formats=args.format.split(','))
    print(f"join: {output_dir}")


def cmd_sort(args):
    """Generate sort benchmark data."""
    gen = SortGenerator(
        n_rows=parse_size(args.rows),
        k=args.k,
        null_pct=args.null_pct,
        seed=args.seed,
    )
    dataset = gen.generate()
    output_dir = Path(args.output) / dataset.name
    dataset.write(output_dir, formats=args.format.split(','))
    print(f"sort: {output_dir}")


def cmd_all(args):
    """Generate all standard benchmark datasets."""
    output_base = Path(args.output)
    formats = args.format.split(',')
    size = args.size
    n = parse_size(size)

    gen = GroupByGenerator(n_rows=n, k=100, seed=args.seed)
    ds = gen.generate()
    ds.write(output_base / ds.name, formats=formats)
    print(f"groupby: {output_base / ds.name}")

    gen = JoinGenerator(n_rows_left=n, n_rows_right=n // 10, seed=args.seed)
    ds = gen.generate()
    ds.write(output_base / ds.name, formats=formats)
    print(f"join: {output_base / ds.name}")

    gen = SortGenerator(n_rows=n, seed=args.seed)
    ds = gen.generate()
    ds.write(output_base / ds.name, formats=formats)
    print(f"sort: {output_base / ds.name}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate benchmark data for rayforce-bench"
    )
    parser.add_argument(
        '-o', '--output',
        default='./data',
        help='Output directory (default: ./data)'
    )
    parser.add_argument(
        '-f', '--format',
        default='csv',
        help='Output format(s), comma-separated: parquet,csv (default: csv)'
    )
    parser.add_argument(
        '--seed',
        type=int,
        default=42,
        help='Random seed for reproducibility (default: 42)'
    )

    subparsers = parser.add_subparsers(dest='command', help='Generator type')

    # groupby
    p_groupby = subparsers.add_parser('groupby', help='Generate groupby benchmark data')
    p_groupby.add_argument('-n', '--rows', default='10m', help='Number of rows (default: 10m)')
    p_groupby.add_argument('-k', type=int, default=100, help='Low cardinality (default: 100)')
    p_groupby.add_argument('--null-pct', type=float, default=0.0, help='Null percentage 0.0-1.0')
    p_groupby.set_defaults(func=cmd_groupby)

    # join
    p_join = subparsers.add_parser('join', help='Generate join benchmark data')
    p_join.add_argument('--left-rows', default='10m', help='Left table rows (default: 10m)')
    p_join.add_argument('--right-rows', default='1m', help='Right table rows (default: 1m)')
    p_join.add_argument('--null-pct', type=float, default=0.0, help='Null percentage 0.0-1.0')
    p_join.set_defaults(func=cmd_join)

    # sort
    p_sort = subparsers.add_parser('sort', help='Generate sort benchmark data')
    p_sort.add_argument('-n', '--rows', default='10m', help='Number of rows (default: 10m)')
    p_sort.add_argument('-k', type=int, default=100, help='Cardinality of id columns (default: 100)')
    p_sort.add_argument('--null-pct', type=float, default=0.0, help='Null percentage 0.0-1.0')
    p_sort.set_defaults(func=cmd_sort)

    # all
    p_all = subparsers.add_parser('all', help='Generate all standard datasets')
    p_all.add_argument('-s', '--size', default='10m', help='Data size (default: 10m)')
    p_all.set_defaults(func=cmd_all)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == '__main__':
    main()
