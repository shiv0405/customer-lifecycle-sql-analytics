from __future__ import annotations

import argparse
from pathlib import Path

from .config import ProjectPaths
from .warehouse import run_warehouse_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Customer Lifecycle Analytics Warehouse")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_all = subparsers.add_parser("run-all", help="Generate source data, build the warehouse, and export marts")
    run_all.add_argument("--accounts", type=int, default=420)
    run_all.add_argument("--months", type=int, default=24)
    run_all.add_argument("--seed", type=int, default=19)

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = Path.cwd()
    paths = ProjectPaths.from_root(root)

    if args.command == "run-all":
        run_warehouse_pipeline(paths, accounts=args.accounts, months=args.months, seed=args.seed)
        return

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
