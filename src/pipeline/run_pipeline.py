from __future__ import annotations

import argparse
from datetime import date

from .config import resolve_paths
from .ingest import ingest_synthetic
from .quality import run_quality_checks
from .transform import run_transforms
from .utils import get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the City Rides Lakehouse pipeline")
    parser.add_argument("--start-date", required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--days", type=int, default=7, help="Number of days to generate")
    parser.add_argument("--rows-per-day", type=int, default=5000, help="Rows per day")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start_date = date.fromisoformat(args.start_date)

    paths = resolve_paths()

    logger.info("Starting ingestion")
    ingest_synthetic(
        bronze_dir=paths.bronze_dir,
        start_date=start_date,
        days=args.days,
        rows_per_day=args.rows_per_day,
        seed=args.seed,
    )

    logger.info("Running transforms")
    run_transforms(
        warehouse_path=paths.warehouse_path,
        bronze_dir=paths.bronze_dir,
        silver_dir=paths.silver_dir,
        gold_dir=paths.gold_dir,
    )

    logger.info("Running quality checks")
    run_quality_checks(warehouse_path=paths.warehouse_path, report_path=paths.quality_report_path)

    logger.info("Pipeline complete")


if __name__ == "__main__":
    main()
