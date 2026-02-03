from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from .generate import generate_trips
from .utils import get_logger

logger = get_logger(__name__)


def write_bronze(df: pd.DataFrame, bronze_dir: Path) -> list[Path]:
    bronze_dir.mkdir(parents=True, exist_ok=True)

    df = df.copy()
    df["pickup_date"] = pd.to_datetime(df["pickup_datetime"]).dt.date

    written_paths: list[Path] = []
    for pickup_date, partition in df.groupby("pickup_date"):
        partition_dir = bronze_dir / f"pickup_date={pickup_date}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        output_path = partition_dir / "trips.parquet"
        partition.drop(columns=["pickup_date"]).to_parquet(output_path, index=False)
        written_paths.append(output_path)

    return written_paths


def ingest_synthetic(
    bronze_dir: Path,
    start_date: date,
    days: int,
    rows_per_day: int,
    seed: int | None = 42,
) -> list[Path]:
    logger.info("Generating synthetic trips")
    df = generate_trips(start_date=start_date, days=days, rows_per_day=rows_per_day, seed=seed)
    logger.info("Writing bronze partitions")
    return write_bronze(df, bronze_dir)
