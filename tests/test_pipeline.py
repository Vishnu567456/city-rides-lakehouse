from datetime import date

import duckdb

from src.pipeline.config import Paths
from src.pipeline.generate import generate_trips
from src.pipeline.ingest import write_bronze
from src.pipeline.quality import run_quality_checks
from src.pipeline.transform import run_transforms


def test_transform_and_quality(tmp_path):
    paths = Paths(
        project_root=tmp_path,
        data_dir=tmp_path / "data",
        bronze_dir=tmp_path / "data" / "bronze" / "trips",
        silver_dir=tmp_path / "data" / "silver",
        gold_dir=tmp_path / "data" / "gold",
        warehouse_path=tmp_path / "data" / "warehouse.duckdb",
        quality_report_path=tmp_path / "data" / "quality_report.json",
    )

    df = generate_trips(start_date=date(2025, 1, 1), days=1, rows_per_day=200, seed=7)
    write_bronze(df, paths.bronze_dir)

    run_transforms(
        warehouse_path=paths.warehouse_path,
        bronze_dir=paths.bronze_dir,
        silver_dir=paths.silver_dir,
        gold_dir=paths.gold_dir,
    )

    report = run_quality_checks(paths.warehouse_path, paths.quality_report_path)
    assert report["overall_pass"] is True

    con = duckdb.connect(str(paths.warehouse_path), read_only=True)
    try:
        silver_count = con.execute("SELECT COUNT(*) FROM silver_trips").fetchone()[0]
        gold_count = con.execute("SELECT COUNT(*) FROM fct_trip_hourly").fetchone()[0]
    finally:
        con.close()

    assert silver_count > 0
    assert gold_count > 0
