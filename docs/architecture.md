# Architecture Overview

This project implements a lakehouse-style pipeline using local storage and DuckDB as a stand-in for a cloud data warehouse. The design separates raw, cleaned, and aggregated datasets while enforcing quality checks.

## Data Flow
- Generate synthetic trip data with realistic distributions
- Write raw Parquet files to a partitioned bronze layer
- Transform to silver and gold tables in DuckDB
- Execute quality checks and emit a machine-readable report

## Storage Layers
- Bronze: raw, append-only Parquet
- Silver: cleaned, typed, deduplicated trips
- Gold: hourly metrics for analytics

## Orchestration
The pipeline is runnable as a single command and can be scheduled in Prefect or a scheduler of choice.
