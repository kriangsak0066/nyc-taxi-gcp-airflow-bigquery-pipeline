from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task


RAW_DATA_DIR = Path("/opt/airflow/data/raw")
FILE_PATTERN = "yellow_tripdata_*.parquet"


@dag(
    dag_id="check_nyc_taxi_raw_files",
    description="Check NYC Taxi raw Parquet files before running the cloud ELT pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nyc-taxi", "data-quality", "raw-files"],
)
def check_nyc_taxi_raw_files():
    @task
    def check_raw_folder_exists() -> str:
        if not RAW_DATA_DIR.exists():
            raise FileNotFoundError(f"Raw data folder not found: {RAW_DATA_DIR}")

        message = f"Raw data folder exists: {RAW_DATA_DIR}"
        print(message)
        return message

    @task
    def list_parquet_files() -> list[str]:
        files = sorted(RAW_DATA_DIR.glob(FILE_PATTERN))

        if not files:
            raise FileNotFoundError(
                f"No NYC Taxi Parquet files found in {RAW_DATA_DIR} with pattern {FILE_PATTERN}"
            )

        file_paths = [str(file) for file in files]

        print(f"Found {len(file_paths)} parquet file(s):")
        for file_path in file_paths:
            print(f"- {file_path}")

        return file_paths

    @task
    def summarize_files(file_paths: list[str]) -> dict:
        summary = {
            "raw_data_dir": str(RAW_DATA_DIR),
            "file_pattern": FILE_PATTERN,
            "file_count": len(file_paths),
            "files": file_paths,
        }

        print("Raw file summary:")
        print(summary)

        return summary

    raw_folder = check_raw_folder_exists()
    parquet_files = list_parquet_files()
    summary = summarize_files(parquet_files)

    raw_folder >> parquet_files >> summary


check_nyc_taxi_raw_files()