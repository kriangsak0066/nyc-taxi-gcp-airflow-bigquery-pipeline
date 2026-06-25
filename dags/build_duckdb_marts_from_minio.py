from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import duckdb
from airflow.sdk import dag, task
from minio import Minio


LOCAL_RAW_DIR = Path("/opt/airflow/data/raw")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "nyc-taxi-raw")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_RAW_PREFIX = os.getenv("MINIO_RAW_PREFIX", "nyc_taxi/raw")

MAX_FILES_TO_DUCKDB = int(os.getenv("MAX_FILES_TO_DUCKDB", "1"))
DUCKDB_SAMPLE_ROWS = int(os.getenv("DUCKDB_SAMPLE_ROWS", "50000"))
DUCKDB_DATABASE_PATH = Path(
    os.getenv(
        "DUCKDB_DATABASE_PATH",
        "/opt/airflow/data/warehouse/nyc_taxi_minio.duckdb",
    )
)


def get_minio_client() -> Minio:
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


@dag(
    dag_id="build_duckdb_marts_from_minio",
    description="Verify MinIO raw objects and build lightweight DuckDB marts from local parquet files",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nyc-taxi", "duckdb", "minio", "marts"],
)
def build_duckdb_marts_from_minio():
    @task
    def verify_minio_raw_objects() -> list[str]:
        client = get_minio_client()

        objects = [
            obj.object_name
            for obj in client.list_objects(
                bucket_name=MINIO_BUCKET_NAME,
                prefix=MINIO_RAW_PREFIX,
                recursive=True,
            )
            if obj.object_name.endswith(".parquet")
        ]

        objects = sorted(objects)

        if not objects:
            raise FileNotFoundError(
                f"No parquet objects found in minio://{MINIO_BUCKET_NAME}/{MINIO_RAW_PREFIX}"
            )

        selected_objects = objects[:MAX_FILES_TO_DUCKDB]

        print(f"Found {len(objects)} parquet object(s) in MinIO.")
        print(f"Selected {len(selected_objects)} object(s).")

        for object_name in selected_objects:
            print(f"- minio://{MINIO_BUCKET_NAME}/{object_name}")

        return selected_objects

    @task
    def resolve_local_raw_files(minio_objects: list[str]) -> list[str]:
        local_files = []

        for object_name in minio_objects:
            file_name = Path(object_name).name
            local_file = LOCAL_RAW_DIR / file_name

            if not local_file.exists():
                raise FileNotFoundError(f"Local raw file not found: {local_file}")

            file_size_mb = local_file.stat().st_size / (1024 * 1024)
            print(f"Using local raw file: {local_file} ({file_size_mb:.2f} MB)")

            local_files.append(str(local_file))

        return local_files

    @task
    def build_duckdb_tables(local_files: list[str]) -> dict:
        if not local_files:
            raise ValueError("No local parquet files provided.")

        DUCKDB_DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)

        parquet_path = local_files[0]

        print(f"Building lightweight DuckDB database: {DUCKDB_DATABASE_PATH}")
        print(f"Reading local parquet file: {parquet_path}")
        print(f"Sample rows limit: {DUCKDB_SAMPLE_ROWS}")

        con = duckdb.connect(str(DUCKDB_DATABASE_PATH))

        con.execute("PRAGMA memory_limit='350MB';")
        con.execute("SET threads TO 1;")

        con.execute(
            f"""
            CREATE OR REPLACE TABLE stg_taxi_trips AS
            SELECT
                CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
                CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
                CAST(trip_distance AS DOUBLE) AS trip_distance,
                CAST(PULocationID AS INTEGER) AS pickup_location_id,
                CAST(DOLocationID AS INTEGER) AS dropoff_location_id,
                CAST(payment_type AS INTEGER) AS payment_type,
                CASE
                    WHEN payment_type = 1 THEN 'Credit card'
                    WHEN payment_type = 2 THEN 'Cash'
                    WHEN payment_type = 3 THEN 'No charge'
                    WHEN payment_type = 4 THEN 'Dispute'
                    WHEN payment_type = 5 THEN 'Unknown'
                    WHEN payment_type = 6 THEN 'Voided trip'
                    ELSE 'Other'
                END AS payment_type_name,
                CAST(fare_amount AS DOUBLE) AS fare_amount,
                CAST(tip_amount AS DOUBLE) AS tip_amount,
                CAST(total_amount AS DOUBLE) AS total_amount,
                DATE_DIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_minutes
            FROM read_parquet('{parquet_path}')
            WHERE tpep_pickup_datetime IS NOT NULL
              AND tpep_dropoff_datetime IS NOT NULL
              AND tpep_dropoff_datetime > tpep_pickup_datetime
              AND trip_distance >= 0
              AND total_amount >= 0
            LIMIT {DUCKDB_SAMPLE_ROWS};
            """
        )

        con.execute(
            f"""
            CREATE OR REPLACE TABLE mart_data_quality_summary AS
            SELECT
                COUNT(*) AS sampled_rows,
                SUM(CASE WHEN tpep_pickup_datetime IS NULL THEN 1 ELSE 0 END) AS missing_pickup_datetime,
                SUM(CASE WHEN tpep_dropoff_datetime IS NULL THEN 1 ELSE 0 END) AS missing_dropoff_datetime,
                SUM(CASE WHEN tpep_dropoff_datetime <= tpep_pickup_datetime THEN 1 ELSE 0 END) AS invalid_time_order,
                SUM(CASE WHEN trip_distance < 0 THEN 1 ELSE 0 END) AS negative_trip_distance,
                SUM(CASE WHEN total_amount < 0 THEN 1 ELSE 0 END) AS negative_total_amount
            FROM (
                SELECT *
                FROM read_parquet('{parquet_path}')
                LIMIT {DUCKDB_SAMPLE_ROWS}
            );
            """
        )

        con.execute(
            """
            CREATE OR REPLACE TABLE mart_daily_kpis AS
            SELECT
                CAST(pickup_datetime AS DATE) AS pickup_date,
                COUNT(*) AS trip_count,
                ROUND(SUM(total_amount), 2) AS total_revenue,
                ROUND(AVG(total_amount), 2) AS avg_total_amount,
                ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
                ROUND(AVG(trip_duration_minutes), 2) AS avg_trip_duration_minutes
            FROM stg_taxi_trips
            GROUP BY 1
            ORDER BY 1;
            """
        )

        con.execute(
            """
            CREATE OR REPLACE TABLE mart_hourly_demand AS
            SELECT
                EXTRACT(hour FROM pickup_datetime) AS pickup_hour,
                COUNT(*) AS trip_count,
                ROUND(SUM(total_amount), 2) AS total_revenue
            FROM stg_taxi_trips
            GROUP BY 1
            ORDER BY 1;
            """
        )

        con.execute(
            """
            CREATE OR REPLACE TABLE mart_payment_mix AS
            SELECT
                payment_type_name,
                COUNT(*) AS trip_count,
                ROUND(SUM(total_amount), 2) AS total_revenue,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS trip_share_pct
            FROM stg_taxi_trips
            GROUP BY 1
            ORDER BY trip_count DESC;
            """
        )

        summary = {
            "database_path": str(DUCKDB_DATABASE_PATH),
            "source_file": parquet_path,
            "sample_rows_limit": DUCKDB_SAMPLE_ROWS,
            "staging_rows": con.execute("SELECT COUNT(*) FROM stg_taxi_trips").fetchone()[0],
            "daily_kpi_rows": con.execute("SELECT COUNT(*) FROM mart_daily_kpis").fetchone()[0],
            "hourly_rows": con.execute("SELECT COUNT(*) FROM mart_hourly_demand").fetchone()[0],
            "payment_rows": con.execute("SELECT COUNT(*) FROM mart_payment_mix").fetchone()[0],
            "quality_rows": con.execute("SELECT COUNT(*) FROM mart_data_quality_summary").fetchone()[0],
        }

        print("DuckDB build summary:")
        print(summary)

        con.close()
        return summary

    minio_objects = verify_minio_raw_objects()
    local_files = resolve_local_raw_files(minio_objects)
    build_duckdb_tables(local_files)


build_duckdb_marts_from_minio()