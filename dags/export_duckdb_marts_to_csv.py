from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import duckdb
from airflow.sdk import dag, task


DUCKDB_DATABASE_PATH = Path(
    os.getenv(
        "DUCKDB_DATABASE_PATH",
        "/opt/airflow/data/warehouse/nyc_taxi_minio.duckdb",
    )
)

EXPORT_DIR = Path(os.getenv("MART_EXPORT_DIR", "/opt/airflow/exports/marts"))

MART_TABLES = [
    "mart_daily_kpis",
    "mart_hourly_demand",
    "mart_payment_mix",
    "mart_data_quality_summary",
]


@dag(
    dag_id="export_duckdb_marts_to_csv",
    description="Export DuckDB mart tables to CSV files for dashboard and reporting",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nyc-taxi", "duckdb", "exports", "marts"],
)
def export_duckdb_marts_to_csv():
    @task
    def check_duckdb_database() -> str:
        if not DUCKDB_DATABASE_PATH.exists():
            raise FileNotFoundError(f"DuckDB database not found: {DUCKDB_DATABASE_PATH}")

        file_size_mb = DUCKDB_DATABASE_PATH.stat().st_size / (1024 * 1024)
        print(f"DuckDB database found: {DUCKDB_DATABASE_PATH} ({file_size_mb:.2f} MB)")

        return str(DUCKDB_DATABASE_PATH)

    @task
    def validate_mart_tables(database_path: str) -> list[str]:
        con = duckdb.connect(database_path)

        existing_tables = {
            row[0]
            for row in con.execute("SHOW TABLES").fetchall()
        }

        missing_tables = [
            table
            for table in MART_TABLES
            if table not in existing_tables
        ]

        if missing_tables:
            raise ValueError(f"Missing mart table(s): {missing_tables}")

        print("All required mart tables exist:")
        for table in MART_TABLES:
            row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"- {table}: {row_count} row(s)")

        con.close()

        return MART_TABLES

    @task
    def export_marts_to_csv(database_path: str, mart_tables: list[str]) -> list[str]:
        EXPORT_DIR.mkdir(parents=True, exist_ok=True)

        con = duckdb.connect(database_path)
        exported_files = []

        for table in mart_tables:
            output_path = EXPORT_DIR / f"{table}.csv"

            con.execute(
                f"""
                COPY (
                    SELECT *
                    FROM {table}
                )
                TO '{output_path}'
                WITH (HEADER, DELIMITER ',');
                """
            )

            file_size_kb = output_path.stat().st_size / 1024
            print(f"Exported {table} to {output_path} ({file_size_kb:.2f} KB)")

            exported_files.append(str(output_path))

        con.close()

        return exported_files

    @task
    def summarize_exports(exported_files: list[str]) -> dict:
        summary = {
            "export_dir": str(EXPORT_DIR),
            "exported_file_count": len(exported_files),
            "files": exported_files,
        }

        print("Export summary:")
        print(summary)

        return summary

    database_path = check_duckdb_database()
    mart_tables = validate_mart_tables(database_path)
    exported_files = export_marts_to_csv(database_path, mart_tables)
    summarize_exports(exported_files)


export_duckdb_marts_to_csv()