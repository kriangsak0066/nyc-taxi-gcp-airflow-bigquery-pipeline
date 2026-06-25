from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="hello_nyc_taxi_pipeline",
    description="First learning DAG for NYC Taxi GCP Airflow BigQuery pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["learning", "nyc-taxi", "airflow"],
)
def hello_nyc_taxi_pipeline():
    @task
    def start_pipeline() -> str:
        message = "Starting NYC Taxi Airflow learning pipeline"
        print(message)
        return message

    @task
    def check_source_files() -> str:
        message = "Checking source files step - placeholder for future NYC Taxi Parquet validation"
        print(message)
        return message

    @task
    def print_pipeline_plan() -> list[str]:
        plan = [
            "1. Read NYC Taxi Parquet files",
            "2. Upload raw files to Google Cloud Storage",
            "3. Load raw data into BigQuery",
            "4. Transform raw data into staging, warehouse, and mart tables",
            "5. Run data quality checks",
        ]

        for step in plan:
            print(step)

        return plan

    @task
    def finish_pipeline() -> str:
        message = "Finished first Airflow DAG successfully"
        print(message)
        return message

    start = start_pipeline()
    check_files = check_source_files()
    plan = print_pipeline_plan()
    finish = finish_pipeline()

    start >> check_files >> plan >> finish


hello_nyc_taxi_pipeline()