# NYC Taxi Cloud-Ready Data Engineering Pipeline

A production-style Data Engineering portfolio project that demonstrates an end-to-end pipeline for NYC Taxi trip data using **Apache Airflow**, **MinIO local object storage**, **DuckDB**, and **CSV analytics exports**.

This project was originally designed for a GCP-based architecture using Google Cloud Storage and BigQuery. However, due to cloud billing and prepayment constraints during development, the current implementation uses **MinIO as a local object storage substitute for GCS** and **DuckDB as a local analytical warehouse**.

The pipeline is designed to be cloud-ready, meaning the object storage and warehouse layers can be migrated to **Google Cloud Storage** and **BigQuery** in the future when billing is available.

---

## Current Architecture

```text
NYC Taxi Raw Parquet Files
        ↓
Airflow DAG: Check Raw Files
        ↓
Airflow DAG: Upload Raw Files to MinIO
        ↓
MinIO Bucket: nyc-taxi-raw
        ↓
Airflow DAG: Verify MinIO Objects
        ↓
DuckDB Local Warehouse
        ↓
Staging and Mart Tables
        ↓
Airflow DAG: Export Marts to CSV
        ↓
Dashboard-ready CSV Outputs
```

---

## Completed Pipeline Stages

| Phase   | Description                                                    | Status    |
| ------- | -------------------------------------------------------------- | --------- |
| Phase 1 | Airflow local setup with Docker, PostgreSQL, Redis, and MinIO  | Completed |
| Phase 2 | Upload NYC Taxi raw parquet files to MinIO object storage      | Completed |
| Phase 3 | Build DuckDB staging and mart tables from verified raw files   | Completed |
| Phase 4 | Export DuckDB mart tables to CSV files for dashboard/reporting | Completed |
| Phase 5 | README and portfolio cleanup                                   | Completed |
| Future  | GCS and BigQuery integration when cloud billing is available   | Planned   |

---

## Why MinIO Instead of GCS?

The original plan was to use Google Cloud Storage and BigQuery. During setup, Google Cloud required a prepayment before enabling full cloud resources. To avoid unnecessary cost during learning and portfolio development, this project uses **MinIO** as a local object storage layer.

This keeps the engineering design realistic while controlling cost:

* MinIO bucket simulates a cloud raw data zone
* Airflow orchestrates the full workflow
* DuckDB works as a lightweight local analytical warehouse
* CSV exports provide dashboard-ready outputs
* The same architecture can later be migrated to GCS and BigQuery

---

## Current Tech Stack

| Layer               | Tool                           |
| ------------------- | ------------------------------ |
| Orchestration       | Apache Airflow                 |
| Container Runtime   | Docker Desktop + WSL 2         |
| Object Storage      | MinIO                          |
| Local Warehouse     | DuckDB                         |
| Data Format         | Parquet, CSV                   |
| Transformation      | SQL                            |
| Version Control     | Git, GitHub                    |
| Future Cloud Target | Google Cloud Storage, BigQuery |

---

## Airflow DAGs

| DAG                             | Purpose                                              | Status    |
| ------------------------------- | ---------------------------------------------------- | --------- |
| `hello_nyc_taxi_pipeline`       | First learning DAG for Airflow concepts              | Completed |
| `check_nyc_taxi_raw_files`      | Validate that raw parquet files exist in `data/raw/` | Completed |
| `upload_nyc_taxi_to_minio`      | Upload raw parquet files to MinIO bucket             | Completed |
| `build_duckdb_marts_from_minio` | Verify MinIO objects and build DuckDB mart tables    | Completed |
| `export_duckdb_marts_to_csv`    | Export DuckDB mart tables to CSV files               | Completed |

---

## DuckDB Mart Tables

| Table                       | Description                                                     |
| --------------------------- | --------------------------------------------------------------- |
| `stg_taxi_trips`            | Cleaned and typed taxi trip records                             |
| `mart_daily_kpis`           | Daily trip count, revenue, average fare, distance, and duration |
| `mart_hourly_demand`        | Trip demand and revenue by pickup hour                          |
| `mart_payment_mix`          | Trip distribution and revenue by payment type                   |
| `mart_data_quality_summary` | Basic data quality summary for raw trip data                    |

---

## CSV Export Outputs

The final mart tables are exported to `exports/marts/` for dashboard or reporting use.

Generated files:

```text
exports/marts/
├── mart_daily_kpis.csv
├── mart_hourly_demand.csv
├── mart_payment_mix.csv
└── mart_data_quality_summary.csv
```

CSV files are generated outputs and are excluded from Git tracking.

---

## Repository Structure

```text
.
├── dags/
│   ├── hello_nyc_taxi_pipeline.py
│   ├── check_nyc_taxi_raw_files.py
│   ├── upload_nyc_taxi_to_minio.py
│   ├── build_duckdb_marts_from_minio.py
│   └── export_duckdb_marts_to_csv.py
│
├── data/
│   ├── raw/
│   ├── processed/
│   ├── rejected/
│   └── warehouse/
│
├── exports/
│   └── marts/
│
├── config/
│   ├── airflow.env.example
│   └── minio.env.example
│
├── docs/
├── plugins/
├── sql/
├── src/
├── dashboards/
├── docker-compose.airflow.yml
├── .gitignore
└── README.md
```

---

## Local Services

| Service       | URL                     | Purpose                            |
| ------------- | ----------------------- | ---------------------------------- |
| Airflow UI    | `http://localhost:8080` | Orchestrate and monitor DAGs       |
| MinIO Console | `http://localhost:9001` | Browse local object storage bucket |

Default local development credentials are documented in the sample config files under `config/`.

---
## Quick Start

### 1. Clone the repository

```powershell
git clone https://github.com/kriangsak0066/nyc-taxi-gcp-airflow-bigquery-pipeline.git
cd nyc-taxi-gcp-airflow-bigquery-pipeline
```

### 2. Start Airflow and MinIO

```powershell
docker compose -f docker-compose.airflow.yml up -d
```

Check running containers:

```powershell
docker ps
```

Expected local services:

| Service       | URL                     |
| ------------- | ----------------------- |
| Airflow UI    | `http://localhost:8080` |
| MinIO Console | `http://localhost:9001` |

### 3. Prepare raw data

Place NYC Taxi parquet files in:

```text
data/raw/
```

Expected file pattern:

```text
yellow_tripdata_*.parquet
```

Example:

```text
data/raw/yellow_tripdata_2026-01.parquet
data/raw/yellow_tripdata_2026-02.parquet
data/raw/yellow_tripdata_2026-03.parquet
```

### 4. Run Airflow DAGs in order

Trigger the DAGs in this order from the Airflow UI:

| Order | DAG                             | Purpose                              |
| ----- | ------------------------------- | ------------------------------------ |
| 1     | `check_nyc_taxi_raw_files`      | Validate raw parquet files           |
| 2     | `upload_nyc_taxi_to_minio`      | Upload raw files to MinIO            |
| 3     | `build_duckdb_marts_from_minio` | Build DuckDB staging and mart tables |
| 4     | `export_duckdb_marts_to_csv`    | Export mart tables to CSV            |

### 5. Check DuckDB tables

```powershell
docker compose -f docker-compose.airflow.yml exec airflow-worker python -c "import duckdb; con=duckdb.connect('/opt/airflow/data/warehouse/nyc_taxi_minio.duckdb'); print(con.execute('SHOW TABLES').fetchall()); con.close()"
```

Expected tables:

```text
stg_taxi_trips
mart_daily_kpis
mart_hourly_demand
mart_payment_mix
mart_data_quality_summary
```

### 6. Check CSV exports

```powershell
dir exports\marts
```

Expected outputs:

```text
mart_daily_kpis.csv
mart_hourly_demand.csv
mart_payment_mix.csv
mart_data_quality_summary.csv
```

### 7. Stop local services

```powershell
docker compose -f docker-compose.airflow.yml down
```

## Portfolio Value

This project demonstrates practical Data Engineering skills:

* Workflow orchestration with Apache Airflow
* Local object storage design using MinIO
* Raw data zone pattern with parquet files
* Data warehouse modeling with DuckDB
* SQL-based staging and mart transformations
* Data quality summary generation
* CSV exports for BI/dashboard usage
* Cost-aware cloud-ready architecture
* Git and GitHub project documentation

The project shows not only the final pipeline output, but also the engineering decisions made to control cost while preserving a cloud-migration-ready design.

---

## Future Improvements

Planned future improvements:

* Add a Mermaid architecture diagram
* Add data dictionary documentation
* Add automated data quality checks as a separate DAG
* Add BigQuery Sandbox SQL examples
* Migrate MinIO raw zone to Google Cloud Storage when billing is available
* Migrate DuckDB marts to BigQuery warehouse tables
