# Task 6 - Snowflake Data Warehouse

## Technology Stack
*   **Snowflake** (Cloud Data Warehouse)
*   **Apache Airflow** (Orchestration)
*   **SQL** (Stored Procedures, DDL, DML)

## Project Structure
*   `dags/airline_etl_dag.py` — Airflow DAG orchestrating the Snowflake pipeline.
*   `sql/` — DDL and DML scripts for Snowflake objects:
    *   `DDL_RAW_LAYER.sql`, `DDL_CORE_LAYER.sql` — Schema definitions (Star Schema).
    *   `PROCEDURES_ETL.sql` — Stored procedures for data loading and transformation.
    *   `SQL_SECURE_VIEW.sql` — Row Level Security (RLS) implementation.
*   `Snowflake PowerBI.pbix` — PowerBI report connected to the warehouse.

## ETL Pipeline Architecture
The pipeline follows a multi-layer Data Warehouse architecture:
1.  **Stage Layer**: External stage loading data from CSV snippets.
2.  **Raw Layer**: Staging table (`AIRLINE_RAW`) receiving raw data.
3.  **Core Layer**: Star schema populated via Stored Procedures.

### Schema Design (Star Schema)
*   **Fact Table**: `FACT_FLIGHTS`
*   **Dimension Tables**: `DIM_PILOT`, `DIM_AIRPORT`, `DIM_PASSENGER`, `DIM_FLIGHT_INFO`

## Workflow (Airflow DAG)
The `airline_snowflake_pipeline` DAG executes two main steps:
1.  **Load Stage to Raw**: Calls `AIRLINE_DWH.RAW_STAGE.LOAD_FROM_STAGE_TO_RAW()` to copy data from the internal stage to the raw table.
2.  **Process Raw to Core**: Calls `CORE_DWH.PROCESS_RAW_TO_CORE()` to transform and load data into the Core layer.

## Key Features
*   **Stored Procedures**: Encapsulate ETL logic including transaction management.
*   **Streams & CD**: Uses `AIRLINE_RAW_STREAM` to process only new data (Change Data Capture).
*   **SCD / Merge**: Uses `MERGE` statements to handle dimension updates and avoid duplicates.
*   **Audit Logging**: Logs ETL execution status (Success/Error) and row counts to `META_OPS.ETL_AUDIT_LOG`.
*   **Security**: Implements Row Level Security (RLS) via Secure Views (`SECURE_FACT_FLIGHTS`) and Row Access Policies to restrict access based on roles (`ACCOUNTADMIN`).
