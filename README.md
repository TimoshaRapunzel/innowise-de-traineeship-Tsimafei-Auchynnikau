# Task 4 - Airflow ETL Pipeline

## Technology Stack
*   **Apache Airflow** (Orchestration)
*   **Docker & Docker Compose** (Containerization)
*   **Python** (Scripting)
*   **MongoDB** (NoSQL Database)

## Project Structure
*   `dags/etl_preprocess_dag.py` — Main ETL pipeline DAG.
*   `dags/load_to_mongo_dag.py` — DAG for loading data to MongoDB.
*   `docker-compose.yaml` — Environment configuration.
*   `data/raw` — Landing zone for input CSV files.
*   `data/processed` — Staging area for transformed data.

## Setup and Running

1.  **Start Services:**
    Run the Airflow environment and MongoDB using Docker Compose:
    ```bash
    docker-compose up -d
    ```

2.  **Access Airflow UI:**
    Open `http://localhost:8080` (Default credentials often `airflow`/`airflow`).

3.  **Trigger Pipeline:**
    Resulting pipeline depends on file presence in `data/raw`.
    *   Place a valid CSV file in `data/raw` to wake the sensor.
    *   Verify execution in the Airflow Grid View.

## DAGs Workflow

### 1. `etl_preprocess_dag`
Ran on a 5-minute schedule.
*   **Sensor**: Waits for files in `data/raw`.
*   **Branching**: Checks if files are empty.
    *   *Empty*: Logs "No processing needed".
    *   *Content Found*: Triggers processing tasks.
*   **Processing Tasks**:
    1.  **Replace Nulls**: Fills missing values.
    2.  **Sort**: Orders records by date.
    3.  **Clean Content**: Cleans specific text columns.
    *   *Output*: Updates `processed_dataset` dataset.

### 2. `load_to_mongo_dag`
Event-driven DAG triggered by dataset updates (`PROCESSED_DIR`).
*   **Trigger**: Starts automatically when `etl_preprocess_dag` completes successfully.
*   **Task**: Clear existing MongoDB collection `processed_data` and bulk load the new CSV files from `data/processed`.
