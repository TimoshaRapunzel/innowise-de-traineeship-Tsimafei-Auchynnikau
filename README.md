# Helsinki Bikes ETL Project

A robust data pipeline for processing Helsinki bike-sharing data using Docker, LocalStack, Apache Airflow, and Spark.

## Project Structure
- `dags/`: Airflow DAGs for orchestration.
- `docker/`: Infrastructure configuration and initialization scripts.
- `src/`: Core Python application logic following SOLID principles.
- `tests/`: Automated tests.

## Setup
1. Ensure Docker and Docker Compose are installed.
2. Clone the repository.
3. Run `docker-compose up -d` to start the infrastructure.
4. Access Airflow at `http://localhost:8081`.
