# Task 1 - Student-Room Database Analysis

## Technology Stack
*   **Python 3.x**
*   **PostgreSQL**
*   **Psycopg2** (PostgreSQL adapter)
*   **Ijson** (Streaming JSON parser)
*   **XML & JSON** (Output formats)

## Project Structure
*   `main.py` — Entry point that orchestrates the entire workflow.
*   `config.py` — Database configuration settings.
*   `db_connector.py` — Database connection handler with context manager.
*   `data_services.py` — Data loading and SQL query execution logic.
*   `formatter.py` — Output formatting (JSON/XML).
*   `students.json` — Input student data.
*   `rooms.json` — Input room data.
*   `results.json` / `results.xml` — Generated analysis results.

## Setup and Running

1.  **Install Requirements:**
    Ensure you have Python installed and install the dependencies:
    ```bash
    pip install psycopg2-binary ijson
    ```

2.  **Database Setup:**
    Create the database `Task-1` (or your preferred name) and the required tables:
    ```sql
    CREATE TABLE "Rooms" ("id" INTEGER PRIMARY KEY, "name" VARCHAR(255));
    CREATE TABLE "Students" ("id" INTEGER PRIMARY KEY, "name" VARCHAR(255), "birthday" DATE, "room" INTEGER REFERENCES "Rooms", "sex" CHAR(1));
    ```

3.  **Configure Connection:**
    Edit `config.py` with your credentials:
    ```python
    DB_CONFIG = { "host": "localhost", "database": "Task-1", "user": "postgres", "password": "...", "port": "5432" }
    ```

4.  **Run Application:**
    Execute the script and choose your output format:
    ```bash
    python main.py
    ```

## Queries
The project runs the following analytical queries on the data:

1.  Count students in each room, ordered by population.
2.  Identify top 5 rooms with the lowest average student age.
3.  Find top 5 rooms with the biggest age difference between the oldest and youngest student.
4.  List rooms that contain mixed-sex students (both Male and Female).

