# Task 5 - Spark Data Analysis

## Technology Stack
*   **Apache Spark** (PySpark)
*   **PostgreSQL** (Source Database)
*   **Docker** (Containerization)
*   **Jupyter Notebook** (Development Environment)

## Project Structure
*   `notebooks/PySpark queries.ipynb` — Jupyter notebook containing PySpark solutions for analytical tasks.
*   `docker-compose.yml` — Configuration for Spark and PostgreSQL containers.
*   **Pagila Database**: Source data loaded in PostgreSQL.

## Setup and Running

1.  **Start Environment:**
    ```bash
    docker-compose up -d
    ```

2.  **Open Jupyter:**
    Navigate to `http://localhost:8888` (check container logs for the token).

3.  **Run Notebook:**
    Open `notebooks/PySpark queries.ipynb` and execute cells sequentially.
    *   Ensures JDBC driver (`org.postgresql:postgresql:42.7.3`) is loaded.
    *   Establishes connection to the `pagila` database.

## Analysis Tasks
The notebook implements the following analysis using PySpark DataFrames:

1.  **Movie Count by Category**: Aggregating films per category.
2.  **Top Actors by Rentals**: Identifying top 10 actors based on rental volume.
3.  **Top Revenue Category**: Finding the category generating the most revenue.
4.  **Missing Inventory**: Identifying films that are not currently in inventory (Left Anti Join).
5.  **Top 'Children' Actors**: Ranking actors who appear most in Children's movies using Window functions (`dense_rank`).
6.  **Customer Status by City**: Counting active vs inactive customers per city.
7.  **Rental Hours Analysis**: Calculating total rental duration by category for specific cities (starting with 'a' or containing '-').
