# Task 3 - SQL Data Analysis

## Technology Stack
*   **PostgreSQL** (Relational Database)
*   **SQL** (Structured Query Language)

## Project Structure
*   `SQL_Task.sql` — Single SQL script containing solutions for 8 analytical tasks.
*   **Database**: Uses the standard `dvdrental` schema (tables: `film`, `actor`, `category`, `customer`, `rental`, `payment`, etc.).

## Setup and Running

1.  **Prerequisites:**
    *   PostgreSQL server installed.
    *   `dvdrental` database restored.

2.  **Execution:**
    Open `SQL_Task.sql` in any PostgreSQL client (pgAdmin, DBeaver, DataGrip, or psql) and execute the queries individually or as a script.

## Queries
The script includes the following analytical queries:

1.  **Movie Count by Category**: Counts movies in each category, sorted descending.
2.  **Top Actors by Rental Rate**: Lists top 10 actors involved in films, ordered by rental rate.
3.  **Top Revenue Category**: Finding the single most profitable category based on payment amounts.
4.  **Missing Inventory**: Finding film titles that are not present in the inventory.
5.  **Top 'Children' Actors**: using CTEs to rank actors who appeared most in the 'Children' category (top 3).
6.  **Customer Status by City**: Aggregating active vs. inactive customers per city.
7.  **Rental Hours Analysis**: Calculating total rental duration (in hours) per category for cities starting with 'A' or containing '-'.
8.  **Top Category per City**: Ranking categories by rental hours within each city (dense rank = 1) for the same city filter.
