# Task 2 - NumPy & Pandas Exercises

## Technology Stack
*   **Python 3.x**
*   **NumPy** (Numerical computing)
*   **Pandas** (Data manipulation and analysis)

## Project Structure
*   `task-2 numpy.py` — Solutions for NumPy vectorization and matrix operation tasks.
*   `task-2 pandas.py` — Solutions for Pandas data analysis on the Adult Census dataset.
*   `data/` — Directory containing datasets (`cereal.csv`, `adult.data.csv`).

## Setup and Running

1.  **Install Requirements:**
    ```bash
    pip install numpy pandas values
    ```

2.  **Run NumPy Exercises:**
    Executes vectorization tasks and CrunchieMunchies analysis.
    ```bash
    python "task-2 numpy.py"
    ```

3.  **Run Pandas Analysis:**
    Executes demographic analysis on census data.
    ```bash
    python "task-2 pandas.py"
    ```

## Tasks & Analysis

### NumPy Tasks
1.  **Array Manipulation**: transformations (sign change, max replacement), set operations, and matrix filtering.
2.  **Vectorization**: Optimization of mathematical operations (diagonal products, Euclidean distance, RLE).
3.  **CrunchieMunchies Analysis**: Statistical analysis of cereal calorie data (distribution, percentiles, standard deviation) to deduce marketing insights.

### Pandas Analysis (Adult Census Data)
Performed demographic analysis on `adult.data.csv` including:
1.  **Demographics**: Gender counts, average age, and citizenship statistics.
2.  **Salary Correlations**: Analyzing education, marital status, and age relative to income (>50K vs <=50K).
3.  **Work Statistics**: Work hours analysis by country and salary level.
4.  **Grouping**: Age group categorization (Young/Adult/Retiree) and occupation-based filtering.
