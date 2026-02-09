from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset
from datetime import datetime
from helpers import (
    read_csv,
    save_csv,
    replace_nulls,
    sort_by_date,
    clean_content_column,
    get_all_raw_files,
    get_processed_file_path,
    check_if_file_empty
)

RAW_DIR = "/opt/airflow/data/raw"
PROCESSED_DIR = "/opt/airflow/data/processed"
processed_dataset = Dataset(PROCESSED_DIR)

def branch_file_check():

    files = get_all_raw_files(RAW_DIR)

    all_files_are_empty = True
    for file_path in files:
        if not check_if_file_empty(file_path):
            all_files_are_empty = False
            break

    if all_files_are_empty:
        return "no_files_task"
    else:
        return "processing_tasks.replace_nulls"


def task_1_replace_nulls_in_all():
    print("Task 1: Replacing nulls...")
    files = get_all_raw_files(RAW_DIR)
    for file_path in files:
        if check_if_file_empty(file_path):
            continue

        processed_path = get_processed_file_path(file_path, PROCESSED_DIR)
        df = read_csv(file_path)
        df = replace_nulls(df)
        save_csv(df, processed_path)
    print("Task 1: Done.")


def task_2_sort_all():
    print("Task 2: Sorting by date...")
    files = get_all_raw_files(PROCESSED_DIR)
    for file_path in files:
        df = read_csv(file_path)
        df = sort_by_date(df)
        save_csv(df, file_path)
    print("Task 2: Done.")


def task_3_clean_all():
    print("Task 3: Cleaning content...")
    files = get_all_raw_files(PROCESSED_DIR)
    for file_path in files:
        df = read_csv(file_path)
        df = clean_content_column(df)
        save_csv(df, file_path)
    print("Task 3: Done.")

with DAG(
        dag_id="etl_preprocess_dag",
        schedule="*/5 * * * *",
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=["etl", "processing"]
) as dag:
    wait_for_file = PythonSensor(
        task_id="wait_for_file",
        python_callable=lambda: bool(get_all_raw_files(RAW_DIR)),
        poke_interval=10,
        timeout=600
    )

    check_file_content = BranchPythonOperator(
        task_id="check_file_content",
        python_callable=branch_file_check
    )

    no_files_task = PythonOperator(
        task_id="no_files_task",
        python_callable=lambda: print("Sensor triggered, but all found files are empty. No processing needed.")
    )

    with TaskGroup("processing_tasks") as processing_tasks:
        t1_replace_nulls = PythonOperator(
            task_id="replace_nulls",
            python_callable=task_1_replace_nulls_in_all
        )

        t2_sort_by_date = PythonOperator(
            task_id="sort_by_date",
            python_callable=task_2_sort_all
        )

        t3_clean_content = PythonOperator(
            task_id="clean_content",
            python_callable=task_3_clean_all,
            outlets=[processed_dataset]
        )

        t1_replace_nulls >> t2_sort_by_date >> t3_clean_content

    wait_for_file >> check_file_content
    check_file_content >> [no_files_task, processing_tasks]