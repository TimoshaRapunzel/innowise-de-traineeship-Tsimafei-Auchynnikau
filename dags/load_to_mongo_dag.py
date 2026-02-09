from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime
from helpers import get_all_raw_files, load_csv_to_mongo, read_csv
from airflow.providers.mongo.hooks.mongo import MongoHook

PROCESSED_DIR = "/opt/airflow/data/processed"
processed_dataset = Dataset(PROCESSED_DIR)


MONGO_CONN_ID = "mongo_default"
DB_NAME = "airflow_db"
COLLECTION_NAME = "processed_data"

def load_all_processed_files():
    files = get_all_raw_files(PROCESSED_DIR)
    if not files:
        print("No processed files found to load.")
        return

    print(f"Found {len(files)} files to load. Clearing collection '{COLLECTION_NAME}'...")

    hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    client = hook.get_conn()
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    collection.delete_many({})

    print(f"Collection cleared. Starting files load...")

    for file_path in files:
        load_csv_to_mongo(file_path, MONGO_CONN_ID, DB_NAME, COLLECTION_NAME)

    print("All files successfully loaded to MongoDB.")


with DAG(
        dag_id="load_to_mongo_dag",
        schedule=[processed_dataset],
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=["etl", "mongo"]
) as dag:
    load_task = PythonOperator(
        task_id="load_all_processed_files",
        python_callable=load_all_processed_files
    )