from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import csv
import os
import time

LOCALSTACK_ENDPOINT = os.environ.get('LOCALSTACK_ENDPOINT', 'http://localstack:4566')
BUCKET_NAME = "helsinki-bikes-data"
TABLE_NAME = "BikeMetrics"
EXPORT_PATH = "/opt/airflow/data/helsinki_bikes_final_metrics.csv"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def export_dynamo_to_csv():
    print(f"Waiting 15 seconds for Lambda to finish background writes...")
    time.sleep(15)

    print(f"Connecting to DynamoDB at {LOCALSTACK_ENDPOINT}...")
    dynamodb = boto3.resource('dynamodb', endpoint_url=LOCALSTACK_ENDPOINT, region_name='us-east-1')
    table = dynamodb.Table(TABLE_NAME)

    print(f"Scanning table {TABLE_NAME}...")
    response = table.scan()
    items = response.get('Items', [])

    if not items:
        print("❌ No data found in DynamoDB. Task failed.")
        return

    headers = ['StationName', 'DepartureCount', 'ReturnCount', 'ProcessedAt']

    print(f"Exporting {len(items)} rows to {EXPORT_PATH}...")
    with open(EXPORT_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')
        writer.writeheader()
        for item in items:
            row = {
                'StationName': item.get('StationName'),
                'DepartureCount': int(item.get('DepartureCount', 0)),
                'ReturnCount': int(item.get('ReturnCount', 0)),
                'ProcessedAt': item.get('ProcessedAt')
            }
            writer.writerow(row)
    
    print(f"✅ Export finished! File ready at: {EXPORT_PATH}")

with DAG(
    'helsinki_bikes_etl_full',
    default_args=default_args,
    description='Full ETL: LocalStack Setup -> Spark -> DynamoDB -> Tableau CSV',
    schedule_interval=None,
    catchup=False,
) as dag:

    init_infra = BashOperator(
        task_id='init_infrastructure',
        bash_command='python /opt/airflow/infrastructure/init_infrastructure.py',
    )

    split_csv = BashOperator(
        task_id='split_big_csv',
        bash_command='python /opt/airflow/src/data_splitter.py',
    )

    upload_to_s3 = BashOperator(
        task_id='upload_files_to_s3',
        bash_command='python /opt/airflow/src/aws/s3_client.py',
    )

    process_spark = BashOperator(
        task_id='process_with_spark',
        bash_command=(
            'python /opt/airflow/src/spark_processor.py '
            f's3a://{BUCKET_NAME}/raw/2017-05.csv '
            f's3a://{BUCKET_NAME}/processed/2017-05_metrics'
        ),
    )

    export_csv = PythonOperator(
        task_id='export_to_csv_for_tableau',
        python_callable=export_dynamo_to_csv,
    )

    init_infra >> split_csv >> upload_to_s3 >> process_spark >> export_csv
