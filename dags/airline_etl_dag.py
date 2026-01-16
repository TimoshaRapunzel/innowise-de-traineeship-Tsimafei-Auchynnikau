from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='airline_snowflake_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['snowflake', 'airline', 'etl']
) as dag:

    load_to_raw = SnowflakeOperator(
        task_id='load_stage_to_raw',
        snowflake_conn_id='snowflake_default',
        database='AIRLINE_DWH',
        sql="CALL AIRLINE_DWH.RAW_STAGE.LOAD_FROM_STAGE_TO_RAW();"
    )

    process_to_core = SnowflakeOperator(
        task_id='process_raw_to_core',
        snowflake_conn_id='snowflake_default',
        database='AIRLINE_DWH',
        sql="CALL CORE_DWH.PROCESS_RAW_TO_CORE();"
    )

    load_to_raw >> process_to_core