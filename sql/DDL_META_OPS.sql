CREATE OR REPLACE TABLE META_OPS.ETL_AUDIT_LOG (
    log_id INT IDENTITY(1,1),
    process_name STRING,
    status STRING,
    rows_affected INT,
    message STRING,
    log_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);