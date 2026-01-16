CREATE OR REPLACE FILE FORMAT RAW_STAGE.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', '')
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE;

CREATE OR REPLACE TABLE RAW_STAGE.AIRLINE_RAW (
    _index STRING,
    passenger_id STRING,
    first_name STRING,
    last_name STRING,
    gender STRING,
    age STRING,
    nationality STRING,
    airport_name STRING,
    airport_country_code STRING,
    country_name STRING,
    airport_continent STRING,
    continents STRING,
    departure_date STRING,
    arrival_airport STRING,
    pilot_name STRING,
    flight_status STRING,
    ticket_type STRING,
    passenger_status STRING,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STAGE RAW_STAGE.MY_INTERNAL_STAGE
    FILE_FORMAT = RAW_STAGE.CSV_FORMAT;

CREATE OR REPLACE STREAM RAW_STAGE.AIRLINE_RAW_STREAM
ON TABLE RAW_STAGE.AIRLINE_RAW;