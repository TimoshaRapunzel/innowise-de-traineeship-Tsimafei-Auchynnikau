CREATE OR REPLACE TABLE CORE_DWH.DIM_PASSENGER (
    passenger_sk INT IDENTITY(1,1) PRIMARY KEY,
    passenger_ref_id STRING,
    first_name STRING,
    last_name STRING,
    gender STRING,
    age INT,
    nationality STRING,
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE CORE_DWH.DIM_AIRPORT (
    airport_sk INT IDENTITY(1,1) PRIMARY KEY,
    airport_name STRING,
    country_code STRING,
    country_name STRING,
    continent STRING
);

CREATE OR REPLACE TABLE CORE_DWH.DIM_PILOT (
    pilot_sk INT IDENTITY(1,1) PRIMARY KEY,
    pilot_name STRING
);

CREATE OR REPLACE TABLE CORE_DWH.DIM_FLIGHT_INFO (
    info_sk INT IDENTITY(1,1) PRIMARY KEY,
    ticket_type STRING,
    passenger_status STRING,
    flight_status STRING
);

CREATE OR REPLACE TABLE CORE_DWH.FACT_FLIGHTS (
    flight_id INT IDENTITY(1,1) PRIMARY KEY,
    passenger_sk INT REFERENCES CORE_DWH.DIM_PASSENGER(passenger_sk),
    departure_airport_sk INT REFERENCES CORE_DWH.DIM_AIRPORT(airport_sk),
    arrival_airport_sk INT REFERENCES CORE_DWH.DIM_AIRPORT(airport_sk),
    pilot_sk INT REFERENCES CORE_DWH.DIM_PILOT(pilot_sk),
    info_sk INT REFERENCES CORE_DWH.DIM_FLIGHT_INFO(info_sk),
    departure_date DATE,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);