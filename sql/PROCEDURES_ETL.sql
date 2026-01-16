CREATE OR REPLACE PROCEDURE AIRLINE_DWH.RAW_STAGE.LOAD_FROM_STAGE_TO_RAW()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    BEGIN TRANSACTION;
    
    COPY INTO AIRLINE_DWH.RAW_STAGE.AIRLINE_RAW
    FROM @AIRLINE_DWH.RAW_STAGE.MY_INTERNAL_STAGE
    FILE_FORMAT = (FORMAT_NAME = AIRLINE_DWH.RAW_STAGE.CSV_FORMAT)
    PATTERN = '.*.csv'
    ON_ERROR = 'SKIP_FILE';
    
    COMMIT;
    RETURN 'Success';

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN 'Error: ' || SQLERRM;
END;
$$;

CREATE OR REPLACE PROCEDURE CORE_DWH.PROCESS_RAW_TO_CORE()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    row_count INT DEFAULT 0;
BEGIN
    IF (SYSTEM$STREAM_HAS_DATA('RAW_STAGE.AIRLINE_RAW_STREAM') = FALSE) THEN
        RETURN 'No new data in stream. Skipping.';
    END IF;

    BEGIN TRANSACTION;

    MERGE INTO CORE_DWH.DIM_PILOT AS target
    USING (
        SELECT DISTINCT pilot_name 
        FROM RAW_STAGE.AIRLINE_RAW_STREAM 
        WHERE pilot_name IS NOT NULL 
          AND METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = FALSE
    ) AS source
    ON target.pilot_name = source.pilot_name
    WHEN NOT MATCHED THEN
        INSERT (pilot_name) VALUES (source.pilot_name);

    MERGE INTO CORE_DWH.DIM_AIRPORT AS target
    USING (
        SELECT DISTINCT airport_name, airport_country_code, country_name, airport_continent 
        FROM RAW_STAGE.AIRLINE_RAW_STREAM 
        WHERE airport_name IS NOT NULL 
          AND METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = FALSE
        UNION 
        SELECT DISTINCT arrival_airport, NULL, NULL, NULL 
        FROM RAW_STAGE.AIRLINE_RAW_STREAM 
        WHERE arrival_airport IS NOT NULL 
          AND METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = FALSE
    ) AS source
    ON target.airport_name = source.airport_name
    WHEN NOT MATCHED THEN
        INSERT (airport_name, country_code, country_name, continent) 
        VALUES (source.airport_name, source.airport_country_code, source.country_name, source.airport_continent);

    MERGE INTO CORE_DWH.DIM_PASSENGER AS target
    USING (
        SELECT DISTINCT 
            passenger_id, first_name, last_name, gender, try_cast(age as int) as age, nationality 
        FROM RAW_STAGE.AIRLINE_RAW_STREAM 
        WHERE passenger_id IS NOT NULL
          AND METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = FALSE
    ) AS source
    ON target.passenger_ref_id = source.passenger_id
    WHEN NOT MATCHED THEN
        INSERT (passenger_ref_id, first_name, last_name, gender, age, nationality)
        VALUES (source.passenger_id, source.first_name, source.last_name, source.gender, source.age, source.nationality);
        
    MERGE INTO CORE_DWH.DIM_FLIGHT_INFO AS target
    USING (
        SELECT DISTINCT ticket_type, passenger_status, flight_status 
        FROM RAW_STAGE.AIRLINE_RAW_STREAM 
        WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = FALSE
    ) AS source
    ON target.ticket_type = source.ticket_type 
       AND target.passenger_status = source.passenger_status 
       AND target.flight_status = source.flight_status
    WHEN NOT MATCHED THEN
        INSERT (ticket_type, passenger_status, flight_status)
        VALUES (source.ticket_type, source.passenger_status, source.flight_status);

    INSERT INTO CORE_DWH.FACT_FLIGHTS (passenger_sk, departure_airport_sk, arrival_airport_sk, pilot_sk, info_sk, departure_date)
    SELECT 
        p.passenger_sk,
        dep.airport_sk,
        arr.airport_sk,
        plt.pilot_sk,
        inf.info_sk,
        TRY_TO_DATE(s.departure_date, 'MM/DD/YYYY') 
    FROM RAW_STAGE.AIRLINE_RAW_STREAM s
    LEFT JOIN CORE_DWH.DIM_PASSENGER p ON s.passenger_id = p.passenger_ref_id
    LEFT JOIN CORE_DWH.DIM_AIRPORT dep ON s.airport_name = dep.airport_name
    LEFT JOIN CORE_DWH.DIM_AIRPORT arr ON s.arrival_airport = arr.airport_name
    LEFT JOIN CORE_DWH.DIM_PILOT plt ON s.pilot_name = plt.pilot_name
    LEFT JOIN CORE_DWH.DIM_FLIGHT_INFO inf ON s.ticket_type = inf.ticket_type 
                                              AND s.passenger_status = inf.passenger_status
                                              AND s.flight_status = inf.flight_status
    WHERE s.METADATA$ACTION = 'INSERT' AND s.METADATA$ISUPDATE = FALSE;

    row_count := SQLROWCOUNT;

    INSERT INTO META_OPS.ETL_AUDIT_LOG (process_name, status, rows_affected, message)
    VALUES ('PROCESS_RAW_TO_CORE', 'SUCCESS', :row_count, 'Core update complete');

    COMMIT;

    RETURN 'Core Layer Updated. Rows inserted: ' || :row_count;

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        INSERT INTO META_OPS.ETL_AUDIT_LOG (process_name, status, rows_affected, message)
        VALUES ('PROCESS_RAW_TO_CORE', 'ERROR', 0, 'Error: ' || SQLERRM);
        RETURN 'Error: ' || SQLERRM;
END;
$$;