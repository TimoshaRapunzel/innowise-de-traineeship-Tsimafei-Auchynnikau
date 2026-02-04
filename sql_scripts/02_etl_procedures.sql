CREATE OR REPLACE PROCEDURE load_stage(filename TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    rows_loaded INT;
BEGIN
    TRUNCATE stage.orders_raw;
    EXECUTE format('COPY stage.orders_raw FROM %L WITH CSV HEADER', '/input_data/' || filename);
    SELECT COUNT(*) INTO rows_loaded FROM stage.orders_raw;
    RAISE NOTICE 'Loaded % rows from %', rows_loaded, filename;
END;
$$;

CREATE OR REPLACE PROCEDURE load_core_customers()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    existing_rec RECORD;
    new_count INT := 0;
    scd1_count INT := 0;
    scd2_count INT := 0;
    unchanged_count INT := 0;
BEGIN
    FOR rec IN (
        SELECT DISTINCT ON (customer_id)
            customer_id, 
            customer_name, 
            region, 
            city,
            order_date::DATE as first_order_date
        FROM stage.orders_raw
        ORDER BY customer_id, order_date
    )
    LOOP
        SELECT * INTO existing_rec
        FROM core.customers
        WHERE customer_id = rec.customer_id 
          AND is_current = TRUE;
        
        IF NOT FOUND THEN
            INSERT INTO core.customers (customer_id, customer_name, region, city, valid_from, valid_to, is_current)
            VALUES (rec.customer_id, rec.customer_name, rec.region, rec.city, rec.first_order_date, '9999-12-31', TRUE);
            new_count := new_count + 1;
            
        ELSE
            IF existing_rec.customer_name != rec.customer_name AND 
               existing_rec.region = rec.region AND 
               existing_rec.city = rec.city THEN
                UPDATE core.customers
                SET customer_name = rec.customer_name
                WHERE customer_sk = existing_rec.customer_sk;
                scd1_count := scd1_count + 1;
                
            ELSIF existing_rec.region != rec.region OR existing_rec.city != rec.city THEN
                UPDATE core.customers
                SET valid_to = rec.first_order_date - INTERVAL '1 day',
                    is_current = FALSE
                WHERE customer_sk = existing_rec.customer_sk;
                
                INSERT INTO core.customers (customer_id, customer_name, region, city, valid_from, valid_to, is_current)
                VALUES (rec.customer_id, rec.customer_name, rec.region, rec.city, rec.first_order_date, '9999-12-31', TRUE);
                scd2_count := scd2_count + 1;
                
            ELSE
                unchanged_count := unchanged_count + 1;
            END IF;
        END IF;
    END LOOP;
    
    RAISE NOTICE 'Customer load complete: New=%, SCD1=%, SCD2=%, Unchanged=%', 
                 new_count, scd1_count, scd2_count, unchanged_count;
END;
$$;

CREATE OR REPLACE PROCEDURE load_core_products()
LANGUAGE plpgsql
AS $$
DECLARE
    rows_inserted INT;
BEGIN
    INSERT INTO core.products (product_id)
    SELECT DISTINCT product_id
    FROM stage.orders_raw
    ON CONFLICT (product_id) DO NOTHING;
    
    GET DIAGNOSTICS rows_inserted = ROW_COUNT;
    RAISE NOTICE 'Inserted % new products', rows_inserted;
END;
$$;

CREATE OR REPLACE PROCEDURE load_core_orders()
LANGUAGE plpgsql
AS $$
DECLARE
    rows_inserted INT;
BEGIN
    INSERT INTO core.orders (order_id, customer_sk, product_id, sales, order_date)
    SELECT DISTINCT
        s.order_id,
        c.customer_sk,
        s.product_id,
        s.sales::NUMERIC,
        s.order_date::DATE
    FROM stage.orders_raw s
    INNER JOIN core.customers c 
        ON s.customer_id = c.customer_id
        AND s.order_date::DATE >= c.valid_from::DATE
        AND s.order_date::DATE <= c.valid_to::DATE
    WHERE NOT EXISTS (
        SELECT 1 FROM core.orders o 
        WHERE o.order_id = s.order_id
    );
    
    GET DIAGNOSTICS rows_inserted = ROW_COUNT;
    RAISE NOTICE 'Inserted % new orders', rows_inserted;
END;
$$;

CREATE OR REPLACE PROCEDURE load_mart()
LANGUAGE plpgsql
AS $$
DECLARE
    dim_customer_count INT;
    dim_product_count INT;
    fact_sales_count INT;
BEGIN
    TRUNCATE mart.fact_sales CASCADE;
    TRUNCATE mart.dim_customer CASCADE;
    TRUNCATE mart.dim_product CASCADE;
    
    INSERT INTO mart.dim_customer
    SELECT * FROM core.customers;
    GET DIAGNOSTICS dim_customer_count = ROW_COUNT;
    
    INSERT INTO mart.dim_product
    SELECT * FROM core.products;
    GET DIAGNOSTICS dim_product_count = ROW_COUNT;
    
    INSERT INTO mart.fact_sales (order_id, customer_sk, product_id, sales, order_date)
    SELECT 
        o.order_id,
        o.customer_sk,
        o.product_id,
        o.sales,
        o.order_date
    FROM core.orders o;
    GET DIAGNOSTICS fact_sales_count = ROW_COUNT;
    
    RAISE NOTICE 'Mart load complete: Customers=%, Products=%, Sales=%', 
                 dim_customer_count, dim_product_count, fact_sales_count;
END;
$$;

CREATE OR REPLACE PROCEDURE run_full_etl(filename TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Starting full ETL pipeline for file: %', filename;
    
    CALL load_stage(filename);
    CALL load_core_customers();
    CALL load_core_products();
    CALL load_core_orders();
    CALL load_mart();
    
    RAISE NOTICE 'ETL pipeline completed successfully';
END;
$$;
