CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS stage.orders_raw;
CREATE TABLE stage.orders_raw (
    order_id TEXT,
    customer_id TEXT,
    customer_name TEXT,
    region TEXT,
    city TEXT,
    product_id TEXT,
    sales TEXT,
    order_date TEXT
);

DROP TABLE IF EXISTS core.orders;
DROP TABLE IF EXISTS core.customers;
DROP TABLE IF EXISTS core.products;

CREATE TABLE core.customers (
    customer_sk SERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL,
    customer_name TEXT,
    region TEXT,
    city TEXT,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);

CREATE TABLE core.products (
    product_id TEXT PRIMARY KEY
);

CREATE TABLE core.orders (
    order_id TEXT,
    customer_sk INT REFERENCES core.customers(customer_sk),
    product_id TEXT REFERENCES core.products(product_id),
    sales NUMERIC,
    order_date DATE,
    PRIMARY KEY (order_id, order_date)
);

DROP TABLE IF EXISTS mart.fact_sales;
DROP TABLE IF EXISTS mart.dim_customer;
DROP TABLE IF EXISTS mart.dim_product;

CREATE TABLE mart.dim_customer AS SELECT * FROM core.customers WITH NO DATA;
ALTER TABLE mart.dim_customer ADD PRIMARY KEY (customer_sk);

CREATE TABLE mart.dim_product AS SELECT * FROM core.products WITH NO DATA;
ALTER TABLE mart.dim_product ADD PRIMARY KEY (product_id);

CREATE TABLE mart.fact_sales (
    sales_sk SERIAL PRIMARY KEY,
    order_id TEXT,
    customer_sk INT REFERENCES mart.dim_customer(customer_sk),
    product_id TEXT REFERENCES mart.dim_product(product_id),
    sales NUMERIC,
    order_date DATE
);
