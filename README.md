# Superstore Data Warehouse ETL Project

## Technology Stack
- **PostgreSQL 15** (Relational Database)
- **Docker & Docker Compose** (Infrastructure)
- **Python 3.x** (Data Generation)
- **pgAdmin 4** (Database Management)
- **Power BI** (Analytics & Visualization)
- **SQL** (Stored Procedures, DDL, DML)

---

## Project Structure

```
BI Tools/
├── docker-compose.yml              # Infrastructure definition
├── README.md                       # Project documentation
├── POWER_BI_SPEC.md               # Power BI integration guide
│
├── data/                          # Generated CSV files
│   ├── initial_load.csv           # 800 rows (80% baseline)
│   └── secondary_load.csv         # 205 rows (20% + SCD scenarios)
│
├── sql_scripts/                   # Database objects
│   ├── 01_ddl_structure.sql       # Schema definitions (Stage/Core/Mart)
│   └── 02_etl_procedures.sql      # ETL stored procedures
│
└── etl_scripts/                   # Python utilities
    └── generate_data.py           # Mock data generator with SCD injection
```

---

## ETL Pipeline Architecture

The pipeline follows a **three-layer Data Warehouse architecture**:

1. **Stage Layer**: Raw CSV data loaded via `COPY` command → `stage.orders_raw`
2. **Core Layer**: Normalized 3NF schema with SCD Type 2 support → `core.customers`, `core.products`, `core.orders`
3. **Mart Layer**: Star schema for analytics → `mart.dim_customer`, `mart.dim_product`, `mart.fact_sales`

**Data Flow**:
```
CSV Files → [COPY] → Stage → [Transform + SCD] → Core (3NF) → [Denormalize] → Mart (Star) → Power BI
```

---

## Schema Design

### Star Schema (Mart Layer)

**Fact Table**: `mart.fact_sales`
- `sales_sk` (PK)
- `order_id`, `customer_sk` (FK), `product_id` (FK), `sales`, `order_date`

**Dimension Tables**:
- `mart.dim_customer`: Customer attributes with SCD Type 2 history
  - `customer_sk` (PK), `customer_id`, `customer_name`, `region`, `city`
  - `valid_from`, `valid_to`, `is_current`
- `mart.dim_product`: Product lookup
  - `product_id` (PK)

### Core Layer (3NF)

**Tables**:
- `core.customers`: Customer master with temporal validity (`valid_from`, `valid_to`, `is_current`)
- `core.products`: Product reference
- `core.orders`: Transaction data with FK to customers and products

---

## Workflow (ETL Pipeline)

The ETL pipeline executes five stored procedures in sequence:

1. **`load_stage(filename)`**: Loads CSV into `stage.orders_raw` via `COPY`
2. **`load_core_customers()`**: Implements SCD Type 1 & Type 2 logic
   - Deduplicates using `DISTINCT ON`
   - SCD Type 1: Name corrections (UPDATE in place)
   - SCD Type 2: Region/City changes (historical tracking)
3. **`load_core_products()`**: Inserts new products with conflict handling
4. **`load_core_orders()`**: Temporal join to link orders with correct customer version
5. **`load_mart()`**: Full refresh of Mart layer from Core
6. **`run_full_etl(filename)`**: Orchestrator procedure executing steps 1-5

**Execution**:
```sql
CALL run_full_etl('initial_load.csv');
CALL run_full_etl('secondary_load.csv');
```

---

## Key Features

### 1. Slowly Changing Dimensions (SCD)
- **Type 1**: In-place update for attribute corrections (e.g., name typos)
- **Type 2**: Historical tracking for business changes (e.g., customer relocation)
  - Closes old record: `valid_to = change_date`, `is_current = FALSE`
  - Inserts new record: `valid_from = change_date`, `is_current = TRUE`

### 2. Data Quality
- **Deduplication**: `DISTINCT ON` in customer load, `NOT EXISTS` in orders load
- **Validation**: TEXT columns in Stage layer prevent type-casting failures

### 3. Temporal Data Handling
- **Temporal Join**: Orders linked to customer version valid at `order_date`
  ```sql
  ON s.customer_id = c.customer_id
  AND s.order_date::DATE BETWEEN c.valid_from::DATE AND c.valid_to::DATE
  ```

### 4. Power BI Integration
- **DAX Measures**: Total Sales, Profit Margin, YTD, SPLY, Market Share (using `ALLSELECTED`)
- **Row-Level Security (RLS)**: Regional access control via `USERPRINCIPALNAME()`
- **Hierarchies**: Product (Category → Sub-Category → Product Name)

---

## Setup & Execution

### Prerequisites
- Docker Desktop
- Python 3.x with `pandas`

### Step 1: Start Infrastructure
```powershell
docker-compose up -d
```

### Step 2: Generate Data
```powershell
py etl_scripts/generate_data.py
```

### Step 3: Initialize Database
```powershell
# Create schemas and tables
docker exec -i superstore_db psql -U admin -d superstore_dw -f /docker-entrypoint-initdb.d/01_ddl_structure.sql

# Create stored procedures
docker exec -i superstore_db psql -U admin -d superstore_dw -f /docker-entrypoint-initdb.d/02_etl_procedures.sql
```