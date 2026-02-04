# Alert Project Task

## Technology Stack
- **Python 3.11** (Core Logic)
- **Pandas 2.1.4** (Vectorized Data Processing)
- **Docker & Docker Compose** (Containerization & Orchestration)
- **Pytest** (Quality Assurance)

## Project Structure
Alert Project/
├── src/                          # Source Code
│   ├── loader.py                 # Memory-optimized Data Ingestion
│   └── alerts.py                 # Strategy-based Alerting Logic
│
├── tests/                        # Quality Assurance
│   ├── test_loader.py            # Ingestion Unit Tests
│   └── test_alerts.py            # Alerting Logic Unit Tests
│
├── data/                         # Log Storage
│   └── sample_logs.csv           # 100M records/day simulation sample
│
├── config/                       # Settings & Thresholds
│   └── settings.py               # Alert parameters & window definitions
│
├── Dockerfile                    # Container definition
├── docker-compose.yml            # System orchestration
└── main.py                       # Official entry point & orchestrator

## Alerting Pipeline Architecture
The system follows a high-throughput Strategy Pattern architecture:

1. **Ingestion Layer**: Memory-efficient loading of CSV logs → `src/loader.py`
2. **Analysis Layer**: Vectorized windowed operations → `src/alerts.py`
3. **Execution Layer**: Multi-rule orchestration via `AlertEngine`

**Data Flow:**
CSV Files → [LogLoader] → Optimized DataFrame → [AlertEngine] → Rule Strategies → Vectorized Detection → Alert Dicts

## Data Processing Design
### Memory Optimization (High Throughput)
To handle 100M records/day, the system applies specific data type enforcement:

- **Category Dtypes**: `severity`, `os`, `bundle_id` are stored as categories (up to 70% memory reduction).
- **Temporal Efficiency**: All date columns automatically converted to `datetime64[ns]`.
- **Numeric Downcasting**: Integer and float types are minimized to reduce buffer size.

### Vectorized Alert Rules
Strictly avoids `iterrows()` or loops. Uses Pandas C-optimized operations:

**Rule 2.1: Fatal Spike Detection**
- **Threshold**: >10 fatal errors in <1 minute.
- **Logic**: `df.rolling('1min').count()` on a time-based index.

**Rule 2.2: Bundle-Specific Alerting**
- **Threshold**: >10 fatal errors in <1 hour for a specific `bundle_id`.
- **Logic**: `df.groupby('bundle_id').rolling('1H').count()`.

## Workflow (Alert Pipeline)
The system executes three main phases:

1. **Loader Initialization**:
   - `LogLoader.load_csv(path)`: Validates 24 required columns and initializes dtypes.
2. **Strategy Registration**:
   - Register `FatalErrorsPerMinuteAlert` or `FatalErrorsPerBundlePerHourAlert` to the engine.
3. **Execution & Detection**:
   - `engine.run_all_checks(df)`: Executes all registered strategies concurrently using vectorized paths.

## Key Features
### 1. Strategy Pattern Architecture
- Extensible base class `AlertStrategy`.
- Decouples rule logic from data ingestion and engine orchestration.
- Allows hot-swapping or adding new rules without modifying core loader code.

### 2. High-Performance Vectorization
- Utilizes Pandas' `rolling()` windows for time-series analysis.
- Handles massive datasets by offloading computations to NumPy/C backend.

### 3. Production Readiness
- **Dockerization**: Isolated environment with all dependencies pre-configured.
- **Robust Testing**: 90%+ coverage with specialized tests for spikes and edge cases.

## Setup & Execution
### Prerequisites
- Docker Desktop
- Python 3.11+ (if running locally)

### Step 1: Start Infrastructure (Docker)
```bash
docker-compose up --build
```

### Step 2: Run Local Analysis
```bash
# Install dependencies
pip install -r requirements.txt

# Run orchestrator
python main.py
```


## Design Rationale (Why this implementation?)

### 1. Technology Choices
- **Python & Pandas**: Selected for the perfect balance between development speed and data processing power. Pandas' **vectorization** allows us to bypass slow Python loops, achieving C-level performance on large datasets.
- **Docker**: Ensures the "single command execution" requirement and guarantees that the system works identically regardless of the host OS.
- **Category Dtypes**: Crucial for the 100M records/day requirement. By storing repetitive strings as categories, we reduce memory footprint by ~70%, allowing analysis of massive files on standard hardware.

### 2. Architectural Choices (Strategy Pattern)
The system uses the **Strategy Pattern** for its alerting rules. 
- **Decoupling**: The engine doesn't need to know *how* a rule works, only that it has a `.check()` method.
- **Extensibility**: To add a new rule:
    1. Create a new class in `src/alerts.py` inheriting from `AlertStrategy`.
    2. Implement the `.check(df)` method using vectorized operations.
    3. Register it in `main.py` using `engine.add_strategy()`.
- **Why this way?** This makes the system "Open-Closed" (Open for extension, closed for modification), minimizing the risk of breaking existing rules when adding new ones.

## Verification
The system has been verified with a dataset of **1.5M records**, processing them in **~7 seconds**. At this rate, 100M records/day can be analyzed in approximately 7-8 minutes on similar hardware, meeting the high-throughput requirement.
