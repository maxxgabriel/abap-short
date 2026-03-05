# Sales ETL System - Python/PySpark Implementation

## Overview

This is a complete ETL (Extract, Transform, Load) system for sales data processing, migrated from SAP ABAP to Python/PySpark. The system extracts raw sales data, applies business transformations, and loads the results into analytics tables for reporting and analysis.

## Features

- **Extract**: Reads raw sales transactions from source data
- **Transform**: Applies business rules for discounts, taxes, profit margins, and categorization
- **Load**: Writes transformed analytics data to target storage
- **Logging**: Comprehensive ETL execution logging and monitoring
- **Error Handling**: Robust exception handling with detailed error tracking
- **Configuration**: Externalized business rules and system parameters

## Architecture

### Component Relationships

```
┌─────────────────────────────────────────────────────────────┐
│                     ETL Orchestrator                         │
│                  (src/orchestrator.py)                       │
└────────┬────────────────┬────────────────┬──────────────────┘
         │                │                │
         ▼                ▼                ▼
    ┌────────┐      ┌──────────┐     ┌─────────┐
    │Extract │      │Transform │     │  Load   │
    │ Module │──────│  Module  │─────│ Module  │
    └────────┘      └──────────┘     └─────────┘
         │                │                │
         └────────────────┴────────────────┘
                          │
                          ▼
                    ┌──────────┐
                    │  Logger  │
                    └──────────┘
```

### Module Dependencies

1. **Logger** (`src/logger.py`)
   - Base logging utility
   - No dependencies on other ETL modules
   - Used by all components

2. **Extractor** (`src/extract.py`)
   - Depends on: Logger
   - Reads raw sales data from source

3. **Transformer** (`src/transform.py`)
   - Depends on: Logger, Constants
   - Applies business rules and calculations

4. **Loader** (`src/load.py`)
   - Depends on: Logger
   - Validates and writes analytics data

5. **Orchestrator** (`src/orchestrator.py`)
   - Depends on: All above modules
   - Coordinates the ETL workflow

## Installation

### Prerequisites

- Python 3.8 or higher
- Java 8 or higher (for PySpark)
- Apache Spark 3.0+ (optional for standalone deployment)

### Installation Order

1. **Install Python and pip**
   ```bash
   python --version  # Should be 3.8+
   pip --version
   ```

2. **Create virtual environment** (recommended)
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -e .
   ```

   Or install individually:
   ```bash
   pip install pyspark==3.4.0
   pip install pyyaml==6.0
   pip install pytest==7.4.0
   pip install pytest-cov==4.1.0
   ```

4. **Verify installation**
   ```bash
   python -c "import pyspark; print(pyspark.__version__)"
   pytest --version
   ```

## Project Structure

```
sales-etl-system/
├── README.md                 # This file
├── setup.py                  # Package installation configuration
├── requirements.txt          # Python dependencies
├── config.yaml              # ETL configuration parameters
├── src/
│   ├── __init__.py
│   ├── constants.py         # Business constants and rules
│   ├── logger.py            # Logging utility
│   ├── extract.py           # Data extraction module
│   ├── transform.py         # Data transformation module
│   ├── load.py              # Data loading module
│   ├── orchestrator.py      # ETL orchestration
│   └── exceptions.py        # Custom exception classes
├── tests/
│   ├── __init__.py
│   ├── test_extract.py
│   ├── test_transform.py
│   ├── test_load.py
│   └── test_orchestrator.py
└── main.py                  # Entry point script
```

## Configuration

Edit `config.yaml` to customize the ETL behavior:

```yaml
etl:
  batch_size: 1000
  commit_interval: 500
  retry_attempts: 3
  timeout_seconds: 3600

business_rules:
  discount:
    qty_tier1: 10
    qty_tier2: 15
    rate_tier1: 0.05
    rate_tier2: 0.10
  tax_rate: 0.08
  cost_ratio: 0.60
  category_thresholds:
    high: 2000.00
    medium: 500.00

spark:
  app_name: "SalesETL"
  master: "local[*]"
  executor_memory: "2g"
  driver_memory: "1g"

paths:
  input: "data/input"
  output: "data/output"
  logs: "logs"
```

## Usage

### Basic Usage

```bash
# Run ETL with default configuration
python main.py --from-date 2024-01-01 --to-date 2024-01-31

# Run in test mode (no data commit)
python main.py --from-date 2024-01-01 --to-date 2024-01-31 --test

# Specify custom config file
python main.py --config custom_config.yaml --from-date 2024-01-01
```

### Programmatic Usage

```python
from pyspark.sql import SparkSession
from src.orchestrator import ETLOrchestrator
from src.logger import ETLLogger
from datetime import date

# Create Spark session
spark = SparkSession.builder \
    .appName("SalesETL") \
    .master("local[*]") \
    .getOrCreate()

# Initialize orchestrator
orchestrator = ETLOrchestrator(spark)

# Run ETL
success = orchestrator.run_etl(
    from_date=date(2024, 1, 1),
    to_date=date(2024, 1, 31)
)

if success:
    print("ETL completed successfully")
    orchestrator.display_summary()
else:
    print("ETL failed - check logs")

spark.stop()
```

## Business Rules

### Discount Calculation
- **5% discount**: Quantity > 10 items
- **10% discount**: Quantity > 15 items

### Tax Calculation
- **Tax rate**: 8% applied to (gross - discount)

### Profit Margin
- **Cost assumption**: 60% of unit price
- **Margin formula**: ((net_amount - cost) / net_amount) × 100

### Sale Categorization
- **HIGH**: Gross amount ≥ $2,000
- **MEDIUM**: Gross amount ≥ $500
- **LOW**: Gross amount < $500

## Testing

### Run All Tests
```bash
pytest tests/ -v
```

### Run Specific Test Module
```bash
pytest tests/test_transform.py -v
```

### Run with Coverage
```bash
pytest tests/ --cov=src --cov-report=html
```

### Test Structure
- **Unit tests**: Test individual components in isolation
- **Integration tests**: Test component interactions
- **Mock data**: Tests use sample data for validation

## Data Schemas

### Raw Sales Data
```python
{
    "trans_id": "T000001",
    "trans_date": "2024-01-15",
    "customer_id": "CUST001",
    "product_id": "PROD001",
    "quantity": 10,
    "unit_price": 99.99,
    "currency": "USD",
    "sales_rep": "John Doe",
    "region": "NORTH",
    "status": "N"
}
```

### Analytics Data
```python
{
    "analytics_id": "ANL_T000001_20240115",
    "trans_date": "2024-01-15",
    "customer_id": "CUST001",
    "product_id": "PROD001",
    "total_quantity": 10,
    "gross_amount": 999.90,
    "net_amount": 1029.89,
    "discount_amount": 0.00,
    "tax_amount": 79.99,
    "currency": "USD",
    "sales_rep": "John Doe",
    "region": "NORTH",
    "profit_margin": 41.67,
    "category": "MEDIUM",
    "etl_run_id": "ETL20240115120000"
}
```

### ETL Log Entry
```python
{
    "log_id": "LOG20240115120000",
    "etl_run_id": "ETL20240115120000",
    "execution_date": "2024-01-15",
    "execution_time": "12:00:00",
    "process_step": "EXTRACT",
    "status": "S",
    "records_processed": 1000,
    "records_success": 1000,
    "records_error": 0,
    "message": "Extraction completed successfully"
}
```

## Performance Considerations

### Memory Management
- Adjust `spark.executor.memory` and `spark.driver.memory` based on data volume
- Use `repartition()` for large datasets to optimize parallelism

### Batch Processing
- Configure `batch_size` in config.yaml
- Larger batches improve throughput but require more memory

### Optimization Tips
- Enable Spark SQL optimization: `spark.sql.adaptive.enabled=true`
- Use columnar formats (Parquet) for better performance
- Partition data by date for efficient querying

## Troubleshooting

### Common Issues

1. **Java not found**
   ```bash
   # Install Java 8 or higher
   sudo apt-get install openjdk-8-jdk  # Ubuntu/Debian
   brew install openjdk@8              # macOS
   ```

2. **PySpark import errors**
   ```bash
   # Ensure JAVA_HOME is set
   export JAVA_HOME=/path/to/java
   export PATH=$JAVA_HOME/bin:$PATH
   ```

3. **Memory errors**
   - Reduce batch_size in config.yaml
   - Increase executor/driver memory in Spark config

4. **Test failures**
   ```bash
   # Clean pytest cache
   pytest --cache-clear
   ```

## Logging

Logs are written to:
- **Console**: Real-time execution output
- **File**: `logs/etl_YYYYMMDD.log`
- **Database**: ETL_LOG table (if configured)

Log levels:
- **INFO**: Normal operation messages
- **WARNING**: Non-critical issues
- **ERROR**: Critical failures requiring attention
- **SUCCESS**: Successful step completion

## Migration Notes from ABAP

### Key Differences
- **Data Types**: ABAP types mapped to Python/Spark types
- **Error Handling**: Exception classes replace ABAP exceptions
- **Database**: Spark DataFrames replace direct table access
- **Logging**: Structured logging replaces ABAP application log

### ABAP → Python Mapping
- `SELECT` → `spark.read()`
- `LOOP AT` → DataFrame operations (`.foreach()`, `.map()`)
- `APPEND` → `.union()`, `.write()`
- `COMMIT WORK` → `.write().mode("append")`
- `ROLLBACK` → Transaction management via orchestrator

## Contributing

1. Fork the repository
2. Create a feature branch
3. Write tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the MIT License.

## Support

For issues and questions:
- Create an issue in the repository
- Email: support@example.com
- Documentation: https://docs.example.com/sales-etl

## Version History

- **1.0.0** (2024-01-15): Initial release
  - Complete ETL pipeline
  - Business rules implementation
  - Comprehensive logging
  - Unit test coverage

## Authors

- Migration Engineer: Senior PySpark Team
- Original ABAP System: SAP Development Team