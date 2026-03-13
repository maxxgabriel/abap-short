# ETL Logger Module

Production-ready Python logger class for ETL processes that generates unique log IDs and persists execution metadata to database.

## Features

- **Unique Log ID Generation**: LOG + 14-digit timestamp + microseconds for guaranteed uniqueness
- **Multiple Database Connectors**: Support for JDBC, SAP HANA, and Delta Lake
- **Execution Metadata Capture**: Captures step, status, record counts, and messages
- **Robust Error Handling**: Continues operation even if logging fails
- **Configurable**: YAML-based configuration for flexibility
- **Production-Ready**: Comprehensive test coverage and error recovery

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Edit `config.yaml` to configure database connection and ETL parameters:

```yaml
database:
  type: jdbc  # or 'hana', 'delta'
  jdbc_url: jdbc:sap://hostname:30015
  driver: com.sap.db.jdbc.Driver
  user: ETL_USER
  password: ${DB_PASSWORD}
  log_table: ZETL_LOG
```

## Usage

```python
from pyspark.sql import SparkSession
from src.logger import ETLLogger, generate_etl_run_id
from src.utils import load_config

# Initialize Spark
spark = SparkSession.builder \
    .appName("Sales_ETL") \
    .getOrCreate()

# Load configuration
config = load_config("config.yaml")

# Generate unique ETL run ID
etl_run_id = generate_etl_run_id()

# Create logger
logger = ETLLogger(spark, etl_run_id, config)

# Log messages
logger.log_message(
    step='EXTRACT',
    status='S',
    message='Starting extraction',
    records_processed=1000,
    records_success=1000,
    records_error=0
)
```

## Database Schema

The ZETL_LOG table should have the following structure:

```sql
CREATE TABLE ZETL_LOG (
    log_id VARCHAR(20) PRIMARY KEY,
    etl_run_id VARCHAR(20) NOT NULL,
    execution_date DATE NOT NULL,
    execution_time TIME NOT NULL,
    process_step VARCHAR(20) NOT NULL,
    status CHAR(1) NOT NULL,
    records_processed INTEGER,
    records_success INTEGER,
    records_error INTEGER,
    message VARCHAR(255),
    created_at TIMESTAMP NOT NULL,
    created_by VARCHAR(50) NOT NULL
);
```

## Status Codes

- `S`: Success
- `E`: Error
- `W`: Warning
- `I`: Info

## Process Steps

- `INIT`: Initialization
- `EXTRACT`: Data extraction
- `TRANSFORM`: Data transformation
- `LOAD`: Data loading
- `VALIDATE`: Data validation
- `COMPLETE`: Process complete
- `ERROR`: Error occurred

## Testing

Run tests with pytest:

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=src --cov-report=html

# Run specific test class
pytest tests/test_logger.py::TestLogIDGeneration -v
```

## Database Connectors

### JDBC (Default)
```yaml
database:
  type: jdbc
  jdbc_url: jdbc:sap://hostname:30015
  driver: com.sap.db.jdbc.Driver
```

### SAP HANA
```yaml
database:
  type: hana
  hana_url: jdbc:sap://hostname:30015
```

### Delta Lake
```yaml
database:
  type: delta
  delta_log_path: /mnt/delta/zetl_log
```

## Error Handling

The logger includes robust error handling:
- Failed log insertions are caught and logged to console
- Process continues even if logging fails
- Invalid configurations generate warnings

## Performance Considerations

- Log entries are inserted individually for real-time visibility
- Consider batching for high-volume logging scenarios
- Delta Lake provides best performance for large-scale logging

## License

Proprietary - Internal Use Only