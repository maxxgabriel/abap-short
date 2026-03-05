# Sales ETL System - PySpark Implementation

## Overview

This project is a production-grade PySpark implementation of a Sales ETL (Extract, Transform, Load) system migrated from ABAP. The system extracts raw sales data, applies business transformations including discount calculations, tax computations, and profit margin analysis, and loads the processed data into an analytics table.

## Package Structure

```
sales-etl-pyspark/
├── README.md
├── setup.py
├── requirements.txt
├── config.yaml
├── src/
│   ├── __init__.py
│   ├── extract.py          # Data extraction component
│   ├── transform.py        # Data transformation component
│   ├── load.py             # Data loading component
│   ├── orchestrator.py     # ETL orchestration
│   ├── logger.py           # Logging utility
│   ├── constants.py        # Constants and configuration
│   ├── exceptions.py       # Custom exceptions
│   └── schemas.py          # PySpark schema definitions
├── tests/
│   ├── __init__.py
│   ├── test_extract.py
│   ├── test_transform.py
│   ├── test_load.py
│   └── test_orchestrator.py
└── scripts/
    └── run_etl.py          # Main executable script
```

## Features

### Core Components

1. **Extractor** - Reads raw sales data from source tables
2. **Transformer** - Applies business rules and calculations:
   - Discount calculation (5% for qty > 10, 10% for qty > 15)
   - Tax calculation (8% on net amount)
   - Profit margin computation (60% cost ratio)
   - Sale categorization (HIGH/MEDIUM/LOW)
3. **Loader** - Validates and writes analytics data to target
4. **Logger** - Comprehensive logging with execution tracking
5. **Orchestrator** - Coordinates the complete ETL pipeline

### Business Rules

- **Discount Tiers**:
  - Tier 1: 5% discount for quantities > 10
  - Tier 2: 10% discount for quantities > 15
- **Tax Rate**: 8% on gross amount after discount
- **Cost Ratio**: 60% of unit price
- **Categories**:
  - HIGH: Gross amount >= $2,000
  - MEDIUM: Gross amount >= $500
  - LOW: Gross amount < $500

## Installation

### Prerequisites

- Python 3.8 or higher
- Java 8 or higher (for PySpark)
- Apache Spark 3.x

### Installation Order

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd sales-etl-pyspark
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install --upgrade pip
   pip install -e .
   ```

4. **Verify installation**
   ```bash
   python -c "from pyspark.sql import SparkSession; print('PySpark installed successfully')"
   ```

### Dependency Installation Order

The `setup.py` installs dependencies in this order:

1. **Core Dependencies** (required):
   - pyspark >= 3.3.0
   - pyyaml >= 6.0
   - python-dateutil >= 2.8.2

2. **Development Dependencies** (optional):
   - pytest >= 7.0.0
   - pytest-cov >= 4.0.0
   - black >= 23.0.0
   - flake8 >= 6.0.0
   - mypy >= 1.0.0

## Configuration

Edit `config.yaml` to customize ETL behavior:

```yaml
etl:
  batch_size: 1000
  commit_interval: 500
  retry_attempts: 3
  timeout_seconds: 3600

business_rules:
  discount:
    tier1_quantity: 10
    tier2_quantity: 15
    tier1_rate: 0.05
    tier2_rate: 0.10
  tax_rate: 0.08
  cost_ratio: 0.60
  
categories:
  high_threshold: 2000.00
  medium_threshold: 500.00

paths:
  source_table: "sales_raw"
  target_table: "sales_analytics"
  log_table: "etl_log"
```

## Usage

### Basic ETL Execution

```python
from src.orchestrator import ETLOrchestrator
from datetime import datetime, timedelta

# Initialize orchestrator
orchestrator = ETLOrchestrator()

# Run ETL for date range
from_date = datetime.now() - timedelta(days=7)
to_date = datetime.now()

success = orchestrator.run_etl(
    from_date=from_date,
    to_date=to_date
)

if success:
    orchestrator.display_summary()
```

### Command-Line Execution

```bash
python scripts/run_etl.py --from-date 2024-01-01 --to-date 2024-01-31 --test-mode
```

### Spark-Submit Execution

```bash
spark-submit \
  --master local[*] \
  --driver-memory 4g \
  --executor-memory 4g \
  scripts/run_etl.py \
  --from-date 2024-01-01 \
  --to-date 2024-01-31
```

## Testing

### Run All Tests

```bash
pytest tests/ -v
```

### Run with Coverage

```bash
pytest tests/ --cov=src --cov-report=html
```

### Run Specific Test Module

```bash
pytest tests/test_transform.py -v
```

## Component Relationships

```
┌─────────────────┐
│  run_etl.py     │
│  (Entry Point)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Orchestrator   │◄──────── Logger
└────────┬────────┘
         │
    ┌────┴────┬────────┬─────────┐
    │         │        │         │
    ▼         ▼        ▼         ▼
┌────────┐ ┌──────┐ ┌──────┐ ┌────────┐
│Extract │ │Trans │ │ Load │ │Constants│
└────────┘ └──────┘ └──────┘ └────────┘
    │         │        │
    ▼         ▼        ▼
┌─────────────────────────────┐
│      Schemas & Types         │
└─────────────────────────────┘
```

### Execution Flow

1. **Initialization**
   - Orchestrator creates Spark session
   - Logger initialized with unique ETL run ID
   - Configuration loaded from config.yaml

2. **Extract Phase**
   - Read raw sales data from source
   - Filter by date range and status
   - Validate data quality

3. **Transform Phase**
   - Calculate gross amounts
   - Apply discount rules
   - Calculate tax and net amounts
   - Compute profit margins
   - Categorize sales

4. **Load Phase**
   - Validate transformed records
   - Write to analytics table
   - Update source record status
   - Log statistics

5. **Completion**
   - Display summary statistics
   - Write execution logs
   - Clean up resources

## Error Handling

The system implements comprehensive error handling:

- **ETLError**: Base exception for all ETL errors
- **ExtractionError**: Raised during data extraction
- **TransformationError**: Raised during data transformation
- **LoadError**: Raised during data loading
- **ValidationError**: Raised for data quality issues

All errors are logged with:
- Timestamp
- Process step
- Error details
- Record identifiers (when applicable)

## Logging

### Log Levels

- **INFO**: Process milestones and statistics
- **WARNING**: Non-critical issues (skipped records)
- **ERROR**: Critical failures requiring attention
- **SUCCESS**: Successful completion of phases

### Log Output

Logs are written to:
1. Console (stdout)
2. ETL log table (database)
3. Log file (optional, configured in config.yaml)

### Log Format

```
2024-01-15 10:30:45 | ETL20240115103045 | EXTRACT | SUCCESS | Extracted 1000 records
2024-01-15 10:31:12 | ETL20240115103045 | TRANSFORM | SUCCESS | Transformed 1000 records
2024-01-15 10:31:45 | ETL20240115103045 | LOAD | SUCCESS | Loaded 998 records (2 errors)
```

## Performance Optimization

### Spark Configuration

```python
spark = SparkSession.builder \
    .appName("SalesETL") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()
```

### Best Practices

1. **Partitioning**: Data partitioned by transaction date
2. **Caching**: Frequently accessed DataFrames cached
3. **Broadcasting**: Small lookup tables broadcast
4. **Batch Processing**: Configurable batch sizes
5. **Parallel Execution**: Multi-threaded processing where applicable

## Monitoring

### Key Metrics

- Total records processed
- Success/error counts
- Processing duration
- Records per second throughput
- Memory usage
- Partition distribution

### Health Checks

```python
orchestrator.get_health_status()
# Returns: {
#   "status": "healthy",
#   "last_run": "2024-01-15T10:30:45",
#   "success_rate": 99.8,
#   "avg_duration": 180
# }
```

## Migration Notes from ABAP

### Key Differences

1. **Type System**: 
   - ABAP: Strongly typed with data dictionaries
   - PySpark: Schema-based with StructType definitions

2. **Data Processing**:
   - ABAP: Row-by-row processing in loops
   - PySpark: Columnar operations on DataFrames

3. **Error Handling**:
   - ABAP: Exception classes with message handling
   - PySpark: Python exceptions with try-catch blocks

4. **Configuration**:
   - ABAP: Includes and constants classes
   - PySpark: YAML configuration files

5. **Logging**:
   - ABAP: Database table logging
   - PySpark: Structured logging framework

### Equivalent Components

| ABAP Component | PySpark Equivalent |
|----------------|-------------------|
| ZCL_ETL_EXTRACTOR | src/extract.py |
| ZCL_ETL_TRANSFORMER | src/transform.py |
| ZCL_ETL_LOADER | src/load.py |
| ZCL_ETL_ORCHESTRATOR | src/orchestrator.py |
| ZCL_ETL_LOGGER | src/logger.py |
| ZCL_ETL_CONSTANTS | src/constants.py |
| ZCX_ETL_ERROR | src/exceptions.py |
| ZETL_TYPES | src/schemas.py |

## Troubleshooting

### Common Issues

1. **Out of Memory**
   - Increase driver/executor memory
   - Reduce batch size in config.yaml
   - Enable adaptive query execution

2. **Slow Performance**
   - Check partition distribution
   - Review shuffle operations
   - Consider caching intermediate results

3. **Data Quality Issues**
   - Review validation rules
   - Check source data quality
   - Examine error logs for patterns

### Debug Mode

```bash
python scripts/run_etl.py --debug --test-mode
```

## Contributing

1. Follow PEP 8 style guidelines
2. Add unit tests for new features
3. Update documentation
4. Run linting before commits:
   ```bash
   black src/ tests/
   flake8 src/ tests/
   mypy src/
   ```

## License

Proprietary - Internal Use Only

## Support

For issues or questions:
- Email: etl-support@company.com
- Slack: #sales-etl-support
- Documentation: https://wiki.company.com/sales-etl

## Version History

- **1.0.0** (2024-01-15): Initial PySpark migration from ABAP
  - Core ETL pipeline
  - Business rule implementation
  - Comprehensive logging
  - Unit test coverage