```
# Sales ETL System - PySpark Migration

This project is a migration of an ABAP-based Sales ETL system to PySpark.

## Overview

The system extracts raw sales data, applies business transformations including discount calculations, tax computations, profit margin analysis, and sales categorization, then loads the processed data into an analytics table.

## Architecture

- **Extractor**: Reads raw sales data from ZSALES_RAW table
- **Transformer**: Applies business rules and calculations
- **Loader**: Validates and writes to ZSALES_ANALYTICS table
- **Orchestrator**: Coordinates the ETL pipeline

## Prerequisites

- Docker and Docker Compose
- Python 3.9+
- Apache Spark 3.3+

## Quick Start

### Linux/Mac
```bash
chmod +x start.sh
./start.sh --date-from 2024-01-01 --date-to 2024-01-31
```

### Windows
```cmd
start.bat --date-from 2024-01-01 --date-to 2024-01-31
```

## Configuration

Edit `config/business_rules.yaml` to customize:
- Discount thresholds and rates
- Tax rate
- Cost ratio
- Category thresholds
- Database connection settings

## Running Tests

```bash
pytest tests/ --cov=src --cov-report=html
```

## Command Line Options

- `--date-from`: Start date (YYYY-MM-DD)
- `--date-to`: End date (YYYY-MM-DD)
- `--test-mode`: Run without committing data
- `--config`: Path to configuration file

## Project Structure

```
sales-etl-pyspark/
├── src/
│   ├── main.py
│   ├── config/
│   │   ├── etl_config.py
│   │   └── schemas.py
│   ├── core/
│   │   ├── extractor.py
│   │   ├── transformer.py
│   │   ├── loader.py
│   │   └── orchestrator.py
│   └── infrastructure/
│       ├── logger.py
│       ├── exceptions.py
│       └── utils.py
├── config/
│   └── business_rules.yaml
├── tests/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Business Rules

- **Discount**: 5% for quantity > 10, 10% for quantity > 15
- **Tax**: 8% on (gross - discount)
- **Cost**: 60% of unit price
- **Categories**: HIGH (≥2000), MEDIUM (≥500), LOW (<500)

## Monitoring

- Spark UI: http://localhost:8080
- Logs: Check container logs with `docker logs spark-master`

## License

Internal use only
```