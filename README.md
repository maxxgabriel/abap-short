# ETL Component Interface - Python Migration

## Overview

This package contains the Python migration of the ABAP `ZIF_ETL_COMPONENT` and `ZIF_ETL_LOGGER` interfaces using Abstract Base Classes (ABC) with `@abstractmethod` decorators.

## Components

### 1. Abstract Base Classes

#### `ETLComponent` (src/etl_component.py)
- Converted from ABAP interface `ZIF_ETL_COMPONENT`
- Defines contract for all ETL components
- Methods:
  - `execute()`: Execute component processing
  - `get_component_name()`: Get component identifier
  - `validate_prerequisites()`: Validate requirements

#### `ETLLogger` (src/etl_component.py)
- Converted from ABAP interface `ZIF_ETL_LOGGER`
- Defines logging contract
- Methods:
  - `log_message()`: Log ETL messages
  - `get_etl_run_id()`: Get run identifier

### 2. Data Classes

#### `ExecutionResult`
- Mapped from ABAP `ty_execution_result` structure
- Fields:
  - `success`: Execution success flag
  - `records_total`: Total records processed
  - `records_success`: Successful records
  - `records_error`: Error records
  - `message`: Result message

### 3. Exception Classes

Converted from ABAP RAISING clauses:
- `ETLComponentError`: Base exception class
- `ExtractError`: Extraction phase errors
- `TransformError`: Transformation phase errors
- `LoadError`: Load phase errors

## Configuration

The `config.yaml` file contains all business rules and configuration parameters migrated from `ZCL_ETL_CONSTANTS`:

```yaml
discount_rules:
  quantity_tier1: 10
  rate_tier1: 0.05

tax_rules:
  tax_rate: 0.08

category_thresholds:
  high: 2000.00
  medium: 500.00
```

## Usage Example

```python
from src.etl_component import ETLComponent, ExecutionResult, ETLComponentError

class MyExtractor(ETLComponent):
    def execute(self) -> ExecutionResult:
        try:
            # Processing logic here
            return ExecutionResult(
                success=True,
                records_total=100,
                records_success=100,
                records_error=0,
                message="Extraction completed"
            )
        except Exception as e:
            raise ETLComponentError(
                message=str(e),
                error_step="EXTRACT"
            )
    
    def get_component_name(self) -> str:
        return "DataExtractor"
    
    def validate_prerequisites(self) -> bool:
        # Validation logic
        return True
```

## Testing

Run unit tests:
```bash
pytest tests/test_etl_component.py -v
pytest tests/test_config.py -v
```

## Key Differences from ABAP

1. **Interfaces → ABC**: ABAP interfaces converted to Python Abstract Base Classes
2. **RAISING → Exceptions**: RAISING clauses converted to exception handling
3. **Structures → Dataclasses**: ABAP structures mapped to Python dataclasses
4. **Constants → Config**: ABAP constants externalized to YAML configuration

## Dependencies

- Python 3.8+
- pytest (testing)
- PyYAML (configuration)
- PySpark (future ETL implementation)