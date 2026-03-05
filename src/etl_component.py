"""
Abstract Base Class for ETL Components

This module defines the interface contract for all ETL components,
converting the ABAP interface ZIF_ETL_COMPONENT to Python.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class ExecutionResult:
    """
    Execution result for ETL component operations.
    
    Converts ABAP ty_execution_result structure to Python dataclass
    with proper type hints and immutability.
    
    Attributes:
        success: Boolean indicating execution success
        records_total: Total number of records processed
        records_success: Number of successfully processed records
        records_error: Number of records with errors
        message: Descriptive message about execution outcome
    """
    success: bool
    records_total: int
    records_success: int
    records_error: int
    message: str
    
    def __post_init__(self):
        """Validate data consistency after initialization."""
        if self.records_total < 0 or self.records_success < 0 or self.records_error < 0:
            raise ValueError("Record counts cannot be negative")
        
        if (self.records_success + self.records_error) > self.records_total:
            raise ValueError("Sum of success and error records cannot exceed total")


class ETLComponent(ABC):
    """
    Abstract Base Class for ETL Components.
    
    Defines the interface contract that all ETL components must implement.
    This is the Python equivalent of ABAP interface ZIF_ETL_COMPONENT.
    
    All concrete ETL components (Extractor, Transformer, Loader) must
    inherit from this class and implement its abstract methods.
    """
    
    @abstractmethod
    def execute(self) -> ExecutionResult:
        """
        Execute the ETL component's main operation.
        
        This is the primary method that performs the component's work.
        Must be implemented by all concrete subclasses.
        
        Returns:
            ExecutionResult: Result object containing execution statistics
            
        Raises:
            ETLError: If execution fails due to processing errors
            ValueError: If invalid data or configuration is encountered
        """
        pass
    
    @abstractmethod
    def get_component_name(self) -> str:
        """
        Get the name of the ETL component.
        
        Returns:
            str: Human-readable component name (e.g., "Extractor", "Transformer")
        """
        pass
    
    @abstractmethod
    def validate_prerequisites(self) -> bool:
        """
        Validate that all prerequisites for execution are met.
        
        Checks configuration, dependencies, connections, and any other
        requirements before attempting to execute.
        
        Returns:
            bool: True if all prerequisites are satisfied, False otherwise
        """
        pass
    
    def __repr__(self) -> str:
        """String representation of the component."""
        return f"{self.__class__.__name__}(name='{self.get_component_name()}')"


===FILE: src/exceptions.py===
"""
ETL Exception Classes

Custom exception hierarchy for ETL processing errors.
Converts ABAP exception class ZCX_ETL_ERROR to Python.
"""


class ETLError(Exception):
    """
    Base exception class for all ETL errors.
    
    Converts ABAP exception class ZCX_ETL_ERROR to Python.
    All specific ETL exceptions should inherit from this class.
    
    Attributes:
        error_text: Detailed error message
        error_step: ETL step where error occurred (EXTRACT, TRANSFORM, LOAD)
        record_id: ID of the record being processed when error occurred
    """
    
    def __init__(
        self,
        error_text: str,
        error_step: str = None,
        record_id: str = None
    ):
        """
        Initialize ETL error.
        
        Args:
            error_text: Detailed error message
            error_step: ETL step where error occurred
            record_id: Record ID being processed when error occurred
        """
        self.error_text = error_text
        self.error_step = error_step
        self.record_id = record_id
        
        # Build full message
        message_parts = [error_text]
        if error_step:
            message_parts.insert(0, f"[{error_step}]")
        if record_id:
            message_parts.append(f"(Record: {record_id})")
        
        super().__init__(" ".join(message_parts))


class ExtractError(ETLError):
    """Exception raised during data extraction phase."""
    
    def __init__(self, error_text: str, record_id: str = None):
        super().__init__(error_text, error_step="EXTRACT", record_id=record_id)


class TransformError(ETLError):
    """Exception raised during data transformation phase."""
    
    def __init__(self, error_text: str, record_id: str = None):
        super().__init__(error_text, error_step="TRANSFORM", record_id=record_id)


class LoadError(ETLError):
    """Exception raised during data loading phase."""
    
    def __init__(self, error_text: str, record_id: str = None):
        super().__init__(error_text, error_step="LOAD", record_id=record_id)


class ValidationError(ETLError):
    """Exception raised during data validation."""
    
    def __init__(self, error_text: str, record_id: str = None):
        super().__init__(error_text, error_step="VALIDATE", record_id=record_id)


===FILE: config.yaml===
# ETL Configuration
# Converted from ABAP constants and configuration

# Status Codes
status_codes:
  new: "N"
  processed: "P"
  error: "E"
  warning: "W"
  success: "S"
  info: "I"

# ETL Process Steps
process_steps:
  init: "INIT"
  extract: "EXTRACT"
  transform: "TRANSFORM"
  load: "LOAD"
  validate: "VALIDATE"
  complete: "COMPLETE"
  error: "ERROR"

# Sale Categories
categories:
  high: "HIGH"
  medium: "MEDIUM"
  low: "LOW"

# Business Rules - Discounts
business_rules:
  discount:
    quantity_tier1: 10
    quantity_tier2: 15
    rate_tier1: 0.05
    rate_tier2: 0.10
  
  # Business Rules - Tax
  tax:
    rate: 0.08
  
  # Business Rules - Cost Ratio
  cost:
    ratio: 0.60
  
  # Business Rules - Category Thresholds
  category_thresholds:
    high: 2000.00
    medium: 500.00

# ETL Configuration Defaults
etl_config:
  batch_size: 1000
  commit_interval: 500
  retry_attempts: 3
  timeout_seconds: 3600
  parallel_jobs: 4

# ID Prefixes
id_prefixes:
  etl_run: "ETL"
  log: "LOG"
  analytics: "ANL"

# Message Templates
messages:
  init_success: "ETL process initialized successfully"
  extract_start: "Starting data extraction"
  extract_complete: "Data extraction completed"
  transform_start: "Starting data transformation"
  transform_complete: "Data transformation completed"
  load_start: "Starting data load"
  load_complete: "Data load completed"
  etl_complete: "ETL process completed successfully"
  etl_error: "ETL process failed"

# Spark Configuration
spark:
  app_name: "SalesETL"
  master: "local[*]"
  log_level: "WARN"
  
  # Spark SQL Configuration
  sql:
    shuffle_partitions: 200
    adaptive_enabled: true
  
  # Memory Configuration
  memory:
    driver: "2g"
    executor: "4g"

# Data Paths (configure for your environment)
data_paths:
  input: "data/input/sales_raw"
  output: "data/output/sales_analytics"
  logs: "data/logs"
  checkpoint: "data/checkpoint"

# Database Configuration (if using JDBC)
database:
  source:
    url: "jdbc:postgresql://localhost:5432/sales_db"
    table: "zsales_raw"
    user: "etl_user"
    driver: "org.postgresql.Driver"
  
  target:
    url: "jdbc:postgresql://localhost:5432/sales_db"
    table: "zsales_analytics"
    user: "etl_user"
    driver: "org.postgresql.Driver"
  
  log:
    url: "jdbc:postgresql://localhost:5432/sales_db"
    table: "zetl_log"
    user: "etl_user"
    driver: "org.postgresql.Driver"

# Logging Configuration
logging:
  version: 1
  disable_existing_loggers: false
  
  formatters:
    standard:
      format: "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
      datefmt: "%Y-%m-%d %H:%M:%S"
    
    detailed:
      format: "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s"
      datefmt: "%Y-%m-%d %H:%M:%S"
  
  handlers:
    console:
      class: logging.StreamHandler
      level: INFO
      formatter: standard
      stream: ext://sys.stdout
    
    file:
      class: logging.handlers.RotatingFileHandler
      level: DEBUG
      formatter: detailed
      filename: logs/etl.log
      maxBytes: 10485760  # 10MB
      backupCount: 5
  
  loggers:
    etl:
      level: DEBUG
      handlers: [console, file]
      propagate: false
  
  root:
    level: INFO
    handlers: [console]


===FILE: tests/test_etl_component.py===
"""
Unit Tests for ETL Component Interface

Tests the abstract base class and ExecutionResult dataclass.
"""

import pytest
from src.etl_component import ETLComponent, ExecutionResult
from src.exceptions import ETLError


class TestExecutionResult:
    """Test cases for ExecutionResult dataclass."""
    
    def test_execution_result_creation(self):
        """Test creating a valid ExecutionResult."""
        result = ExecutionResult(
            success=True,
            records_total=100,
            records_success=95,
            records_error=5,
            message="Processing completed"
        )
        
        assert result.success is True
        assert result.records_total == 100
        assert result.records_success == 95
        assert result.records_error == 5
        assert result.message == "Processing completed"
    
    def test_execution_result_negative_counts(self):
        """Test that negative record counts raise ValueError."""
        with pytest.raises(ValueError, match="Record counts cannot be negative"):
            ExecutionResult(
                success=True,
                records_total=-1,
                records_success=0,
                records_error=0,
                message="Invalid"
            )
    
    def test_execution_result_inconsistent_totals(self):
        """Test that inconsistent totals raise ValueError."""
        with pytest.raises(ValueError, match="cannot exceed total"):
            ExecutionResult(
                success=True,
                records_total=100,
                records_success=80,
                records_error=30,  # 80 + 30 > 100
                message="Invalid"
            )
    
    def test_execution_result_success_scenario(self):
        """Test typical success scenario."""
        result = ExecutionResult(
            success=True,
            records_total=1000,
            records_success=1000,
            records_error=0,
            message="All records processed successfully"
        )
        
        assert result.success is True
        assert result.records_error == 0
    
    def test_execution_result_partial_failure(self):
        """Test partial failure scenario."""
        result = ExecutionResult(
            success=False,
            records_total=500,
            records_success=450,
            records_error=50,
            message="Processing completed with errors"
        )
        
        assert result.success is False
        assert result.records_error == 50


class ConcreteETLComponent(ETLComponent):
    """Concrete implementation for testing."""
    
    def __init__(self, name: str = "TestComponent"):
        self._name = name
        self._prerequisites_valid = True
    
    def execute(self) -> ExecutionResult:
        """Test implementation of execute."""
        return ExecutionResult(
            success=True,
            records_total=10,
            records_success=10,
            records_error=0,
            message="Test execution completed"
        )
    
    def get_component_name(self) -> str:
        """Test implementation of get_component_name."""
        return self._name
    
    def validate_prerequisites(self) -> bool:
        """Test implementation of validate_prerequisites."""
        return self._prerequisites_valid
    
    def set_prerequisites_valid(self, valid: bool):
        """Helper method for testing."""
        self._prerequisites_valid = valid


class TestETLComponent:
    """Test cases for ETLComponent abstract base class."""
    
    def test_cannot_instantiate_abstract_class(self):
        """Test that ETLComponent cannot be instantiated directly."""
        with pytest.raises(TypeError):
            ETLComponent()
    
    def test_concrete_implementation(self):
        """Test that concrete implementation works."""
        component = ConcreteETLComponent("TestETL")
        
        assert component.get_component_name() == "TestETL"
        assert component.validate_prerequisites() is True
    
    def test_execute_returns_execution_result(self):
        """Test that execute returns ExecutionResult."""
        component = ConcreteETLComponent()
        result = component.execute()
        
        assert isinstance(result, ExecutionResult)
        assert result.success is True
        assert result.records_total == 10
    
    def test_repr_method(self):
        """Test string representation of component."""
        component = ConcreteETLComponent("MyComponent")
        repr_str = repr(component)
        
        assert "ConcreteETLComponent" in repr_str
        assert "MyComponent" in repr_str
    
    def test_prerequisites_validation(self):
        """Test prerequisites validation."""
        component = ConcreteETLComponent()
        
        # Initially valid
        assert component.validate_prerequisites() is True
        
        # Set invalid
        component.set_prerequisites_valid(False)
        assert component.validate_prerequisites() is False


class IncompleteComponent(ETLComponent):
    """Component missing required method implementations."""
    
    def get_component_name(self) -> str:
        return "Incomplete"
    
    # Missing execute() and validate_prerequisites()


class TestAbstractMethodEnforcement:
    """Test that abstract methods must be implemented."""
    
    def test_incomplete_implementation_fails(self):
        """Test that incomplete implementation cannot be instantiated."""
        with pytest.raises(TypeError):
            IncompleteComponent()


class TestExecutionResultEdgeCases:
    """Test edge cases for ExecutionResult."""
    
    def test_zero_records(self):
        """Test with zero records."""
        result = ExecutionResult(
            success=True,
            records_total=0,
            records_success=0,
            records_error=0,
            message="No records to process"
        )
        
        assert result.records_total == 0
    
    def test_all_errors(self):
        """Test when all records fail."""
        result = ExecutionResult(
            success=False,
            records_total=100,
            records_success=0,
            records_error=100,
            message="All records failed"
        )
        
        assert result.success is False
        assert result.records_success == 0
        assert result.records_error == 100
    
    def test_empty_message(self):
        """Test with empty message."""
        result = ExecutionResult(
            success=True,
            records_total=50,
            records_success=50,
            records_error=0,
            message=""
        )
        
        assert result.message == ""
    
    def test_long_message(self):
        """Test with very long message."""
        long_msg = "x" * 10000
        result = ExecutionResult(
            success=True,
            records_total=1,
            records_success=1,
            records_error=0,
            message=long_msg
        )
        
        assert len(result.message) == 10000


===FILE: tests/test_exceptions.py===
"""
Unit Tests for ETL Exception Classes

Tests the custom exception hierarchy.
"""

import pytest
from src.exceptions import (
    ETLError,
    ExtractError,
    TransformError,
    LoadError,
    ValidationError
)


class TestETLError:
    """Test cases for base ETLError class."""
    
    def test_basic_error(self):
        """Test creating basic ETL error."""
        error = ETLError("Something went wrong")
        
        assert error.error_text == "Something went wrong"
        assert error.error_step is None
        assert error.record_id is None
        assert str(error) == "Something went wrong"
    
    def test_error_with_step(self):
        """Test error with process step."""
        error = ETLError("Error occurred", error_step="TRANSFORM")
        
        assert error.error_step == "TRANSFORM"
        assert "[TRANSFORM]" in str(error)
    
    def test_error_with_record_id(self):
        """Test error with record ID."""
        error = ETLError("Invalid data", record_id="REC12345")
        
        assert error.record_id == "REC12345"
        assert "Record: REC12345" in str(error)
    
    def test_error_with_all_fields(self):
        """Test error with all fields populated."""
        error = ETLError(
            "Processing failed",
            error_step="LOAD",
            record_id="REC99999"
        )
        
        assert error.error_text == "Processing failed"
        assert error.error_step == "LOAD"
        assert error.record_id == "REC99999"
        
        error_str = str(error)
        assert "[LOAD]" in error_str
        assert "Processing failed" in error_str
        assert "Record: REC99999" in error_str
    
    def test_error_inheritance(self):
        """Test that ETLError inherits from Exception."""
        error = ETLError("Test error")
        
        assert isinstance(error, Exception)
        assert isinstance(error, ETLError)


class TestExtractError:
    """Test cases for ExtractError."""
    
    def test_extract_error_basic(self):
        """Test basic extract error."""
        error = ExtractError("Failed to read source data")
        
        assert error.error_step == "EXTRACT"
        assert error.error_text == "Failed to read source data"
        assert "[EXTRACT]" in str(error)
    
    def test_extract_error_with_record(self):
        """Test extract error with record ID."""
        error = ExtractError("Invalid record format", record_id="T000001")
        
        assert error.record_id == "T000001"
        assert "Record: T000001" in str(error)
    
    def test_extract_error_inheritance(self):
        """Test inheritance hierarchy."""
        error = ExtractError("Test")
        
        assert isinstance(error, ExtractError)
        assert isinstance(error, ETLError)
        assert isinstance(error, Exception)


class TestTransformError:
    """Test cases for TransformError."""
    
    def test_transform_error_basic(self):
        """Test basic transform error."""
        error = TransformError("Calculation error")
        
        assert error.error_step == "TRANSFORM"
        assert error.error_text == "Calculation error"
        assert "[TRANSFORM]" in str(error)
    
    def test_transform_error_with_record(self):
        """Test transform error with record ID."""
        error = TransformError("Invalid quantity", record_id="T000002")
        
        assert error.record_id == "T000002"
        assert "Record: T000002" in str(error)


class TestLoadError:
    """Test cases for LoadError."""
    
    def test_load_error_basic(self):
        """Test basic load error."""
        error = LoadError("Database connection failed")
        
        assert error.error_step == "LOAD"
        assert error.error_text == "Database connection failed"
        assert "[LOAD]" in str(error)
    
    def test_load_error_with_record(self):
        """Test load error with record ID."""
        error = LoadError("Duplicate key", record_id="ANL12345")
        
        assert error.record_id == "ANL12345"
        assert "Record: ANL12345" in str(error)


class TestValidationError:
    """Test cases for ValidationError."""
    
    def test_validation_error_basic(self):
        """Test basic validation error."""
        error = ValidationError("Missing required field")
        
        assert error.error_step == "VALIDATE"
        assert error.error_text == "Missing required field"
        assert "[VALIDATE]" in str(error)
    
    def test_validation_error_with_record(self):
        """Test validation error with record ID."""
        error = ValidationError("Invalid currency code", record_id="T000003")
        
        assert error.record_id == "T000003"
        assert "Record: T000003" in str(error)


class TestExceptionRaising:
    """Test raising and catching exceptions."""
    
    def test_raise_and_catch_etl_error(self):
        """Test raising and catching ETLError."""
        with pytest.raises(ETLError) as exc_info:
            raise ETLError("Test error")
        
        assert exc_info.value.error_text == "Test error"
    
    def test_raise_and_catch_extract_error(self):
        """Test raising and catching ExtractError."""
        with pytest.raises(ExtractError) as exc_info:
            raise ExtractError("Extract failed")
        
        assert exc_info.value.error_step == "EXTRACT"
    
    def test_catch_specific_as_base(self):
        """Test that specific errors can be caught as base ETLError."""
        with pytest.raises(ETLError):
            raise TransformError("Transform failed")
    
    def test_exception_chaining(self):
        """Test exception chaining."""
        try:
            try:
                raise ValueError("Original error")
            except ValueError as e:
                raise LoadError("Load failed") from e
        except LoadError as load_err:
            assert load_err.__cause__ is not None
            assert isinstance(load_err.__cause__, ValueError)


class TestExceptionMessages:
    """Test exception message formatting."""
    
    def test_message_format_step_only(self):
        """Test message format with step only."""
        error = ETLError("Error", error_step="INIT")
        assert str(error) == "[INIT] Error"
    
    def test_message_format_record_only(self):
        """Test message format with record only."""
        error = ETLError("Error", record_id="REC001")
        assert str(error) == "Error (Record: REC001)"
    
    def test_message_format_step_and_record(self):
        """Test message format with both step and record."""
        error = ETLError("Error", error_step="LOAD", record_id="REC001")
        expected = "[LOAD] Error (Record: REC001)"
        assert str(error) == expected
    
    def test_empty_error_text(self):
        """Test with empty error text."""
        error = ETLError("", error_step="TEST")
        assert "[TEST]" in str(error)


===FILE: pytest.ini===
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = 
    -v
    --tb=short
    --strict-markers
    --disable-warnings
markers =
    unit: Unit tests
    integration: Integration tests
    slow: Slow running tests


===FILE: requirements.txt===
# Core dependencies
pyspark>=3.3.0
pyyaml>=6.0
python-dateutil>=2.8.2

# Testing dependencies
pytest>=7.3.0
pytest-cov>=4.0.0
pytest-mock>=3.10.0

# Development dependencies
black>=23.0.0
flake8>=6.0.0
mypy>=1.0.0
pylint>=2.17.0


===FILE: README.md===
# ETL Component Interface - Python ABC

This module provides a Python Abstract Base Class (ABC) implementation of the ABAP ETL component interface (`ZIF_ETL_COMPONENT`).

## Overview

The `ETLComponent` abstract base class defines the interface contract that all ETL components must implement. It provides:

- **ExecutionResult dataclass**: Type-safe result structure with validation
- **ETLComponent ABC**: Abstract interface for all ETL components
- **Custom exception hierarchy**: Specific exceptions for each ETL phase

## Architecture

### ExecutionResult

A dataclass representing the outcome of ETL component execution:

```python
@dataclass
class ExecutionResult:
    success: bool
    records_total: int
    records_success: int
    records_error: int
    message: str
```

### ETLComponent Interface

Abstract base class that defines three required methods:

```python
class ETLComponent(ABC):
    @abstractmethod
    def execute(self) -> ExecutionResult:
        """Execute the component's main operation."""
        pass
    
    @abstractmethod
    def get_component_name(self) -> str:
        """Return component name."""
        pass
    
    @abstractmethod
    def validate_prerequisites(self) -> bool:
        """Validate prerequisites before execution."""
        pass
```

### Exception Hierarchy

```
Exception
└── ETLError (base)
    ├── ExtractError
    ├── TransformError
    ├── LoadError
    └── ValidationError
```

## Usage

### Implementing a Component

```python
from src.etl_component import ETLComponent, ExecutionResult

class MyExtractor(ETLComponent):
    def execute(self) -> ExecutionResult:
        # Implementation
        return ExecutionResult(
            success=True,
            records_total=100,
            records_success=100,
            records_error=0,
            message="Extraction completed"
        )
    
    def get_component_name(self) -> str:
        return "DataExtractor"
    
    def validate_prerequisites(self) -> bool:
        # Check connections, config, etc.
        return True
```

### Error Handling

```python
from src.exceptions import ExtractError

try:
    result = extractor.execute()
except ExtractError as e:
    print(f"Extraction failed: {e}")
    print(f"Step: {e.error_step}")
    print(f"Record: {e.record_id}")
```

## Configuration

The `config.yaml` file contains all ETL configuration parameters converted from ABAP constants:

- Business rules (discounts, tax rates, thresholds)
- ETL configuration (batch sizes, timeouts)
- Status codes and process steps
- Spark configuration
- Database connections

## Testing

Run tests with pytest:

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/test_etl_component.py -v
```

## Type Safety

The module uses Python type hints throughout:

- All method signatures include return type annotations
- Dataclasses provide runtime type validation
- Compatible with mypy for static type checking

```bash
# Run type checking
mypy src/
```

## ABAP to Python Mapping

| ABAP | Python |
|------|--------|
| `INTERFACE zif_etl_component` | `class ETLComponent(ABC)` |
| `ty_execution_result` structure | `@dataclass ExecutionResult` |
| `abap_bool` | `bool` |
| `TYPE i` | `int` |
| `TYPE string` | `str` |
| `RAISING zcx_etl_error` | `raises ETLError` |

## Best Practices

1. **Always validate prerequisites** before calling `execute()`
2. **Use specific exception types** (ExtractError, TransformError, etc.)
3. **Include record IDs** in exceptions when processing fails
4. **Return detailed ExecutionResult** with accurate counts
5. **Implement proper logging** in concrete implementations

## Migration Notes

Key differences from ABAP:

- Python uses `None` instead of ABAP's `INITIAL`
- Return type annotations replace ABAP `RETURNING VALUE(...)`
- Dataclasses provide automatic `__init__`, `__repr__`, etc.
- Abstract methods use `@abstractmethod` decorator
- Exception handling uses try/except instead of TRY/CATCH

## Dependencies

- Python 3.8+
- No external runtime dependencies for core interface
- pytest for testing
- mypy for type checking (optional)

## License

This code is provided as-is for SAP to Python migration purposes.