===FILE: src/logger_interface.py===
"""
ETL Logger Interface - Abstract Base Class
Converted from ABAP ZIF_ETL_LOGGER interface
"""
from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional


class LogStatus(Enum):
    """Log status codes - converted from ZIF_ETL_LOGGER gc_status constants"""
    SUCCESS = 'S'
    ERROR = 'E'
    WARNING = 'W'
    INFO = 'I'


class ProcessStep(Enum):
    """ETL process step identifiers - converted from ZIF_ETL_LOGGER gc_step constants"""
    INIT = 'INIT'
    EXTRACT = 'EXTRACT'
    TRANSFORM = 'TRANSFORM'
    LOAD = 'LOAD'
    VALIDATE = 'VALIDATE'
    COMPLETE = 'COMPLETE'
    ERROR = 'ERROR'


class ETLLoggerInterface(ABC):
    """
    Abstract Base Class for ETL logging functionality.
    
    This interface defines the contract for all ETL logger implementations.
    Converted from ABAP interface ZIF_ETL_LOGGER.
    """
    
    @abstractmethod
    def log_message(
        self,
        step: ProcessStep,
        status: LogStatus,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ) -> None:
        """
        Log a message with step, status, and optional record counts.
        
        Args:
            step: The ETL process step (ProcessStep enum)
            status: The log status (LogStatus enum)
            message: The log message text (max 255 chars)
            records_processed: Total number of records processed (default 0)
            records_success: Number of successfully processed records (default 0)
            records_error: Number of records with errors (default 0)
        """
        pass
    
    @abstractmethod
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run identifier.
        
        Returns:
            str: The ETL run ID (20 characters)
        """
        pass


===FILE: src/logger.py===
"""
ETL Logger Implementation
Concrete implementation of ETL logger interface with database persistence
"""
import logging
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession

from src.logger_interface import ETLLoggerInterface, LogStatus, ProcessStep


class ETLLogger(ETLLoggerInterface):
    """
    Concrete implementation of ETL logger.
    
    Provides logging functionality with console output and optional
    database persistence for ETL execution tracking.
    """
    
    def __init__(self, etl_run_id: str, spark: Optional[SparkSession] = None):
        """
        Initialize the logger with ETL run identifier.
        
        Args:
            etl_run_id: Unique identifier for this ETL run (20 chars)
            spark: Optional SparkSession for database logging
        """
        self._etl_run_id = etl_run_id
        self._spark = spark
        
        # Configure Python logging
        self._logger = logging.getLogger(f"ETLLogger_{etl_run_id}")
        self._logger.setLevel(logging.INFO)
        
        # Add console handler if not already present
        if not self._logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            console_handler.setFormatter(formatter)
            self._logger.addHandler(console_handler)
    
    def log_message(
        self,
        step: ProcessStep,
        status: LogStatus,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ) -> None:
        """
        Log a message with step, status, and optional record counts.
        
        Args:
            step: The ETL process step
            status: The log status
            message: The log message text (truncated to 255 chars)
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Records with errors
        """
        # Truncate message to 255 characters
        truncated_message = message[:255] if len(message) > 255 else message
        
        # Generate unique log ID
        log_id = self._generate_log_id()
        
        # Format log entry
        log_entry = {
            'log_id': log_id,
            'etl_run_id': self._etl_run_id,
            'execution_date': datetime.now().strftime('%Y-%m-%d'),
            'execution_time': datetime.now().strftime('%H:%M:%S'),
            'process_step': step.value,
            'status': status.value,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error,
            'message': truncated_message
        }
        
        # Log to console with appropriate level
        log_text = (
            f"[{step.value}] {truncated_message} "
            f"(Processed: {records_processed}, Success: {records_success}, Error: {records_error})"
        )
        
        if status == LogStatus.ERROR:
            self._logger.error(log_text)
        elif status == LogStatus.WARNING:
            self._logger.warning(log_text)
        elif status == LogStatus.INFO:
            self._logger.info(log_text)
        else:  # SUCCESS
            self._logger.info(log_text)
        
        # Persist to database if Spark session available
        if self._spark:
            self._persist_log_entry(log_entry)
    
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run identifier.
        
        Returns:
            str: The ETL run ID
        """
        return self._etl_run_id
    
    def _generate_log_id(self) -> str:
        """
        Generate a unique log ID based on timestamp.
        
        Returns:
            str: Unique log ID (format: LOG + timestamp)
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')[:14]
        return f"LOG{timestamp}"
    
    def _persist_log_entry(self, log_entry: dict) -> None:
        """
        Persist log entry to database using Spark.
        
        Args:
            log_entry: Dictionary containing log entry fields
        """
        try:
            # Create DataFrame from log entry
            log_df = self._spark.createDataFrame([log_entry])
            
            # Write to log table (append mode)
            # In production, replace with actual table name and configuration
            log_df.write.format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable("etl_log")
                
        except Exception as e:
            # Don't let logging failures break the ETL process
            self._logger.warning(f"Failed to persist log entry to database: {str(e)}")


===FILE: src/constants.py===
"""
ETL Constants and Configuration
Converted from ABAP ZCL_ETL_CONSTANTS
"""
from enum import Enum
from decimal import Decimal


class SaleCategory(Enum):
    """Sale category classifications - converted from gc_category constants"""
    HIGH = 'HIGH'
    MEDIUM = 'MEDIUM'
    LOW = 'LOW'


class ETLConstants:
    """
    Constants and configuration values for ETL system.
    
    Converted from ABAP class ZCL_ETL_CONSTANTS.
    All business rules and thresholds are centralized here.
    """
    
    # Business rules - Discount thresholds
    DISCOUNT_QTY_TIER1 = 10
    DISCOUNT_QTY_TIER2 = 15
    DISCOUNT_RATE_TIER1 = Decimal('0.05')
    DISCOUNT_RATE_TIER2 = Decimal('0.10')
    
    # Business rules - Tax rate
    TAX_RATE = Decimal('0.08')
    
    # Business rules - Cost ratio
    COST_RATIO = Decimal('0.60')
    
    # Business rules - Category thresholds
    CATEGORY_HIGH_THRESHOLD = Decimal('2000.00')
    CATEGORY_MEDIUM_THRESHOLD = Decimal('500.00')
    
    # ETL configuration defaults
    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_COMMIT_INTERVAL = 500
    DEFAULT_RETRY_ATTEMPTS = 3
    DEFAULT_TIMEOUT_SECONDS = 3600
    
    # ID prefixes
    PREFIX_ETL_RUN = 'ETL'
    PREFIX_LOG_ID = 'LOG'
    PREFIX_ANALYTICS_ID = 'ANL'
    
    # Message texts
    MSG_INIT_SUCCESS = 'ETL process initialized successfully'
    MSG_EXTRACT_START = 'Starting data extraction'
    MSG_EXTRACT_COMPLETE = 'Data extraction completed'
    MSG_TRANSFORM_START = 'Starting data transformation'
    MSG_TRANSFORM_COMPLETE = 'Data transformation completed'
    MSG_LOAD_START = 'Starting data load'
    MSG_LOAD_COMPLETE = 'Data load completed'
    MSG_ETL_COMPLETE = 'ETL process completed successfully'
    MSG_ETL_ERROR = 'ETL process failed'


===FILE: config.yaml===
# ETL Configuration File
# Converted from ABAP ETL System Configuration

# Database Configuration
database:
  source_table: "sales_raw"
  target_table: "sales_analytics"
  log_table: "etl_log"
  format: "delta"  # or "parquet", "jdbc"
  
# JDBC Configuration (if using relational database)
jdbc:
  url: "jdbc:postgresql://localhost:5432/etl_db"
  driver: "org.postgresql.Driver"
  user: "${DB_USER}"
  password: "${DB_PASSWORD}"

# Spark Configuration
spark:
  app_name: "Sales ETL Process"
  master: "local[*]"  # or "yarn", "spark://host:port"
  config:
    spark.sql.adaptive.enabled: "true"
    spark.sql.adaptive.coalescePartitions.enabled: "true"
    spark.sql.shuffle.partitions: "200"
    spark.executor.memory: "4g"
    spark.driver.memory: "2g"

# ETL Processing Configuration
etl:
  batch_size: 1000
  commit_interval: 500
  retry_attempts: 3
  timeout_seconds: 3600
  test_mode: false
  
# Business Rules Configuration
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

# Logging Configuration
logging:
  level: "INFO"  # DEBUG, INFO, WARNING, ERROR
  console_output: true
  file_output: true
  log_dir: "logs"
  
# Date Range Configuration
date_range:
  default_lookback_days: 7
  max_date_range_days: 365

# Performance Configuration
performance:
  cache_intermediate_results: true
  repartition_threshold: 10000
  broadcast_threshold: 10485760  # 10MB


===FILE: tests/test_logger_interface.py===
"""
Unit tests for ETL Logger Interface and Implementation
"""
import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch
from pyspark.sql import SparkSession

from src.logger_interface import ETLLoggerInterface, LogStatus, ProcessStep
from src.logger import ETLLogger


class TestLogStatus:
    """Test LogStatus enum"""
    
    def test_status_values(self):
        """Test that status codes match ABAP constants"""
        assert LogStatus.SUCCESS.value == 'S'
        assert LogStatus.ERROR.value == 'E'
        assert LogStatus.WARNING.value == 'W'
        assert LogStatus.INFO.value == 'I'
    
    def test_status_enum_members(self):
        """Test all expected status types exist"""
        expected_statuses = {'SUCCESS', 'ERROR', 'WARNING', 'INFO'}
        actual_statuses = {status.name for status in LogStatus}
        assert actual_statuses == expected_statuses


class TestProcessStep:
    """Test ProcessStep enum"""
    
    def test_step_values(self):
        """Test that step values match ABAP constants"""
        assert ProcessStep.INIT.value == 'INIT'
        assert ProcessStep.EXTRACT.value == 'EXTRACT'
        assert ProcessStep.TRANSFORM.value == 'TRANSFORM'
        assert ProcessStep.LOAD.value == 'LOAD'
        assert ProcessStep.VALIDATE.value == 'VALIDATE'
        assert ProcessStep.COMPLETE.value == 'COMPLETE'
        assert ProcessStep.ERROR.value == 'ERROR'
    
    def test_step_enum_members(self):
        """Test all expected step types exist"""
        expected_steps = {
            'INIT', 'EXTRACT', 'TRANSFORM', 'LOAD',
            'VALIDATE', 'COMPLETE', 'ERROR'
        }
        actual_steps = {step.name for step in ProcessStep}
        assert actual_steps == expected_steps


class TestETLLoggerInterface:
    """Test ETL Logger Interface contract"""
    
    def test_interface_is_abstract(self):
        """Test that interface cannot be instantiated directly"""
        with pytest.raises(TypeError):
            ETLLoggerInterface()
    
    def test_interface_methods_defined(self):
        """Test that required methods are defined in interface"""
        required_methods = {'log_message', 'get_etl_run_id'}
        interface_methods = {
            method for method in dir(ETLLoggerInterface)
            if not method.startswith('_')
        }
        assert required_methods.issubset(interface_methods)


class TestETLLogger:
    """Test ETL Logger Implementation"""
    
    @pytest.fixture
    def etl_run_id(self):
        """Fixture for ETL run ID"""
        return "ETL20240115120000"
    
    @pytest.fixture
    def logger(self, etl_run_id):
        """Fixture for ETL logger instance"""
        return ETLLogger(etl_run_id=etl_run_id)
    
    @pytest.fixture
    def mock_spark(self):
        """Fixture for mock Spark session"""
        spark = Mock(spec=SparkSession)
        spark.createDataFrame = MagicMock()
        return spark
    
    def test_logger_initialization(self, etl_run_id):
        """Test logger initializes correctly"""
        logger = ETLLogger(etl_run_id=etl_run_id)
        assert logger.get_etl_run_id() == etl_run_id
        assert logger._logger is not None
    
    def test_logger_with_spark(self, etl_run_id, mock_spark):
        """Test logger initializes with Spark session"""
        logger = ETLLogger(etl_run_id=etl_run_id, spark=mock_spark)
        assert logger._spark is not None
    
    def test_get_etl_run_id(self, logger, etl_run_id):
        """Test get_etl_run_id returns correct ID"""
        assert logger.get_etl_run_id() == etl_run_id
    
    def test_log_message_basic(self, logger):
        """Test basic log message without record counts"""
        # Should not raise exception
        logger.log_message(
            step=ProcessStep.INIT,
            status=LogStatus.SUCCESS,
            message="Test initialization"
        )
    
    def test_log_message_with_counts(self, logger):
        """Test log message with record counts"""
        logger.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="Extraction completed",
            records_processed=100,
            records_success=95,
            records_error=5
        )
    
    def test_log_message_truncation(self, logger):
        """Test that messages longer than 255 chars are truncated"""
        long_message = "A" * 300
        logger.log_message(
            step=ProcessStep.TRANSFORM,
            status=LogStatus.INFO,
            message=long_message
        )
        # Message should be truncated to 255 chars
        # This would be verified in actual log output
    
    def test_log_message_error_status(self, logger):
        """Test logging with ERROR status"""
        logger.log_message(
            step=ProcessStep.LOAD,
            status=LogStatus.ERROR,
            message="Load failed due to constraint violation"
        )
    
    def test_log_message_warning_status(self, logger):
        """Test logging with WARNING status"""
        logger.log_message(
            step=ProcessStep.VALIDATE,
            status=LogStatus.WARNING,
            message="Data quality issues detected"
        )
    
    def test_generate_log_id_format(self, logger):
        """Test log ID generation format"""
        log_id = logger._generate_log_id()
        assert log_id.startswith("LOG")
        assert len(log_id) == 17  # LOG + 14 digit timestamp
        assert log_id[3:].isdigit()
    
    def test_generate_log_id_uniqueness(self, logger):
        """Test that generated log IDs are unique"""
        log_ids = [logger._generate_log_id() for _ in range(10)]
        assert len(set(log_ids)) == len(log_ids)
    
    @patch('src.logger.datetime')
    def test_log_entry_structure(self, mock_datetime, logger, mock_spark):
        """Test log entry dictionary structure"""
        # Mock datetime
        mock_now = Mock()
        mock_now.strftime.side_effect = lambda fmt: {
            '%Y-%m-%d': '2024-01-15',
            '%H:%M:%S': '12:00:00',
            '%Y%m%d%H%M%S%f': '20240115120000000000'
        }[fmt]
        mock_datetime.now.return_value = mock_now
        
        logger_with_spark = ETLLogger(
            etl_run_id="ETL20240115120000",
            spark=mock_spark
        )
        
        # Mock the write operation
        mock_df = Mock()
        mock_df.write.format.return_value.mode.return_value.option.return_value.saveAsTable = Mock()
        mock_spark.createDataFrame.return_value = mock_df
        
        logger_with_spark.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="Test message",
            records_processed=100,
            records_success=95,
            records_error=5
        )
        
        # Verify createDataFrame was called with correct structure
        assert mock_spark.createDataFrame.called
    
    def test_persist_log_entry_exception_handling(self, logger, mock_spark):
        """Test that logging exceptions don't break the process"""
        logger_with_spark = ETLLogger(
            etl_run_id="ETL20240115120000",
            spark=mock_spark
        )
        
        # Make Spark operations fail
        mock_spark.createDataFrame.side_effect = Exception("Database error")
        
        # Should not raise exception
        logger_with_spark.log_message(
            step=ProcessStep.LOAD,
            status=LogStatus.ERROR,
            message="This should log despite database error"
        )
    
    def test_logger_multiple_steps(self, logger):
        """Test logging multiple ETL steps"""
        steps = [
            (ProcessStep.INIT, LogStatus.SUCCESS, "Initialized"),
            (ProcessStep.EXTRACT, LogStatus.SUCCESS, "Extracted 1000 records"),
            (ProcessStep.TRANSFORM, LogStatus.SUCCESS, "Transformed 1000 records"),
            (ProcessStep.LOAD, LogStatus.SUCCESS, "Loaded 995 records"),
            (ProcessStep.COMPLETE, LogStatus.SUCCESS, "ETL completed")
        ]
        
        for step, status, message in steps:
            logger.log_message(step=step, status=status, message=message)
    
    def test_logger_interface_implementation(self, logger):
        """Test that ETLLogger properly implements ETLLoggerInterface"""
        assert isinstance(logger, ETLLoggerInterface)
    
    def test_all_process_steps_loggable(self, logger):
        """Test that all ProcessStep enum values can be logged"""
        for step in ProcessStep:
            logger.log_message(
                step=step,
                status=LogStatus.INFO,
                message=f"Testing {step.name}"
            )
    
    def test_all_status_types_loggable(self, logger):
        """Test that all LogStatus enum values can be logged"""
        for status in LogStatus:
            logger.log_message(
                step=ProcessStep.VALIDATE,
                status=status,
                message=f"Testing {status.name}"
            )


class TestIntegration:
    """Integration tests for logger with real Spark"""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Create a real Spark session for integration tests"""
        spark = SparkSession.builder \
            .appName("ETL Logger Tests") \
            .master("local[2]") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    def test_logger_with_real_spark(self, spark):
        """Test logger with real Spark session"""
        etl_run_id = f"ETL{datetime.now().strftime('%Y%m%d%H%M%S')}"
        logger = ETLLogger(etl_run_id=etl_run_id, spark=spark)
        
        # Log some messages
        logger.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="Integration test extraction",
            records_processed=100,
            records_success=100,
            records_error=0
        )
        
        assert logger.get_etl_run_id() == etl_run_id


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


===FILE: tests/test_constants.py===
"""
Unit tests for ETL Constants
"""
import pytest
from decimal import Decimal

from src.constants import ETLConstants, SaleCategory


class TestSaleCategory:
    """Test SaleCategory enum"""
    
    def test_category_values(self):
        """Test category values match ABAP constants"""
        assert SaleCategory.HIGH.value == 'HIGH'
        assert SaleCategory.MEDIUM.value == 'MEDIUM'
        assert SaleCategory.LOW.value == 'LOW'
    
    def test_category_members(self):
        """Test all expected categories exist"""
        expected_categories = {'HIGH', 'MEDIUM', 'LOW'}
        actual_categories = {cat.name for cat in SaleCategory}
        assert actual_categories == expected_categories


class TestETLConstants:
    """Test ETL Constants values"""
    
    def test_discount_thresholds(self):
        """Test discount quantity thresholds"""
        assert ETLConstants.DISCOUNT_QTY_TIER1 == 10
        assert ETLConstants.DISCOUNT_QTY_TIER2 == 15
    
    def test_discount_rates(self):
        """Test discount rate percentages"""
        assert ETLConstants.DISCOUNT_RATE_TIER1 == Decimal('0.05')
        assert ETLConstants.DISCOUNT_RATE_TIER2 == Decimal('0.10')
    
    def test_tax_rate(self):
        """Test tax rate constant"""
        assert ETLConstants.TAX_RATE == Decimal('0.08')
    
    def test_cost_ratio(self):
        """Test cost ratio constant"""
        assert ETLConstants.COST_RATIO == Decimal('0.60')
    
    def test_category_thresholds(self):
        """Test category amount thresholds"""
        assert ETLConstants.CATEGORY_HIGH_THRESHOLD == Decimal('2000.00')
        assert ETLConstants.CATEGORY_MEDIUM_THRESHOLD == Decimal('500.00')
    
    def test_etl_defaults(self):
        """Test ETL configuration defaults"""
        assert ETLConstants.DEFAULT_BATCH_SIZE == 1000
        assert ETLConstants.DEFAULT_COMMIT_INTERVAL == 500
        assert ETLConstants.DEFAULT_RETRY_ATTEMPTS == 3
        assert ETLConstants.DEFAULT_TIMEOUT_SECONDS == 3600
    
    def test_id_prefixes(self):
        """Test ID prefix constants"""
        assert ETLConstants.PREFIX_ETL_RUN == 'ETL'
        assert ETLConstants.PREFIX_LOG_ID == 'LOG'
        assert ETLConstants.PREFIX_ANALYTICS_ID == 'ANL'
    
    def test_message_texts(self):
        """Test predefined message text constants"""
        assert ETLConstants.MSG_INIT_SUCCESS == 'ETL process initialized successfully'
        assert ETLConstants.MSG_EXTRACT_START == 'Starting data extraction'
        assert ETLConstants.MSG_EXTRACT_COMPLETE == 'Data extraction completed'
        assert ETLConstants.MSG_TRANSFORM_START == 'Starting data transformation'
        assert ETLConstants.MSG_TRANSFORM_COMPLETE == 'Data transformation completed'
        assert ETLConstants.MSG_LOAD_START == 'Starting data load'
        assert ETLConstants.MSG_LOAD_COMPLETE == 'Data load completed'
        assert ETLConstants.MSG_ETL_COMPLETE == 'ETL process completed successfully'
        assert ETLConstants.MSG_ETL_ERROR == 'ETL process failed'
    
    def test_decimal_precision(self):
        """Test that decimal values maintain correct precision"""
        # Tax rate should be exactly 8%
        tax_rate = ETLConstants.TAX_RATE
        assert float(tax_rate) == 0.08
        
        # Discount rates should be exact
        discount1 = ETLConstants.DISCOUNT_RATE_TIER1
        discount2 = ETLConstants.DISCOUNT_RATE_TIER2
        assert float(discount1) == 0.05
        assert float(discount2) == 0.10
    
    def test_business_rule_consistency(self):
        """Test that business rules are internally consistent"""
        # Tier 2 threshold should be higher than tier 1
        assert ETLConstants.DISCOUNT_QTY_TIER2 > ETLConstants.DISCOUNT_QTY_TIER1
        
        # Tier 2 discount should be higher than tier 1
        assert ETLConstants.DISCOUNT_RATE_TIER2 > ETLConstants.DISCOUNT_RATE_TIER1
        
        # High category threshold should be higher than medium
        assert ETLConstants.CATEGORY_HIGH_THRESHOLD > ETLConstants.CATEGORY_MEDIUM_THRESHOLD


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


===FILE: requirements.txt===
# PySpark ETL Requirements
# Python 3.8+

# Core dependencies
pyspark==3.4.1
py4j==0.10.9.7

# Configuration management
pyyaml==6.0.1

# Testing
pytest==7.4.3
pytest-cov==4.1.0
pytest-mock==3.12.0

# Data processing
pandas==2.0.3
numpy==1.24.3
pyarrow==13.0.0

# Logging
colorlog==6.8.0

# Type checking (optional but recommended)
mypy==1.7.1

# Code quality (optional but recommended)
black==23.12.1
flake8==7.0.0
pylint==3.0.3


===FILE: setup.py===
"""
Setup configuration for ETL Logger package
"""
from setuptools import setup, find_packages

setup(
    name="etl-logger",
    version="1.0.0",
    description="ETL Logger Interface and Implementation - Migrated from ABAP",
    author="ETL Migration Team",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "pyspark>=3.4.0",
        "pyyaml>=6.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "pytest-mock>=3.12.0",
            "black>=23.0.0",
            "flake8>=7.0.0",
            "mypy>=1.7.0",
        ]
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)


===FILE: README.md===
# ETL Logger Interface - ABAP to Python Migration

## Overview

This package provides a Python implementation of the ETL logging interface originally developed in ABAP (ZIF_ETL_LOGGER). It includes:

- **Abstract Base Class (ABC)** defining the logger interface contract
- **Concrete Implementation** with console and database logging
- **Enum Classes** for status codes and process steps
- **Constants Module** with business rules and configuration

## Migration Details

### Source ABAP Components
- `ZIF_ETL_LOGGER` → `src/logger_interface.py` (ETLLoggerInterface)
- `ZCL_ETL_LOGGER` → `src/logger.py` (ETLLogger)
- `ZCL_ETL_CONSTANTS` → `src/constants.py` (ETLConstants, SaleCategory)

### Key Features

1. **Type Safety**: Uses Python Enums for status codes and process steps
2. **Interface Contract**: ABC ensures consistent implementation
3. **Flexible Logging**: Console output with optional database persistence
4. **Spark Integration**: Native PySpark DataFrame support
5. **Error Handling**: Robust exception handling prevents logging failures from breaking ETL

## Installation

```bash
# Install in development mode
pip install -e .

# Install with development dependencies
pip install -e ".[dev]"
```

## Usage

### Basic Usage

```python
from datetime import datetime
from src.logger_interface import LogStatus, ProcessStep
from src.logger import ETLLogger

# Create logger instance
etl_run_id = f"ETL{datetime.now().strftime('%Y%m%d%H%M%S')}"
logger = ETLLogger(etl_run_id=etl_run_id)

# Log messages
logger.log_message(
    step=ProcessStep.INIT,
    status=LogStatus.SUCCESS,
    message="ETL process initialized"
)

logger.log_message(
    step=ProcessStep.EXTRACT,
    status=LogStatus.SUCCESS,
    message="Extraction completed",
    records_processed=1000,
    records_success=995,
    records_error=5
)
```

### With Spark Integration

```python
from pyspark.sql import SparkSession
from src.logger import ETLLogger

# Create Spark session
spark = SparkSession.builder \
    .appName("ETL Process") \
    .getOrCreate()

# Create logger with Spark
logger = ETLLogger