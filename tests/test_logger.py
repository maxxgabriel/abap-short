"""
Unit tests for ETL Logger module.
Tests logging functionality and DataFrame persistence.
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import tempfile
import os
import shutil

from src.logger import ETLLogger, LogEntry, generate_etl_run_id


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    spark = (
        SparkSession.builder
        .appName("ETLLoggerTest")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def etl_run_id():
    """Generate a test ETL run ID."""
    return generate_etl_run_id("TEST")


@pytest.fixture
def logger(spark, etl_run_id):
    """Create a logger instance for testing."""
    return ETLLogger(etl_run_id, spark, log_level="DEBUG")


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test outputs."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path)


class TestLogEntry:
    """Test LogEntry dataclass functionality."""
    
    def test_log_entry_creation(self):
        """Test creating a log entry."""
        entry = LogEntry(
            log_id="LOG001",
            etl_run_id="ETL001",
            execution_date="2024-01-15",
            execution_time="10:30:00",
            process_step="EXTRACT",
            status="S",
            records_processed=100,
            records_success=95,
            records_error=5,
            message="Test message"
        )
        
        assert entry.log_id == "LOG001"
        assert entry.etl_run_id == "ETL001"
        assert entry.records_processed == 100
        assert entry.records_success == 95
        assert entry.records_error == 5
        assert entry.message == "Test message"
    
    def test_log_entry_defaults(self):
        """Test default values in log entry."""
        entry = LogEntry(
            log_id="LOG001",
            etl_run_id="ETL001",
            execution_date="2024-01-15",
            execution_time="10:30:00",
            process_step="INIT",
            status="S"
        )
        
        assert entry.records_processed == 0
        assert entry.records_success == 0
        assert entry.records_error == 0
        assert entry.message == ""
        assert isinstance(entry.created_at, datetime)
        assert entry.created_by == "system"
    
    def test_log_entry_to_dict(self):
        """Test converting log entry to dictionary."""
        entry = LogEntry(
            log_id="LOG001",
            etl_run_id="ETL001",
            execution_date="2024-01-15",
            execution_time="10:30:00",
            process_step="EXTRACT",
            status="S"
        )
        
        entry_dict = entry.to_dict()
        
        assert isinstance(entry_dict, dict)
        assert entry_dict["log_id"] == "LOG001"
        assert entry_dict["etl_run_id"] == "ETL001"
        assert "created_at" in entry_dict
        assert isinstance(entry_dict["created_at"], str)


class TestETLLogger:
    """Test ETLLogger class functionality."""
    
    def test_logger_initialization(self, spark, etl_run_id):
        """Test logger initialization."""
        logger = ETLLogger(etl_run_id, spark)
        
        assert logger.etl_run_id == etl_run_id
        assert logger.spark == spark
        assert len(logger.log_entries) == 0
        assert logger.logger is not None
    
    def test_generate_log_id(self, logger):
        """Test log ID generation."""
        log_id1 = logger._generate_log_id()
        log_id2 = logger._generate_log_id()
        
        assert log_id1.startswith("LOG")
        assert log_id2.startswith("LOG")
        assert log_id1 != log_id2
        assert len(log_id1) == 23  # LOG + 20 digit timestamp
    
    def test_log_message_basic(self, logger):
        """Test basic message logging."""
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Test extraction"
        )
        
        assert len(logger.log_entries) == 1
        entry = logger.log_entries[0]
        assert entry.process_step == ETLLogger.STEP_EXTRACT
        assert entry.status == ETLLogger.STATUS_SUCCESS
        assert entry.message == "Test extraction"
    
    def test_log_message_with_statistics(self, logger):
        """Test message logging with statistics."""
        logger.log_message(
            step=ETLLogger.STEP_TRANSFORM,
            status=ETLLogger.STATUS_SUCCESS,
            message="Transformation complete",
            records_processed=1000,
            records_success=950,
            records_error=50
        )
        
        assert len(logger.log_entries) == 1
        entry = logger.log_entries[0]
        assert entry.records_processed == 1000
        assert entry.records_success == 950
        assert entry.records_error == 50
    
    def test_multiple_log_entries(self, logger):
        """Test logging multiple messages."""
        logger.log_message(
            step=ETLLogger.STEP_INIT,
            status=ETLLogger.STATUS_INFO,
            message="Initialization"
        )
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Extraction"
        )
        logger.log_message(
            step=ETLLogger.STEP_TRANSFORM,
            status=ETLLogger.STATUS_SUCCESS,
            message="Transformation"
        )
        
        assert len(logger.log_entries) == 3
        assert logger.log_entries[0].process_step == ETLLogger.STEP_INIT
        assert logger.log_entries[1].process_step == ETLLogger.STEP_EXTRACT
        assert logger.log_entries[2].process_step == ETLLogger.STEP_TRANSFORM
    
    def test_get_etl_run_id(self, logger, etl_run_id):
        """Test retrieving ETL run ID."""
        assert logger.get_etl_run_id() == etl_run_id
    
    def test_log_schema(self, logger):
        """Test log DataFrame schema."""
        schema = logger.get_log_schema()
        
        assert isinstance(schema, StructType)
        field_names = [field.name for field in schema.fields]
        
        assert "log_id" in field_names
        assert "etl_run_id" in field_names
        assert "execution_date" in field_names
        assert "process_step" in field_names
        assert "status" in field_names
        assert "records_processed" in field_names
        assert "message" in field_names
    
    def test_get_logs_as_dataframe_empty(self, logger):
        """Test converting empty logs to DataFrame."""
        df = logger.get_logs_as_dataframe()
        
        assert df.count() == 0
        assert len(df.columns) > 0
    
    def test_get_logs_as_dataframe_with_data(self, logger):
        """Test converting logs to DataFrame."""
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Test 1",
            records_processed=100
        )
        logger.log_message(
            step=ETLLogger.STEP_TRANSFORM,
            status=ETLLogger.STATUS_SUCCESS,
            message="Test 2",
            records_processed=100,
            records_success=95
        )
        
        df = logger.get_logs_as_dataframe()
        
        assert df.count() == 2
        assert "log_id" in df.columns
        assert "process_step" in df.columns
        
        rows = df.collect()
        assert rows[0]["process_step"] == ETLLogger.STEP_EXTRACT
        assert rows[1]["process_step"] == ETLLogger.STEP_TRANSFORM
    
    def test_persist_logs_parquet(self, logger, temp_dir):
        """Test persisting logs to Parquet format."""
        logger.log_message(
            step=ETLLogger.STEP_LOAD,
            status=ETLLogger.STATUS_SUCCESS,
            message="Load complete",
            records_processed=500
        )
        
        output_path = os.path.join(temp_dir, "logs_parquet")
        logger.persist_logs(output_path, mode="overwrite", format="parquet")
        
        # Verify file was created
        assert os.path.exists(output_path)
        
        # Read back and verify
        df_read = logger.spark.read.parquet(output_path)
        assert df_read.count() == 1
        assert df_read.first()["message"] == "Load complete"
    
    def test_persist_logs_csv(self, logger, temp_dir):
        """Test persisting logs to CSV format."""
        logger.log_message(
            step=ETLLogger.STEP_COMPLETE,
            status=ETLLogger.STATUS_SUCCESS,
            message="ETL complete"
        )
        
        output_path = os.path.join(temp_dir, "logs_csv")
        logger.persist_logs(output_path, mode="overwrite", format="csv")
        
        assert os.path.exists(output_path)
        
        df_read = logger.spark.read.csv(output_path, header=True)
        assert df_read.count() == 1
    
    def test_get_summary_statistics_empty(self, logger):
        """Test summary statistics with no logs."""
        stats = logger.get_summary_statistics()
        
        assert stats["total_logs"] == 0
        assert stats["errors"] == 0
        assert stats["warnings"] == 0
        assert stats["success"] == 0
    
    def test_get_summary_statistics(self, logger):
        """Test summary statistics calculation."""
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Extract",
            records_processed=100,
            records_success=100
        )
        logger.log_message(
            step=ETLLogger.STEP_TRANSFORM,
            status=ETLLogger.STATUS_WARNING,
            message="Transform with warnings",
            records_processed=100,
            records_success=95,
            records_error=5
        )
        logger.log_message(
            step=ETLLogger.STEP_LOAD,
            status=ETLLogger.STATUS_ERROR,
            message="Load failed",
            records_processed=95,
            records_error=95
        )
        
        stats = logger.get_summary_statistics()
        
        assert stats["total_logs"] == 3
        assert stats["errors"] == 1
        assert stats["warnings"] == 1
        assert stats["success"] == 1
        assert stats["total_records_processed"] == 295
        assert stats["total_records_success"] == 195
        assert stats["total_records_error"] == 100
    
    def test_clear_logs(self, logger):
        """Test clearing log entries."""
        logger.log_message(
            step=ETLLogger.STEP_INIT,
            status=ETLLogger.STATUS_INFO,
            message="Init"
        )
        
        assert len(logger.log_entries) == 1
        
        logger.clear_logs()
        
        assert len(logger.log_entries) == 0


class TestGenerateETLRunID:
    """Test ETL run ID generation."""
    
    def test_generate_etl_run_id_default(self):
        """Test generating ETL run ID with default prefix."""
        run_id = generate_etl_run_id()
        
        assert run_id.startswith("ETL")
        assert len(run_id) > 3
    
    def test_generate_etl_run_id_custom_prefix(self):
        """Test generating ETL run ID with custom prefix."""
        run_id = generate_etl_run_id("CUSTOM")
        
        assert run_id.startswith("CUSTOM")
    
    def test_generate_etl_run_id_uniqueness(self):
        """Test that generated IDs are unique."""
        run_id1 = generate_etl_run_id()
        run_id2 = generate_etl_run_id()
        
        assert run_id1 != run_id2


class TestIntegration:
    """Integration tests for logger functionality."""
    
    def test_complete_logging_workflow(self, spark, temp_dir):
        """Test complete logging workflow."""
        # Initialize logger
        run_id = generate_etl_run_id("INTEG")
        logger = ETLLogger(run_id, spark)
        
        # Log various stages
        logger.log_message(
            step=ETLLogger.STEP_INIT,
            status=ETLLogger.STATUS_INFO,
            message="Starting ETL process"
        )
        
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Extracted data",
            records_processed=1000,
            records_success=1000
        )
        
        logger.log_message(
            step=ETLLogger.STEP_TRANSFORM,
            status=ETLLogger.STATUS_SUCCESS,
            message="Transformed data",
            records_processed=1000,
            records_success=980,
            records_error=20
        )
        
        logger.log_message(
            step=ETLLogger.STEP_LOAD,
            status=ETLLogger.STATUS_SUCCESS,
            message="Loaded data",
            records_processed=980,
            records_success=980
        )
        
        logger.log_message(
            step=ETLLogger.STEP_COMPLETE,
            status=ETLLogger.STATUS_SUCCESS,
            message="ETL process completed"
        )
        
        # Verify log entries
        assert len(logger.log_entries) == 5
        
        # Get statistics
        stats = logger.get_summary_statistics()
        assert stats["total_logs"] == 5
        assert stats["success"] == 5
        
        # Convert to DataFrame
        df = logger.get_logs_as_dataframe()
        assert df.count() == 5
        
        # Persist logs
        output_path = os.path.join(temp_dir, "integration_logs")
        logger.persist_logs(output_path, mode="overwrite")
        
        # Verify persistence
        df_read = spark.read.parquet(output_path)
        assert df_read.count() == 5
        
        # Verify all steps are present
        steps = [row["process_step"] for row in df_read.collect()]
        assert ETLLogger.STEP_INIT in steps
        assert ETLLogger.STEP_EXTRACT in steps
        assert ETLLogger.STEP_TRANSFORM in steps
        assert ETLLogger.STEP_LOAD in steps
        assert ETLLogger.STEP_COMPLETE in steps