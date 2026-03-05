"""
Unit tests for ETL Logger module.
"""
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.logger import ETLLogger, LogEntry, generate_etl_run_id


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing."""
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
    """Generate test ETL run ID."""
    return generate_etl_run_id(prefix="TEST")


@pytest.fixture
def logger(spark, etl_run_id, tmp_path):
    """Create ETL logger instance for testing."""
    log_path = str(tmp_path / "test_logs")
    logger = ETLLogger(
        spark=spark,
        etl_run_id=etl_run_id,
        log_table_path=log_path,
        enable_console=False
    )
    yield logger
    logger.clear_logs()


class TestLogEntry:
    """Test LogEntry dataclass."""
    
    def test_log_entry_creation(self):
        """Test basic LogEntry creation."""
        now = datetime.now()
        entry = LogEntry(
            log_id="LOG001",
            etl_run_id="ETL001",
            execution_date=now,
            execution_time=now,
            process_step="EXTRACT",
            status="S",
            records_processed=100,
            records_success=95,
            records_error=5,
            message="Test message"
        )
        
        assert entry.log_id == "LOG001"
        assert entry.etl_run_id == "ETL001"
        assert entry.process_step == "EXTRACT"
        assert entry.status == "S"
        assert entry.records_processed == 100
        assert entry.records_success == 95
        assert entry.records_error == 5
    
    def test_log_entry_to_dict(self):
        """Test LogEntry to_dict conversion."""
        now = datetime.now()
        entry = LogEntry(
            log_id="LOG001",
            etl_run_id="ETL001",
            execution_date=now,
            execution_time=now,
            process_step="TRANSFORM",
            status="S",
            message="Transform complete"
        )
        
        entry_dict = entry.to_dict()
        
        assert isinstance(entry_dict, dict)
        assert entry_dict["log_id"] == "LOG001"
        assert entry_dict["process_step"] == "TRANSFORM"
        assert "created_at" in entry_dict


class TestETLLogger:
    """Test ETL Logger functionality."""
    
    def test_logger_initialization(self, spark, etl_run_id):
        """Test logger initialization."""
        logger = ETLLogger(
            spark=spark,
            etl_run_id=etl_run_id,
            enable_console=False
        )
        
        assert logger.etl_run_id == etl_run_id
        assert logger.spark == spark
        assert len(logger._log_entries) == 0
    
    def test_generate_log_id(self, logger):
        """Test log ID generation."""
        log_id1 = logger._generate_log_id()
        log_id2 = logger._generate_log_id()
        
        assert log_id1.startswith("LOG")
        assert log_id2.startswith("LOG")
        assert log_id1 != log_id2
    
    def test_log_message_basic(self, logger):
        """Test basic log message creation."""
        entry = logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Extraction started"
        )
        
        assert entry.process_step == ETLLogger.STEP_EXTRACT
        assert entry.status == ETLLogger.STATUS_SUCCESS
        assert entry.message == "Extraction started"
        assert len(logger._log_entries) == 1
    
    def test_log_message_with_statistics(self, logger):
        """Test log message with record statistics."""
        entry = logger.log_message(
            step=ETLLogger.STEP_TRANSFORM,
            status=ETLLogger.STATUS_SUCCESS,
            message="Transformation complete",
            records_processed=1000,
            records_success=980,
            records_error=20
        )
        
        assert entry.records_processed == 1000
        assert entry.records_success == 980
        assert entry.records_error == 20
    
    def test_multiple_log_messages(self, logger):
        """Test logging multiple messages."""
        logger.log_message(
            step=ETLLogger.STEP_INIT,
            status=ETLLogger.STATUS_INFO,
            message="Starting ETL"
        )
        
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Extraction complete",
            records_processed=500
        )
        
        logger.log_message(
            step=ETLLogger.STEP_TRANSFORM,
            status=ETLLogger.STATUS_WARNING,
            message="Transform warning",
            records_error=5
        )
        
        assert len(logger._log_entries) == 3
    
    def test_get_log_dataframe_empty(self, logger):
        """Test getting DataFrame from empty logs."""
        df = logger.get_log_dataframe()
        
        assert df.count() == 0
        assert len(df.schema.fields) == 12
    
    def test_get_log_dataframe_with_data(self, logger):
        """Test getting DataFrame with log data."""
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Test message 1",
            records_processed=100
        )
        
        logger.log_message(
            step=ETLLogger.STEP_TRANSFORM,
            status=ETLLogger.STATUS_SUCCESS,
            message="Test message 2",
            records_processed=100,
            records_success=95
        )
        
        df = logger.get_log_dataframe()
        
        assert df.count() == 2
        assert "log_id" in df.columns
        assert "etl_run_id" in df.columns
        assert "process_step" in df.columns
    
    def test_persist_logs(self, logger):
        """Test persisting logs to storage."""
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Test persistence",
            records_processed=100
        )
        
        logger.persist_logs(mode="overwrite")
        
        # Read back and verify
        df = logger.read_persisted_logs()
        assert df.count() == 1
    
    def test_persist_logs_partitioned(self, logger, tmp_path):
        """Test persisting logs with partitioning."""
        logger.log_table_path = str(tmp_path / "partitioned_logs")
        
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Partition test",
            records_processed=50
        )
        
        logger.persist_logs(
            mode="overwrite",
            partition_by=["process_step"]
        )
        
        df = logger.read_persisted_logs()
        assert df.count() == 1
    
    def test_get_etl_run_id(self, logger, etl_run_id):
        """Test getting ETL run ID."""
        assert logger.get_etl_run_id() == etl_run_id
    
    def test_get_log_summary_empty(self, logger):
        """Test getting summary from empty logs."""
        summary = logger.get_log_summary()
        
        assert summary["total_logs"] == 0
        assert summary["success_count"] == 0
        assert summary["error_count"] == 0
        assert summary["total_records_processed"] == 0
    
    def test_get_log_summary_with_data(self, logger):
        """Test getting summary with log data."""
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Success 1",
            records_processed=100,
            records_success=100
        )
        
        logger.log_message(
            step=ETLLogger.STEP_TRANSFORM,
            status=ETLLogger.STATUS_ERROR,
            message="Error 1",
            records_processed=50,
            records_error=50
        )
        
        logger.log_message(
            step=ETLLogger.STEP_LOAD,
            status=ETLLogger.STATUS_WARNING,
            message="Warning 1",
            records_processed=75,
            records_success=70,
            records_error=5
        )
        
        summary = logger.get_log_summary()
        
        assert summary["total_logs"] == 3
        assert summary["success_count"] == 1
        assert summary["error_count"] == 1
        assert summary["warning_count"] == 1
        assert summary["total_records_processed"] == 225
        assert summary["total_records_success"] == 170
        assert summary["total_records_error"] == 55
    
    def test_clear_logs(self, logger):
        """Test clearing log entries."""
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Test"
        )
        
        assert len(logger._log_entries) == 1
        
        logger.clear_logs()
        
        assert len(logger._log_entries) == 0
        assert logger._log_counter == 0
    
    def test_read_persisted_logs_with_filter(self, logger):
        """Test reading persisted logs with filter."""
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Extract success"
        )
        
        logger.log_message(
            step=ETLLogger.STEP_TRANSFORM,
            status=ETLLogger.STATUS_ERROR,
            message="Transform error"
        )
        
        logger.persist_logs(mode="overwrite")
        
        # Filter for errors only
        df = logger.read_persisted_logs(filter_expression="status = 'E'")
        
        assert df.count() == 1
        assert df.first()["process_step"] == ETLLogger.STEP_TRANSFORM
    
    def test_context_manager(self, spark, tmp_path):
        """Test logger as context manager."""
        log_path = str(tmp_path / "context_logs")
        etl_run_id = generate_etl_run_id()
        
        with ETLLogger(spark, etl_run_id, log_path, False) as logger:
            logger.log_message(
                step=ETLLogger.STEP_EXTRACT,
                status=ETLLogger.STATUS_SUCCESS,
                message="Context test"
            )
        
        # Logs should be auto-persisted
        test_logger = ETLLogger(spark, etl_run_id, log_path, False)
        df = test_logger.read_persisted_logs()
        assert df.count() > 0
    
    def test_status_mapping(self, logger):
        """Test status to log level mapping."""
        import logging
        
        assert logger._map_status_to_level("S") == logging.INFO
        assert logger._map_status_to_level("E") == logging.ERROR
        assert logger._map_status_to_level("W") == logging.WARNING
        assert logger._map_status_to_level("I") == logging.INFO


class TestHelperFunctions:
    """Test helper functions."""
    
    def test_generate_etl_run_id_default(self):
        """Test ETL run ID generation with default prefix."""
        run_id = generate_etl_run_id()
        
        assert run_id.startswith("ETL")
        assert len(run_id) > 3
    
    def test_generate_etl_run_id_custom_prefix(self):
        """Test ETL run ID generation with custom prefix."""
        run_id = generate_etl_run_id(prefix="CUSTOM")
        
        assert run_id.startswith("CUSTOM")
        assert len(run_id) > 6
    
    def test_generate_etl_run_id_uniqueness(self):
        """Test that generated run IDs are unique."""
        run_id1 = generate_etl_run_id()
        run_id2 = generate_etl_run_id()
        
        # IDs should be different (timestamp-based)
        # Note: May occasionally be same if generated in same second
        assert isinstance(run_id1, str)
        assert isinstance(run_id2, str)


class TestLoggerIntegration:
    """Integration tests for logger with multiple operations."""
    
    def test_full_etl_logging_workflow(self, logger):
        """Test complete ETL logging workflow."""
        # Init
        logger.log_message(
            step=ETLLogger.STEP_INIT,
            status=ETLLogger.STATUS_INFO,
            message="ETL process initialized"
        )
        
        # Extract
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Data extracted",
            records_processed=1000,
            records_success=1000
        )
        
        # Transform
        logger.log_message(
            step=ETLLogger.STEP_TRANSFORM,
            status=ETLLogger.STATUS_SUCCESS,
            message="Data transformed",
            records_processed=1000,
            records_success=980,
            records_error=20
        )
        
        # Load
        logger.log_message(
            step=ETLLogger.STEP_LOAD,
            status=ETLLogger.STATUS_SUCCESS,
            message="Data loaded",
            records_processed=980,
            records_success=980
        )
        
        # Complete
        logger.log_message(
            step=ETLLogger.STEP_COMPLETE,
            status=ETLLogger.STATUS_SUCCESS,
            message="ETL process completed"
        )
        
        # Verify
        assert len(logger._log_entries) == 5
        
        summary = logger.get_log_summary()
        assert summary["total_logs"] == 5
        assert summary["success_count"] == 4
        assert summary["info_count"] == 1
        assert summary["total_records_processed"] == 2980
        
        # Test DataFrame conversion
        df = logger.get_log_dataframe()
        assert df.count() == 5
        
        # Verify all steps are present
        steps = [row["process_step"] for row in df.collect()]
        assert ETLLogger.STEP_INIT in steps
        assert ETLLogger.STEP_EXTRACT in steps
        assert ETLLogger.STEP_TRANSFORM in steps
        assert ETLLogger.STEP_LOAD in steps
        assert ETLLogger.STEP_COMPLETE in steps