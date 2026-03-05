"""
Unit tests for ETL Logger module.
"""
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from src.logger import ETLLogger, LogEntry, create_logger
from src.utils import generate_etl_run_id


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return (SparkSession.builder
            .appName("ETLLoggerTest")
            .master("local[2]")
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
            .getOrCreate())


@pytest.fixture
def etl_run_id():
    """Generate a test ETL run ID."""
    return generate_etl_run_id()


@pytest.fixture
def logger(spark, etl_run_id, tmp_path):
    """Create a logger instance for testing."""
    log_path = str(tmp_path / "test_logs")
    return ETLLogger(etl_run_id, spark, log_path)


class TestLogEntry:
    """Tests for LogEntry dataclass."""

    def test_log_entry_creation(self):
        """Test creating a log entry with all fields."""
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
        assert entry.status == "S"
        assert entry.records_processed == 100
        assert entry.created_at is not None
        assert entry.created_by == "etl_system"

    def test_log_entry_defaults(self):
        """Test log entry with default values."""
        entry = LogEntry(
            log_id="LOG002",
            etl_run_id="ETL002",
            execution_date="2024-01-15",
            execution_time="10:30:00",
            process_step="TRANSFORM",
            status="S"
        )

        assert entry.records_processed == 0
        assert entry.records_success == 0
        assert entry.records_error == 0
        assert entry.message == ""
        assert entry.created_at is not None


class TestETLLogger:
    """Tests for ETLLogger class."""

    def test_logger_initialization(self, logger, etl_run_id):
        """Test logger initialization."""
        assert logger.etl_run_id == etl_run_id
        assert logger.spark is not None
        assert len(logger.get_log_entries()) == 1  # Init log

    def test_log_message(self, logger):
        """Test logging a message."""
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Test extraction",
            records_processed=100,
            records_success=100
        )

        entries = logger.get_log_entries()
        assert len(entries) >= 2  # Init + extract log
        
        # Find the extract log entry
        extract_log = next(e for e in entries if e.process_step == ETLLogger.STEP_EXTRACT)
        assert extract_log.status == ETLLogger.STATUS_SUCCESS
        assert extract_log.records_processed == 100

    def test_multiple_log_messages(self, logger):
        """Test logging multiple messages."""
        steps = [
            ETLLogger.STEP_EXTRACT,
            ETLLogger.STEP_TRANSFORM,
            ETLLogger.STEP_LOAD
        ]

        for step in steps:
            logger.log_message(
                step=step,
                status=ETLLogger.STATUS_SUCCESS,
                message=f"Processing {step}"
            )

        entries = logger.get_log_entries()
        assert len(entries) >= len(steps) + 1  # Init + all steps

        # Verify all steps are logged
        logged_steps = [e.process_step for e in entries]
        for step in steps:
            assert step in logged_steps

    def test_log_with_errors(self, logger):
        """Test logging error messages."""
        logger.log_message(
            step=ETLLogger.STEP_LOAD,
            status=ETLLogger.STATUS_ERROR,
            message="Load failed",
            records_processed=100,
            records_success=90,
            records_error=10
        )

        entries = logger.get_log_entries()
        error_log = next(e for e in entries 
                        if e.process_step == ETLLogger.STEP_LOAD 
                        and e.status == ETLLogger.STATUS_ERROR)
        
        assert error_log.records_error == 10
        assert "failed" in error_log.message.lower()

    def test_to_dataframe(self, logger):
        """Test converting logs to DataFrame."""
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Test",
            records_processed=50
        )

        df = logger.to_dataframe()
        assert df is not None
        assert df.count() >= 2  # Init + extract log
        assert "log_id" in df.columns
        assert "etl_run_id" in df.columns
        assert "process_step" in df.columns

    def test_to_dataframe_empty(self, spark, etl_run_id):
        """Test DataFrame creation with no logs."""
        logger = ETLLogger(etl_run_id, spark, None)
        logger._log_entries = []  # Clear init log
        
        df = logger.to_dataframe()
        assert df is not None
        assert df.count() == 0

    def test_persist_logs(self, logger, tmp_path):
        """Test persisting logs to storage."""
        # Log some messages
        for i in range(5):
            logger.log_message(
                step=ETLLogger.STEP_EXTRACT,
                status=ETLLogger.STATUS_SUCCESS,
                message=f"Test message {i}",
                records_processed=10 * (i + 1)
            )

        # Persist logs
        success = logger.persist_logs()
        assert success is True

        # Verify logs were written
        df = logger.spark.read.format("delta").load(logger.log_table_path)
        assert df.count() >= 5

    def test_persist_logs_no_path(self, spark, etl_run_id):
        """Test persist logs when no path is configured."""
        logger = ETLLogger(etl_run_id, spark, None)
        success = logger.persist_logs()
        assert success is False

    def test_get_summary_statistics(self, logger):
        """Test summary statistics calculation."""
        # Log various messages
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
            message="Transform warning",
            records_processed=100,
            records_success=95,
            records_error=5
        )
        logger.log_message(
            step=ETLLogger.STEP_LOAD,
            status=ETLLogger.STATUS_ERROR,
            message="Load error",
            records_processed=95,
            records_success=90,
            records_error=5
        )

        stats = logger.get_summary_statistics()
        
        assert stats["total_logs"] >= 4  # Init + 3 logs
        assert stats["success_count"] >= 1
        assert stats["error_count"] >= 1
        assert stats["warning_count"] >= 1
        assert stats["total_records_processed"] == 295
        assert stats["total_records_success"] == 285
        assert stats["total_records_error"] == 10

    def test_unique_log_ids(self, logger):
        """Test that log IDs are unique."""
        for i in range(10):
            logger.log_message(
                step=ETLLogger.STEP_EXTRACT,
                status=ETLLogger.STATUS_SUCCESS,
                message=f"Message {i}"
            )

        entries = logger.get_log_entries()
        log_ids = [e.log_id for e in entries]
        
        # All log IDs should be unique
        assert len(log_ids) == len(set(log_ids))

    def test_get_etl_run_id(self, logger, etl_run_id):
        """Test getting ETL run ID."""
        assert logger.get_etl_run_id() == etl_run_id

    def test_close_logger(self, logger):
        """Test closing the logger."""
        initial_count = len(logger.get_log_entries())
        logger.close()
        
        # Should have one more log entry for close
        assert len(logger.get_log_entries()) == initial_count + 1


class TestCreateLogger:
    """Tests for logger factory function."""

    def test_create_logger(self, spark, etl_run_id, tmp_path):
        """Test creating logger with factory function."""
        config = {
            'log_table_path': str(tmp_path / "test_logs")
        }
        
        logger = create_logger(etl_run_id, spark, config)
        
        assert logger is not None
        assert logger.etl_run_id == etl_run_id
        assert logger.log_table_path == config['log_table_path']

    def test_create_logger_no_path(self, spark, etl_run_id):
        """Test creating logger without log path."""
        config = {}
        logger = create_logger(etl_run_id, spark, config)
        
        assert logger is not None
        assert logger.log_table_path is None


class TestLoggerConstants:
    """Tests for logger constants."""

    def test_status_constants(self):
        """Test status constants are defined."""
        assert ETLLogger.STATUS_SUCCESS == 'S'
        assert ETLLogger.STATUS_ERROR == 'E'
        assert ETLLogger.STATUS_WARNING == 'W'
        assert ETLLogger.STATUS_INFO == 'I'

    def test_step_constants(self):
        """Test step constants are defined."""
        assert ETLLogger.STEP_INIT == 'INIT'
        assert ETLLogger.STEP_EXTRACT == 'EXTRACT'
        assert ETLLogger.STEP_TRANSFORM == 'TRANSFORM'
        assert ETLLogger.STEP_LOAD == 'LOAD'
        assert ETLLogger.STEP_VALIDATE == 'VALIDATE'
        assert ETLLogger.STEP_COMPLETE == 'COMPLETE'
        assert ETLLogger.STEP_ERROR == 'ERROR'


class TestLoggerSchema:
    """Tests for log schema."""

    def test_log_schema_structure(self):
        """Test log schema has required fields."""
        schema = ETLLogger.LOG_SCHEMA
        field_names = [field.name for field in schema.fields]
        
        required_fields = [
            "log_id", "etl_run_id", "execution_date", 
            "execution_time", "process_step", "status",
            "records_processed", "records_success", "records_error",
            "message", "created_at", "created_by"
        ]
        
        for field in required_fields:
            assert field in field_names

    def test_log_schema_types(self):
        """Test log schema field types."""
        from pyspark.sql.types import StringType, IntegerType
        
        schema = ETLLogger.LOG_SCHEMA
        field_types = {field.name: type(field.dataType) for field in schema.fields}
        
        assert field_types["log_id"] == StringType
        assert field_types["records_processed"] == IntegerType
        assert field_types["records_success"] == IntegerType
        assert field_types["records_error"] == IntegerType