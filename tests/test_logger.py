"""
Unit tests for ETL Logger Interface and Implementation
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from unittest.mock import Mock, patch, MagicMock

from src.logger_interface import (
    ETLLoggerInterface,
    LogStatus,
    ProcessStep,
    LogEntry
)
from src.logger_impl import SparkETLLogger, ConsoleETLLogger


@pytest.fixture
def spark_session():
    """Create a Spark session for testing."""
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("TestETLLogger")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def etl_run_id():
    """Generate a test ETL run ID."""
    return f"ETL{datetime.now().strftime('%Y%m%d%H%M%S')}"


class TestLogStatus:
    """Tests for LogStatus enum."""
    
    def test_log_status_values(self):
        """Test that LogStatus enum has correct values."""
        assert LogStatus.SUCCESS.value == 'S'
        assert LogStatus.ERROR.value == 'E'
        assert LogStatus.WARNING.value == 'W'
        assert LogStatus.INFO.value == 'I'
    
    def test_log_status_members(self):
        """Test that all expected members are present."""
        expected_members = {'SUCCESS', 'ERROR', 'WARNING', 'INFO'}
        actual_members = {member.name for member in LogStatus}
        assert actual_members == expected_members


class TestProcessStep:
    """Tests for ProcessStep enum."""
    
    def test_process_step_values(self):
        """Test that ProcessStep enum has correct values."""
        assert ProcessStep.INIT.value == 'INIT'
        assert ProcessStep.EXTRACT.value == 'EXTRACT'
        assert ProcessStep.TRANSFORM.value == 'TRANSFORM'
        assert ProcessStep.LOAD.value == 'LOAD'
        assert ProcessStep.VALIDATE.value == 'VALIDATE'
        assert ProcessStep.COMPLETE.value == 'COMPLETE'
        assert ProcessStep.ERROR.value == 'ERROR'
    
    def test_process_step_members(self):
        """Test that all expected members are present."""
        expected_members = {
            'INIT', 'EXTRACT', 'TRANSFORM', 'LOAD',
            'VALIDATE', 'COMPLETE', 'ERROR'
        }
        actual_members = {member.name for member in ProcessStep}
        assert actual_members == expected_members


class TestLogEntry:
    """Tests for LogEntry data class."""
    
    def test_log_entry_creation(self):
        """Test creating a log entry."""
        now = datetime.now()
        log_entry = LogEntry(
            log_id="LOG001",
            etl_run_id="ETL001",
            execution_date=now,
            execution_time=now,
            process_step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            records_processed=100,
            records_success=95,
            records_error=5,
            message="Test message"
        )
        
        assert log_entry.log_id == "LOG001"
        assert log_entry.etl_run_id == "ETL001"
        assert log_entry.process_step == ProcessStep.EXTRACT
        assert log_entry.status == LogStatus.SUCCESS
        assert log_entry.records_processed == 100
        assert log_entry.records_success == 95
        assert log_entry.records_error == 5
        assert log_entry.message == "Test message"
    
    def test_log_entry_to_dict(self):
        """Test converting log entry to dictionary."""
        now = datetime.now()
        log_entry = LogEntry(
            log_id="LOG001",
            etl_run_id="ETL001",
            execution_date=now,
            execution_time=now,
            process_step=ProcessStep.TRANSFORM,
            status=LogStatus.INFO,
            records_processed=50,
            records_success=50,
            records_error=0,
            message="Transform complete"
        )
        
        result = log_entry.to_dict()
        
        assert isinstance(result, dict)
        assert result['log_id'] == "LOG001"
        assert result['etl_run_id'] == "ETL001"
        assert result['process_step'] == 'TRANSFORM'
        assert result['status'] == 'I'
        assert result['records_processed'] == 50
        assert result['records_success'] == 50
        assert result['records_error'] == 0
        assert result['message'] == "Transform complete"


class TestConsoleETLLogger:
    """Tests for ConsoleETLLogger implementation."""
    
    def test_console_logger_initialization(self, etl_run_id):
        """Test console logger initialization."""
        logger = ConsoleETLLogger(etl_run_id)
        
        assert logger.get_etl_run_id() == etl_run_id
        assert len(logger.get_log_entries()) == 0
    
    def test_console_logger_log_message(self, etl_run_id):
        """Test logging a message."""
        logger = ConsoleETLLogger(etl_run_id)
        
        logger.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="Extraction started",
            records_processed=100,
            records_success=100,
            records_error=0
        )
        
        entries = logger.get_log_entries()
        assert len(entries) == 1
        assert entries[0]['process_step'] == 'EXTRACT'
        assert entries[0]['status'] == 'S'
        assert entries[0]['message'] == "Extraction started"
    
    def test_console_logger_multiple_messages(self, etl_run_id):
        """Test logging multiple messages."""
        logger = ConsoleETLLogger(etl_run_id)
        
        logger.log_message(
            step=ProcessStep.INIT,
            status=LogStatus.INFO,
            message="Initialization"
        )
        logger.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="Extract complete",
            records_processed=50
        )
        logger.log_message(
            step=ProcessStep.TRANSFORM,
            status=LogStatus.WARNING,
            message="Some warnings",
            records_processed=50,
            records_success=48,
            records_error=2
        )
        
        entries = logger.get_log_entries()
        assert len(entries) == 3
        assert entries[0]['process_step'] == 'INIT'
        assert entries[1]['process_step'] == 'EXTRACT'
        assert entries[2]['process_step'] == 'TRANSFORM'
    
    def test_console_logger_flush(self, etl_run_id):
        """Test that flush doesn't raise errors."""
        logger = ConsoleETLLogger(etl_run_id)
        logger.flush()  # Should do nothing but not fail


class TestSparkETLLogger:
    """Tests for SparkETLLogger implementation."""
    
    def test_spark_logger_initialization(self, spark_session, etl_run_id):
        """Test Spark logger initialization."""
        logger = SparkETLLogger(
            etl_run_id=etl_run_id,
            spark=spark_session
        )
        
        assert logger.get_etl_run_id() == etl_run_id
        assert len(logger.get_log_entries()) == 0
    
    def test_spark_logger_without_spark(self, etl_run_id):
        """Test Spark logger can work without SparkSession."""
        logger = SparkETLLogger(etl_run_id=etl_run_id)
        
        logger.log_message(
            step=ProcessStep.INIT,
            status=LogStatus.INFO,
            message="Test message"
        )
        
        entries = logger.get_log_entries()
        assert len(entries) == 1
    
    def test_spark_logger_log_message(self, spark_session, etl_run_id):
        """Test logging with Spark logger."""
        logger = SparkETLLogger(
            etl_run_id=etl_run_id,
            spark=spark_session
        )
        
        logger.log_message(
            step=ProcessStep.TRANSFORM,
            status=LogStatus.SUCCESS,
            message="Transform completed",
            records_processed=200,
            records_success=195,
            records_error=5
        )
        
        entries = logger.get_log_entries()
        assert len(entries) == 1
        assert entries[0]['records_processed'] == 200
        assert entries[0]['records_success'] == 195
        assert entries[0]['records_error'] == 5
    
    def test_spark_logger_error_with_exception(self, spark_session, etl_run_id):
        """Test logging error with exception."""
        logger = SparkETLLogger(
            etl_run_id=etl_run_id,
            spark=spark_session
        )
        
        test_exception = ValueError("Test error")
        
        logger.log_message(
            step=ProcessStep.LOAD,
            status=LogStatus.ERROR,
            message="Load failed",
            exception=test_exception
        )
        
        entries = logger.get_log_entries()
        assert len(entries) == 1
        assert entries[0]['status'] == 'E'
        assert entries[0]['message'] == "Load failed"
    
    def test_spark_logger_generate_unique_ids(self, spark_session, etl_run_id):
        """Test that log IDs are unique."""
        logger = SparkETLLogger(
            etl_run_id=etl_run_id,
            spark=spark_session
        )
        
        logger.log_message(ProcessStep.INIT, LogStatus.INFO, "Message 1")
        logger.log_message(ProcessStep.EXTRACT, LogStatus.INFO, "Message 2")
        logger.log_message(ProcessStep.TRANSFORM, LogStatus.INFO, "Message 3")
        
        entries = logger.get_log_entries()
        log_ids = [entry['log_id'] for entry in entries]
        
        # All log IDs should be unique
        assert len(log_ids) == len(set(log_ids))
    
    @patch('src.logger_impl.SparkSession')
    def test_spark_logger_flush_to_delta(self, mock_spark_class, etl_run_id, tmp_path):
        """Test flushing logs to Delta table."""
        # Setup mock
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.createDataFrame.return_value = mock_df
        
        log_path = str(tmp_path / "logs")
        
        logger = SparkETLLogger(
            etl_run_id=etl_run_id,
            spark=mock_spark,
            log_table_path=log_path
        )
        
        # Log some messages
        logger.log_message(ProcessStep.EXTRACT, LogStatus.SUCCESS, "Test")
        logger.log_message(ProcessStep.TRANSFORM, LogStatus.SUCCESS, "Test 2")
        
        # Flush
        logger.flush()
        
        # Verify DataFrame was created and written
        mock_spark.createDataFrame.assert_called_once()
        mock_df.write.format.assert_called_once_with("delta")
    
    def test_spark_logger_flush_without_spark(self, etl_run_id):
        """Test flush without Spark session doesn't fail."""
        logger = SparkETLLogger(etl_run_id=etl_run_id)
        logger.log_message(ProcessStep.INIT, LogStatus.INFO, "Test")
        logger.flush()  # Should not raise error


class TestETLLoggerInterface:
    """Tests for ETL Logger Interface compliance."""
    
    def test_console_logger_implements_interface(self, etl_run_id):
        """Test that ConsoleETLLogger implements the interface."""
        logger = ConsoleETLLogger(etl_run_id)
        assert isinstance(logger, ETLLoggerInterface)
    
    def test_spark_logger_implements_interface(self, spark_session, etl_run_id):
        """Test that SparkETLLogger implements the interface."""
        logger = SparkETLLogger(etl_run_id, spark_session)
        assert isinstance(logger, ETLLoggerInterface)
    
    def test_interface_methods_required(self):
        """Test that interface defines required abstract methods."""
        abstract_methods = ETLLoggerInterface.__abstractmethods__
        
        expected_methods = {
            '__init__',
            'log_message',
            'get_etl_run_id',
            'get_log_entries',
            'flush'
        }
        
        assert abstract_methods == expected_methods
    
    def test_cannot_instantiate_interface_directly(self):
        """Test that interface cannot be instantiated directly."""
        with pytest.raises(TypeError):
            ETLLoggerInterface("test_run_id")


class TestIntegration:
    """Integration tests for logger implementations."""
    
    def test_full_etl_logging_workflow(self, spark_session, etl_run_id):
        """Test a complete ETL logging workflow."""
        logger = SparkETLLogger(
            etl_run_id=etl_run_id,
            spark=spark_session
        )
        
        # Initialization
        logger.log_message(
            step=ProcessStep.INIT,
            status=LogStatus.INFO,
            message="ETL process initialized"
        )
        
        # Extract phase
        logger.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="Data extraction completed",
            records_processed=1000,
            records_success=1000,
            records_error=0
        )
        
        # Transform phase
        logger.log_message(
            step=ProcessStep.TRANSFORM,
            status=LogStatus.WARNING,
            message="Data transformation completed with warnings",
            records_processed=1000,
            records_success=980,
            records_error=20
        )
        
        # Load phase
        logger.log_message(
            step=ProcessStep.LOAD,
            status=LogStatus.SUCCESS,
            message="Data load completed",
            records_processed=980,
            records_success=980,
            records_error=0
        )
        
        # Complete
        logger.log_message(
            step=ProcessStep.COMPLETE,
            status=LogStatus.SUCCESS,
            message="ETL process completed successfully"
        )
        
        # Verify all entries
        entries = logger.get_log_entries()
        assert len(entries) == 5
        assert entries[0]['process_step'] == 'INIT'
        assert entries[1]['process_step'] == 'EXTRACT'
        assert entries[2]['process_step'] == 'TRANSFORM'
        assert entries[3]['process_step'] == 'LOAD'
        assert entries[4]['process_step'] == 'COMPLETE'
        
        # Verify ETL run ID is consistent
        assert all(
            entry['etl_run_id'] == etl_run_id
            for entry in entries
        )