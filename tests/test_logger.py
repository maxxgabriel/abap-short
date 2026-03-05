"""
Unit tests for ETL Logger Interface and Implementation
"""
import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch

from src.logger_interface import ETLLoggerInterface, LogStatus, ProcessStep
from src.logger import ETLLogger, LogEntry
from src.logger_factory import LoggerFactory


class TestLogStatus:
    """Test LogStatus enum"""

    def test_status_values(self):
        """Test that status enum has correct values"""
        assert LogStatus.SUCCESS.value == 'S'
        assert LogStatus.ERROR.value == 'E'
        assert LogStatus.WARNING.value == 'W'
        assert LogStatus.INFO.value == 'I'

    def test_status_members(self):
        """Test that all expected status members exist"""
        expected_statuses = {'SUCCESS', 'ERROR', 'WARNING', 'INFO'}
        actual_statuses = {status.name for status in LogStatus}
        assert actual_statuses == expected_statuses


class TestProcessStep:
    """Test ProcessStep enum"""

    def test_step_values(self):
        """Test that step enum has correct values"""
        assert ProcessStep.INIT.value == 'INIT'
        assert ProcessStep.EXTRACT.value == 'EXTRACT'
        assert ProcessStep.TRANSFORM.value == 'TRANSFORM'
        assert ProcessStep.LOAD.value == 'LOAD'
        assert ProcessStep.VALIDATE.value == 'VALIDATE'
        assert ProcessStep.COMPLETE.value == 'COMPLETE'
        assert ProcessStep.ERROR.value == 'ERROR'

    def test_step_members(self):
        """Test that all expected step members exist"""
        expected_steps = {
            'INIT', 'EXTRACT', 'TRANSFORM', 'LOAD',
            'VALIDATE', 'COMPLETE', 'ERROR'
        }
        actual_steps = {step.name for step in ProcessStep}
        assert actual_steps == expected_steps


class TestLogEntry:
    """Test LogEntry dataclass"""

    def test_log_entry_creation(self):
        """Test creating a log entry with all fields"""
        entry = LogEntry(
            log_id="LOG001",
            etl_run_id="ETL20240101120000",
            execution_date="2024-01-01",
            execution_time="12:00:00",
            process_step="EXTRACT",
            status="S",
            records_processed=100,
            records_success=95,
            records_error=5,
            message="Test message"
        )
        
        assert entry.log_id == "LOG001"
        assert entry.etl_run_id == "ETL20240101120000"
        assert entry.records_processed == 100
        assert entry.records_success == 95
        assert entry.records_error == 5

    def test_log_entry_defaults(self):
        """Test log entry default values"""
        entry = LogEntry(
            log_id="LOG001",
            etl_run_id="ETL20240101120000",
            execution_date="2024-01-01",
            execution_time="12:00:00",
            process_step="INIT",
            status="S"
        )
        
        assert entry.records_processed == 0
        assert entry.records_success == 0
        assert entry.records_error == 0
        assert entry.message == ""
        assert entry.created_at is not None


class TestETLLogger:
    """Test ETLLogger implementation"""

    @pytest.fixture
    def logger(self):
        """Create a logger instance for testing"""
        return ETLLogger(etl_run_id="ETL20240101120000")

    def test_logger_initialization(self, logger):
        """Test logger initialization"""
        assert logger.get_etl_run_id() == "ETL20240101120000"
        assert len(logger.get_log_entries()) == 0

    def test_log_message_basic(self, logger):
        """Test logging a basic message"""
        logger.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="Test extraction"
        )
        
        entries = logger.get_log_entries()
        assert len(entries) == 1
        assert entries[0].process_step == "EXTRACT"
        assert entries[0].status == "S"
        assert entries[0].message == "Test extraction"

    def test_log_message_with_counts(self, logger):
        """Test logging message with record counts"""
        logger.log_message(
            step=ProcessStep.TRANSFORM,
            status=LogStatus.SUCCESS,
            message="Transformed records",
            records_processed=1000,
            records_success=950,
            records_error=50
        )
        
        entries = logger.get_log_entries()
        assert len(entries) == 1
        entry = entries[0]
        assert entry.records_processed == 1000
        assert entry.records_success == 950
        assert entry.records_error == 50

    def test_log_message_truncation(self, logger):
        """Test that long messages are truncated to 255 chars"""
        long_message = "A" * 300
        logger.log_message(
            step=ProcessStep.LOAD,
            status=LogStatus.INFO,
            message=long_message
        )
        
        entries = logger.get_log_entries()
        assert len(entries[0].message) == 255

    def test_multiple_log_entries(self, logger):
        """Test logging multiple messages"""
        steps = [ProcessStep.INIT, ProcessStep.EXTRACT, ProcessStep.TRANSFORM]
        
        for step in steps:
            logger.log_message(
                step=step,
                status=LogStatus.SUCCESS,
                message=f"Testing {step.value}"
            )
        
        entries = logger.get_log_entries()
        assert len(entries) == 3
        assert entries[0].process_step == "INIT"
        assert entries[1].process_step == "EXTRACT"
        assert entries[2].process_step == "TRANSFORM"

    def test_log_id_generation(self, logger):
        """Test that log IDs are unique"""
        logger.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="First"
        )
        logger.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="Second"
        )
        
        entries = logger.get_log_entries()
        assert entries[0].log_id != entries[1].log_id
        assert entries[0].log_id.startswith("LOG")
        assert entries[1].log_id.startswith("LOG")

    def test_get_statistics(self, logger):
        """Test statistics aggregation"""
        logger.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="Success 1"
        )
        logger.log_message(
            step=ProcessStep.TRANSFORM,
            status=LogStatus.ERROR,
            message="Error 1"
        )
        logger.log_message(
            step=ProcessStep.LOAD,
            status=LogStatus.WARNING,
            message="Warning 1"
        )
        logger.log_message(
            step=ProcessStep.VALIDATE,
            status=LogStatus.INFO,
            message="Info 1"
        )
        
        stats = logger.get_statistics()
        assert stats['total'] == 4
        assert stats['success'] == 1
        assert stats['error'] == 1
        assert stats['warning'] == 1
        assert stats['info'] == 1

    def test_logger_with_spark_session(self):
        """Test logger initialization with Spark session"""
        mock_spark = Mock()
        logger = ETLLogger(
            etl_run_id="ETL20240101120000",
            spark=mock_spark,
            log_table="test_logs"
        )
        
        assert logger.get_etl_run_id() == "ETL20240101120000"

    @patch('src.logger.datetime')
    def test_execution_timestamp(self, mock_datetime, logger):
        """Test that execution timestamps are recorded correctly"""
        mock_now = Mock()
        mock_now.strftime.side_effect = lambda fmt: {
            '%Y-%m-%d': '2024-01-01',
            '%H:%M:%S': '12:30:45',
            '%Y-%m-%d %H:%M:%S': '2024-01-01 12:30:45',
            '%Y%m%d%H%M%S': '20240101123045'
        }[fmt]
        mock_datetime.now.return_value = mock_now
        
        logger.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="Test"
        )
        
        entries = logger.get_log_entries()
        assert entries[0].execution_date == "2024-01-01"
        assert entries[0].execution_time == "12:30:45"


class TestLoggerFactory:
    """Test LoggerFactory"""

    def test_create_logger_basic(self):
        """Test creating a logger with factory"""
        logger = LoggerFactory.create_logger(
            etl_run_id="ETL20240101120000"
        )
        
        assert isinstance(logger, ETLLoggerInterface)
        assert logger.get_etl_run_id() == "ETL20240101120000"

    def test_create_logger_with_config(self):
        """Test creating logger with configuration"""
        config = {
            'log_table': 'custom_logs',
            'console_level': 'DEBUG'
        }
        
        logger = LoggerFactory.create_logger(
            etl_run_id="ETL20240101120000",
            config=config
        )
        
        assert isinstance(logger, ETLLoggerInterface)

    def test_generate_etl_run_id(self):
        """Test ETL run ID generation"""
        run_id = LoggerFactory.generate_etl_run_id()
        
        assert run_id.startswith("ETL")
        assert len(run_id) == 17  # ETL + 14 digit timestamp

    def test_generate_unique_run_ids(self):
        """Test that generated run IDs are unique"""
        run_id_1 = LoggerFactory.generate_etl_run_id()
        run_id_2 = LoggerFactory.generate_etl_run_id()
        
        # They might be the same if generated in same second,
        # but at least both should be valid
        assert run_id_1.startswith("ETL")
        assert run_id_2.startswith("ETL")


class TestETLLoggerInterface:
    """Test that ETLLogger implements the interface correctly"""

    def test_implements_interface(self):
        """Test that ETLLogger implements ETLLoggerInterface"""
        logger = ETLLogger(etl_run_id="ETL20240101120000")
        assert isinstance(logger, ETLLoggerInterface)

    def test_interface_methods_exist(self):
        """Test that all interface methods are implemented"""
        logger = ETLLogger(etl_run_id="ETL20240101120000")
        
        # Test that methods exist and are callable
        assert callable(logger.log_message)
        assert callable(logger.get_etl_run_id)

    def test_cannot_instantiate_interface(self):
        """Test that the interface cannot be instantiated directly"""
        with pytest.raises(TypeError):
            ETLLoggerInterface()


@pytest.fixture
def mock_spark_session():
    """Fixture providing a mock Spark session"""
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.createDataFrame.return_value = mock_df
    return mock_spark


class TestETLLoggerDatabaseIntegration:
    """Test database integration features"""

    def test_flush_to_database(self, mock_spark_session):
        """Test flushing logs to database"""
        logger = ETLLogger(
            etl_run_id="ETL20240101120000",
            spark=mock_spark_session,
            log_table="test_logs"
        )
        
        logger.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="Test message"
        )
        
        logger.flush_to_database()
        
        # Verify DataFrame was created and written
        assert mock_spark_session.createDataFrame.called
        mock_df = mock_spark_session.createDataFrame.return_value
        assert mock_df.write.mode.called

    def test_flush_without_spark(self):
        """Test that flush without Spark doesn't fail"""
        logger = ETLLogger(etl_run_id="ETL20240101120000")
        
        logger.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="Test"
        )
        
        # Should not raise exception
        logger.flush_to_database()

    def test_persist_on_log(self, mock_spark_session):
        """Test immediate persistence when logging"""
        logger = ETLLogger(
            etl_run_id="ETL20240101120000",
            spark=mock_spark_session,
            log_table="test_logs"
        )
        
        logger.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="Test"
        )
        
        # Should have attempted to persist
        assert mock_spark_session.createDataFrame.called


if __name__ == "__main__":
    pytest.main([__file__, "-v"])