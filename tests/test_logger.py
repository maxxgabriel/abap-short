"""
Unit Tests for ETL Logger Module
"""

import pytest
from datetime import datetime
from src.logger import (
    ETLLogger,
    LogStatus,
    ProcessStep,
    LogEntry,
    create_logger
)


class TestLogStatus:
    """Test LogStatus enum."""
    
    def test_status_values(self):
        """Test all status enum values."""
        assert LogStatus.SUCCESS.value == 'S'
        assert LogStatus.ERROR.value == 'E'
        assert LogStatus.WARNING.value == 'W'
        assert LogStatus.INFO.value == 'I'


class TestProcessStep:
    """Test ProcessStep enum."""
    
    def test_step_values(self):
        """Test all process step enum values."""
        assert ProcessStep.INIT.value == 'INIT'
        assert ProcessStep.EXTRACT.value == 'EXTRACT'
        assert ProcessStep.TRANSFORM.value == 'TRANSFORM'
        assert ProcessStep.LOAD.value == 'LOAD'
        assert ProcessStep.VALIDATE.value == 'VALIDATE'
        assert ProcessStep.COMPLETE.value == 'COMPLETE'
        assert ProcessStep.ERROR.value == 'ERROR'


class TestLogEntry:
    """Test LogEntry dataclass."""
    
    def test_log_entry_creation(self):
        """Test creating a log entry."""
        entry = LogEntry(
            log_id="LOG001",
            etl_run_id="ETL001",
            execution_date="2024-01-01",
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
        """Test log entry with default values."""
        entry = LogEntry(
            log_id="LOG001",
            etl_run_id="ETL001",
            execution_date="2024-01-01",
            execution_time="10:30:00",
            process_step="INIT",
            status="S"
        )
        
        assert entry.records_processed == 0
        assert entry.records_success == 0
        assert entry.records_error == 0
        assert entry.message == ""
        assert isinstance(entry.timestamp, datetime)


class TestETLLogger:
    """Test ETLLogger class."""
    
    def test_logger_initialization(self):
        """Test logger initialization."""
        logger = ETLLogger("ETL_TEST_001")
        
        assert logger.etl_run_id == "ETL_TEST_001"
        assert logger.logger is not None
    
    def test_get_etl_run_id(self):
        """Test getting ETL run ID."""
        logger = ETLLogger("ETL_TEST_002")
        assert logger.get_etl_run_id() == "ETL_TEST_002"
    
    def test_log_message(self):
        """Test logging a message."""
        logger = ETLLogger("ETL_TEST_003")
        
        # Should not raise any exceptions
        logger.log_message(
            step=ProcessStep.EXTRACT.value,
            status=LogStatus.SUCCESS.value,
            message="Test extraction"
        )
    
    def test_log_message_with_counts(self):
        """Test logging message with record counts."""
        logger = ETLLogger("ETL_TEST_004")
        
        logger.log_message(
            step=ProcessStep.TRANSFORM.value,
            status=LogStatus.SUCCESS.value,
            message="Transformation complete",
            records_processed=100,
            records_success=95,
            records_error=5
        )
    
    def test_log_error_message(self):
        """Test logging an error message."""
        logger = ETLLogger("ETL_TEST_005")
        
        logger.log_message(
            step=ProcessStep.LOAD.value,
            status=LogStatus.ERROR.value,
            message="Load failed"
        )
    
    def test_generate_log_id(self):
        """Test log ID generation."""
        logger = ETLLogger("ETL_TEST_006")
        
        log_id_1 = logger._generate_log_id()
        log_id_2 = logger._generate_log_id()
        
        assert log_id_1.startswith("LOG")
        assert log_id_2.startswith("LOG")
        assert len(log_id_1) == 17  # LOG + 14 digits
        # IDs should be different
        assert log_id_1 != log_id_2
    
    def test_format_log_message(self):
        """Test log message formatting."""
        logger = ETLLogger("ETL_TEST_007")
        
        entry = LogEntry(
            log_id="LOG001",
            etl_run_id="ETL001",
            execution_date="2024-01-01",
            execution_time="10:30:00",
            process_step="EXTRACT",
            status="S",
            records_processed=100,
            records_success=95,
            records_error=5,
            message="Extraction complete"
        )
        
        formatted = logger._format_log_message(entry)
        
        assert "EXTRACT" in formatted
        assert "Extraction complete" in formatted
        assert "100" in formatted
        assert "95" in formatted
        assert "5" in formatted


class TestCreateLogger:
    """Test logger factory function."""
    
    def test_create_logger_with_run_id(self):
        """Test creating logger with provided run ID."""
        logger = create_logger(etl_run_id="ETL_CUSTOM_001")
        
        assert logger.get_etl_run_id() == "ETL_CUSTOM_001"
    
    def test_create_logger_without_run_id(self):
        """Test creating logger without run ID (auto-generate)."""
        logger = create_logger()
        
        run_id = logger.get_etl_run_id()
        assert run_id.startswith("ETL")
        assert len(run_id) == 17  # ETL + 14 digits
    
    def test_create_logger_with_log_level(self):
        """Test creating logger with custom log level."""
        logger = create_logger(log_level="DEBUG")
        
        assert logger.logger.level == 10  # DEBUG level


class TestLoggerIntegration:
    """Integration tests for logger."""
    
    def test_complete_logging_workflow(self):
        """Test a complete logging workflow."""
        logger = create_logger("ETL_INTEGRATION_001")
        
        # Log initialization
        logger.log_message(
            step=ProcessStep.INIT.value,
            status=LogStatus.SUCCESS.value,
            message="Starting ETL process"
        )
        
        # Log extraction
        logger.log_message(
            step=ProcessStep.EXTRACT.value,
            status=LogStatus.SUCCESS.value,
            message="Extracted records",
            records_processed=1000,
            records_success=1000
        )
        
        # Log transformation
        logger.log_message(
            step=ProcessStep.TRANSFORM.value,
            status=LogStatus.SUCCESS.value,
            message="Transformed records",
            records_processed=1000,
            records_success=995,
            records_error=5
        )
        
        # Log load
        logger.log_message(
            step=ProcessStep.LOAD.value,
            status=LogStatus.SUCCESS.value,
            message="Loaded records",
            records_processed=995,
            records_success=995
        )
        
        # Log completion
        logger.log_message(
            step=ProcessStep.COMPLETE.value,
            status=LogStatus.SUCCESS.value,
            message="ETL process completed"
        )
        
        assert logger.get_etl_run_id() == "ETL_INTEGRATION_001"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])