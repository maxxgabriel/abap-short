"""
Unit tests for ETL Logger Interface.
Tests the logger interface implementation and enumerations.
"""

import pytest
from src.interfaces.etl_logger import (
    ETLLoggerInterface,
    LogStatus,
    ProcessStep
)


class MockETLLogger(ETLLoggerInterface):
    """Mock implementation of ETL logger for testing."""

    def __init__(self, etl_run_id: str):
        self.etl_run_id = etl_run_id
        self.logged_messages = []

    def log_message(
        self,
        step: ProcessStep,
        status: LogStatus,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ) -> None:
        """Log a message."""
        log_entry = {
            'step': step,
            'status': status,
            'message': message,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error
        }
        self.logged_messages.append(log_entry)

    def get_etl_run_id(self) -> str:
        """Get ETL run ID."""
        return self.etl_run_id


class TestLogStatus:
    """Test LogStatus enumeration."""

    def test_log_status_values(self):
        """Test log status enumeration values."""
        assert LogStatus.SUCCESS.value == 'S'
        assert LogStatus.ERROR.value == 'E'
        assert LogStatus.WARNING.value == 'W'
        assert LogStatus.INFO.value == 'I'

    def test_log_status_string_representation(self):
        """Test log status string representation."""
        assert str(LogStatus.SUCCESS) == 'S'
        assert str(LogStatus.ERROR) == 'E'


class TestProcessStep:
    """Test ProcessStep enumeration."""

    def test_process_step_values(self):
        """Test process step enumeration values."""
        assert ProcessStep.INIT.value == 'INIT'
        assert ProcessStep.EXTRACT.value == 'EXTRACT'
        assert ProcessStep.TRANSFORM.value == 'TRANSFORM'
        assert ProcessStep.LOAD.value == 'LOAD'
        assert ProcessStep.VALIDATE.value == 'VALIDATE'
        assert ProcessStep.COMPLETE.value == 'COMPLETE'
        assert ProcessStep.ERROR.value == 'ERROR'


class TestETLLoggerInterface:
    """Test ETL Logger Interface."""

    def test_logger_instantiation(self):
        """Test logger instantiation."""
        logger = MockETLLogger("TEST_RUN_001")
        assert logger.get_etl_run_id() == "TEST_RUN_001"

    def test_log_message_basic(self):
        """Test basic message logging."""
        logger = MockETLLogger("TEST_RUN_001")
        
        logger.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="Extraction completed"
        )
        
        assert len(logger.logged_messages) == 1
        assert logger.logged_messages[0]['step'] == ProcessStep.EXTRACT
        assert logger.logged_messages[0]['status'] == LogStatus.SUCCESS
        assert logger.logged_messages[0]['message'] == "Extraction completed"

    def test_log_message_with_statistics(self):
        """Test message logging with statistics."""
        logger = MockETLLogger("TEST_RUN_001")
        
        logger.log_message(
            step=ProcessStep.TRANSFORM,
            status=LogStatus.SUCCESS,
            message="Transformation completed",
            records_processed=100,
            records_success=95,
            records_error=5
        )
        
        log_entry = logger.logged_messages[0]
        assert log_entry['records_processed'] == 100
        assert log_entry['records_success'] == 95
        assert log_entry['records_error'] == 5

    def test_multiple_log_messages(self):
        """Test logging multiple messages."""
        logger = MockETLLogger("TEST_RUN_001")
        
        logger.log_message(
            step=ProcessStep.INIT,
            status=LogStatus.INFO,
            message="Starting ETL"
        )
        
        logger.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="Extraction done"
        )
        
        logger.log_message(
            step=ProcessStep.COMPLETE,
            status=LogStatus.SUCCESS,
            message="ETL complete"
        )
        
        assert len(logger.logged_messages) == 3

    def test_log_error_message(self):
        """Test logging error messages."""
        logger = MockETLLogger("TEST_RUN_001")
        
        logger.log_message(
            step=ProcessStep.ERROR,
            status=LogStatus.ERROR,
            message="ETL failed",
            records_error=10
        )
        
        log_entry = logger.logged_messages[0]
        assert log_entry['status'] == LogStatus.ERROR
        assert log_entry['step'] == ProcessStep.ERROR

    def test_get_etl_run_id(self):
        """Test getting ETL run ID."""
        logger = MockETLLogger("ETL_2024_001")
        assert logger.get_etl_run_id() == "ETL_2024_001"


class TestETLLoggerIntegration:
    """Integration tests for ETL Logger Interface."""

    def test_full_logging_flow(self):
        """Test complete logging flow."""
        logger = MockETLLogger("INT_TEST_001")
        
        # Initialize
        logger.log_message(
            step=ProcessStep.INIT,
            status=LogStatus.INFO,
            message="ETL initialized"
        )
        
        # Extract
        logger.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="Data extracted",
            records_processed=100,
            records_success=100
        )
        
        # Transform
        logger.log_message(
            step=ProcessStep.TRANSFORM,
            status=LogStatus.SUCCESS,
            message="Data transformed",
            records_processed=100,
            records_success=95,
            records_error=5
        )
        
        # Load
        logger.log_message(
            step=ProcessStep.LOAD,
            status=LogStatus.SUCCESS,
            message="Data loaded",
            records_processed=95,
            records_success=95
        )
        
        # Complete
        logger.log_message(
            step=ProcessStep.COMPLETE,
            status=LogStatus.SUCCESS,
            message="ETL complete"
        )
        
        assert len(logger.logged_messages) == 5
        assert logger.logged_messages[0]['step'] == ProcessStep.INIT
        assert logger.logged_messages[-1]['step'] == ProcessStep.COMPLETE


if __name__ == "__main__":
    pytest.main([__file__, "-v"])