"""
Unit tests for ETL Logger Interface and Implementation
"""

import pytest
from datetime import datetime
from src.logger_interface import (
    ETLStatus,
    ETLStep,
    ETLLogEntry,
    ETLLoggerInterface
)
from src.logger import ETLLogger


class TestETLStatus:
    """Test cases for ETLStatus enum."""

    def test_status_values(self):
        """Test that status enum has correct values."""
        assert ETLStatus.SUCCESS.value == 'S'
        assert ETLStatus.ERROR.value == 'E'
        assert ETLStatus.WARNING.value == 'W'
        assert ETLStatus.INFO.value == 'I'

    def test_status_members(self):
        """Test that all expected status members exist."""
        expected_members = {'SUCCESS', 'ERROR', 'WARNING', 'INFO'}
        actual_members = {member.name for member in ETLStatus}
        assert actual_members == expected_members


class TestETLStep:
    """Test cases for ETLStep enum."""

    def test_step_values(self):
        """Test that step enum has correct values."""
        assert ETLStep.INIT.value == 'INIT'
        assert ETLStep.EXTRACT.value == 'EXTRACT'
        assert ETLStep.TRANSFORM.value == 'TRANSFORM'
        assert ETLStep.LOAD.value == 'LOAD'
        assert ETLStep.VALIDATE.value == 'VALIDATE'
        assert ETLStep.COMPLETE.value == 'COMPLETE'
        assert ETLStep.ERROR.value == 'ERROR'

    def test_step_members(self):
        """Test that all expected step members exist."""
        expected_members = {
            'INIT', 'EXTRACT', 'TRANSFORM', 'LOAD',
            'VALIDATE', 'COMPLETE', 'ERROR'
        }
        actual_members = {member.name for member in ETLStep}
        assert actual_members == expected_members


class TestETLLogEntry:
    """Test cases for ETLLogEntry data class."""

    @pytest.fixture
    def sample_log_entry(self):
        """Create a sample log entry for testing."""
        now = datetime.now()
        return ETLLogEntry(
            log_id='LOG20231215120000',
            etl_run_id='ETL20231215120000',
            execution_date=now,
            execution_time=now,
            process_step=ETLStep.EXTRACT,
            status=ETLStatus.SUCCESS,
            records_processed=100,
            records_success=95,
            records_error=5,
            message='Extraction completed'
        )

    def test_log_entry_creation(self, sample_log_entry):
        """Test that log entry is created with correct attributes."""
        assert sample_log_entry.log_id == 'LOG20231215120000'
        assert sample_log_entry.etl_run_id == 'ETL20231215120000'
        assert sample_log_entry.process_step == ETLStep.EXTRACT
        assert sample_log_entry.status == ETLStatus.SUCCESS
        assert sample_log_entry.records_processed == 100
        assert sample_log_entry.records_success == 95
        assert sample_log_entry.records_error == 5

    def test_log_entry_to_dict(self, sample_log_entry):
        """Test conversion of log entry to dictionary."""
        log_dict = sample_log_entry.to_dict()
        
        assert isinstance(log_dict, dict)
        assert log_dict['log_id'] == 'LOG20231215120000'
        assert log_dict['etl_run_id'] == 'ETL20231215120000'
        assert log_dict['process_step'] == 'EXTRACT'
        assert log_dict['status'] == 'S'
        assert log_dict['records_processed'] == 100
        assert log_dict['records_success'] == 95
        assert log_dict['records_error'] == 5

    def test_log_entry_repr(self, sample_log_entry):
        """Test string representation of log entry."""
        repr_str = repr(sample_log_entry)
        assert 'ETLLogEntry' in repr_str
        assert 'LOG20231215120000' in repr_str
        assert 'EXTRACT' in repr_str
        assert 'SUCCESS' in repr_str


class TestETLLogger:
    """Test cases for ETLLogger implementation."""

    @pytest.fixture
    def etl_logger(self):
        """Create an ETL logger instance for testing."""
        return ETLLogger(etl_run_id='TEST_RUN_001')

    def test_logger_initialization(self, etl_logger):
        """Test that logger initializes correctly."""
        assert etl_logger.get_etl_run_id() == 'TEST_RUN_001'
        assert len(etl_logger.get_log_entries()) == 0

    def test_log_message_basic(self, etl_logger):
        """Test basic message logging."""
        etl_logger.log_message(
            step=ETLStep.INIT,
            status=ETLStatus.SUCCESS,
            message='ETL process initialized'
        )

        entries = etl_logger.get_log_entries()
        assert len(entries) == 1
        assert entries[0]['process_step'] == 'INIT'
        assert entries[0]['status'] == 'S'
        assert entries[0]['message'] == 'ETL process initialized'

    def test_log_message_with_counts(self, etl_logger):
        """Test logging with record counts."""
        etl_logger.log_message(
            step=ETLStep.EXTRACT,
            status=ETLStatus.SUCCESS,
            message='Data extracted',
            records_processed=100,
            records_success=95,
            records_error=5
        )

        entries = etl_logger.get_log_entries()
        assert len(entries) == 1
        assert entries[0]['records_processed'] == 100
        assert entries[0]['records_success'] == 95
        assert entries[0]['records_error'] == 5

    def test_multiple_log_messages(self, etl_logger):
        """Test logging multiple messages."""
        etl_logger.log_message(
            step=ETLStep.INIT,
            status=ETLStatus.SUCCESS,
            message='Initialized'
        )
        etl_logger.log_message(
            step=ETLStep.EXTRACT,
            status=ETLStatus.SUCCESS,
            message='Extracted'
        )
        etl_logger.log_message(
            step=ETLStep.TRANSFORM,
            status=ETLStatus.WARNING,
            message='Transformed with warnings'
        )

        entries = etl_logger.get_log_entries()
        assert len(entries) == 3
        assert entries[0]['process_step'] == 'INIT'
        assert entries[1]['process_step'] == 'EXTRACT'
        assert entries[2]['process_step'] == 'TRANSFORM'
        assert entries[2]['status'] == 'W'

    def test_log_error_status(self, etl_logger):
        """Test logging error status."""
        etl_logger.log_message(
            step=ETLStep.LOAD,
            status=ETLStatus.ERROR,
            message='Load failed: Database connection error',
            records_processed=100,
            records_success=0,
            records_error=100
        )

        entries = etl_logger.get_log_entries()
        assert len(entries) == 1
        assert entries[0]['status'] == 'E'
        assert 'failed' in entries[0]['message']

    def test_set_etl_run_id(self, etl_logger):
        """Test changing ETL run ID."""
        original_id = etl_logger.get_etl_run_id()
        new_id = 'TEST_RUN_002'
        
        etl_logger.set_etl_run_id(new_id)
        assert etl_logger.get_etl_run_id() == new_id
        assert etl_logger.get_etl_run_id() != original_id

    def test_clear_logs(self, etl_logger):
        """Test clearing log entries."""
        # Add some log entries
        for i in range(5):
            etl_logger.log_message(
                step=ETLStep.EXTRACT,
                status=ETLStatus.SUCCESS,
                message=f'Message {i}'
            )
        
        assert len(etl_logger.get_log_entries()) == 5
        
        etl_logger.clear_logs()
        assert len(etl_logger.get_log_entries()) == 0

    def test_get_statistics(self, etl_logger):
        """Test getting logging statistics."""
        # Log messages with different statuses
        etl_logger.log_message(
            step=ETLStep.EXTRACT,
            status=ETLStatus.SUCCESS,
            message='Success 1'
        )
        etl_logger.log_message(
            step=ETLStep.EXTRACT,
            status=ETLStatus.SUCCESS,
            message='Success 2'
        )
        etl_logger.log_message(
            step=ETLStep.TRANSFORM,
            status=ETLStatus.WARNING,
            message='Warning 1'
        )
        etl_logger.log_message(
            step=ETLStep.LOAD,
            status=ETLStatus.ERROR,
            message='Error 1'
        )

        stats = etl_logger.get_statistics()
        assert stats['total_entries'] == 4
        assert stats['status_counts']['success'] == 2
        assert stats['status_counts']['warning'] == 1
        assert stats['status_counts']['error'] == 1
        assert stats['status_counts']['info'] == 0
        assert stats['etl_run_id'] == 'TEST_RUN_001'

    def test_generate_unique_log_ids(self, etl_logger):
        """Test that each log entry gets a unique ID."""
        for i in range(10):
            etl_logger.log_message(
                step=ETLStep.EXTRACT,
                status=ETLStatus.SUCCESS,
                message=f'Message {i}'
            )

        entries = etl_logger.get_log_entries()
        log_ids = [entry['log_id'] for entry in entries]
        
        # All IDs should be unique
        assert len(log_ids) == len(set(log_ids))
        
        # All IDs should start with 'LOG'
        assert all(log_id.startswith('LOG') for log_id in log_ids)

    def test_logger_interface_compliance(self, etl_logger):
        """Test that ETLLogger implements the interface correctly."""
        assert isinstance(etl_logger, ETLLoggerInterface)
        
        # Verify all interface methods are implemented
        assert hasattr(etl_logger, 'log_message')
        assert hasattr(etl_logger, 'get_etl_run_id')
        assert hasattr(etl_logger, 'set_etl_run_id')
        assert hasattr(etl_logger, 'get_log_entries')
        assert hasattr(etl_logger, 'clear_logs')

    def test_timestamp_format_in_log_entries(self, etl_logger):
        """Test that timestamps are properly formatted."""
        etl_logger.log_message(
            step=ETLStep.EXTRACT,
            status=ETLStatus.SUCCESS,
            message='Test message'
        )

        entries = etl_logger.get_log_entries()
        entry = entries[0]
        
        # Check date format (YYYY-MM-DD)
        assert len(entry['execution_date']) == 10
        assert entry['execution_date'][4] == '-'
        assert entry['execution_date'][7] == '-'
        
        # Check time format (HH:MM:SS)
        assert len(entry['execution_time']) == 8
        assert entry['execution_time'][2] == ':'
        assert entry['execution_time'][5] == ':'


class TestETLLoggerIntegration:
    """Integration tests for ETL logger."""

    def test_complete_etl_workflow_logging(self):
        """Test logging throughout a complete ETL workflow."""
        logger = ETLLogger(etl_run_id='ETL_INTEGRATION_TEST')

        # Initialize
        logger.log_message(
            step=ETLStep.INIT,
            status=ETLStatus.SUCCESS,
            message='ETL process initialized'
        )

        # Extract
        logger.log_message(
            step=ETLStep.EXTRACT,
            status=ETLStatus.SUCCESS,
            message='Data extracted from source',
            records_processed=1000,
            records_success=1000,
            records_error=0
        )

        # Transform
        logger.log_message(
            step=ETLStep.TRANSFORM,
            status=ETLStatus.WARNING,
            message='Data transformed with some warnings',
            records_processed=1000,
            records_success=980,
            records_error=20
        )

        # Load
        logger.log_message(
            step=ETLStep.LOAD,
            status=ETLStatus.SUCCESS,
            message='Data loaded to target',
            records_processed=980,
            records_success=980,
            records_error=0
        )

        # Complete
        logger.log_message(
            step=ETLStep.COMPLETE,
            status=ETLStatus.SUCCESS,
            message='ETL process completed successfully'
        )

        # Verify workflow
        entries = logger.get_log_entries()
        assert len(entries) == 5

        steps = [entry['process_step'] for entry in entries]
        expected_steps = ['INIT', 'EXTRACT', 'TRANSFORM', 'LOAD', 'COMPLETE']
        assert steps == expected_steps

        # Verify statistics
        stats = logger.get_statistics()
        assert stats['total_entries'] == 5
        assert stats['status_counts']['success'] == 4
        assert stats['status_counts']['warning'] == 1


if __name__ == '__main__':
    pytest.main([__file__, '-v'])