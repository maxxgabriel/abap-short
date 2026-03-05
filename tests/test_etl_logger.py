"""
Unit tests for ETL Logger module
"""
import pytest
from datetime import datetime
from src.etl_logger import (
    ETLLogger,
    LogStatus,
    ProcessStep,
    LogEntry,
    create_logger
)


@pytest.fixture
def etl_logger():
    """Fixture to create ETL logger instance"""
    return ETLLogger(etl_run_id="TEST_RUN_001", log_level="DEBUG")


def test_logger_initialization(etl_logger):
    """Test logger initialization"""
    assert etl_logger.etl_run_id == "TEST_RUN_001"
    assert len(etl_logger.log_entries) == 0


def test_log_message_simple(etl_logger):
    """Test simple message logging"""
    etl_logger.log_message(
        step=ProcessStep.EXTRACT.value,
        status=LogStatus.SUCCESS.value,
        message="Test message"
    )
    
    assert len(etl_logger.log_entries) == 1
    log_entry = etl_logger.log_entries[0]
    
    assert log_entry.process_step == ProcessStep.EXTRACT.value
    assert log_entry.status == LogStatus.SUCCESS.value
    assert log_entry.message == "Test message"
    assert log_entry.records_processed == 0


def test_log_message_with_statistics(etl_logger):
    """Test message logging with statistics"""
    etl_logger.log_message(
        step=ProcessStep.TRANSFORM.value,
        status=LogStatus.SUCCESS.value,
        message="Transformation completed",
        records_processed=100,
        records_success=95,
        records_error=5
    )
    
    assert len(etl_logger.log_entries) == 1
    log_entry = etl_logger.log_entries[0]
    
    assert log_entry.records_processed == 100
    assert log_entry.records_success == 95
    assert log_entry.records_error == 5


def test_log_etl_message(etl_logger):
    """Test log_etl_message method (macro replacement)"""
    etl_logger.log_etl_message(
        step="EXTRACT",
        status="S",
        message="Extraction started"
    )
    
    assert len(etl_logger.log_entries) == 1
    log_entry = etl_logger.log_entries[0]
    assert log_entry.message == "Extraction started"


def test_log_etl_statistics(etl_logger):
    """Test log_etl_statistics method (macro replacement)"""
    etl_logger.log_etl_statistics(
        step="LOAD",
        status="S",
        records_processed=1000,
        records_success=950,
        records_error=50,
        message="Load completed"
    )
    
    assert len(etl_logger.log_entries) == 1
    log_entry = etl_logger.log_entries[0]
    
    assert log_entry.records_processed == 1000
    assert log_entry.records_success == 950
    assert log_entry.records_error == 50
    assert log_entry.message == "Load completed"


def test_get_etl_run_id(etl_logger):
    """Test getting ETL run ID"""
    assert etl_logger.get_etl_run_id() == "TEST_RUN_001"


def test_get_log_entries(etl_logger):
    """Test getting all log entries"""
    etl_logger.log_message("INIT", "S", "Init")
    etl_logger.log_message("EXTRACT", "S", "Extract")
    etl_logger.log_message("TRANSFORM", "S", "Transform")
    
    entries = etl_logger.get_log_entries()
    assert len(entries) == 3


def test_log_id_generation(etl_logger):
    """Test unique log ID generation"""
    etl_logger.log_message("TEST", "S", "Message 1")
    etl_logger.log_message("TEST", "S", "Message 2")
    
    log_ids = [entry.log_id for entry in etl_logger.log_entries]
    assert len(log_ids) == len(set(log_ids))  # All unique


def test_log_entry_timestamps(etl_logger):
    """Test log entry timestamps are set correctly"""
    etl_logger.log_message("TEST", "S", "Test message")
    
    log_entry = etl_logger.log_entries[0]
    assert log_entry.execution_date is not None
    assert log_entry.execution_time is not None
    assert log_entry.created_at is not None


def test_different_status_levels(etl_logger):
    """Test logging with different status levels"""
    etl_logger.log_message("TEST", LogStatus.INFO.value, "Info message")
    etl_logger.log_message("TEST", LogStatus.WARNING.value, "Warning message")
    etl_logger.log_message("TEST", LogStatus.ERROR.value, "Error message")
    etl_logger.log_message("TEST", LogStatus.SUCCESS.value, "Success message")
    
    assert len(etl_logger.log_entries) == 4
    
    statuses = [entry.status for entry in etl_logger.log_entries]
    assert LogStatus.INFO.value in statuses
    assert LogStatus.WARNING.value in statuses
    assert LogStatus.ERROR.value in statuses
    assert LogStatus.SUCCESS.value in statuses


def test_create_logger_factory():
    """Test logger creation via factory function"""
    logger = create_logger("FACTORY_TEST_001", log_level="WARNING")
    
    assert logger.etl_run_id == "FACTORY_TEST_001"
    assert isinstance(logger, ETLLogger)


def test_log_message_format(etl_logger):
    """Test log message formatting"""
    etl_logger.log_message(
        step="TRANSFORM",
        status="S",
        message="Processing data",
        records_processed=100,
        records_success=98,
        records_error=2
    )
    
    log_entry = etl_logger.log_entries[0]
    formatted = etl_logger._format_log_message(log_entry)
    
    assert "TRANSFORM" in formatted
    assert "Processing data" in formatted
    assert "Processed: 100" in formatted
    assert "Success: 98" in formatted
    assert "Error: 2" in formatted


def test_multiple_loggers_independence():
    """Test that multiple logger instances are independent"""
    logger1 = ETLLogger("RUN_001")
    logger2 = ETLLogger("RUN_002")
    
    logger1.log_message("TEST", "S", "Logger 1 message")
    logger2.log_message("TEST", "S", "Logger 2 message")
    
    assert len(logger1.log_entries) == 1
    assert len(logger2.log_entries) == 1
    assert logger1.log_entries[0].etl_run_id == "RUN_001"
    assert logger2.log_entries[0].etl_run_id == "RUN_002"


def test_log_entry_etl_run_id_consistency(etl_logger):
    """Test that all log entries have correct ETL run ID"""
    etl_logger.log_message("STEP1", "S", "Message 1")
    etl_logger.log_message("STEP2", "S", "Message 2")
    etl_logger.log_message("STEP3", "S", "Message 3")
    
    for entry in etl_logger.log_entries:
        assert entry.etl_run_id == "TEST_RUN_001"