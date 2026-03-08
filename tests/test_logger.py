"""
Unit tests for ETL Logger component
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
from src.logger import ETLLogger, LogEntry


@pytest.fixture
def spark_session():
    """Create a Spark session for testing"""
    spark = (SparkSession.builder
             .appName("test_etl_logger")
             .master("local[2]")
             .getOrCreate())
    yield spark
    spark.stop()


@pytest.fixture
def etl_logger(spark_session):
    """Create an ETL Logger instance for testing"""
    return ETLLogger(etl_run_id="ETL20240101120000", spark=spark_session)


class TestLogEntry:
    """Tests for LogEntry dataclass"""
    
    def test_log_entry_creation(self):
        """Test creation of LogEntry"""
        entry = LogEntry(
            log_id="LOG20240101120000",
            etl_run_id="ETL20240101120000",
            execution_date="2024-01-01",
            execution_time="12:00:00",
            process_step="EXTRACT",
            status="S",
            records_processed=100,
            records_success=95,
            records_error=5,
            message="Test message",
            duration_ms=1500
        )
        
        assert entry.log_id == "LOG20240101120000"
        assert entry.etl_run_id == "ETL20240101120000"
        assert entry.process_step == "EXTRACT"
        assert entry.status == "S"
        assert entry.records_processed == 100
        assert entry.duration_ms == 1500
    
    def test_log_entry_to_dict(self):
        """Test conversion of LogEntry to dictionary"""
        entry = LogEntry(
            log_id="LOG20240101120000",
            etl_run_id="ETL20240101120000",
            execution_date="2024-01-01",
            execution_time="12:00:00",
            process_step="EXTRACT",
            status="S",
            message="Test"
        )
        
        entry_dict = entry.to_dict()
        
        assert isinstance(entry_dict, dict)
        assert entry_dict['log_id'] == "LOG20240101120000"
        assert entry_dict['process_step'] == "EXTRACT"


class TestETLLogger:
    """Tests for ETLLogger class"""
    
    def test_logger_initialization(self, etl_logger):
        """Test logger initialization"""
        assert etl_logger.etl_run_id == "ETL20240101120000"
        assert etl_logger.spark is not None
        assert len(etl_logger.log_entries) == 0
    
    def test_generate_log_id(self):
        """Test log ID generation"""
        log_id = ETLLogger.generate_log_id()
        
        assert log_id.startswith("LOG")
        assert len(log_id) == 17  # LOG + 14 digit timestamp
        assert log_id[3:].isdigit()
    
    def test_generate_etl_run_id(self):
        """Test ETL run ID generation"""
        run_id = ETLLogger.generate_etl_run_id()
        
        assert run_id.startswith("ETL")
        assert len(run_id) == 17  # ETL + 14 digit timestamp
        assert run_id[3:].isdigit()
    
    def test_unique_log_ids(self):
        """Test that generated log IDs are unique"""
        log_ids = [ETLLogger.generate_log_id() for _ in range(10)]
        
        assert len(set(log_ids)) == len(log_ids)
    
    def test_log_message_basic(self, etl_logger):
        """Test basic message logging"""
        entry = etl_logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Test extraction",
            records_processed=100,
            records_success=100,
            records_error=0
        )
        
        assert isinstance(entry, LogEntry)
        assert entry.process_step == ETLLogger.STEP_EXTRACT
        assert entry.status == ETLLogger.STATUS_SUCCESS
        assert entry.records_processed == 100
        assert entry.message == "Test extraction"
        assert len(etl_logger.log_entries) == 1
    
    def test_log_message_with_duration(self, etl_logger):
        """Test message logging with duration tracking"""
        # Start step
        etl_logger.start_step(ETLLogger.STEP_TRANSFORM)
        
        # Simulate some work
        import time
        time.sleep(0.1)
        
        # Log message with duration
        entry = etl_logger.log_message(
            step=ETLLogger.STEP_TRANSFORM,
            status=ETLLogger.STATUS_SUCCESS,
            message="Transformation complete",
            include_duration=True
        )
        
        assert entry.duration_ms is not None
        assert entry.duration_ms > 0
    
    def test_step_duration_tracking(self, etl_logger):
        """Test step duration tracking"""
        step = ETLLogger.STEP_LOAD
        
        # Before starting step
        assert etl_logger.get_step_duration_ms(step) is None
        
        # Start step
        etl_logger.start_step(step)
        
        # Small delay
        import time
        time.sleep(0.05)
        
        # Get duration
        duration = etl_logger.get_step_duration_ms(step)
        
        assert duration is not None
        assert duration > 0
    
    def test_multiple_log_entries(self, etl_logger):
        """Test logging multiple entries"""
        steps = [
            ETLLogger.STEP_INIT,
            ETLLogger.STEP_EXTRACT,
            ETLLogger.STEP_TRANSFORM,
            ETLLogger.STEP_LOAD
        ]
        
        for step in steps:
            etl_logger.log_message(
                step=step,
                status=ETLLogger.STATUS_SUCCESS,
                message=f"{step} completed"
            )
        
        assert len(etl_logger.log_entries) == len(steps)
        
        # Verify order is maintained
        for i, step in enumerate(steps):
            assert etl_logger.log_entries[i].process_step == step
    
    def test_get_etl_run_id(self, etl_logger):
        """Test getting ETL run ID"""
        run_id = etl_logger.get_etl_run_id()
        
        assert run_id == "ETL20240101120000"
    
    def test_get_log_entries(self, etl_logger):
        """Test getting all log entries"""
        # Add some log entries
        for i in range(3):
            etl_logger.log_message(
                step=ETLLogger.STEP_EXTRACT,
                status=ETLLogger.STATUS_SUCCESS,
                message=f"Message {i}"
            )
        
        entries = etl_logger.get_log_entries()
        
        assert len(entries) == 3
        assert all(isinstance(e, LogEntry) for e in entries)
    
    def test_get_statistics(self, etl_logger):
        """Test getting aggregated statistics"""
        # Log some entries with different metrics
        etl_logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Extracted",
            records_processed=100,
            records_success=100,
            records_error=0
        )
        
        etl_logger.log_message(
            step=ETLLogger.STEP_TRANSFORM,
            status=ETLLogger.STATUS_WARNING,
            message="Transformed with warnings",
            records_processed=100,
            records_success=95,
            records_error=5
        )
        
        etl_logger.log_message(
            step=ETLLogger.STEP_LOAD,
            status=ETLLogger.STATUS_ERROR,
            message="Load failed",
            records_processed=50,
            records_success=40,
            records_error=10
        )
        
        stats = etl_logger.get_statistics()
        
        assert stats['etl_run_id'] == "ETL20240101120000"
        assert stats['total_records_processed'] == 250
        assert stats['total_records_success'] == 235
        assert stats['total_records_error'] == 15
        assert stats['error_count'] == 1
        assert stats['warning_count'] == 1
        assert stats['total_log_entries'] == 3
    
    def test_save_logs_to_dataframe(self, etl_logger):
        """Test saving logs to Spark DataFrame"""
        # Add some log entries
        etl_logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Test",
            records_processed=100
        )
        
        etl_logger.log_message(
            step=ETLLogger.STEP_TRANSFORM,
            status=ETLLogger.STATUS_SUCCESS,
            message="Test 2",
            records_processed=100
        )
        
        # Save to DataFrame
        df = etl_logger.save_logs_to_dataframe()
        
        assert df.count() == 2
        assert 'log_id' in df.columns
        assert 'etl_run_id' in df.columns
        assert 'process_step' in df.columns
        
        # Verify data
        rows = df.collect()
        assert rows[0]['process_step'] == ETLLogger.STEP_EXTRACT
        assert rows[1]['process_step'] == ETLLogger.STEP_TRANSFORM
    
    def test_save_logs_without_spark(self):
        """Test that saving logs without Spark raises error"""
        logger = ETLLogger(etl_run_id="TEST123", spark=None)
        
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Test"
        )
        
        with pytest.raises(ValueError, match="SparkSession required"):
            logger.save_logs_to_dataframe()
    
    def test_log_status_types(self, etl_logger):
        """Test logging different status types"""
        statuses = [
            (ETLLogger.STATUS_SUCCESS, "Success message"),
            (ETLLogger.STATUS_ERROR, "Error message"),
            (ETLLogger.STATUS_WARNING, "Warning message"),
            (ETLLogger.STATUS_INFO, "Info message")
        ]
        
        for status, message in statuses:
            etl_logger.log_message(
                step=ETLLogger.STEP_INIT,
                status=status,
                message=message
            )
        
        entries = etl_logger.get_log_entries()
        assert len(entries) == 4
        
        for i, (status, _) in enumerate(statuses):
            assert entries[i].status == status
    
    def test_log_entry_timestamp_format(self, etl_logger):
        """Test that log entries have correct timestamp format"""
        entry = etl_logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Test"
        )
        
        # Verify date format (YYYY-MM-DD)
        datetime.strptime(entry.execution_date, '%Y-%m-%d')
        
        # Verify time format (HH:MM:SS)
        datetime.strptime(entry.execution_time, '%H:%M:%S')
    
    def test_display_summary(self, etl_logger, capsys):
        """Test display summary output"""
        # Add some log entries
        etl_logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Test",
            records_processed=100,
            records_success=100
        )
        
        # Display summary
        etl_logger.display_summary()
        
        # Capture output
        captured = capsys.readouterr()
        
        assert "ETL Process Summary" in captured.out
        assert "ETL20240101120000" in captured.out
        assert "Total Records:" in captured.out
        assert "100" in captured.out


class TestLoggerIntegration:
    """Integration tests for ETL Logger"""
    
    def test_full_etl_logging_workflow(self, etl_logger):
        """Test complete ETL logging workflow"""
        # Initialize
        etl_logger.log_message(
            step=ETLLogger.STEP_INIT,
            status=ETLLogger.STATUS_SUCCESS,
            message="ETL initialized"
        )
        
        # Extract
        etl_logger.start_step(ETLLogger.STEP_EXTRACT)
        import time
        time.sleep(0.05)
        etl_logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Extracted data",
            records_processed=1000,
            records_success=1000
        )
        
        # Transform
        etl_logger.start_step(ETLLogger.STEP_TRANSFORM)
        time.sleep(0.05)
        etl_logger.log_message(
            step=ETLLogger.STEP_TRANSFORM,
            status=ETLLogger.STATUS_WARNING,
            message="Transformed with warnings",
            records_processed=1000,
            records_success=950,
            records_error=50
        )
        
        # Load
        etl_logger.start_step(ETLLogger.STEP_LOAD)
        time.sleep(0.05)
        etl_logger.log_message(
            step=ETLLogger.STEP_LOAD,
            status=ETLLogger.STATUS_SUCCESS,
            message="Loaded data",
            records_processed=950,
            records_success=950
        )
        
        # Complete
        etl_logger.log_message(
            step=ETLLogger.STEP_COMPLETE,
            status=ETLLogger.STATUS_SUCCESS,
            message="ETL completed"
        )
        
        # Verify
        assert len(etl_logger.log_entries) == 5
        
        stats = etl_logger.get_statistics()
        assert stats['total_records_processed'] == 2950
        assert stats['total_records_success'] == 2900
        assert stats['total_records_error'] == 50
        assert stats['warning_count'] == 1
        assert stats['total_duration_ms'] > 0
    
    def test_concurrent_step_tracking(self, etl_logger):
        """Test tracking multiple steps concurrently"""
        steps = [
            ETLLogger.STEP_EXTRACT,
            ETLLogger.STEP_TRANSFORM,
            ETLLogger.STEP_LOAD
        ]
        
        # Start all steps
        for step in steps:
            etl_logger.start_step(step)
        
        import time
        time.sleep(0.1)
        
        # All steps should have durations
        for step in steps:
            duration = etl_logger.get_step_duration_ms(step)
            assert duration is not None
            assert duration > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])