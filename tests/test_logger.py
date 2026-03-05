"""
Unit Tests for ETL Logger Interface and Implementation
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch
from pyspark.sql import SparkSession

from src.logger import (
    ETLLoggerInterface, LogEntry, LogStatus, ProcessStep
)
from src.logger_impl import ETLLogger


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing"""
    return (
        SparkSession.builder
        .appName("ETLLoggerTest")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .getOrCreate()
    )


@pytest.fixture
def etl_run_id():
    """Generate test ETL run ID"""
    return f"ETL{datetime.now().strftime('%Y%m%d%H%M%S')}"


@pytest.fixture
def logger(spark, etl_run_id):
    """Create ETL logger instance"""
    return ETLLogger(etl_run_id=etl_run_id, spark=spark)


class TestLogEnums:
    """Test Enum definitions"""
    
    def test_log_status_enum_values(self):
        """Test LogStatus enum values"""
        assert LogStatus.SUCCESS.value == 'S'
        assert LogStatus.ERROR.value == 'E'
        assert LogStatus.WARNING.value == 'W'
        assert LogStatus.INFO.value == 'I'
    
    def test_process_step_enum_values(self):
        """Test ProcessStep enum values"""
        assert ProcessStep.INIT.value == 'INIT'
        assert ProcessStep.EXTRACT.value == 'EXTRACT'
        assert ProcessStep.TRANSFORM.value == 'TRANSFORM'
        assert ProcessStep.LOAD.value == 'LOAD'
        assert ProcessStep.VALIDATE.value == 'VALIDATE'
        assert ProcessStep.COMPLETE.value == 'COMPLETE'
        assert ProcessStep.ERROR.value == 'ERROR'
    
    def test_enum_membership(self):
        """Test enum membership"""
        assert ProcessStep.EXTRACT in ProcessStep
        assert LogStatus.SUCCESS in LogStatus


class TestLogEntry:
    """Test LogEntry dataclass"""
    
    def test_log_entry_creation(self, etl_run_id):
        """Test creating a log entry"""
        entry = LogEntry(
            log_id="LOG123",
            etl_run_id=etl_run_id,
            execution_date=datetime.now(),
            process_step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            records_processed=100,
            records_success=98,
            records_error=2,
            message="Test message"
        )
        
        assert entry.log_id == "LOG123"
        assert entry.etl_run_id == etl_run_id
        assert entry.process_step == ProcessStep.EXTRACT
        assert entry.status == LogStatus.SUCCESS
        assert entry.records_processed == 100
    
    def test_log_entry_defaults(self, etl_run_id):
        """Test log entry default values"""
        entry = LogEntry(
            log_id="LOG123",
            etl_run_id=etl_run_id,
            execution_date=datetime.now(),
            process_step=ProcessStep.INIT,
            status=LogStatus.INFO
        )
        
        assert entry.records_processed == 0
        assert entry.records_success == 0
        assert entry.records_error == 0
        assert entry.message == ""


class TestETLLoggerInterface:
    """Test ETL Logger abstract interface"""
    
    def test_interface_is_abstract(self):
        """Test that interface cannot be instantiated"""
        with pytest.raises(TypeError):
            ETLLoggerInterface("ETL123")
    
    def test_interface_defines_required_methods(self):
        """Test that interface defines all required methods"""
        required_methods = [
            '__init__',
            'log_message',
            'get_etl_run_id',
            'get_log_entries',
            'generate_log_id'
        ]
        
        for method in required_methods:
            assert hasattr(ETLLoggerInterface, method)


class TestETLLogger:
    """Test concrete ETL Logger implementation"""
    
    def test_logger_initialization(self, logger, etl_run_id):
        """Test logger initialization"""
        assert logger.get_etl_run_id() == etl_run_id
        assert len(logger.get_log_entries()) == 0
    
    def test_generate_log_id(self, logger):
        """Test log ID generation"""
        log_id = logger.generate_log_id()
        
        assert log_id.startswith("LOG")
        assert len(log_id) > 3
        
        # Test uniqueness
        log_id2 = logger.generate_log_id()
        assert log_id != log_id2
    
    def test_log_message_basic(self, logger):
        """Test basic message logging"""
        logger.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="Test extraction"
        )
        
        entries = logger.get_log_entries()
        assert len(entries) == 1
        
        entry = entries[0]
        assert entry.process_step == ProcessStep.EXTRACT
        assert entry.status == LogStatus.SUCCESS
        assert entry.message == "Test extraction"
    
    def test_log_message_with_counts(self, logger):
        """Test logging with record counts"""
        logger.log_message(
            step=ProcessStep.TRANSFORM,
            status=LogStatus.SUCCESS,
            message="Transformation complete",
            records_processed=1000,
            records_success=980,
            records_error=20
        )
        
        entry = logger.get_log_entries()[0]
        assert entry.records_processed == 1000
        assert entry.records_success == 980
        assert entry.records_error == 20
    
    def test_log_multiple_messages(self, logger):
        """Test logging multiple messages"""
        steps = [
            (ProcessStep.INIT, LogStatus.INFO, "Initializing"),
            (ProcessStep.EXTRACT, LogStatus.SUCCESS, "Extracting"),
            (ProcessStep.TRANSFORM, LogStatus.SUCCESS, "Transforming"),
            (ProcessStep.LOAD, LogStatus.SUCCESS, "Loading"),
        ]
        
        for step, status, message in steps:
            logger.log_message(step=step, status=status, message=message)
        
        entries = logger.get_log_entries()
        assert len(entries) == 4
        
        # Verify order
        assert entries[0].process_step == ProcessStep.INIT
        assert entries[-1].process_step == ProcessStep.LOAD
    
    def test_log_error_message(self, logger):
        """Test logging error messages"""
        logger.log_message(
            step=ProcessStep.LOAD,
            status=LogStatus.ERROR,
            message="Database connection failed",
            records_error=50
        )
        
        entry = logger.get_log_entries()[0]
        assert entry.status == LogStatus.ERROR
        assert entry.records_error == 50
    
    def test_log_warning_message(self, logger):
        """Test logging warning messages"""
        logger.log_message(
            step=ProcessStep.VALIDATE,
            status=LogStatus.WARNING,
            message="Some records failed validation",
            records_error=10
        )
        
        entry = logger.get_log_entries()[0]
        assert entry.status == LogStatus.WARNING
    
    def test_get_logs_as_dataframe(self, spark, etl_run_id):
        """Test getting logs as DataFrame"""
        logger = ETLLogger(etl_run_id=etl_run_id, spark=spark)
        
        # Log some messages
        logger.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="Extraction complete",
            records_processed=100
        )
        logger.log_message(
            step=ProcessStep.TRANSFORM,
            status=LogStatus.SUCCESS,
            message="Transformation complete",
            records_processed=100
        )
        
        # Get as DataFrame
        df = logger.get_logs_as_dataframe()
        
        assert df.count() == 2
        assert "log_id" in df.columns
        assert "etl_run_id" in df.columns
        assert "process_step" in df.columns
        assert "status" in df.columns
        
        # Verify data
        rows = df.collect()
        assert rows[0]["process_step"] == "EXTRACT"
        assert rows[1]["process_step"] == "TRANSFORM"
    
    def test_empty_logs_dataframe(self, spark, etl_run_id):
        """Test getting empty logs DataFrame"""
        logger = ETLLogger(etl_run_id=etl_run_id, spark=spark)
        df = logger.get_logs_as_dataframe()
        
        assert df.count() == 0
        assert df.schema == ETLLogger.LOG_SCHEMA


class TestLoggerWithPersistence:
    """Test logger with database persistence"""
    
    @patch.object(ETLLogger, '_persist_log_entry')
    def test_persistence_called(self, mock_persist, spark, etl_run_id):
        """Test that persistence is called when configured"""
        logger = ETLLogger(
            etl_run_id=etl_run_id,
            spark=spark,
            log_table="test_etl_log"
        )
        
        logger.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="Test"
        )
        
        # Verify persistence was called
        assert mock_persist.called
        assert mock_persist.call_count == 1
    
    def test_persistence_failure_handling(self, spark, etl_run_id):
        """Test handling of persistence failures"""
        logger = ETLLogger(
            etl_run_id=etl_run_id,
            spark=spark,
            log_table="invalid_table"
        )
        
        # Should not raise exception even if persistence fails
        try:
            logger.log_message(
                step=ProcessStep.EXTRACT,
                status=LogStatus.SUCCESS,
                message="Test"
            )
        except Exception as e:
            pytest.fail(f"Logger raised exception on persistence failure: {e}")


class TestLoggerIntegration:
    """Integration tests for logger"""
    
    def test_complete_etl_workflow_logging(self, spark, etl_run_id):
        """Test logging throughout complete ETL workflow"""
        logger = ETLLogger(etl_run_id=etl_run_id, spark=spark)
        
        # Init
        logger.log_message(
            step=ProcessStep.INIT,
            status=LogStatus.INFO,
            message="ETL process initialized"
        )
        
        # Extract
        logger.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="Data extracted",
            records_processed=1000,
            records_success=1000
        )
        
        # Transform
        logger.log_message(
            step=ProcessStep.TRANSFORM,
            status=LogStatus.SUCCESS,
            message="Data transformed",
            records_processed=1000,
            records_success=995,
            records_error=5
        )
        
        # Validate
        logger.log_message(
            step=ProcessStep.VALIDATE,
            status=LogStatus.WARNING,
            message="Some validation failures",
            records_error=5
        )
        
        # Load
        logger.log_message(
            step=ProcessStep.LOAD,
            status=LogStatus.SUCCESS,
            message="Data loaded",
            records_processed=995,
            records_success=995
        )
        
        # Complete
        logger.log_message(
            step=ProcessStep.COMPLETE,
            status=LogStatus.SUCCESS,
            message="ETL completed successfully"
        )
        
        # Verify logs
        entries = logger.get_log_entries()
        assert len(entries) == 6
        
        # Verify workflow progression
        steps = [e.process_step for e in entries]
        assert steps == [
            ProcessStep.INIT,
            ProcessStep.EXTRACT,
            ProcessStep.TRANSFORM,
            ProcessStep.VALIDATE,
            ProcessStep.LOAD,
            ProcessStep.COMPLETE
        ]
        
        # Verify statistics
        df = logger.get_logs_as_dataframe()
        total_processed = df.agg({"records_processed": "sum"}).collect()[0][0]
        total_success = df.agg({"records_success": "sum"}).collect()[0][0]
        total_error = df.agg({"records_error": "sum"}).collect()[0][0]
        
        assert total_processed == 2995  # 1000 + 1000 + 995
        assert total_success == 2990    # 1000 + 995 + 995
        assert total_error == 10        # 5 + 5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])