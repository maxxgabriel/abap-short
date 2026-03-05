"""
Unit tests for ETL logging framework
Comprehensive test coverage for logger functionality
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import tempfile
import shutil
from pathlib import Path

from src.logger import ETLLogger, LogEntry, LoggerFactory
from src.utils import generate_etl_run_id, format_log_message, calculate_duration


@pytest.fixture(scope="session")
def spark():
    """Create SparkSession for testing"""
    builder = (
        SparkSession.builder
        .appName("ETLLoggerTests")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
    )
    
    spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark_session
    spark_session.stop()


@pytest.fixture
def temp_delta_path():
    """Create temporary directory for Delta table"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def etl_run_id():
    """Generate test ETL run ID"""
    return generate_etl_run_id()


@pytest.fixture
def logger(spark, temp_delta_path, etl_run_id):
    """Create logger instance for testing"""
    logger_instance = ETLLogger(
        spark=spark,
        etl_run_id=etl_run_id,
        delta_table_path=temp_delta_path,
        user="test_user",
        console_logging=False
    )
    yield logger_instance
    logger_instance.close()


class TestLogEntry:
    """Test LogEntry dataclass"""
    
    def test_log_entry_creation(self):
        """Test creating a log entry"""
        entry = LogEntry(
            log_id="LOG001",
            etl_run_id="ETL001",
            execution_timestamp=datetime.now(),
            process_step="EXTRACT",
            status="S",
            records_processed=100,
            records_success=95,
            records_error=5,
            message="Test message",
            created_by="test_user"
        )
        
        assert entry.log_id == "LOG001"
        assert entry.status == "S"
        assert entry.records_processed == 100
    
    def test_log_entry_to_dict(self):
        """Test converting log entry to dictionary"""
        entry = LogEntry(
            log_id="LOG001",
            etl_run_id="ETL001",
            execution_timestamp=datetime.now(),
            process_step="EXTRACT",
            status="S",
            records_processed=100,
            records_success=95,
            records_error=5,
            message="Test message",
            created_by="test_user"
        )
        
        entry_dict = entry.to_dict()
        
        assert isinstance(entry_dict, dict)
        assert "log_id" in entry_dict
        assert "execution_timestamp" in entry_dict


class TestETLLogger:
    """Test ETLLogger class"""
    
    def test_logger_initialization(self, logger):
        """Test logger initialization"""
        assert logger.etl_run_id is not None
        assert logger.user == "test_user"
        assert isinstance(logger._log_buffer, list)
    
    def test_delta_table_creation(self, spark, temp_delta_path, etl_run_id):
        """Test Delta table is created on initialization"""
        logger = ETLLogger(
            spark=spark,
            etl_run_id=etl_run_id,
            delta_table_path=temp_delta_path,
            console_logging=False
        )
        
        # Check that Delta table exists
        from delta import DeltaTable
        delta_table = DeltaTable.forPath(spark, temp_delta_path)
        assert delta_table is not None
        
        logger.close()
    
    def test_log_message(self, logger):
        """Test logging a message"""
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Test extraction",
            records_processed=100,
            records_success=100,
            records_error=0
        )
        
        assert len(logger._log_buffer) == 1
        assert logger._log_buffer[0].process_step == "EXTRACT"
        assert logger._log_buffer[0].records_processed == 100
    
    def test_flush_to_delta(self, logger, spark):
        """Test flushing logs to Delta table"""
        # Add multiple log entries
        for i in range(5):
            logger.log_message(
                step=ETLLogger.STEP_TRANSFORM,
                status=ETLLogger.STATUS_SUCCESS,
                message=f"Transform step {i}",
                records_processed=10 * i,
                records_success=10 * i,
                records_error=0
            )
        
        # Flush to Delta
        logger.flush_to_delta()
        
        # Verify buffer is cleared
        assert len(logger._log_buffer) == 0
        
        # Verify data in Delta table
        from delta import DeltaTable
        delta_table = DeltaTable.forPath(spark, logger.delta_table_path)
        log_df = delta_table.toDF()
        
        assert log_df.count() == 5
    
    def test_get_logs_for_run(self, logger, spark):
        """Test retrieving logs for specific run"""
        # Log some messages
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Extract test",
            records_processed=50
        )
        logger.log_message(
            step=ETLLogger.STEP_LOAD,
            status=ETLLogger.STATUS_SUCCESS,
            message="Load test",
            records_processed=50
        )
        
        logger.flush_to_delta()
        
        # Retrieve logs
        logs_df = logger.get_logs_for_run()
        
        assert logs_df.count() == 2
        assert logs_df.filter(logs_df.process_step == "EXTRACT").count() == 1
    
    def test_get_summary_stats(self, logger):
        """Test getting summary statistics"""
        # Log multiple messages with different statuses
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Extract",
            records_processed=100,
            records_success=100,
            records_error=0
        )
        logger.log_message(
            step=ETLLogger.STEP_TRANSFORM,
            status=ETLLogger.STATUS_WARNING,
            message="Transform",
            records_processed=100,
            records_success=95,
            records_error=5
        )
        logger.log_message(
            step=ETLLogger.STEP_LOAD,
            status=ETLLogger.STATUS_SUCCESS,
            message="Load",
            records_processed=95,
            records_success=95,
            records_error=0
        )
        
        logger.flush_to_delta()
        
        # Get summary
        stats = logger.get_summary_stats()
        
        assert stats["total_log_entries"] == 3
        assert stats["total_processed"] == 295
        assert stats["total_success"] == 290
        assert stats["total_errors"] == 5
        assert "status_breakdown" in stats
    
    def test_immediate_flush(self, logger, spark):
        """Test immediate flush functionality"""
        logger.log_message(
            step=ETLLogger.STEP_INIT,
            status=ETLLogger.STATUS_INFO,
            message="Initialization",
            flush_immediate=True
        )
        
        # Buffer should be empty after immediate flush
        assert len(logger._log_buffer) == 0
        
        # Data should be in Delta table
        from delta import DeltaTable
        delta_table = DeltaTable.forPath(spark, logger.delta_table_path)
        assert delta_table.toDF().count() == 1
    
    def test_context_manager(self, spark, temp_delta_path, etl_run_id):
        """Test using logger as context manager"""
        with ETLLogger(
            spark=spark,
            etl_run_id=etl_run_id,
            delta_table_path=temp_delta_path,
            console_logging=False
        ) as logger:
            logger.log_message(
                step=ETLLogger.STEP_EXTRACT,
                status=ETLLogger.STATUS_SUCCESS,
                message="Test message"
            )
            
            # Buffer should have entry
            assert len(logger._log_buffer) == 1
        
        # After context exit, logs should be flushed
        from delta import DeltaTable
        delta_table = DeltaTable.forPath(spark, temp_delta_path)
        assert delta_table.toDF().count() == 1


class TestLoggerFactory:
    """Test LoggerFactory class"""
    
    def test_create_logger(self, spark, temp_delta_path, etl_run_id):
        """Test creating logger from factory"""
        config = {
            "delta_table_path": temp_delta_path,
            "user": "factory_user",
            "console_logging": False,
            "log_level": "DEBUG"
        }
        
        logger = LoggerFactory.create_logger(
            spark=spark,
            etl_run_id=etl_run_id,
            config=config
        )
        
        assert logger.user == "factory_user"
        assert logger.etl_run_id == etl_run_id
        
        logger.close()


class TestUtilityFunctions:
    """Test utility functions"""
    
    def test_generate_etl_run_id(self):
        """Test ETL run ID generation"""
        run_id = generate_etl_run_id()
        
        assert run_id.startswith("ETL")
        assert len(run_id) > 10
        
        # Test custom prefix
        custom_id = generate_etl_run_id(prefix="TEST")
        assert custom_id.startswith("TEST")
    
    def test_format_log_message(self):
        """Test log message formatting"""
        message = format_log_message(
            step="EXTRACT",
            status="S",
            message="Data extracted",
            records_processed=100,
            records_success=95,
            records_error=5
        )
        
        assert "[EXTRACT]" in message
        assert "[S]" in message
        assert "Data extracted" in message
        assert "Processed: 100" in message
    
    def test_calculate_duration(self):
        """Test duration calculation"""
        start = datetime(2024, 1, 1, 10, 0, 0)
        end = datetime(2024, 1, 1, 11, 30, 0)
        
        duration = calculate_duration(start, end)
        
        assert duration["total_seconds"] == 5400
        assert duration["minutes"] == 90
        assert duration["hours"] == 1.5


class TestIntegrationScenarios:
    """Integration tests for complete scenarios"""
    
    def test_complete_etl_logging_scenario(self, spark, temp_delta_path, etl_run_id):
        """Test complete ETL logging workflow"""
        with ETLLogger(
            spark=spark,
            etl_run_id=etl_run_id,
            delta_table_path=temp_delta_path,
            user="integration_test",
            console_logging=False
        ) as logger:
            # Initialization
            logger.log_message(
                step=ETLLogger.STEP_INIT,
                status=ETLLogger.STATUS_INFO,
                message="ETL process started"
            )
            
            # Extract phase
            logger.log_message(
                step=ETLLogger.STEP_EXTRACT,
                status=ETLLogger.STATUS_SUCCESS,
                message="Data extraction completed",
                records_processed=1000,
                records_success=1000,
                records_error=0
            )
            
            # Transform phase with warnings
            logger.log_message(
                step=ETLLogger.STEP_TRANSFORM,
                status=ETLLogger.STATUS_WARNING,
                message="Data transformation completed with warnings",
                records_processed=1000,
                records_success=950,
                records_error=50
            )
            
            # Load phase
            logger.log_message(
                step=ETLLogger.STEP_LOAD,
                status=ETLLogger.STATUS_SUCCESS,
                message="Data load completed",
                records_processed=950,
                records_success=950,
                records_error=0
            )
            
            # Completion
            logger.log_message(
                step=ETLLogger.STEP_COMPLETE,
                status=ETLLogger.STATUS_SUCCESS,
                message="ETL process completed successfully"
            )
        
        # Verify all logs were persisted
        from delta import DeltaTable
        delta_table = DeltaTable.forPath(spark, temp_delta_path)
        logs_df = delta_table.toDF()
        
        assert logs_df.count() == 5
        
        # Verify statistics
        from pyspark.sql.functions import sum as _sum
        totals = logs_df.agg(
            _sum("records_processed").alias("total_processed"),
            _sum("records_error").alias("total_errors")
        ).collect()[0]
        
        assert totals["total_processed"] == 3900
        assert totals["total_errors"] == 50