"""
Unit tests for ETL Logger
Tests ABAP ZCL_ETL_LOGGER to PySpark migration
"""
import pytest
from pyspark.sql import SparkSession
from datetime import datetime
import tempfile
import shutil
import os

from src.logger import ETLLogger, LogConstants, create_logger


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("test_etl_logger") \
        .master("local[2]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture
def temp_delta_path():
    """Create temporary directory for Delta Lake"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def logger(spark, temp_delta_path):
    """Create logger instance for testing"""
    etl_run_id = "TEST_ETL_RUN_001"
    return ETLLogger(spark, etl_run_id, temp_delta_path)


class TestETLLogger:
    """Test suite for ETL Logger functionality"""
    
    def test_logger_initialization(self, logger):
        """Test logger initializes correctly"""
        assert logger is not None
        assert logger.etl_run_id == "TEST_ETL_RUN_001"
        assert logger._log_buffer == []
    
    def test_get_log_schema(self):
        """Test log schema matches ZETL_LOG structure"""
        schema = ETLLogger.get_log_schema()
        
        expected_fields = [
            "log_id", "etl_run_id", "execution_date", "execution_time",
            "execution_timestamp", "process_step", "status",
            "records_processed", "records_success", "records_error",
            "message", "created_at", "created_by"
        ]
        
        actual_fields = [field.name for field in schema.fields]
        assert actual_fields == expected_fields
    
    def test_log_message_basic(self, logger):
        """Test basic log message creation"""
        logger.log_message(
            process_step=LogConstants.Step.INIT,
            status=LogConstants.Status.SUCCESS,
            message="Test initialization"
        )
        
        assert len(logger._log_buffer) == 1
        
        log_entry = logger._log_buffer[0]
        assert log_entry["process_step"] == "INIT"
        assert log_entry["status"] == "S"
        assert log_entry["message"] == "Test initialization"
        assert log_entry["etl_run_id"] == "TEST_ETL_RUN_001"
    
    def test_log_message_with_metrics(self, logger):
        """Test log message with record metrics"""
        logger.log_message(
            process_step=LogConstants.Step.EXTRACT,
            status=LogConstants.Status.SUCCESS,
            message="Extraction completed",
            records_processed=1000,
            records_success=950,
            records_error=50
        )
        
        log_entry = logger._log_buffer[0]
        assert log_entry["records_processed"] == 1000
        assert log_entry["records_success"] == 950
        assert log_entry["records_error"] == 50
    
    def test_generate_log_id(self, logger):
        """Test log ID generation"""
        log_id = logger._generate_log_id()
        
        assert log_id.startswith("LOG")
        assert len(log_id) > 14  # LOG + timestamp + uuid
    
    def test_generate_etl_run_id(self):
        """Test ETL run ID generation"""
        run_id = ETLLogger.generate_etl_run_id()
        
        assert run_id.startswith("ETL")
        assert len(run_id) > 14
    
    def test_get_etl_run_id(self, logger):
        """Test getting ETL run ID"""
        assert logger.get_etl_run_id() == "TEST_ETL_RUN_001"
    
    def test_flush_logs_to_delta(self, spark, temp_delta_path):
        """Test writing logs to Delta Lake"""
        logger = ETLLogger(spark, "TEST_RUN_002", temp_delta_path)
        
        # Add multiple log entries
        for i in range(5):
            logger.log_message(
                process_step=f"STEP_{i}",
                status=LogConstants.Status.SUCCESS,
                message=f"Test message {i}",
                records_processed=i * 100
            )
        
        # Flush to Delta
        logger.flush_logs()
        
        # Verify buffer is cleared
        assert len(logger._log_buffer) == 0
        
        # Verify data written to Delta
        df = spark.read.format("delta").load(temp_delta_path)
        assert df.count() == 5
        
        # Verify schema
        assert set(df.columns) == set([field.name for field in ETLLogger.get_log_schema().fields])
    
    def test_read_logs(self, spark, temp_delta_path):
        """Test reading logs from Delta Lake"""
        logger = ETLLogger(spark, "TEST_RUN_003", temp_delta_path)
        
        # Write some logs
        logger.log_message(
            process_step=LogConstants.Step.EXTRACT,
            status=LogConstants.Status.SUCCESS,
            message="Test extract"
        )
        logger.log_message(
            process_step=LogConstants.Step.TRANSFORM,
            status=LogConstants.Status.SUCCESS,
            message="Test transform"
        )
        logger.flush_logs()
        
        # Read all logs
        df = logger.read_logs()
        assert df.count() == 2
        
        # Read with filter
        df_filtered = logger.read_logs(filters={"process_step": "EXTRACT"})
        assert df_filtered.count() == 1
    
    def test_get_run_summary(self, spark, temp_delta_path):
        """Test getting run summary statistics"""
        logger = ETLLogger(spark, "TEST_RUN_004", temp_delta_path)
        
        # Log multiple steps with different statuses
        logger.log_message(
            process_step=LogConstants.Step.EXTRACT,
            status=LogConstants.Status.SUCCESS,
            message="Extract success",
            records_processed=1000,
            records_success=1000
        )
        logger.log_message(
            process_step=LogConstants.Step.TRANSFORM,
            status=LogConstants.Status.WARNING,
            message="Transform warning",
            records_processed=1000,
            records_success=900,
            records_error=100
        )
        logger.flush_logs()
        
        # Get summary
        summary_df = logger.get_run_summary()
        assert summary_df.count() == 2
    
    def test_multiple_flush_cycles(self, spark, temp_delta_path):
        """Test multiple flush cycles append correctly"""
        logger = ETLLogger(spark, "TEST_RUN_005", temp_delta_path)
        
        # First batch
        logger.log_message(
            process_step=LogConstants.Step.EXTRACT,
            status=LogConstants.Status.SUCCESS,
            message="Batch 1"
        )
        logger.flush_logs()
        
        # Second batch
        logger.log_message(
            process_step=LogConstants.Step.TRANSFORM,
            status=LogConstants.Status.SUCCESS,
            message="Batch 2"
        )
        logger.flush_logs()
        
        # Verify total count
        df = spark.read.format("delta").load(temp_delta_path)
        assert df.count() == 2
    
    def test_create_logger_factory(self, spark, temp_delta_path):
        """Test factory function creates logger with auto-generated ID"""
        logger = create_logger(spark, temp_delta_path)
        
        assert logger is not None
        assert logger.etl_run_id.startswith("ETL")
        assert len(logger.etl_run_id) > 14


class TestLogConstants:
    """Test suite for logging constants"""
    
    def test_status_constants(self):
        """Test status constants match ABAP definitions"""
        assert LogConstants.Status.NEW == "N"
        assert LogConstants.Status.PROCESSED == "P"
        assert LogConstants.Status.ERROR == "E"
        assert LogConstants.Status.WARNING == "W"
        assert LogConstants.Status.SUCCESS == "S"
        assert LogConstants.Status.INFO == "I"
    
    def test_step_constants(self):
        """Test step constants match ABAP definitions"""
        assert LogConstants.Step.INIT == "INIT"
        assert LogConstants.Step.EXTRACT == "EXTRACT"
        assert LogConstants.Step.TRANSFORM == "TRANSFORM"
        assert LogConstants.Step.LOAD == "LOAD"
        assert LogConstants.Step.VALIDATE == "VALIDATE"
        assert LogConstants.Step.COMPLETE == "COMPLETE"
        assert LogConstants.Step.ERROR == "ERROR"
    
    def test_message_constants(self):
        """Test message constants match ABAP definitions"""
        assert LogConstants.Message.INIT_SUCCESS == "ETL process initialized successfully"
        assert LogConstants.Message.EXTRACT_START == "Starting data extraction"
        assert LogConstants.Message.ETL_COMPLETE == "ETL process completed successfully"


class TestErrorHandling:
    """Test suite for error handling scenarios"""
    
    def test_flush_empty_buffer(self, logger):
        """Test flushing empty buffer doesn't cause errors"""
        logger.flush_logs()  # Should not raise
        assert len(logger._log_buffer) == 0
    
    def test_read_nonexistent_delta_table(self, spark):
        """Test reading from nonexistent Delta table"""
        logger = ETLLogger(spark, "TEST_RUN_ERROR", "/nonexistent/path")
        
        with pytest.raises(Exception):
            logger.read_logs()
    
    def test_log_with_long_message(self, logger):
        """Test logging with message exceeding max length"""
        long_message = "A" * 500  # Longer than CHAR255
        
        logger.log_message(
            process_step=LogConstants.Step.ERROR,
            status=LogConstants.Status.ERROR,
            message=long_message
        )
        
        # Should truncate or handle gracefully
        assert len(logger._log_buffer) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])