"""
Unit tests for ETL Logger with Delta Lake Audit Trail
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from delta import configure_spark_with_delta_pip
import tempfile
import shutil
from datetime import datetime
import os

from src.logger import ETLLogger, generate_etl_run_id, create_logger
from src.constants import ETLConstants


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    builder = (
        SparkSession.builder
        .appName("test_etl_logger")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    
    spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark_session
    spark_session.stop()


@pytest.fixture
def temp_delta_path():
    """Create temporary directory for Delta Lake"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def logger(spark, temp_delta_path):
    """Create ETL Logger instance for testing"""
    etl_run_id = generate_etl_run_id()
    return ETLLogger(
        spark=spark,
        etl_run_id=etl_run_id,
        delta_path=temp_delta_path,
        username="test_user"
    )


class TestETLLogger:
    """Test suite for ETL Logger"""
    
    def test_generate_etl_run_id(self):
        """Test ETL run ID generation"""
        run_id = generate_etl_run_id()
        
        assert run_id.startswith("ETL")
        assert len(run_id) == 17  # ETL + 14 digit timestamp
    
    def test_logger_initialization(self, spark, temp_delta_path):
        """Test logger initialization creates Delta table"""
        etl_run_id = generate_etl_run_id()
        logger = ETLLogger(spark, etl_run_id, temp_delta_path)
        
        assert logger.etl_run_id == etl_run_id
        assert logger.delta_path == temp_delta_path
        
        # Check Delta table exists
        df = spark.read.format("delta").load(temp_delta_path)
        assert df.schema == ETLLogger.LOG_SCHEMA
    
    def test_log_message_basic(self, logger, spark):
        """Test basic log message writing"""
        logger.log_message(
            step=ETLConstants.Step.EXTRACT,
            status=ETLConstants.Status.SUCCESS,
            message="Test message"
        )
        
        # Read logs
        df = spark.read.format("delta").load(logger.delta_path)
        assert df.count() == 1
        
        row = df.collect()[0]
        assert row.etl_run_id == logger.etl_run_id
        assert row.process_step == ETLConstants.Step.EXTRACT
        assert row.status == ETLConstants.Status.SUCCESS
        assert row.message == "Test message"
        assert row.created_by == "test_user"
    
    def test_log_message_with_counts(self, logger, spark):
        """Test log message with record counts"""
        logger.log_message(
            step=ETLConstants.Step.TRANSFORM,
            status=ETLConstants.Status.SUCCESS,
            message="Transformation complete",
            records_processed=100,
            records_success=95,
            records_error=5
        )
        
        df = spark.read.format("delta").load(logger.delta_path)
        row = df.collect()[0]
        
        assert row.records_processed == 100
        assert row.records_success == 95
        assert row.records_error == 5
    
    def test_multiple_log_entries(self, logger, spark):
        """Test multiple log entries"""
        steps = [
            ETLConstants.Step.INIT,
            ETLConstants.Step.EXTRACT,
            ETLConstants.Step.TRANSFORM,
            ETLConstants.Step.LOAD,
            ETLConstants.Step.COMPLETE
        ]
        
        for step in steps:
            logger.log_message(
                step=step,
                status=ETLConstants.Status.SUCCESS,
                message=f"Step {step} complete"
            )
        
        df = spark.read.format("delta").load(logger.delta_path)
        assert df.count() == len(steps)
        
        # Verify all steps logged
        logged_steps = [row.process_step for row in df.collect()]
        assert set(logged_steps) == set(steps)
    
    def test_get_logs_no_filter(self, logger, spark):
        """Test retrieving all logs without filter"""
        logger.log_message(
            step=ETLConstants.Step.EXTRACT,
            status=ETLConstants.Status.SUCCESS,
            message="Extract"
        )
        logger.log_message(
            step=ETLConstants.Step.TRANSFORM,
            status=ETLConstants.Status.WARNING,
            message="Transform"
        )
        
        logs_df = logger.get_logs()
        assert logs_df.count() == 2
    
    def test_get_logs_with_step_filter(self, logger):
        """Test retrieving logs filtered by step"""
        logger.log_message(
            step=ETLConstants.Step.EXTRACT,
            status=ETLConstants.Status.SUCCESS,
            message="Extract"
        )
        logger.log_message(
            step=ETLConstants.Step.TRANSFORM,
            status=ETLConstants.Status.SUCCESS,
            message="Transform"
        )
        
        logs_df = logger.get_logs(filter_step=ETLConstants.Step.EXTRACT)
        assert logs_df.count() == 1
        assert logs_df.collect()[0].process_step == ETLConstants.Step.EXTRACT
    
    def test_get_logs_with_status_filter(self, logger):
        """Test retrieving logs filtered by status"""
        logger.log_message(
            step=ETLConstants.Step.EXTRACT,
            status=ETLConstants.Status.SUCCESS,
            message="Success"
        )
        logger.log_message(
            step=ETLConstants.Step.TRANSFORM,
            status=ETLConstants.Status.ERROR,
            message="Error"
        )
        
        logs_df = logger.get_logs(filter_status=ETLConstants.Status.ERROR)
        assert logs_df.count() == 1
        assert logs_df.collect()[0].status == ETLConstants.Status.ERROR
    
    def test_get_summary_statistics(self, logger):
        """Test summary statistics calculation"""
        logger.log_message(
            step=ETLConstants.Step.EXTRACT,
            status=ETLConstants.Status.SUCCESS,
            message="Extract",
            records_processed=100,
            records_success=100,
            records_error=0
        )
        logger.log_message(
            step=ETLConstants.Step.TRANSFORM,
            status=ETLConstants.Status.WARNING,
            message="Transform",
            records_processed=100,
            records_success=95,
            records_error=5
        )
        
        stats = logger.get_summary_statistics()
        
        assert stats["etl_run_id"] == logger.etl_run_id
        assert stats["total_records_processed"] == 200
        assert stats["total_records_success"] == 195
        assert stats["total_records_error"] == 5
        assert stats["error_count"] == 0
        assert stats["warning_count"] == 1
        assert stats["start_time"] is not None
        assert stats["end_time"] is not None
        assert stats["duration_seconds"] is not None
    
    def test_create_logger_factory(self, spark, temp_delta_path):
        """Test factory function for creating logger"""
        logger = create_logger(
            spark=spark,
            delta_path=temp_delta_path,
            username="factory_user"
        )
        
        assert logger.etl_run_id.startswith("ETL")
        assert logger.username == "factory_user"
    
    def test_create_logger_with_run_id(self, spark, temp_delta_path):
        """Test factory function with provided run ID"""
        custom_run_id = "ETL20240101120000"
        logger = create_logger(
            spark=spark,
            delta_path=temp_delta_path,
            etl_run_id=custom_run_id
        )
        
        assert logger.etl_run_id == custom_run_id
    
    def test_log_id_uniqueness(self, logger):
        """Test that log IDs are unique"""
        logger.log_message(
            step=ETLConstants.Step.INIT,
            status=ETLConstants.Status.SUCCESS,
            message="First"
        )
        logger.log_message(
            step=ETLConstants.Step.INIT,
            status=ETLConstants.Status.SUCCESS,
            message="Second"
        )
        
        logs_df = logger.get_logs()
        log_ids = [row.log_id for row in logs_df.collect()]
        
        assert len(log_ids) == len(set(log_ids))  # All unique
    
    def test_status_constants(self):
        """Test status constants match ABAP"""
        assert ETLLogger.STATUS_SUCCESS == "S"
        assert ETLLogger.STATUS_ERROR == "E"
        assert ETLLogger.STATUS_WARNING == "W"
        assert ETLLogger.STATUS_INFO == "I"
    
    def test_step_constants(self):
        """Test step constants match ABAP"""
        assert ETLLogger.STEP_INIT == "INIT"
        assert ETLLogger.STEP_EXTRACT == "EXTRACT"
        assert ETLLogger.STEP_TRANSFORM == "TRANSFORM"
        assert ETLLogger.STEP_LOAD == "LOAD"
        assert ETLLogger.STEP_VALIDATE == "VALIDATE"
        assert ETLLogger.STEP_COMPLETE == "COMPLETE"
        assert ETLLogger.STEP_ERROR == "ERROR"
    
    def test_error_logging(self, logger):
        """Test error status logging"""
        logger.log_message(
            step=ETLConstants.Step.LOAD,
            status=ETLConstants.Status.ERROR,
            message="Load failed: Connection timeout",
            records_processed=100,
            records_success=50,
            records_error=50
        )
        
        logs_df = logger.get_logs(filter_status=ETLConstants.Status.ERROR)
        assert logs_df.count() == 1
        
        row = logs_df.collect()[0]
        assert "Load failed" in row.message
        assert row.records_error == 50
    
    def test_schema_validation(self, logger, spark):
        """Test that logged data conforms to schema"""
        logger.log_message(
            step=ETLConstants.Step.VALIDATE,
            status=ETLConstants.Status.SUCCESS,
            message="Validation passed"
        )
        
        df = spark.read.format("delta").load(logger.delta_path)
        
        # Check schema fields
        schema_fields = {field.name for field in df.schema.fields}
        expected_fields = {field.name for field in ETLLogger.LOG_SCHEMA.fields}
        
        assert schema_fields == expected_fields
    
    def test_concurrent_logging(self, spark, temp_delta_path):
        """Test multiple loggers writing to same Delta table"""
        run_id_1 = generate_etl_run_id()
        run_id_2 = generate_etl_run_id()
        
        logger1 = ETLLogger(spark, run_id_1, temp_delta_path, "user1")
        logger2 = ETLLogger(spark, run_id_2, temp_delta_path, "user2")
        
        logger1.log_message(
            step=ETLConstants.Step.EXTRACT,
            status=ETLConstants.Status.SUCCESS,
            message="Logger 1"
        )
        logger2.log_message(
            step=ETLConstants.Step.EXTRACT,
            status=ETLConstants.Status.SUCCESS,
            message="Logger 2"
        )
        
        # Both logs should be present
        df = spark.read.format("delta").load(temp_delta_path)
        assert df.count() == 2
        
        # Each logger should only see its own logs
        assert logger1.get_logs().count() == 1
        assert logger2.get_logs().count() == 1


class TestETLConstants:
    """Test suite for ETL Constants"""
    
    def test_status_constants(self):
        """Test status code constants"""
        assert ETLConstants.Status.NEW == "N"
        assert ETLConstants.Status.PROCESSED == "P"
        assert ETLConstants.Status.ERROR == "E"
        assert ETLConstants.Status.WARNING == "W"
        assert ETLConstants.Status.SUCCESS == "S"
        assert ETLConstants.Status.INFO == "I"
    
    def test_step_constants(self):
        """Test step constants"""
        assert ETLConstants.Step.INIT == "INIT"
        assert ETLConstants.Step.EXTRACT == "EXTRACT"
        assert ETLConstants.Step.TRANSFORM == "TRANSFORM"
        assert ETLConstants.Step.LOAD == "LOAD"
    
    def test_business_rules(self):
        """Test business rule constants"""
        assert ETLConstants.DISCOUNT_QTY_TIER1 == 10
        assert ETLConstants.DISCOUNT_QTY_TIER2 == 15
        assert ETLConstants.DISCOUNT_RATE_TIER1 == 0.05
        assert ETLConstants.DISCOUNT_RATE_TIER2 == 0.10
        assert ETLConstants.TAX_RATE == 0.08
        assert ETLConstants.COST_RATIO == 0.60
    
    def test_category_thresholds(self):
        """Test category threshold constants"""
        assert ETLConstants.CATEGORY_HIGH_THRESHOLD == 2000.00
        assert ETLConstants.CATEGORY_MEDIUM_THRESHOLD == 500.00
    
    def test_constants_to_dict(self):
        """Test constants export to dictionary"""
        config_dict = ETLConstants.to_dict()
        
        assert config_dict["tax_rate"] == 0.08
        assert config_dict["default_batch_size"] == 1000
        assert "discount_qty_tier1" in config_dict


if __name__ == "__main__":
    pytest.main([__file__, "-v"])