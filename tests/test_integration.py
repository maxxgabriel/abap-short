"""
Integration tests for ETL Logger with full ETL workflow
"""
import pytest
from pyspark.sql import SparkSession
import tempfile
import shutil
import yaml

from src.logger import ETLLogger, LogConstants


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for integration tests"""
    spark = SparkSession.builder \
        .appName("integration_test_logger") \
        .master("local[2]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    yield spark
    spark.stop()


@pytest.fixture
def test_env(spark):
    """Create complete test environment with Delta paths"""
    base_dir = tempfile.mkdtemp()
    
    env = {
        "base_dir": base_dir,
        "log_path": f"{base_dir}/zetl_log",
        "raw_path": f"{base_dir}/zsales_raw",
        "analytics_path": f"{base_dir}/zsales_analytics"
    }
    
    yield env
    
    shutil.rmtree(base_dir)


class TestFullETLWorkflow:
    """Integration tests simulating full ETL workflow with logging"""
    
    def test_complete_etl_with_logging(self, spark, test_env):
        """Test complete ETL workflow with audit logging"""
        # Initialize logger
        logger = ETLLogger(spark, "INTEGRATION_001", test_env["log_path"])
        
        # INIT phase
        logger.log_message(
            process_step=LogConstants.Step.INIT,
            status=LogConstants.Status.SUCCESS,
            message=LogConstants.Message.INIT_SUCCESS
        )
        
        # EXTRACT phase
        logger.log_message(
            process_step=LogConstants.Step.EXTRACT,
            status=LogConstants.Status.INFO,
            message=LogConstants.Message.EXTRACT_START
        )
        
        # Simulate extraction
        extracted_count = 1000
        logger.log_message(
            process_step=LogConstants.Step.EXTRACT,
            status=LogConstants.Status.SUCCESS,
            message=LogConstants.Message.EXTRACT_COMPLETE,
            records_processed=extracted_count,
            records_success=extracted_count
        )
        
        # TRANSFORM phase
        logger.log_message(
            process_step=LogConstants.Step.TRANSFORM,
            status=LogConstants.Status.INFO,
            message=LogConstants.Message.TRANSFORM_START
        )
        
        # Simulate transformation with some errors
        transformed_success = 950
        transformed_error = 50
        logger.log_message(
            process_step=LogConstants.Step.TRANSFORM,
            status=LogConstants.Status.WARNING,
            message=LogConstants.Message.TRANSFORM_COMPLETE,
            records_processed=extracted_count,
            records_success=transformed_success,
            records_error=transformed_error
        )
        
        # LOAD phase
        logger.log_message(
            process_step=LogConstants.Step.LOAD,
            status=LogConstants.Status.INFO,
            message=LogConstants.Message.LOAD_START
        )
        
        logger.log_message(
            process_step=LogConstants.Step.LOAD,
            status=LogConstants.Status.SUCCESS,
            message=LogConstants.Message.LOAD_COMPLETE,
            records_processed=transformed_success,
            records_success=transformed_success
        )
        
        # COMPLETE phase
        logger.log_message(
            process_step=LogConstants.Step.COMPLETE,
            status=LogConstants.Status.SUCCESS,
            message=LogConstants.Message.ETL_COMPLETE
        )
        
        # Flush all logs
        logger.flush_logs()
        
        # Verify audit trail
        audit_df = spark.read.format("delta").load(test_env["log_path"])
        assert audit_df.count() == 7  # All logged steps
        
        # Verify ETL run ID consistency
        assert audit_df.filter(audit_df.etl_run_id != "INTEGRATION_001").count() == 0
        
        # Verify step progression
        steps = [row.process_step for row in audit_df.orderBy("execution_timestamp").collect()]
        expected_steps = ["INIT", "EXTRACT", "EXTRACT", "TRANSFORM", "TRANSFORM", "LOAD", "LOAD", "COMPLETE"]
        assert steps == expected_steps
    
    def test_etl_with_error_handling(self, spark, test_env):
        """Test ETL workflow with error handling and logging"""
        logger = ETLLogger(spark, "INTEGRATION_002", test_env["log_path"])
        
        try:
            # Start ETL
            logger.log_message(
                process_step=LogConstants.Step.INIT,
                status=LogConstants.Status.SUCCESS,
                message="Starting ETL with potential errors"
            )
            
            # Simulate error in extraction
            logger.log_message(
                process_step=LogConstants.Step.EXTRACT,
                status=LogConstants.Status.ERROR,
                message="Database connection failed",
                records_processed=0,
                records_error=0
            )
            
            # Log error state
            logger.log_message(
                process_step=LogConstants.Step.ERROR,
                status=LogConstants.Status.ERROR,
                message=LogConstants.Message.ETL_ERROR
            )
            
        finally:
            logger.flush_logs()
        
        # Verify error was logged
        audit_df = spark.read.format("delta").load(test_env["log_path"])
        error_logs = audit_df.filter(audit_df.status == "E")
        assert error_logs.count() == 2
    
    def test_concurrent_etl_runs(self, spark, test_env):
        """Test multiple concurrent ETL runs logging to same Delta table"""
        logger1 = ETLLogger(spark, "CONCURRENT_001", test_env["log_path"])
        logger2 = ETLLogger(spark, "CONCURRENT_002", test_env["log_path"])
        
        # Log from first run
        logger1.log_message(
            process_step=LogConstants.Step.EXTRACT,
            status=LogConstants.Status.SUCCESS,
            message="Run 1 extraction",
            records_processed=500
        )
        logger1.flush_logs()
        
        # Log from second run
        logger2.log_message(
            process_step=LogConstants.Step.EXTRACT,
            status=LogConstants.Status.SUCCESS,
            message="Run 2 extraction",
            records_processed=750
        )
        logger2.flush_logs()
        
        # Verify both runs are logged
        audit_df = spark.read.format("delta").load(test_env["log_path"])
        assert audit_df.count() == 2
        
        # Verify run separation
        run1_logs = audit_df.filter(audit_df.etl_run_id == "CONCURRENT_001")
        run2_logs = audit_df.filter(audit_df.etl_run_id == "CONCURRENT_002")
        
        assert run1_logs.count() == 1
        assert run2_logs.count() == 1
        
        # Verify metrics
        run1_metrics = run1_logs.select("records_processed").collect()[0][0]
        run2_metrics = run2_logs.select("records_processed").collect()[0][0]
        
        assert run1_metrics == 500
        assert run2_metrics == 750
    
    def test_summary_statistics(self, spark, test_env):
        """Test generating summary statistics from audit trail"""
        logger = ETLLogger(spark, "SUMMARY_001", test_env["log_path"])
        
        # Log complete ETL cycle with various metrics
        steps = [
            ("EXTRACT", 1000, 1000, 0),
            ("TRANSFORM", 1000, 950, 50),
            ("LOAD", 950, 950, 0)
        ]
        
        for step, processed, success, errors in steps:
            logger.log_message(
                process_step=step,
                status=LogConstants.Status.SUCCESS if errors == 0 else LogConstants.Status.WARNING,
                message=f"{step} completed",
                records_processed=processed,
                records_success=success,
                records_error=errors
            )
        
        logger.flush_logs()
        
        # Get summary
        summary = logger.get_run_summary()
        
        # Verify aggregations
        summary_data = summary.collect()
        assert len(summary_data) >= 2  # At least SUCCESS and WARNING statuses


class TestConfigurationIntegration:
    """Test logger with configuration from YAML"""
    
    def test_logger_with_config(self, spark, test_env):
        """Test logger initialization from config file"""
        # Create test config
        config = {
            "delta": {
                "log_table_path": test_env["log_path"]
            },
            "prefixes": {
                "etl_run": "ETL",
                "log_id": "LOG"
            }
        }
        
        config_path = f"{test_env['base_dir']}/config.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        
        # Load config and create logger
        with open(config_path, 'r') as f:
            loaded_config = yaml.safe_load(f)
        
        logger = ETLLogger(
            spark,
            "CONFIG_TEST_001",
            loaded_config["delta"]["log_table_path"]
        )
        
        # Test logging
        logger.log_message(
            process_step=LogConstants.Step.INIT,
            status=LogConstants.Status.SUCCESS,
            message="Config-based initialization"
        )
        logger.flush_logs()
        
        # Verify
        df = spark.read.format("delta").load(test_env["log_path"])
        assert df.count() == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])