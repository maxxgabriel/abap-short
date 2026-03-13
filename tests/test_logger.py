"""
Unit tests for ETL Logger.
"""

import pytest
from pyspark.sql import SparkSession
from datetime import datetime
import time
from src.logger import ETLLogger, generate_etl_run_id
from src.utils import load_config


@pytest.fixture(scope="session")
def spark():
    """Create SparkSession for testing."""
    spark = SparkSession.builder \
        .appName("ETL_Logger_Test") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture
def config():
    """Load test configuration."""
    return {
        'database': {
            'type': 'delta',
            'delta_log_path': '/tmp/test_zetl_log',
            'log_table': 'ZETL_LOG'
        },
        'user': 'test_user',
        'status_codes': {
            'success': 'S',
            'error': 'E',
            'warning': 'W',
            'info': 'I'
        },
        'process_steps': {
            'init': 'INIT',
            'extract': 'EXTRACT',
            'transform': 'TRANSFORM',
            'load': 'LOAD',
            'complete': 'COMPLETE',
            'error': 'ERROR'
        }
    }


@pytest.fixture
def etl_run_id():
    """Generate test ETL run ID."""
    return generate_etl_run_id()


@pytest.fixture
def logger(spark, etl_run_id, config):
    """Create ETLLogger instance for testing."""
    return ETLLogger(spark, etl_run_id, config)


class TestETLRunID:
    """Test ETL run ID generation."""

    def test_generate_etl_run_id_format(self):
        """Test ETL run ID format."""
        run_id = generate_etl_run_id()
        
        assert run_id.startswith('ETL')
        assert len(run_id) == 19  # ETL + 14 digits + 2 microsecond digits
        assert run_id[3:].isdigit()

    def test_generate_etl_run_id_uniqueness(self):
        """Test that generated run IDs are unique."""
        run_id1 = generate_etl_run_id()
        time.sleep(0.001)  # Small delay to ensure different timestamp
        run_id2 = generate_etl_run_id()
        
        assert run_id1 != run_id2


class TestLogIDGeneration:
    """Test log ID generation."""

    def test_generate_log_id_format(self, logger):
        """Test log ID format."""
        log_id = logger.generate_log_id()
        
        assert log_id.startswith('LOG')
        assert len(log_id) == 19  # LOG + 14 digits + 2 microsecond digits
        assert log_id[3:].isdigit()

    def test_generate_log_id_uniqueness(self, logger):
        """Test that generated log IDs are unique."""
        log_id1 = logger.generate_log_id()
        time.sleep(0.001)
        log_id2 = logger.generate_log_id()
        
        assert log_id1 != log_id2

    def test_generate_log_id_timestamp_accuracy(self, logger):
        """Test that log ID timestamp is accurate."""
        before = datetime.now()
        log_id = logger.generate_log_id()
        after = datetime.now()
        
        # Extract timestamp from log ID
        timestamp_str = log_id[3:17]  # 14 digits
        log_timestamp = datetime.strptime(timestamp_str, '%Y%m%d%H%M%S')
        
        assert before <= log_timestamp <= after


class TestLoggerInitialization:
    """Test logger initialization."""

    def test_logger_initialization(self, spark, etl_run_id, config):
        """Test logger can be initialized with required parameters."""
        logger = ETLLogger(spark, etl_run_id, config)
        
        assert logger.spark == spark
        assert logger.etl_run_id == etl_run_id
        assert logger.config == config

    def test_get_etl_run_id(self, logger, etl_run_id):
        """Test getting ETL run ID."""
        assert logger.get_etl_run_id() == etl_run_id


class TestLogMessage:
    """Test log message functionality."""

    def test_log_message_basic(self, logger, spark):
        """Test basic log message creation."""
        logger.log_message(
            step='EXTRACT',
            status='S',
            message='Test message'
        )
        
        # Verify log was written to Delta
        df = spark.read.format("delta").load("/tmp/test_zetl_log")
        assert df.count() >= 1
        
        latest_log = df.orderBy(df.created_at.desc()).first()
        assert latest_log['process_step'] == 'EXTRACT'
        assert latest_log['status'] == 'S'
        assert latest_log['message'] == 'Test message'

    def test_log_message_with_counts(self, logger, spark):
        """Test log message with record counts."""
        logger.log_message(
            step='TRANSFORM',
            status='S',
            message='Transformation complete',
            records_processed=100,
            records_success=95,
            records_error=5
        )
        
        df = spark.read.format("delta").load("/tmp/test_zetl_log")
        latest_log = df.orderBy(df.created_at.desc()).first()
        
        assert latest_log['records_processed'] == 100
        assert latest_log['records_success'] == 95
        assert latest_log['records_error'] == 5

    def test_log_message_metadata(self, logger, spark, etl_run_id):
        """Test log message includes correct metadata."""
        logger.log_message(
            step='LOAD',
            status='S',
            message='Load complete'
        )
        
        df = spark.read.format("delta").load("/tmp/test_zetl_log")
        latest_log = df.orderBy(df.created_at.desc()).first()
        
        assert latest_log['log_id'].startswith('LOG')
        assert latest_log['etl_run_id'] == etl_run_id
        assert latest_log['execution_date'] is not None
        assert latest_log['execution_time'] is not None
        assert latest_log['created_by'] == 'test_user'

    def test_log_message_truncation(self, logger, spark):
        """Test that long messages are truncated."""
        long_message = 'A' * 300
        logger.log_message(
            step='ERROR',
            status='E',
            message=long_message
        )
        
        df = spark.read.format("delta").load("/tmp/test_zetl_log")
        latest_log = df.orderBy(df.created_at.desc()).first()
        
        assert len(latest_log['message']) == 255

    def test_log_message_error_handling(self, logger):
        """Test that logging errors don't crash the system."""
        # Create logger with invalid configuration
        bad_config = {'database': {'type': 'invalid'}}
        bad_logger = ETLLogger(logger.spark, logger.etl_run_id, bad_config)
        
        # Should not raise exception
        bad_logger.log_message(
            step='TEST',
            status='E',
            message='This should not crash'
        )


class TestDatabaseConnectors:
    """Test different database connector types."""

    def test_delta_connector(self, spark, etl_run_id):
        """Test Delta Lake connector."""
        config = {
            'database': {
                'type': 'delta',
                'delta_log_path': '/tmp/test_delta_log'
            },
            'user': 'test_user'
        }
        
        logger = ETLLogger(spark, etl_run_id, config)
        logger.log_message('TEST', 'S', 'Test delta')
        
        df = spark.read.format("delta").load("/tmp/test_delta_log")
        assert df.count() >= 1

    @pytest.mark.skip(reason="Requires JDBC connection")
    def test_jdbc_connector(self, spark, etl_run_id):
        """Test JDBC connector (requires actual database)."""
        config = {
            'database': {
                'type': 'jdbc',
                'jdbc_url': 'jdbc:sap://localhost:30015',
                'user': 'test_user',
                'password': 'test_password',
                'driver': 'com.sap.db.jdbc.Driver',
                'log_table': 'ZETL_LOG'
            },
            'user': 'test_user'
        }
        
        logger = ETLLogger(spark, etl_run_id, config)
        logger.log_message('TEST', 'S', 'Test JDBC')


class TestLoggerIntegration:
    """Integration tests for logger."""

    def test_multiple_log_entries(self, logger, spark):
        """Test logging multiple entries."""
        steps = ['INIT', 'EXTRACT', 'TRANSFORM', 'LOAD', 'COMPLETE']
        
        for step in steps:
            logger.log_message(
                step=step,
                status='S',
                message=f'{step} completed'
            )
        
        df = spark.read.format("delta").load("/tmp/test_zetl_log")
        assert df.count() >= len(steps)

    def test_concurrent_logging(self, spark, config):
        """Test concurrent logging from multiple processes."""
        import concurrent.futures
        
        def log_entry(i):
            run_id = generate_etl_run_id()
            logger = ETLLogger(spark, run_id, config)
            logger.log_message('TEST', 'S', f'Concurrent log {i}')
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            list(executor.map(log_entry, range(10)))
        
        df = spark.read.format("delta").load("/tmp/test_zetl_log")
        assert df.count() >= 10

    def test_error_recovery(self, logger, spark):
        """Test that logger recovers from errors."""
        # Log successful entry
        logger.log_message('TEST', 'S', 'Before error')
        
        # Simulate error condition
        try:
            logger._insert_via_jdbc({})  # This will fail
        except:
            pass
        
        # Log should still work
        logger.log_message('TEST', 'S', 'After error')
        
        df = spark.read.format("delta").load("/tmp/test_zetl_log")
        assert df.count() >= 2