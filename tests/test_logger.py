"""
Unit tests for ETL Logger module
Tests dataclass functionality and DataFrame persistence
"""
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import tempfile
import shutil
import os

from src.logger import ETLLogger, LogEntry, create_logger


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession for testing"""
    spark = SparkSession.builder \
        .appName("ETL_Logger_Test") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def test_config():
    """Create test configuration"""
    return {
        'log_level': 'INFO',
        'log_format': '%(asctime)s - %(levelname)s - %(message)s',
        'log_output_path': '/tmp/test_etl_logs',
        'log_write_mode': 'overwrite'
    }


@pytest.fixture
def temp_dir():
    """Create temporary directory for test outputs"""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path)


class TestLogEntry:
    """Test LogEntry dataclass"""
    
    def test_log_entry_creation(self):
        """Test creating a log entry"""
        entry = LogEntry(
            log_id="LOG001",
            etl_run_id="ETL001",
            execution_date=datetime.now(),
            execution_time=datetime.now(),
            process_step="EXTRACT",
            status="S",
            records_processed=100,
            records_success=95,
            records_error=5,
            message="Test message"
        )
        
        assert entry.log_id == "LOG001"
        assert entry.etl_run_id == "ETL001"
        assert entry.process_step == "EXTRACT"
        assert entry.status == "S"
        assert entry.records_processed == 100
        assert entry.records_success == 95
        assert entry.records_error == 5
        assert entry.message == "Test message"
    
    def test_log_entry_defaults(self):
        """Test default values in log entry"""
        entry = LogEntry(
            log_id="LOG001",
            etl_run_id="ETL001",
            execution_date=datetime.now(),
            execution_time=datetime.now(),
            process_step="EXTRACT",
            status="S"
        )
        
        assert entry.records_processed == 0
        assert entry.records_success == 0
        assert entry.records_error == 0
        assert entry.message == ""
        assert entry.created_by == "etl_system"
    
    def test_log_entry_to_dict(self):
        """Test converting log entry to dictionary"""
        now = datetime.now()
        entry = LogEntry(
            log_id="LOG001",
            etl_run_id="ETL001",
            execution_date=now,
            execution_time=now,
            process_step="EXTRACT",
            status="S",
            message="Test"
        )
        
        entry_dict = entry.to_dict()
        
        assert isinstance(entry_dict, dict)
        assert entry_dict['log_id'] == "LOG001"
        assert entry_dict['process_step'] == "EXTRACT"
        assert 'execution_date' in entry_dict
        assert 'created_at' in entry_dict


class TestETLLogger:
    """Test ETLLogger class"""
    
    def test_logger_initialization(self, spark, test_config):
        """Test logger initialization"""
        logger = ETLLogger("ETL001", spark, test_config)
        
        assert logger.etl_run_id == "ETL001"
        assert logger.spark == spark
        assert logger.config == test_config
        assert len(logger.log_entries) == 0
    
    def test_generate_log_id(self, spark, test_config):
        """Test unique log ID generation"""
        logger = ETLLogger("ETL001", spark, test_config)
        
        log_id1 = logger._generate_log_id()
        log_id2 = logger._generate_log_id()
        
        assert log_id1.startswith("LOG")
        assert log_id2.startswith("LOG")
        assert log_id1 != log_id2
    
    def test_log_message(self, spark, test_config):
        """Test logging a message"""
        logger = ETLLogger("ETL001", spark, test_config)
        
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Extraction completed",
            records_processed=100,
            records_success=98,
            records_error=2
        )
        
        assert len(logger.log_entries) == 1
        entry = logger.log_entries[0]
        
        assert entry.process_step == ETLLogger.STEP_EXTRACT
        assert entry.status == ETLLogger.STATUS_SUCCESS
        assert entry.records_processed == 100
        assert entry.records_success == 98
        assert entry.records_error == 2
        assert "Extraction completed" in entry.message
    
    def test_multiple_log_messages(self, spark, test_config):
        """Test logging multiple messages"""
        logger = ETLLogger("ETL001", spark, test_config)
        
        logger.log_message(
            step=ETLLogger.STEP_INIT,
            status=ETLLogger.STATUS_INFO,
            message="Starting ETL"
        )
        
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Data extracted",
            records_processed=100
        )
        
        logger.log_message(
            step=ETLLogger.STEP_TRANSFORM,
            status=ETLLogger.STATUS_SUCCESS,
            message="Data transformed",
            records_processed=100
        )
        
        assert len(logger.log_entries) == 3
        assert logger.log_entries[0].process_step == ETLLogger.STEP_INIT
        assert logger.log_entries[1].process_step == ETLLogger.STEP_EXTRACT
        assert logger.log_entries[2].process_step == ETLLogger.STEP_TRANSFORM
    
    def test_get_etl_run_id(self, spark, test_config):
        """Test getting ETL run ID"""
        logger = ETLLogger("ETL123", spark, test_config)
        assert logger.get_etl_run_id() == "ETL123"
    
    def test_get_log_entries_as_dataframe_empty(self, spark, test_config):
        """Test getting empty DataFrame"""
        logger = ETLLogger("ETL001", spark, test_config)
        df = logger.get_log_entries_as_dataframe()
        
        assert df.count() == 0
        assert 'log_id' in df.columns
        assert 'etl_run_id' in df.columns
        assert 'process_step' in df.columns
    
    def test_get_log_entries_as_dataframe_with_data(self, spark, test_config):
        """Test getting DataFrame with data"""
        logger = ETLLogger("ETL001", spark, test_config)
        
        # Add some log entries
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Test 1",
            records_processed=50
        )
        
        logger.log_message(
            step=ETLLogger.STEP_TRANSFORM,
            status=ETLLogger.STATUS_SUCCESS,
            message="Test 2",
            records_processed=50
        )
        
        df = logger.get_log_entries_as_dataframe()
        
        assert df.count() == 2
        assert df.filter("process_step = 'EXTRACT'").count() == 1
        assert df.filter("process_step = 'TRANSFORM'").count() == 1
        
        # Verify data types
        assert dict(df.dtypes)['records_processed'] == 'int'
        assert dict(df.dtypes)['log_id'] == 'string'
    
    def test_persist_logs(self, spark, test_config, temp_dir):
        """Test persisting logs to storage"""
        test_config['log_output_path'] = temp_dir
        logger = ETLLogger("ETL001", spark, test_config)
        
        # Add log entries
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Test message"
        )
        
        # Persist logs
        logger.persist_logs()
        
        # Verify files were created
        assert os.path.exists(temp_dir)
        
        # Read back and verify
        df_read = spark.read.parquet(temp_dir)
        assert df_read.count() == 1
    
    def test_get_summary_statistics_empty(self, spark, test_config):
        """Test summary statistics with no logs"""
        logger = ETLLogger("ETL001", spark, test_config)
        stats = logger.get_summary_statistics()
        
        assert stats['total_entries'] == 0
        assert stats['success_count'] == 0
        assert stats['error_count'] == 0
        assert stats['warning_count'] == 0
    
    def test_get_summary_statistics_with_data(self, spark, test_config):
        """Test summary statistics with log data"""
        logger = ETLLogger("ETL001", spark, test_config)
        
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Success",
            records_processed=100,
            records_success=95,
            records_error=5
        )
        
        logger.log_message(
            step=ETLLogger.STEP_TRANSFORM,
            status=ETLLogger.STATUS_ERROR,
            message="Error",
            records_processed=50,
            records_error=50
        )
        
        logger.log_message(
            step=ETLLogger.STEP_LOAD,
            status=ETLLogger.STATUS_WARNING,
            message="Warning",
            records_processed=30,
            records_success=25,
            records_error=5
        )
        
        stats = logger.get_summary_statistics()
        
        assert stats['total_entries'] == 3
        assert stats['success_count'] == 1
        assert stats['error_count'] == 1
        assert stats['warning_count'] == 1
        assert stats['total_records_processed'] == 180
        assert stats['total_records_success'] == 120
        assert stats['total_records_error'] == 60
    
    def test_clear_logs(self, spark, test_config):
        """Test clearing log entries"""
        logger = ETLLogger("ETL001", spark, test_config)
        
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Test"
        )
        
        assert len(logger.log_entries) == 1
        
        logger.clear_logs()
        
        assert len(logger.log_entries) == 0
        assert logger._log_counter == 0
    
    def test_status_constants(self):
        """Test status constants match ABAP"""
        assert ETLLogger.STATUS_SUCCESS == 'S'
        assert ETLLogger.STATUS_ERROR == 'E'
        assert ETLLogger.STATUS_WARNING == 'W'
        assert ETLLogger.STATUS_INFO == 'I'
        assert ETLLogger.STATUS_NEW == 'N'
        assert ETLLogger.STATUS_PROCESSED == 'P'
    
    def test_step_constants(self):
        """Test step constants match ABAP"""
        assert ETLLogger.STEP_INIT == 'INIT'
        assert ETLLogger.STEP_EXTRACT == 'EXTRACT'
        assert ETLLogger.STEP_TRANSFORM == 'TRANSFORM'
        assert ETLLogger.STEP_LOAD == 'LOAD'
        assert ETLLogger.STEP_VALIDATE == 'VALIDATE'
        assert ETLLogger.STEP_COMPLETE == 'COMPLETE'
        assert ETLLogger.STEP_ERROR == 'ERROR'


class TestFactoryFunction:
    """Test factory function"""
    
    def test_create_logger(self, spark, test_config):
        """Test creating logger via factory function"""
        logger = create_logger("ETL999", spark, test_config)
        
        assert isinstance(logger, ETLLogger)
        assert logger.get_etl_run_id() == "ETL999"
        assert logger.spark == spark
        assert logger.config == test_config


class TestIntegration:
    """Integration tests"""
    
    def test_full_logging_workflow(self, spark, test_config, temp_dir):
        """Test complete logging workflow"""
        test_config['log_output_path'] = temp_dir
        
        # Create logger
        logger = create_logger("ETL_INTEGRATION_001", spark, test_config)
        
        # Simulate ETL process logging
        logger.log_message(
            step=ETLLogger.STEP_INIT,
            status=ETLLogger.STATUS_INFO,
            message="ETL process initialized"
        )
        
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Extracted data from source",
            records_processed=1000,
            records_success=1000
        )
        
        logger.log_message(
            step=ETLLogger.STEP_TRANSFORM,
            status=ETLLogger.STATUS_SUCCESS,
            message="Transformed data",
            records_processed=1000,
            records_success=980,
            records_error=20
        )
        
        logger.log_message(
            step=ETLLogger.STEP_LOAD,
            status=ETLLogger.STATUS_SUCCESS,
            message="Loaded data to target",
            records_processed=980,
            records_success=980
        )
        
        logger.log_message(
            step=ETLLogger.STEP_COMPLETE,
            status=ETLLogger.STATUS_SUCCESS,
            message="ETL process completed successfully"
        )
        
        # Get statistics
        stats = logger.get_summary_statistics()
        assert stats['total_entries'] == 5
        assert stats['success_count'] == 4
        
        # Get DataFrame
        df = logger.get_log_entries_as_dataframe()
        assert df.count() == 5
        
        # Persist logs
        logger.persist_logs()
        
        # Verify persistence
        df_read = spark.read.parquet(temp_dir)
        assert df_read.count() == 5
        
        # Verify all steps are present
        steps = [row.process_step for row in df_read.collect()]
        assert ETLLogger.STEP_INIT in steps
        assert ETLLogger.STEP_EXTRACT in steps
        assert ETLLogger.STEP_TRANSFORM in steps
        assert ETLLogger.STEP_LOAD in steps
        assert ETLLogger.STEP_COMPLETE in steps