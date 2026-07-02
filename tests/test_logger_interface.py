"""
Unit tests for ETL Logger Interface and Enums
"""
import pytest
from datetime import datetime
from unittest.mock import Mock, patch

from src.logger_interface import (
    LogStatus,
    ProcessStep,
    ETLComponentStatus,
    SaleCategory,
    ETLConstants,
    IETLLogger,
    ETLLoggerProtocol
)
from src.logger_impl import ETLLogger, verify_logger_protocol


class TestEnums:
    """Test enum definitions"""
    
    def test_log_status_values(self):
        """Test LogStatus enum values match ABAP constants"""
        assert LogStatus.SUCCESS.value == "S"
        assert LogStatus.ERROR.value == "E"
        assert LogStatus.WARNING.value == "W"
        assert LogStatus.INFO.value == "I"
    
    def test_process_step_values(self):
        """Test ProcessStep enum values match ABAP constants"""
        assert ProcessStep.INIT.value == "INIT"
        assert ProcessStep.EXTRACT.value == "EXTRACT"
        assert ProcessStep.TRANSFORM.value == "TRANSFORM"
        assert ProcessStep.LOAD.value == "LOAD"
        assert ProcessStep.VALIDATE.value == "VALIDATE"
        assert ProcessStep.COMPLETE.value == "COMPLETE"
        assert ProcessStep.ERROR.value == "ERROR"
    
    def test_component_status_values(self):
        """Test ETLComponentStatus enum values"""
        assert ETLComponentStatus.NEW.value == "N"
        assert ETLComponentStatus.PROCESSED.value == "P"
        assert ETLComponentStatus.ERROR.value == "E"
        assert ETLComponentStatus.WARNING.value == "W"
        assert ETLComponentStatus.SUCCESS.value == "S"
        assert ETLComponentStatus.INFO.value == "I"
    
    def test_sale_category_values(self):
        """Test SaleCategory enum values"""
        assert SaleCategory.HIGH.value == "HIGH"
        assert SaleCategory.MEDIUM.value == "MEDIUM"
        assert SaleCategory.LOW.value == "LOW"
    
    def test_enum_membership(self):
        """Test enum membership checks"""
        assert LogStatus.SUCCESS in LogStatus
        assert ProcessStep.EXTRACT in ProcessStep
        assert SaleCategory.HIGH in SaleCategory


class TestETLConstants:
    """Test ETL constants class"""
    
    def test_discount_constants(self):
        """Test discount-related constants"""
        assert ETLConstants.DISCOUNT_QTY_TIER1 == 10
        assert ETLConstants.DISCOUNT_QTY_TIER2 == 15
        assert ETLConstants.DISCOUNT_RATE_TIER1 == 0.05
        assert ETLConstants.DISCOUNT_RATE_TIER2 == 0.10
    
    def test_tax_constants(self):
        """Test tax constants"""
        assert ETLConstants.TAX_RATE == 0.08
    
    def test_cost_constants(self):
        """Test cost constants"""
        assert ETLConstants.COST_RATIO == 0.60
    
    def test_category_thresholds(self):
        """Test category threshold constants"""
        assert ETLConstants.CATEGORY_HIGH_THRESHOLD == 2000.00
        assert ETLConstants.CATEGORY_MEDIUM_THRESHOLD == 500.00
    
    def test_etl_config_defaults(self):
        """Test ETL configuration defaults"""
        assert ETLConstants.DEFAULT_BATCH_SIZE == 1000
        assert ETLConstants.DEFAULT_COMMIT_INTERVAL == 500
        assert ETLConstants.DEFAULT_RETRY_ATTEMPTS == 3
        assert ETLConstants.DEFAULT_TIMEOUT_SECONDS == 3600
    
    def test_id_prefixes(self):
        """Test ID prefix constants"""
        assert ETLConstants.PREFIX_ETL_RUN == "ETL"
        assert ETLConstants.PREFIX_LOG_ID == "LOG"
        assert ETLConstants.PREFIX_ANALYTICS_ID == "ANL"
    
    def test_message_texts(self):
        """Test message text constants"""
        assert ETLConstants.MSG_INIT_SUCCESS == "ETL process initialized successfully"
        assert ETLConstants.MSG_EXTRACT_START == "Starting data extraction"
        assert ETLConstants.MSG_ETL_COMPLETE == "ETL process completed successfully"


class TestETLLoggerInterface:
    """Test ETL logger interface and protocol"""
    
    def test_abstract_methods(self):
        """Test that interface defines abstract methods"""
        with pytest.raises(TypeError):
            # Cannot instantiate abstract class
            IETLLogger()
    
    def test_protocol_verification(self):
        """Test protocol verification function"""
        # Mock logger that implements protocol
        mock_logger = Mock(spec=ETLLoggerProtocol)
        mock_logger.log_message = Mock()
        mock_logger.get_etl_run_id = Mock(return_value="ETL123")
        
        assert verify_logger_protocol(mock_logger)
    
    def test_protocol_method_signatures(self):
        """Test protocol method signatures"""
        mock_logger = Mock(spec=ETLLoggerProtocol)
        
        # Verify method exists
        assert hasattr(mock_logger, 'log_message')
        assert hasattr(mock_logger, 'get_etl_run_id')


class TestETLLoggerImplementation:
    """Test concrete ETL logger implementation"""
    
    @pytest.fixture
    def etl_run_id(self):
        """Fixture for ETL run ID"""
        return "ETL20240101120000"
    
    @pytest.fixture
    def logger(self, etl_run_id):
        """Fixture for logger instance"""
        return ETLLogger(etl_run_id=etl_run_id, spark=None)
    
    def test_logger_initialization(self, logger, etl_run_id):
        """Test logger initialization"""
        assert logger.get_etl_run_id() == etl_run_id
        assert logger.get_log_entries() == []
    
    def test_log_message_basic(self, logger):
        """Test basic log message creation"""
        logger.log_message(
            step=ProcessStep.INIT,
            status=LogStatus.SUCCESS,
            message="Test message"
        )
        
        entries = logger.get_log_entries()
        assert len(entries) == 1
        assert entries[0]["process_step"] == "INIT"
        assert entries[0]["status"] == "S"
        assert entries[0]["message"] == "Test message"
    
    def test_log_message_with_counts(self, logger):
        """Test log message with record counts"""
        logger.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="Extraction complete",
            records_processed=100,
            records_success=95,
            records_error=5
        )
        
        entries = logger.get_log_entries()
        assert len(entries) == 1
        assert entries[0]["records_processed"] == 100
        assert entries[0]["records_success"] == 95
        assert entries[0]["records_error"] == 5
    
    def test_log_id_generation(self, logger):
        """Test unique log ID generation"""
        logger.log_message(
            step=ProcessStep.EXTRACT,
            status=LogStatus.SUCCESS,
            message="Message 1"
        )
        
        logger.log_message(
            step=ProcessStep.TRANSFORM,
            status=LogStatus.SUCCESS,
            message="Message 2"
        )
        
        entries = logger.get_log_entries()
        assert len(entries) == 2
        assert entries[0]["log_id"] != entries[1]["log_id"]
        assert entries[0]["log_id"].startswith("LOG")
        assert entries[1]["log_id"].startswith("LOG")
    
    def test_multiple_log_entries(self, logger):
        """Test multiple log entries accumulation"""
        steps = [
            ProcessStep.INIT,
            ProcessStep.EXTRACT,
            ProcessStep.TRANSFORM,
            ProcessStep.LOAD,
            ProcessStep.COMPLETE
        ]
        
        for step in steps:
            logger.log_message(
                step=step,
                status=LogStatus.SUCCESS,
                message=f"{step.value} completed"
            )
        
        entries = logger.get_log_entries()
        assert len(entries) == 5
        assert [e["process_step"] for e in entries] == [s.value for s in steps]
    
    def test_timestamp_fields(self, logger):
        """Test timestamp field population"""
        logger.log_message(
            step=ProcessStep.INIT,
            status=LogStatus.SUCCESS,
            message="Test"
        )
        
        entry = logger.get_log_entries()[0]
        assert "execution_date" in entry
        assert "execution_time" in entry
        assert "timestamp" in entry
        
        # Verify format
        assert len(entry["execution_date"]) == 10  # YYYY-MM-DD
        assert len(entry["execution_time"]) == 8   # HH:MM:SS
    
    def test_etl_run_id_consistency(self, logger, etl_run_id):
        """Test ETL run ID is consistent across log entries"""
        for i in range(3):
            logger.log_message(
                step=ProcessStep.EXTRACT,
                status=LogStatus.SUCCESS,
                message=f"Message {i}"
            )
        
        entries = logger.get_log_entries()
        for entry in entries:
            assert entry["etl_run_id"] == etl_run_id
    
    def test_error_logging(self, logger):
        """Test error status logging"""
        logger.log_message(
            step=ProcessStep.TRANSFORM,
            status=LogStatus.ERROR,
            message="Transformation failed",
            records_processed=100,
            records_success=50,
            records_error=50
        )
        
        entry = logger.get_log_entries()[0]
        assert entry["status"] == "E"
        assert entry["records_error"] == 50
    
    def test_warning_logging(self, logger):
        """Test warning status logging"""
        logger.log_message(
            step=ProcessStep.VALIDATE,
            status=LogStatus.WARNING,
            message="Validation warning",
            records_processed=10,
            records_success=9,
            records_error=1
        )
        
        entry = logger.get_log_entries()[0]
        assert entry["status"] == "W"
        assert entry["message"] == "Validation warning"
    
    @patch('builtins.print')
    def test_console_output(self, mock_print, logger):
        """Test console output is generated"""
        logger.log_message(
            step=ProcessStep.INIT,
            status=LogStatus.SUCCESS,
            message="Test message"
        )
        
        # Verify print was called
        assert mock_print.called
    
    def test_protocol_compliance(self, logger):
        """Test logger implements protocol"""
        assert isinstance(logger, IETLLogger)
        assert verify_logger_protocol(logger)


class TestLoggerWithSpark:
    """Test logger with Spark integration"""
    
    @pytest.fixture
    def mock_spark(self):
        """Fixture for mock Spark session"""
        spark = Mock()
        spark.createDataFrame = Mock(return_value=Mock())
        return spark
    
    def test_logger_with_spark_session(self, mock_spark):
        """Test logger initialization with Spark"""
        logger = ETLLogger(
            etl_run_id="ETL123",
            spark=mock_spark
        )
        
        assert logger._spark is not None
    
    @patch('src.logger_impl.ETLLogger._persist_log')
    def test_persistence_called_with_spark(self, mock_persist, mock_spark):
        """Test log persistence is called when Spark is available"""
        logger = ETLLogger(
            etl_run_id="ETL123",
            spark=mock_spark
        )
        
        logger.log_message(
            step=ProcessStep.INIT,
            status=LogStatus.SUCCESS,
            message="Test"
        )
        
        assert mock_persist.called


class TestEnumUsage:
    """Test enum usage in practical scenarios"""
    
    def test_status_iteration(self):
        """Test iterating over status enum"""
        statuses = [status for status in LogStatus]
        assert len(statuses) == 4
        assert LogStatus.SUCCESS in statuses
    
    def test_process_step_comparison(self):
        """Test process step comparison"""
        step1 = ProcessStep.EXTRACT
        step2 = ProcessStep.EXTRACT
        step3 = ProcessStep.TRANSFORM
        
        assert step1 == step2
        assert step1 != step3
    
    def test_enum_value_access(self):
        """Test accessing enum values"""
        assert LogStatus.SUCCESS.value == "S"
        assert ProcessStep.LOAD.value == "LOAD"
        assert SaleCategory.HIGH.value == "HIGH"
    
    def test_enum_from_value(self):
        """Test creating enum from value"""
        status = LogStatus("S")
        assert status == LogStatus.SUCCESS
        
        step = ProcessStep("EXTRACT")
        assert step == ProcessStep.EXTRACT