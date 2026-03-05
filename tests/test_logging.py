"""
Unit Tests for ETL Logging Utilities

Tests the logging, validation, and formatting utility functions.
"""

import pytest
from datetime import datetime
from src.utils.logging import (
    ETLLogger,
    log_etl_message,
    log_etl_statistics,
    generate_unique_id,
    calculate_percentage
)


class TestETLLogger:
    """Test suite for ETLLogger class."""
    
    def test_logger_initialization(self):
        """Test logger initialization."""
        etl_run_id = "ETL20240101120000"
        logger = ETLLogger(etl_run_id)
        
        assert logger.etl_run_id == etl_run_id
        assert logger.logger is not None
    
    def test_log_message(self, caplog):
        """Test basic message logging."""
        logger = ETLLogger("TEST001")
        
        logger.log_message(
            step="TEST",
            status="S",
            message="Test message"
        )
        
        assert "Test message" in caplog.text
        assert "TEST" in caplog.text
    
    def test_log_statistics(self, caplog):
        """Test statistics logging."""
        logger = ETLLogger("TEST002")
        
        logger.log_statistics(
            step="TRANSFORM",
            status="S",
            records_processed=100,
            records_success=95,
            records_error=5,
            message="Transformation complete"
        )
        
        assert "Processed: 100" in caplog.text
        assert "Success: 95" in caplog.text
        assert "Errors: 5" in caplog.text
    
    def test_get_etl_run_id(self):
        """Test ETL run ID retrieval."""
        run_id = "ETL20240101"
        logger = ETLLogger(run_id)
        
        assert logger.get_etl_run_id() == run_id
    
    def test_generate_log_id(self):
        """Test log ID generation."""
        log_id = ETLLogger.generate_log_id()
        
        assert log_id.startswith("LOG")
        assert len(log_id) > 10
    
    def test_generate_etl_run_id(self):
        """Test ETL run ID generation."""
        run_id = ETLLogger.generate_etl_run_id()
        
        assert run_id.startswith("ETL")
        assert len(run_id) == 17  # ETL + 14 digit timestamp


class TestLoggingUtilities:
    """Test suite for logging utility functions."""
    
    def test_log_etl_message(self, caplog):
        """Test log_etl_message utility."""
        logger = ETLLogger("TEST003")
        
        log_etl_message(
            logger=logger,
            step="EXTRACT",
            status="S",
            message="Extraction started"
        )
        
        assert "Extraction started" in caplog.text
    
    def test_log_etl_statistics(self, caplog):
        """Test log_etl_statistics utility."""
        logger = ETLLogger("TEST004")
        
        log_etl_statistics(
            logger=logger,
            step="LOAD",
            status="S",
            records_processed=50,
            records_success=48,
            records_error=2,
            message="Load complete"
        )
        
        assert "Load complete" in caplog.text
        assert "Processed: 50" in caplog.text
    
    def test_generate_unique_id(self):
        """Test unique ID generation."""
        id1 = generate_unique_id("TEST")
        id2 = generate_unique_id("TEST")
        
        assert id1.startswith("TEST")
        assert id2.startswith("TEST")
        assert id1 != id2  # Should be unique
    
    def test_calculate_percentage(self):
        """Test percentage calculation."""
        # Normal case
        result = calculate_percentage(25, 100)
        assert result == 25.0
        
        # Zero denominator
        result = calculate_percentage(10, 0)
        assert result == 0.0
        
        # Fractional result
        result = calculate_percentage(1, 3)
        assert abs(result - 33.333333) < 0.001


class TestValidation:
    """Test suite for validation utilities."""
    
    def test_validate_field(self):
        """Test field validation."""
        from src.utils.validation import validate_field
        
        # Valid cases
        assert validate_field("value") is True
        assert validate_field(123) is True
        assert validate_field(0) is True
        
        # Invalid cases
        assert validate_field(None) is False
        assert validate_field("") is False
        assert validate_field("   ") is False
    
    def test_validate_mandatory_fields(self):
        """Test mandatory fields validation."""
        from src.utils.validation import validate_mandatory_fields
        
        record = {
            "field1": "value1",
            "field2": "value2",
            "field3": "value3"
        }
        
        # Valid case
        is_valid, error = validate_mandatory_fields(
            record,
            ["field1", "field2"]
        )
        assert is_valid is True
        assert error is None
        
        # Missing field
        is_valid, error = validate_mandatory_fields(
            record,
            ["field1", "field4"]
        )
        assert is_valid is False
        assert "field4" in error
    
    def test_validate_numeric_field(self):
        """Test numeric field validation."""
        from src.utils.validation import validate_numeric_field
        
        # Valid number
        is_valid, error = validate_numeric_field(123.45, "amount")
        assert is_valid is True
        
        # With minimum value
        is_valid, error = validate_numeric_field(50, "amount", min_value=100)
        assert is_valid is False
        assert "below minimum" in error
        
        # Invalid number
        is_valid, error = validate_numeric_field("abc", "amount")
        assert is_valid is False
    
    def test_validate_category(self):
        """Test category validation."""
        from src.utils.validation import validate_category
        
        valid_categories = ["HIGH", "MEDIUM", "LOW"]
        
        # Valid category
        is_valid, error = validate_category("HIGH", valid_categories)
        assert is_valid is True
        
        # Invalid category
        is_valid, error = validate_category("INVALID", valid_categories)
        assert is_valid is False


class TestFormatting:
    """Test suite for formatting utilities."""
    
    def test_format_currency(self):
        """Test currency formatting."""
        from src.utils.formatting import format_currency
        
        result = format_currency(1234.56, "USD")
        assert "USD" in result
        assert "1,234.56" in result
    
    def test_format_percentage(self):
        """Test percentage formatting."""
        from src.utils.formatting import format_percentage
        
        result = format_percentage(15.555, decimals=2)
        assert result == "15.56%"
    
    def test_format_duration(self):
        """Test duration formatting."""
        from src.utils.formatting import format_duration
        
        # 1 hour, 23 minutes, 45 seconds
        result = format_duration(5025)
        assert "1h" in result
        assert "23m" in result
        assert "45s" in result
    
    def test_add_days_to_date(self):
        """Test date addition."""
        from src.utils.formatting import add_days_to_date
        
        result = add_days_to_date("2024-01-01", 7)
        assert result == "2024-01-08"


class TestErrorHandling:
    """Test suite for error handling utilities."""
    
    def test_etl_error(self):
        """Test ETL error creation."""
        from src.utils.error_handling import ETLError
        
        error = ETLError(
            message="Test error",
            error_step="EXTRACT",
            record_id="REC001"
        )
        
        assert "Test error" in str(error)
        assert "EXTRACT" in str(error)
        assert "REC001" in str(error)
    
    def test_extract_error(self):
        """Test ExtractError exception."""
        from src.utils.error_handling import ExtractError
        
        with pytest.raises(ExtractError):
            raise ExtractError("Extraction failed", error_step="EXTRACT")
    
    def test_transform_error(self):
        """Test TransformError exception."""
        from src.utils.error_handling import TransformError
        
        with pytest.raises(TransformError):
            raise TransformError("Transformation failed", error_step="TRANSFORM")
    
    def test_load_error(self):
        """Test LoadError exception."""
        from src.utils.error_handling import LoadError
        
        with pytest.raises(LoadError):
            raise LoadError("Load failed", error_step="LOAD")
    
    def test_handle_etl_error(self, caplog):
        """Test error handling function."""
        from src.utils.error_handling import handle_etl_error
        
        logger = ETLLogger("TEST005")
        error = Exception("Test error")
        
        result = handle_etl_error(error, logger, "TEST")
        
        assert result is False
        assert "Test error" in caplog.text


if __name__ == "__main__":
    pytest.main([__file__, "-v"])