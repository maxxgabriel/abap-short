"""
Unit tests for custom ETL exception hierarchy.

Tests cover:
- Exception instantiation and attributes
- Error context handling
- Exception inheritance
- Serialization methods
"""

import pytest
from datetime import datetime
from src.exceptions import (
    ETLError,
    ExtractError,
    TransformError,
    LoadError,
    ValidationError,
    ConfigurationError,
    ETLTimeoutError
)


class TestETLError:
    """Test suite for base ETLError exception."""
    
    def test_basic_error_creation(self):
        """Test creating basic ETL error."""
        error = ETLError(
            error_text="Test error",
            error_step="TEST"
        )
        
        assert error.error_text == "Test error"
        assert error.error_step == "TEST"
        assert error.record_id is None
        assert isinstance(error.timestamp, datetime)
        assert error.context == {}
    
    def test_error_with_record_id(self):
        """Test error with record ID."""
        error = ETLError(
            error_text="Record failed",
            error_step="PROCESS",
            record_id="T000001"
        )
        
        assert error.record_id == "T000001"
        assert "T000001" in str(error)
    
    def test_error_with_context(self):
        """Test error with additional context."""
        context = {
            "table": "sales_raw",
            "batch_id": "BATCH123"
        }
        error = ETLError(
            error_text="Context test",
            context=context
        )
        
        assert error.context["table"] == "sales_raw"
        assert error.context["batch_id"] == "BATCH123"
    
    def test_error_to_dict(self):
        """Test error serialization to dictionary."""
        error = ETLError(
            error_text="Serialization test",
            error_step="TEST",
            record_id="REC001",
            context={"key": "value"}
        )
        
        error_dict = error.to_dict()
        
        assert error_dict["error_type"] == "ETLError"
        assert error_dict["error_text"] == "Serialization test"
        assert error_dict["error_step"] == "TEST"
        assert error_dict["record_id"] == "REC001"
        assert "timestamp" in error_dict
        assert error_dict["context"]["key"] == "value"
    
    def test_error_string_representation(self):
        """Test error string formatting."""
        error = ETLError(
            error_text="String test",
            error_step="FORMAT",
            record_id="REC123"
        )
        
        error_str = str(error)
        assert "[FORMAT]" in error_str
        assert "String test" in error_str
        assert "(Record: REC123)" in error_str
    
    def test_error_default_step(self):
        """Test default error step."""
        error = ETLError(error_text="No step provided")
        assert error.error_step == "UNKNOWN"


class TestExtractError:
    """Test suite for ExtractError exception."""
    
    def test_extract_error_basic(self):
        """Test basic extraction error."""
        error = ExtractError(
            error_text="Failed to read source table",
            source="sales_raw"
        )
        
        assert error.error_text == "Failed to read source table"
        assert error.error_step == "EXTRACT"
        assert error.source == "sales_raw"
        assert error.context["source"] == "sales_raw"
    
    def test_extract_error_with_query(self):
        """Test extraction error with query information."""
        error = ExtractError(
            error_text="Query execution failed",
            source="sales_raw",
            query="SELECT * FROM sales_raw WHERE date > '2024-01-01'",
            record_id="T000001"
        )
        
        assert error.query is not None
        assert error.context["query"] == "SELECT * FROM sales_raw WHERE date > '2024-01-01'"
        assert error.record_id == "T000001"
    
    def test_extract_error_inheritance(self):
        """Test that ExtractError inherits from ETLError."""
        error = ExtractError(error_text="Test")
        assert isinstance(error, ETLError)
        assert isinstance(error, ExtractError)
    
    def test_extract_error_serialization(self):
        """Test extraction error serialization."""
        error = ExtractError(
            error_text="Source unavailable",
            source="database.sales",
            query="SELECT count(*)"
        )
        
        error_dict = error.to_dict()
        assert error_dict["error_type"] == "ExtractError"
        assert error_dict["error_step"] == "EXTRACT"


class TestTransformError:
    """Test suite for TransformError exception."""
    
    def test_transform_error_basic(self):
        """Test basic transformation error."""
        error = TransformError(
            error_text="Calculation failed",
            record_id="T000001"
        )
        
        assert error.error_text == "Calculation failed"
        assert error.error_step == "TRANSFORM"
        assert error.record_id == "T000001"
    
    def test_transform_error_with_field(self):
        """Test transformation error with field information."""
        error = TransformError(
            error_text="Invalid value for discount",
            record_id="T000001",
            field_name="discount_amount",
            field_value=-10.50
        )
        
        assert error.field_name == "discount_amount"
        assert error.field_value == -10.50
        assert error.context["field_name"] == "discount_amount"
        assert error.context["field_value"] == "-10.5"
    
    def test_transform_error_with_transformation(self):
        """Test transformation error with transformation name."""
        error = TransformError(
            error_text="Profit margin calculation failed",
            record_id="T000002",
            transformation="calculate_profit_margin",
            field_name="profit_margin"
        )
        
        assert error.transformation == "calculate_profit_margin"
        assert error.context["transformation"] == "calculate_profit_margin"
    
    def test_transform_error_inheritance(self):
        """Test that TransformError inherits from ETLError."""
        error = TransformError(error_text="Test")
        assert isinstance(error, ETLError)
        assert isinstance(error, TransformError)


class TestLoadError:
    """Test suite for LoadError exception."""
    
    def test_load_error_basic(self):
        """Test basic load error."""
        error = LoadError(
            error_text="Failed to insert record",
            target="sales_analytics"
        )
        
        assert error.error_text == "Failed to insert record"
        assert error.error_step == "LOAD"
        assert error.target == "sales_analytics"
        assert error.context["target"] == "sales_analytics"
    
    def test_load_error_with_operation(self):
        """Test load error with operation type."""
        error = LoadError(
            error_text="Constraint violation",
            target="sales_analytics",
            operation="INSERT",
            record_id="ANL123"
        )
        
        assert error.operation == "INSERT"
        assert error.context["operation"] == "INSERT"
        assert error.record_id == "ANL123"
    
    def test_load_error_with_constraint(self):
        """Test load error with constraint information."""
        error = LoadError(
            error_text="Primary key violation",
            target="sales_analytics",
            constraint="pk_analytics_id",
            record_id="ANL123"
        )
        
        assert error.constraint == "pk_analytics_id"
        assert error.context["constraint"] == "pk_analytics_id"
    
    def test_load_error_inheritance(self):
        """Test that LoadError inherits from ETLError."""
        error = LoadError(error_text="Test")
        assert isinstance(error, ETLError)
        assert isinstance(error, LoadError)


class TestValidationError:
    """Test suite for ValidationError exception."""
    
    def test_validation_error_basic(self):
        """Test basic validation error."""
        error = ValidationError(
            error_text="Required field missing",
            record_id="T000001"
        )
        
        assert error.error_text == "Required field missing"
        assert error.error_step == "VALIDATE"
    
    def test_validation_error_with_rule(self):
        """Test validation error with rule information."""
        error = ValidationError(
            error_text="Value out of range",
            record_id="T000001",
            validation_rule="quantity_range",
            expected_value="0-1000",
            actual_value=1500
        )
        
        assert error.validation_rule == "quantity_range"
        assert error.expected_value == "0-1000"
        assert error.actual_value == 1500
        assert error.context["validation_rule"] == "quantity_range"


class TestConfigurationError:
    """Test suite for ConfigurationError exception."""
    
    def test_configuration_error_basic(self):
        """Test basic configuration error."""
        error = ConfigurationError(
            error_text="Missing configuration file"
        )
        
        assert error.error_text == "Missing configuration file"
        assert error.error_step == "INIT"
    
    def test_configuration_error_with_key(self):
        """Test configuration error with key information."""
        error = ConfigurationError(
            error_text="Invalid configuration value",
            config_key="batch_size",
            config_value=-100
        )
        
        assert error.config_key == "batch_size"
        assert error.config_value == -100
        assert error.context["config_key"] == "batch_size"


class TestETLTimeoutError:
    """Test suite for ETLTimeoutError exception."""
    
    def test_timeout_error_basic(self):
        """Test basic timeout error."""
        error = ETLTimeoutError(
            error_text="Operation timed out"
        )
        
        assert error.error_text == "Operation timed out"
        assert error.error_step == "TIMEOUT"
    
    def test_timeout_error_with_duration(self):
        """Test timeout error with duration information."""
        error = ETLTimeoutError(
            error_text="Extract phase timed out",
            timeout_seconds=3600,
            error_step="EXTRACT"
        )
        
        assert error.timeout_seconds == 3600
        assert error.error_step == "EXTRACT"
        assert error.context["timeout_seconds"] == 3600


class TestExceptionChaining:
    """Test exception chaining and catching."""
    
    def test_catch_specific_exception(self):
        """Test catching specific exception type."""
        with pytest.raises(ExtractError) as exc_info:
            raise ExtractError("Extract failed")
        
        assert "Extract failed" in str(exc_info.value)
    
    def test_catch_base_exception(self):
        """Test catching base ETLError."""
        with pytest.raises(ETLError):
            raise TransformError("Transform failed")
    
    def test_exception_chain(self):
        """Test exception chaining with context."""
        try:
            try:
                raise ValueError("Original error")
            except ValueError as e:
                raise ExtractError(
                    error_text="Extract failed due to value error",
                    context={"original_error": str(e)}
                )
        except ExtractError as exc:
            assert "Original error" in exc.context["original_error"]


class TestExceptionUsagePatterns:
    """Test common exception usage patterns."""
    
    def test_error_context_accumulation(self):
        """Test accumulating context across error handling."""
        context = {"batch_id": "BATCH001"}
        
        error = TransformError(
            error_text="Failed transformation",
            record_id="T000001",
            field_name="amount",
            context=context
        )
        
        # Add more context
        error.context["retry_count"] = 3
        error.context["last_error"] = "Division by zero"
        
        assert len(error.context) >= 3
        assert error.context["batch_id"] == "BATCH001"
    
    def test_error_re_raise_pattern(self):
        """Test re-raising with additional context."""
        def inner_function():
            raise ExtractError("Source unavailable")
        
        def outer_function():
            try:
                inner_function()
            except ExtractError as e:
                # Add context and re-raise
                e.context["function"] = "outer_function"
                raise
        
        with pytest.raises(ExtractError) as exc_info:
            outer_function()
        
        assert exc_info.value.context["function"] == "outer_function"