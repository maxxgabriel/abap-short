"""
Unit tests for ETL exception hierarchy
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
    OrchestrationError
)


class TestETLError:
    """Test cases for base ETLError class"""
    
    def test_basic_error_creation(self):
        """Test basic error creation with minimal arguments"""
        error = ETLError(message="Test error")
        
        assert error.message == "Test error"
        assert error.error_step == "UNKNOWN"
        assert error.record_id is None
        assert isinstance(error.timestamp, datetime)
        assert error.context == {}
    
    def test_error_with_all_attributes(self):
        """Test error creation with all attributes"""
        context = {"source": "test_table", "query": "SELECT * FROM test"}
        error = ETLError(
            message="Test error",
            error_step="TEST_STEP",
            record_id="REC001",
            context=context
        )
        
        assert error.message == "Test error"
        assert error.error_step == "TEST_STEP"
        assert error.record_id == "REC001"
        assert error.context == context
    
    def test_get_error_details(self):
        """Test get_error_details method"""
        context = {"key": "value"}
        error = ETLError(
            message="Test error",
            error_step="TEST",
            record_id="REC001",
            context=context
        )
        
        details = error.get_error_details()
        
        assert details["error_type"] == "ETLError"
        assert details["message"] == "Test error"
        assert details["error_step"] == "TEST"
        assert details["record_id"] == "REC001"
        assert details["context"] == context
        assert "timestamp" in details
    
    def test_error_string_representation(self):
        """Test string representation of error"""
        error = ETLError(message="Test error", error_step="TEST")
        error_str = str(error)
        
        assert "ETLError" in error_str
        assert "Test error" in error_str


class TestExtractError:
    """Test cases for ExtractError class"""
    
    def test_extract_error_basic(self):
        """Test basic extraction error"""
        error = ExtractError(message="Failed to read data")
        
        assert error.message == "Failed to read data"
        assert error.error_step == "EXTRACT"
        assert isinstance(error, ETLError)
    
    def test_extract_error_with_source(self):
        """Test extraction error with source information"""
        error = ExtractError(
            message="Table not found",
            source="zsales_raw"
        )
        
        assert error.message == "Table not found"
        assert error.context["source"] == "zsales_raw"
    
    def test_extract_error_with_query(self):
        """Test extraction error with query information"""
        query = "SELECT * FROM zsales_raw WHERE status = 'N'"
        error = ExtractError(
            message="Query failed",
            source="zsales_raw",
            query=query
        )
        
        assert error.context["source"] == "zsales_raw"
        assert error.context["query"] == query
    
    def test_extract_error_with_record_id(self):
        """Test extraction error with record ID"""
        error = ExtractError(
            message="Invalid record",
            record_id="T000001"
        )
        
        assert error.record_id == "T000001"


class TestTransformError:
    """Test cases for TransformError class"""
    
    def test_transform_error_basic(self):
        """Test basic transformation error"""
        error = TransformError(message="Transformation failed")
        
        assert error.message == "Transformation failed"
        assert error.error_step == "TRANSFORM"
        assert isinstance(error, ETLError)
    
    def test_transform_error_with_transformation(self):
        """Test transformation error with transformation name"""
        error = TransformError(
            message="Discount calculation failed",
            transformation="calculate_discount"
        )
        
        assert error.context["transformation"] == "calculate_discount"
    
    def test_transform_error_with_field_info(self):
        """Test transformation error with field information"""
        error = TransformError(
            message="Invalid field value",
            field_name="quantity",
            field_value=-5
        )
        
        assert error.context["field_name"] == "quantity"
        assert error.context["field_value"] == "-5"
    
    def test_transform_error_complete(self):
        """Test transformation error with all attributes"""
        error = TransformError(
            message="Type conversion failed",
            transformation="convert_to_decimal",
            field_name="unit_price",
            field_value="invalid",
            record_id="T000001"
        )
        
        assert error.context["transformation"] == "convert_to_decimal"
        assert error.context["field_name"] == "unit_price"
        assert error.context["field_value"] == "invalid"
        assert error.record_id == "T000001"


class TestLoadError:
    """Test cases for LoadError class"""
    
    def test_load_error_basic(self):
        """Test basic load error"""
        error = LoadError(message="Failed to load data")
        
        assert error.message == "Failed to load data"
        assert error.error_step == "LOAD"
        assert isinstance(error, ETLError)
    
    def test_load_error_with_target(self):
        """Test load error with target information"""
        error = LoadError(
            message="Target table not found",
            target="zsales_analytics"
        )
        
        assert error.context["target"] == "zsales_analytics"
    
    def test_load_error_with_operation(self):
        """Test load error with operation type"""
        error = LoadError(
            message="Insert failed",
            target="zsales_analytics",
            operation="INSERT"
        )
        
        assert error.context["target"] == "zsales_analytics"
        assert error.context["operation"] == "INSERT"
    
    def test_load_error_with_statistics(self):
        """Test load error with processing statistics"""
        error = LoadError(
            message="Batch insert failed",
            target="zsales_analytics",
            records_processed=500,
            records_failed=10
        )
        
        assert error.context["records_processed"] == 500
        assert error.context["records_failed"] == 10
    
    def test_load_error_complete(self):
        """Test load error with all attributes"""
        error = LoadError(
            message="Load failed",
            target="zsales_analytics",
            operation="UPSERT",
            record_id="ANL001",
            records_processed=1000,
            records_failed=5
        )
        
        assert error.context["target"] == "zsales_analytics"
        assert error.context["operation"] == "UPSERT"
        assert error.record_id == "ANL001"
        assert error.context["records_processed"] == 1000
        assert error.context["records_failed"] == 5


class TestValidationError:
    """Test cases for ValidationError class"""
    
    def test_validation_error_basic(self):
        """Test basic validation error"""
        error = ValidationError(message="Validation failed")
        
        assert error.message == "Validation failed"
        assert error.error_step == "VALIDATE"
        assert isinstance(error, ETLError)
    
    def test_validation_error_with_rule(self):
        """Test validation error with validation rule"""
        error = ValidationError(
            message="Required field missing",
            validation_rule="required_fields"
        )
        
        assert error.context["validation_rule"] == "required_fields"
    
    def test_validation_error_with_field_info(self):
        """Test validation error with field information"""
        error = ValidationError(
            message="Invalid value",
            field_name="quantity",
            field_value=0,
            expected_value="> 0"
        )
        
        assert error.context["field_name"] == "quantity"
        assert error.context["field_value"] == "0"
        assert error.context["expected_value"] == "> 0"
    
    def test_validation_error_complete(self):
        """Test validation error with all attributes"""
        error = ValidationError(
            message="Business rule violation",
            validation_rule="minimum_quantity",
            field_name="quantity",
            field_value=0,
            expected_value=">= 1",
            record_id="T000001"
        )
        
        assert error.context["validation_rule"] == "minimum_quantity"
        assert error.context["field_name"] == "quantity"
        assert error.record_id == "T000001"


class TestConfigurationError:
    """Test cases for ConfigurationError class"""
    
    def test_configuration_error_basic(self):
        """Test basic configuration error"""
        error = ConfigurationError(message="Configuration invalid")
        
        assert error.message == "Configuration invalid"
        assert error.error_step == "CONFIG"
        assert isinstance(error, ETLError)
    
    def test_configuration_error_with_key(self):
        """Test configuration error with config key"""
        error = ConfigurationError(
            message="Missing required parameter",
            config_key="batch_size"
        )
        
        assert error.context["config_key"] == "batch_size"
    
    def test_configuration_error_with_value(self):
        """Test configuration error with config value"""
        error = ConfigurationError(
            message="Invalid value",
            config_key="batch_size",
            config_value=-1
        )
        
        assert error.context["config_key"] == "batch_size"
        assert error.context["config_value"] == "-1"


class TestOrchestrationError:
    """Test cases for OrchestrationError class"""
    
    def test_orchestration_error_basic(self):
        """Test basic orchestration error"""
        error = OrchestrationError(message="Orchestration failed")
        
        assert error.message == "Orchestration failed"
        assert error.error_step == "ORCHESTRATION"
        assert isinstance(error, ETLError)
    
    def test_orchestration_error_with_workflow_step(self):
        """Test orchestration error with workflow step"""
        error = OrchestrationError(
            message="Workflow step failed",
            workflow_step="EXTRACT"
        )
        
        assert error.context["workflow_step"] == "EXTRACT"
    
    def test_orchestration_error_with_component(self):
        """Test orchestration error with failed component"""
        error = OrchestrationError(
            message="Component initialization failed",
            failed_component="extractor"
        )
        
        assert error.context["failed_component"] == "extractor"
    
    def test_orchestration_error_complete(self):
        """Test orchestration error with all attributes"""
        error = OrchestrationError(
            message="Workflow failed",
            workflow_step="TRANSFORM",
            failed_component="transformer",
            context={"reason": "timeout"}
        )
        
        assert error.context["workflow_step"] == "TRANSFORM"
        assert error.context["failed_component"] == "transformer"
        assert error.context["reason"] == "timeout"


class TestExceptionInheritance:
    """Test exception hierarchy and inheritance"""
    
    def test_all_inherit_from_etl_error(self):
        """Test that all custom exceptions inherit from ETLError"""
        assert issubclass(ExtractError, ETLError)
        assert issubclass(TransformError, ETLError)
        assert issubclass(LoadError, ETLError)
        assert issubclass(ValidationError, ETLError)
        assert issubclass(ConfigurationError, ETLError)
        assert issubclass(OrchestrationError, ETLError)
    
    def test_all_inherit_from_exception(self):
        """Test that all custom exceptions inherit from base Exception"""
        assert issubclass(ETLError, Exception)
        assert issubclass(ExtractError, Exception)
        assert issubclass(TransformError, Exception)
        assert issubclass(LoadError, Exception)
    
    def test_exception_catching(self):
        """Test that exceptions can be caught properly"""
        with pytest.raises(ETLError):
            raise ExtractError(message="Test error")
        
        with pytest.raises(ETLError):
            raise TransformError(message="Test error")
        
        with pytest.raises(ETLError):
            raise LoadError(message="Test error")
    
    def test_specific_exception_catching(self):
        """Test catching specific exception types"""
        with pytest.raises(ExtractError):
            raise ExtractError(message="Extract failed")
        
        with pytest.raises(TransformError):
            raise TransformError(message="Transform failed")
        
        with pytest.raises(LoadError):
            raise LoadError(message="Load failed")


class TestExceptionUsagePatterns:
    """Test real-world usage patterns"""
    
    def test_extract_with_database_error(self):
        """Test extraction error in database context"""
        try:
            # Simulate database extraction failure
            raise ExtractError(
                message="Connection to database failed",
                source="zsales_raw",
                query="SELECT * FROM zsales_raw WHERE status = 'N'",
                context={"error_code": "08001", "host": "localhost"}
            )
        except ExtractError as e:
            assert e.error_step == "EXTRACT"
            assert e.context["source"] == "zsales_raw"
            assert "error_code" in e.context
    
    def test_transform_with_calculation_error(self):
        """Test transformation error in calculation context"""
        try:
            # Simulate calculation failure
            raise TransformError(
                message="Division by zero in profit margin calculation",
                transformation="calculate_profit_margin",
                field_name="net_amount",
                field_value=0,
                record_id="T000001"
            )
        except TransformError as e:
            assert e.error_step == "TRANSFORM"
            assert e.record_id == "T000001"
            assert e.context["transformation"] == "calculate_profit_margin"
    
    def test_load_with_batch_error(self):
        """Test load error in batch processing context"""
        try:
            # Simulate batch load failure
            raise LoadError(
                message="Batch insert failed after 500 records",
                target="zsales_analytics",
                operation="INSERT",
                records_processed=500,
                records_failed=10
            )
        except LoadError as e:
            assert e.error_step == "LOAD"
            assert e.context["records_processed"] == 500
            assert e.context["records_failed"] == 10
    
    def test_chained_exception_handling(self):
        """Test exception handling with cause chain"""
        try:
            try:
                # Simulate nested error
                raise ValueError("Invalid data type")
            except ValueError as ve:
                raise TransformError(
                    message="Type conversion failed",
                    field_name="unit_price",
                    context={"original_error": str(ve)}
                )
        except TransformError as e:
            assert "Invalid data type" in e.context["original_error"]