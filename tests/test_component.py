"""
Unit tests for ETL Component Interface

Tests the abstract base class, execution result dataclass, and error handling.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
from src.component import (
    ETLComponent,
    ExecutionResult,
    ETLComponentError,
    ExtractError,
    TransformError,
    LoadError,
    ValidationError
)


class TestExecutionResult:
    """Test cases for ExecutionResult dataclass."""
    
    def test_execution_result_creation(self):
        """Test creating an ExecutionResult instance."""
        result = ExecutionResult(
            success=True,
            records_total=100,
            records_success=95,
            records_error=5,
            message="Test execution completed",
            component_name="TestComponent"
        )
        
        assert result.success is True
        assert result.records_total == 100
        assert result.records_success == 95
        assert result.records_error == 5
        assert result.message == "Test execution completed"
        assert result.component_name == "TestComponent"
    
    def test_execution_result_defaults(self):
        """Test ExecutionResult with default values."""
        result = ExecutionResult()
        
        assert result.success is False
        assert result.records_total == 0
        assert result.records_success == 0
        assert result.records_error == 0
        assert result.message == ""
        assert result.component_name == ""
        assert result.metadata == {}
    
    def test_duration_calculation(self):
        """Test duration calculation in ExecutionResult."""
        start_time = datetime(2024, 1, 1, 10, 0, 0)
        end_time = datetime(2024, 1, 1, 10, 5, 30)
        
        result = ExecutionResult(
            success=True,
            start_time=start_time,
            end_time=end_time
        )
        
        assert result.duration_seconds == 330.0  # 5 minutes 30 seconds
    
    def test_duration_none_times(self):
        """Test duration when times are None."""
        result = ExecutionResult(success=True)
        assert result.duration_seconds == 0.0
    
    def test_success_rate_calculation(self):
        """Test success rate calculation."""
        result = ExecutionResult(
            success=True,
            records_total=100,
            records_success=95,
            records_error=5
        )
        
        assert result.success_rate == 95.0
    
    def test_success_rate_zero_total(self):
        """Test success rate with zero total records."""
        result = ExecutionResult(
            success=True,
            records_total=0,
            records_success=0
        )
        
        assert result.success_rate == 0.0
    
    def test_error_rate_calculation(self):
        """Test error rate calculation."""
        result = ExecutionResult(
            success=True,
            records_total=100,
            records_success=95,
            records_error=5
        )
        
        assert result.error_rate == 5.0
    
    def test_to_dict_conversion(self):
        """Test converting ExecutionResult to dictionary."""
        start_time = datetime(2024, 1, 1, 10, 0, 0)
        end_time = datetime(2024, 1, 1, 10, 5, 0)
        
        result = ExecutionResult(
            success=True,
            records_total=100,
            records_success=95,
            records_error=5,
            message="Test completed",
            start_time=start_time,
            end_time=end_time,
            component_name="TestComponent",
            metadata={"key": "value"}
        )
        
        result_dict = result.to_dict()
        
        assert result_dict['success'] is True
        assert result_dict['records_total'] == 100
        assert result_dict['records_success'] == 95
        assert result_dict['records_error'] == 5
        assert result_dict['message'] == "Test completed"
        assert result_dict['component_name'] == "TestComponent"
        assert result_dict['duration_seconds'] == 300.0
        assert result_dict['success_rate'] == 95.0
        assert result_dict['error_rate'] == 5.0
        assert result_dict['metadata'] == {"key": "value"}
        assert 'start_time' in result_dict
        assert 'end_time' in result_dict
    
    def test_string_representation(self):
        """Test string representation of ExecutionResult."""
        result = ExecutionResult(
            success=True,
            records_total=100,
            records_success=95,
            component_name="TestComponent",
            start_time=datetime(2024, 1, 1, 10, 0, 0),
            end_time=datetime(2024, 1, 1, 10, 5, 0)
        )
        
        result_str = str(result)
        assert "SUCCESS" in result_str
        assert "TestComponent" in result_str
        assert "95/100" in result_str
        assert "300.00s" in result_str
    
    def test_string_representation_failure(self):
        """Test string representation for failed execution."""
        result = ExecutionResult(
            success=False,
            records_total=100,
            records_error=10,
            component_name="TestComponent",
            start_time=datetime(2024, 1, 1, 10, 0, 0),
            end_time=datetime(2024, 1, 1, 10, 1, 0)
        )
        
        result_str = str(result)
        assert "FAILED" in result_str


class ConcreteETLComponent(ETLComponent):
    """Concrete implementation of ETLComponent for testing."""
    
    def execute(self, **kwargs) -> ExecutionResult:
        """Test implementation of execute."""
        start_time = datetime.now()
        # Simulate processing
        records = kwargs.get('records', [])
        end_time = datetime.now()
        
        return self._create_result(
            success=True,
            records_total=len(records),
            records_success=len(records),
            records_error=0,
            message="Execution completed successfully",
            start_time=start_time,
            end_time=end_time
        )
    
    def validate_prerequisites(self) -> bool:
        """Test implementation of validate_prerequisites."""
        return True


class TestETLComponent:
    """Test cases for ETLComponent abstract base class."""
    
    def test_cannot_instantiate_abstract_class(self):
        """Test that ETLComponent cannot be instantiated directly."""
        with pytest.raises(TypeError):
            ETLComponent("test", {})
    
    def test_concrete_component_creation(self):
        """Test creating a concrete ETL component."""
        config = {"batch_size": 1000}
        component = ConcreteETLComponent("TestComponent", config)
        
        assert component.get_component_name() == "TestComponent"
        assert component.get_config() == config
        assert component.is_initialized() is False
    
    def test_component_without_config(self):
        """Test creating component without config."""
        component = ConcreteETLComponent("TestComponent")
        
        assert component.get_component_name() == "TestComponent"
        assert component.get_config() == {}
    
    def test_get_config_returns_copy(self):
        """Test that get_config returns a copy, not reference."""
        config = {"batch_size": 1000}
        component = ConcreteETLComponent("TestComponent", config)
        
        returned_config = component.get_config()
        returned_config["batch_size"] = 2000
        
        # Original config should be unchanged
        assert component.get_config()["batch_size"] == 1000
    
    def test_execute_method(self):
        """Test execute method implementation."""
        component = ConcreteETLComponent("TestComponent")
        records = [1, 2, 3, 4, 5]
        
        result = component.execute(records=records)
        
        assert isinstance(result, ExecutionResult)
        assert result.success is True
        assert result.records_total == 5
        assert result.records_success == 5
        assert result.component_name == "TestComponent"
    
    def test_validate_prerequisites(self):
        """Test validate_prerequisites method."""
        component = ConcreteETLComponent("TestComponent")
        assert component.validate_prerequisites() is True
    
    def test_create_result_helper(self):
        """Test _create_result helper method."""
        component = ConcreteETLComponent("TestComponent")
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=60)
        
        result = component._create_result(
            success=True,
            records_total=100,
            records_success=95,
            records_error=5,
            message="Test message",
            start_time=start_time,
            end_time=end_time,
            custom_field="custom_value"
        )
        
        assert result.success is True
        assert result.records_total == 100
        assert result.records_success == 95
        assert result.records_error == 5
        assert result.message == "Test message"
        assert result.component_name == "TestComponent"
        assert result.start_time == start_time
        assert result.end_time == end_time
        assert result.metadata["custom_field"] == "custom_value"
    
    def test_component_repr(self):
        """Test string representation of component."""
        component = ConcreteETLComponent("TestComponent")
        repr_str = repr(component)
        
        assert "ConcreteETLComponent" in repr_str
        assert "TestComponent" in repr_str


class TestETLComponentError:
    """Test cases for ETLComponentError and subclasses."""
    
    def test_basic_error_creation(self):
        """Test creating basic ETLComponentError."""
        error = ETLComponentError("Test error message")
        
        assert error.message == "Test error message"
        assert "Test error message" in str(error)
    
    def test_error_with_all_fields(self):
        """Test error with all fields populated."""
        error = ETLComponentError(
            message="Processing failed",
            component_name="TestComponent",
            error_step="TRANSFORM",
            record_id="REC001"
        )
        
        assert error.message == "Processing failed"
        assert error.component_name == "TestComponent"
        assert error.error_step == "TRANSFORM"
        assert error.record_id == "REC001"
        
        error_str = str(error)
        assert "Component: TestComponent" in error_str
        assert "Step: TRANSFORM" in error_str
        assert "Record: REC001" in error_str
        assert "Processing failed" in error_str
    
    def test_error_with_original_exception(self):
        """Test error wrapping another exception."""
        original = ValueError("Original error")
        error = ETLComponentError(
            message="Wrapped error",
            component_name="TestComponent",
            original_exception=original
        )
        
        assert error.original_exception == original
        assert "Caused by: Original error" in str(error)
    
    def test_extract_error(self):
        """Test ExtractError subclass."""
        error = ExtractError(
            message="Extraction failed",
            component_name="Extractor"
        )
        
        assert isinstance(error, ETLComponentError)
        assert error.message == "Extraction failed"
    
    def test_transform_error(self):
        """Test TransformError subclass."""
        error = TransformError(
            message="Transformation failed",
            component_name="Transformer"
        )
        
        assert isinstance(error, ETLComponentError)
        assert error.message == "Transformation failed"
    
    def test_load_error(self):
        """Test LoadError subclass."""
        error = LoadError(
            message="Load failed",
            component_name="Loader"
        )
        
        assert isinstance(error, ETLComponentError)
        assert error.message == "Load failed"
    
    def test_validation_error(self):
        """Test ValidationError subclass."""
        error = ValidationError(
            message="Validation failed",
            component_name="Validator"
        )
        
        assert isinstance(error, ETLComponentError)
        assert error.message == "Validation failed"
    
    def test_error_raising(self):
        """Test raising ETLComponentError."""
        component = ConcreteETLComponent("TestComponent")
        
        with pytest.raises(ETLComponentError) as exc_info:
            raise ETLComponentError(
                message="Test error",
                component_name="TestComponent"
            )
        
        assert "Test error" in str(exc_info.value)


class TestComponentIntegration:
    """Integration tests for component framework."""
    
    def test_full_execution_flow(self):
        """Test complete execution flow with result."""
        component = ConcreteETLComponent("TestComponent", {"batch_size": 100})
        
        # Validate prerequisites
        assert component.validate_prerequisites() is True
        
        # Execute
        records = list(range(100))
        result = component.execute(records=records)
        
        # Verify result
        assert result.success is True
        assert result.records_total == 100
        assert result.component_name == "TestComponent"
        assert result.duration_seconds >= 0
        
        # Convert to dict
        result_dict = result.to_dict()
        assert result_dict['success'] is True
        assert result_dict['records_total'] == 100
    
    def test_component_failure_handling(self):
        """Test handling component failures."""
        
        class FailingComponent(ETLComponent):
            def execute(self, **kwargs):
                raise ETLComponentError(
                    message="Intentional failure",
                    component_name=self.get_component_name(),
                    error_step="EXECUTE"
                )
            
            def validate_prerequisites(self):
                return True
        
        component = FailingComponent("FailingComponent")
        
        with pytest.raises(ETLComponentError) as exc_info:
            component.execute()
        
        assert "Intentional failure" in str(exc_info.value)
        assert "FailingComponent" in str(exc_info.value)
    
    def test_component_validation_failure(self):
        """Test prerequisite validation failure."""
        
        class InvalidComponent(ETLComponent):
            def execute(self, **kwargs):
                return self._create_result(success=True)
            
            def validate_prerequisites(self):
                return False
        
        component = InvalidComponent("InvalidComponent")
        assert component.validate_prerequisites() is False


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_execution_result_with_no_records(self):
        """Test ExecutionResult with zero records."""
        result = ExecutionResult(
            success=True,
            records_total=0,
            records_success=0,
            records_error=0
        )
        
        assert result.success_rate == 0.0
        assert result.error_rate == 0.0
    
    def test_execution_result_all_errors(self):
        """Test ExecutionResult with all records failing."""
        result = ExecutionResult(
            success=False,
            records_total=100,
            records_success=0,
            records_error=100
        )
        
        assert result.success_rate == 0.0
        assert result.error_rate == 100.0
    
    def test_component_with_empty_name(self):
        """Test component with empty name."""
        component = ConcreteETLComponent("")
        assert component.get_component_name() == ""
    
    def test_execution_result_with_large_numbers(self):
        """Test ExecutionResult with large record counts."""
        result = ExecutionResult(
            success=True,
            records_total=1_000_000,
            records_success=999_999,
            records_error=1
        )
        
        assert result.success_rate == pytest.approx(99.9999, rel=1e-4)
        assert result.error_rate == pytest.approx(0.0001, rel=1e-4)
    
    def test_metadata_immutability(self):
        """Test that metadata dictionary is properly isolated."""
        metadata = {"key": "value"}
        result = ExecutionResult(metadata=metadata)
        
        # Modify original metadata
        metadata["key"] = "modified"
        
        # Result metadata should be unchanged (if properly copied)
        # Note: dataclass field(default_factory=dict) creates new dict
        assert result.metadata.get("key") != "modified" or len(result.metadata) == 0