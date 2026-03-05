"""
Unit tests for ETL Component Interface.
Tests the abstract base class implementation and ExecutionResult dataclass.
"""

import pytest
from src.interfaces.etl_component import ETLComponentInterface, ExecutionResult
from src.exceptions.etl_error import ETLError


class MockETLComponent(ETLComponentInterface):
    """Mock implementation of ETL component for testing."""

    def __init__(self, name: str = "MockComponent"):
        self.name = name
        self.prerequisites_valid = True

    def execute(self) -> ExecutionResult:
        if not self.prerequisites_valid:
            raise ETLError("Prerequisites not met")
        
        return ExecutionResult(
            success=True,
            records_total=100,
            records_success=95,
            records_error=5,
            message="Mock execution completed"
        )

    def get_component_name(self) -> str:
        return self.name

    def validate_prerequisites(self) -> bool:
        return self.prerequisites_valid


class TestExecutionResult:
    """Test ExecutionResult dataclass."""

    def test_valid_execution_result(self):
        """Test creating a valid execution result."""
        result = ExecutionResult(
            success=True,
            records_total=100,
            records_success=95,
            records_error=5,
            message="Success"
        )
        
        assert result.success is True
        assert result.records_total == 100
        assert result.records_success == 95
        assert result.records_error == 5
        assert result.message == "Success"

    def test_negative_records_total(self):
        """Test that negative records_total raises ValueError."""
        with pytest.raises(ValueError, match="records_total cannot be negative"):
            ExecutionResult(
                success=True,
                records_total=-1,
                records_success=0,
                records_error=0,
                message="Test"
            )

    def test_negative_records_success(self):
        """Test that negative records_success raises ValueError."""
        with pytest.raises(ValueError, match="records_success cannot be negative"):
            ExecutionResult(
                success=True,
                records_total=100,
                records_success=-1,
                records_error=0,
                message="Test"
            )

    def test_negative_records_error(self):
        """Test that negative records_error raises ValueError."""
        with pytest.raises(ValueError, match="records_error cannot be negative"):
            ExecutionResult(
                success=True,
                records_total=100,
                records_success=95,
                records_error=-1,
                message="Test"
            )

    def test_sum_exceeds_total(self):
        """Test that sum of success and error exceeding total raises ValueError."""
        with pytest.raises(ValueError, match="Sum of success and error records exceeds total"):
            ExecutionResult(
                success=True,
                records_total=100,
                records_success=60,
                records_error=50,
                message="Test"
            )

    def test_zero_records(self):
        """Test execution result with zero records."""
        result = ExecutionResult(
            success=True,
            records_total=0,
            records_success=0,
            records_error=0,
            message="No records"
        )
        
        assert result.records_total == 0
        assert result.records_success == 0
        assert result.records_error == 0


class TestETLComponentInterface:
    """Test ETL Component Interface."""

    def test_component_instantiation(self):
        """Test that component can be instantiated."""
        component = MockETLComponent("TestComponent")
        assert component.get_component_name() == "TestComponent"

    def test_execute_success(self):
        """Test successful execution."""
        component = MockETLComponent()
        result = component.execute()
        
        assert result.success is True
        assert result.records_total == 100
        assert result.records_success == 95
        assert result.records_error == 5

    def test_execute_failure(self):
        """Test execution failure."""
        component = MockETLComponent()
        component.prerequisites_valid = False
        
        with pytest.raises(ETLError, match="Prerequisites not met"):
            component.execute()

    def test_validate_prerequisites(self):
        """Test prerequisite validation."""
        component = MockETLComponent()
        
        # Valid prerequisites
        assert component.validate_prerequisites() is True
        
        # Invalid prerequisites
        component.prerequisites_valid = False
        assert component.validate_prerequisites() is False

    def test_get_component_name(self):
        """Test getting component name."""
        component = MockETLComponent("CustomName")
        assert component.get_component_name() == "CustomName"

    def test_abstract_methods_required(self):
        """Test that abstract methods must be implemented."""
        with pytest.raises(TypeError):
            # Cannot instantiate abstract class without implementing all methods
            ETLComponentInterface()


class TestETLComponentIntegration:
    """Integration tests for ETL Component Interface."""

    def test_full_execution_flow(self):
        """Test complete execution flow."""
        component = MockETLComponent("IntegrationTest")
        
        # Validate prerequisites
        assert component.validate_prerequisites() is True
        
        # Execute
        result = component.execute()
        
        # Verify results
        assert result.success is True
        assert result.records_total > 0
        assert result.message is not None

    def test_error_handling_flow(self):
        """Test error handling flow."""
        component = MockETLComponent("ErrorTest")
        component.prerequisites_valid = False
        
        # Validation should fail
        assert component.validate_prerequisites() is False
        
        # Execution should raise error
        with pytest.raises(ETLError):
            component.execute()

    def test_component_name_consistency(self):
        """Test that component name remains consistent."""
        component = MockETLComponent("ConsistencyTest")
        
        name1 = component.get_component_name()
        name2 = component.get_component_name()
        
        assert name1 == name2
        assert name1 == "ConsistencyTest"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])