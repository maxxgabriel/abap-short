"""
Unit Tests for ETL Component Interface
"""
import pytest
from datetime import datetime
from decimal import Decimal
from unittest.mock import Mock, MagicMock

from src.etl_component import ETLComponent, ExecutionResult
from src.exceptions import ETLError, ETLPrerequisiteError
from src.data_structures import ETLConfiguration


class MockETLComponent(ETLComponent):
    """Mock implementation for testing abstract base class"""
    
    def __init__(self, logger=None, should_fail=False):
        super().__init__(logger)
        self.should_fail = should_fail
        self.execution_count = 0
    
    def execute(self, *args, **kwargs) -> ExecutionResult:
        self._start_execution()
        self.execution_count += 1
        
        if self.should_fail:
            self._end_execution(False, "Mock execution failed")
            raise ETLError("Mock execution failed")
        
        result = ExecutionResult(
            success=True,
            records_total=100,
            records_success=95,
            records_error=5,
            message="Mock execution completed"
        )
        
        self._end_execution(True, "Mock execution completed")
        return result
    
    def get_component_name(self) -> str:
        return "MockComponent"
    
    def validate_prerequisites(self) -> bool:
        return not self.should_fail


class TestExecutionResult:
    """Test ExecutionResult dataclass"""
    
    def test_execution_result_creation(self):
        """Test creating ExecutionResult with all fields"""
        result = ExecutionResult(
            success=True,
            records_total=100,
            records_success=95,
            records_error=5,
            message="Test execution",
            execution_time=10.5
        )
        
        assert result.success is True
        assert result.records_total == 100
        assert result.records_success == 95
        assert result.records_error == 5
        assert result.message == "Test execution"
        assert result.execution_time == 10.5
    
    def test_execution_result_defaults(self):
        """Test ExecutionResult default values"""
        result = ExecutionResult(success=True)
        
        assert result.records_total == 0
        assert result.records_success == 0
        assert result.records_error == 0
        assert result.message == ""
        assert result.execution_time is None
    
    def test_records_failed_property(self):
        """Test calculation of failed records"""
        result = ExecutionResult(
            success=True,
            records_total=100,
            records_success=95
        )
        
        assert result.records_failed == 5
    
    def test_success_rate_property(self):
        """Test success rate calculation"""
        result = ExecutionResult(
            success=True,
            records_total=100,
            records_success=95
        )
        
        assert result.success_rate == 95.0
    
    def test_success_rate_zero_records(self):
        """Test success rate with zero records"""
        result = ExecutionResult(success=True)
        
        assert result.success_rate == 0.0


class TestETLComponent:
    """Test ETLComponent abstract base class"""
    
    def test_component_instantiation(self):
        """Test creating a concrete implementation"""
        component = MockETLComponent()
        
        assert component is not None
        assert isinstance(component, ETLComponent)
    
    def test_component_with_logger(self):
        """Test component initialization with logger"""
        mock_logger = Mock()
        component = MockETLComponent(logger=mock_logger)
        
        assert component._logger is mock_logger
    
    def test_execute_success(self):
        """Test successful execution"""
        component = MockETLComponent()
        result = component.execute()
        
        assert result.success is True
        assert result.records_total == 100
        assert result.records_success == 95
        assert component.execution_count == 1
    
    def test_execute_failure(self):
        """Test failed execution"""
        component = MockETLComponent(should_fail=True)
        
        with pytest.raises(ETLError):
            component.execute()
        
        assert component.execution_count == 1
    
    def test_get_component_name(self):
        """Test getting component name"""
        component = MockETLComponent()
        
        assert component.get_component_name() == "MockComponent"
    
    def test_validate_prerequisites_success(self):
        """Test prerequisite validation - success"""
        component = MockETLComponent()
        
        assert component.validate_prerequisites() is True
    
    def test_validate_prerequisites_failure(self):
        """Test prerequisite validation - failure"""
        component = MockETLComponent(should_fail=True)
        
        assert component.validate_prerequisites() is False
    
    def test_execution_timing(self):
        """Test execution timing tracking"""
        component = MockETLComponent()
        
        assert component._start_time is None
        assert component._end_time is None
        
        component.execute()
        
        assert component._start_time is not None
        assert component._end_time is not None
        assert component._end_time > component._start_time
    
    def test_get_execution_duration(self):
        """Test getting execution duration"""
        component = MockETLComponent()
        
        # Before execution
        assert component.get_execution_duration() is None
        
        # After execution
        component.execute()
        duration = component.get_execution_duration()
        
        assert duration is not None
        assert duration >= 0
    
    def test_logging_on_start(self):
        """Test logging when execution starts"""
        mock_logger = Mock()
        component = MockETLComponent(logger=mock_logger)
        
        component.execute()
        
        # Verify logger was called for start
        calls = mock_logger.log_message.call_args_list
        assert len(calls) >= 2
        
        start_call = calls[0]
        assert start_call[1]['step'] == 'MockComponent'
        assert start_call[1]['status'] == 'I'
        assert 'Starting' in start_call[1]['message']
    
    def test_logging_on_success(self):
        """Test logging when execution succeeds"""
        mock_logger = Mock()
        component = MockETLComponent(logger=mock_logger)
        
        component.execute()
        
        # Verify logger was called for completion
        calls = mock_logger.log_message.call_args_list
        end_call = calls[-1]
        
        assert end_call[1]['step'] == 'MockComponent'
        assert end_call[1]['status'] == 'S'
    
    def test_logging_on_failure(self):
        """Test logging when execution fails"""
        mock_logger = Mock()
        component = MockETLComponent(logger=mock_logger, should_fail=True)
        
        with pytest.raises(ETLError):
            component.execute()
        
        # Verify logger was called for error
        calls = mock_logger.log_message.call_args_list
        end_call = calls[-1]
        
        assert end_call[1]['step'] == 'MockComponent'
        assert end_call[1]['status'] == 'E'


class TestETLComponentIntegration:
    """Integration tests for ETL component workflow"""
    
    def test_full_execution_workflow(self):
        """Test complete execution workflow with timing and logging"""
        mock_logger = Mock()
        component = MockETLComponent(logger=mock_logger)
        
        # Validate prerequisites
        assert component.validate_prerequisites() is True
        
        # Execute component
        result = component.execute()
        
        # Verify results
        assert result.success is True
        assert result.records_total > 0
        assert component.execution_count == 1
        
        # Verify timing
        duration = component.get_execution_duration()
        assert duration is not None
        assert duration >= 0
        
        # Verify logging
        assert mock_logger.log_message.call_count >= 2
    
    def test_multiple_executions(self):
        """Test executing component multiple times"""
        component = MockETLComponent()
        
        # First execution
        result1 = component.execute()
        duration1 = component.get_execution_duration()
        
        # Second execution
        result2 = component.execute()
        duration2 = component.get_execution_duration()
        
        # Verify both succeeded
        assert result1.success is True
        assert result2.success is True
        assert component.execution_count == 2
        
        # Verify timing updated
        assert duration2 is not None
        # Note: duration2 may be different from duration1
    
    def test_error_recovery(self):
        """Test component behavior after error"""
        component = MockETLComponent(should_fail=True)
        
        # First execution fails
        with pytest.raises(ETLError):
            component.execute()
        
        # "Fix" the component
        component.should_fail = False
        
        # Second execution succeeds
        result = component.execute()
        assert result.success is True
        assert component.execution_count == 2