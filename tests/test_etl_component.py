"""
Unit tests for ETL Component Abstract Base Class
Tests the interface contract and exception handling
"""
import pytest
from datetime import datetime
from src.etl_component import (
    ETLComponent,
    ETLLogger,
    ExecutionResult,
    ETLComponentError,
    ExtractError,
    TransformError,
    LoadError
)


class ConcreteETLComponent(ETLComponent):
    """Concrete implementation for testing"""
    
    def __init__(self, name: str = "TestComponent", should_fail: bool = False):
        self.name = name
        self.should_fail = should_fail
        self.prerequisites_valid = True
    
    def execute(self) -> ExecutionResult:
        if self.should_fail:
            raise ETLComponentError(
                message="Execution failed",
                error_step="TEST",
                record_id="TEST001"
            )
        return ExecutionResult(
            success=True,
            records_total=100,
            records_success=95,
            records_error=5,
            message="Test execution completed"
        )
    
    def get_component_name(self) -> str:
        return self.name
    
    def validate_prerequisites(self) -> bool:
        return self.prerequisites_valid


class ConcreteETLLogger(ETLLogger):
    """Concrete logger implementation for testing"""
    
    def __init__(self, run_id: str = "TEST_RUN_001"):
        self.run_id = run_id
        self.messages = []
    
    def log_message(self, step: str, status: str, message: str,
                    records_processed: int = 0, records_success: int = 0,
                    records_error: int = 0) -> None:
        log_entry = {
            'timestamp': datetime.now(),
            'run_id': self.run_id,
            'step': step,
            'status': status,
            'message': message,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error
        }
        self.messages.append(log_entry)
    
    def get_etl_run_id(self) -> str:
        return self.run_id


class TestExecutionResult:
    """Test ExecutionResult dataclass"""
    
    def test_execution_result_creation(self):
        """Test creating ExecutionResult instance"""
        result = ExecutionResult(
            success=True,
            records_total=100,
            records_success=95,
            records_error=5,
            message="Processing completed"
        )
        
        assert result.success is True
        assert result.records_total == 100
        assert result.records_success == 95
        assert result.records_error == 5
        assert result.message == "Processing completed"
    
    def test_execution_result_failure(self):
        """Test ExecutionResult for failure scenario"""
        result = ExecutionResult(
            success=False,
            records_total=100,
            records_success=0,
            records_error=100,
            message="Processing failed"
        )
        
        assert result.success is False
        assert result.records_error == 100


class TestETLComponentError:
    """Test ETL exception classes"""
    
    def test_base_exception(self):
        """Test base ETLComponentError"""
        error = ETLComponentError(
            message="Test error",
            error_step="EXTRACT",
            record_id="REC001"
        )
        
        assert str(error) == "Test error"
        assert error.error_step == "EXTRACT"
        assert error.record_id == "REC001"
    
    def test_extract_error(self):
        """Test ExtractError exception"""
        error = ExtractError("Extraction failed")
        assert isinstance(error, ETLComponentError)
        assert str(error) == "Extraction failed"
    
    def test_transform_error(self):
        """Test TransformError exception"""
        error = TransformError("Transformation failed")
        assert isinstance(error, ETLComponentError)
        assert str(error) == "Transformation failed"
    
    def test_load_error(self):
        """Test LoadError exception"""
        error = LoadError("Load failed")
        assert isinstance(error, ETLComponentError)
        assert str(error) == "Load failed"


class TestETLComponent:
    """Test ETLComponent abstract base class"""
    
    def test_cannot_instantiate_abstract_class(self):
        """Test that abstract class cannot be instantiated"""
        with pytest.raises(TypeError):
            ETLComponent()
    
    def test_concrete_implementation(self):
        """Test concrete implementation of ETLComponent"""
        component = ConcreteETLComponent("TestComp")
        
        assert component.get_component_name() == "TestComp"
        assert component.validate_prerequisites() is True
        
        result = component.execute()
        assert isinstance(result, ExecutionResult)
        assert result.success is True
    
    def test_execute_with_exception(self):
        """Test execute method raising exception"""
        component = ConcreteETLComponent(should_fail=True)
        
        with pytest.raises(ETLComponentError) as exc_info:
            component.execute()
        
        assert "Execution failed" in str(exc_info.value)
        assert exc_info.value.error_step == "TEST"
        assert exc_info.value.record_id == "TEST001"
    
    def test_prerequisites_validation(self):
        """Test prerequisites validation"""
        component = ConcreteETLComponent()
        component.prerequisites_valid = False
        
        assert component.validate_prerequisites() is False


class TestETLLogger:
    """Test ETLLogger abstract base class"""
    
    def test_cannot_instantiate_abstract_class(self):
        """Test that abstract logger cannot be instantiated"""
        with pytest.raises(TypeError):
            ETLLogger()
    
    def test_logger_constants(self):
        """Test logger status and step constants"""
        assert ETLLogger.STATUS_SUCCESS == 'S'
        assert ETLLogger.STATUS_ERROR == 'E'
        assert ETLLogger.STATUS_WARNING == 'W'
        assert ETLLogger.STATUS_INFO == 'I'
        
        assert ETLLogger.STEP_INIT == 'INIT'
        assert ETLLogger.STEP_EXTRACT == 'EXTRACT'
        assert ETLLogger.STEP_TRANSFORM == 'TRANSFORM'
        assert ETLLogger.STEP_LOAD == 'LOAD'
        assert ETLLogger.STEP_VALIDATE == 'VALIDATE'
        assert ETLLogger.STEP_COMPLETE == 'COMPLETE'
        assert ETLLogger.STEP_ERROR == 'ERROR'
    
    def test_concrete_logger_implementation(self):
        """Test concrete logger implementation"""
        logger = ConcreteETLLogger("RUN_123")
        
        assert logger.get_etl_run_id() == "RUN_123"
        
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Extraction completed",
            records_processed=100,
            records_success=100,
            records_error=0
        )
        
        assert len(logger.messages) == 1
        assert logger.messages[0]['step'] == 'EXTRACT'
        assert logger.messages[0]['status'] == 'S'
        assert logger.messages[0]['message'] == "Extraction completed"
        assert logger.messages[0]['records_processed'] == 100
    
    def test_multiple_log_entries(self):
        """Test logging multiple messages"""
        logger = ConcreteETLLogger()
        
        logger.log_message(
            step=ETLLogger.STEP_INIT,
            status=ETLLogger.STATUS_INFO,
            message="Starting ETL"
        )
        
        logger.log_message(
            step=ETLLogger.STEP_EXTRACT,
            status=ETLLogger.STATUS_SUCCESS,
            message="Extracted 1000 records",
            records_processed=1000,
            records_success=1000
        )
        
        logger.log_message(
            step=ETLLogger.STEP_TRANSFORM,
            status=ETLLogger.STATUS_WARNING,
            message="5 records failed transformation",
            records_processed=1000,
            records_success=995,
            records_error=5
        )
        
        assert len(logger.messages) == 3
        assert logger.messages[0]['step'] == 'INIT'
        assert logger.messages[1]['step'] == 'EXTRACT'
        assert logger.messages[2]['step'] == 'TRANSFORM'
        assert logger.messages[2]['records_error'] == 5


class TestIntegration:
    """Integration tests for component and logger"""
    
    def test_component_with_logger(self):
        """Test ETL component using logger"""
        logger = ConcreteETLLogger("INT_TEST_001")
        component = ConcreteETLComponent("IntegrationTest")
        
        # Log start
        logger.log_message(
            step=ETLLogger.STEP_INIT,
            status=ETLLogger.STATUS_INFO,
            message=f"Initializing {component.get_component_name()}"
        )
        
        # Validate prerequisites
        if component.validate_prerequisites():
            logger.log_message(
                step=ETLLogger.STEP_VALIDATE,
                status=ETLLogger.STATUS_SUCCESS,
                message="Prerequisites validated"
            )
        
        # Execute
        result = component.execute()
        
        # Log result
        logger.log_message(
            step=ETLLogger.STEP_COMPLETE,
            status=ETLLogger.STATUS_SUCCESS if result.success else ETLLogger.STATUS_ERROR,
            message=result.message,
            records_processed=result.records_total,
            records_success=result.records_success,
            records_error=result.records_error
        )
        
        assert len(logger.messages) == 3
        assert logger.messages[-1]['records_processed'] == 100
        assert logger.messages[-1]['records_success'] == 95
    
    def test_error_handling_with_logging(self):
        """Test error handling with logger integration"""
        logger = ConcreteETLLogger("ERROR_TEST_001")
        component = ConcreteETLComponent(should_fail=True)
        
        try:
            component.execute()
        except ETLComponentError as e:
            logger.log_message(
                step=ETLLogger.STEP_ERROR,
                status=ETLLogger.STATUS_ERROR,
                message=str(e)
            )
        
        assert len(logger.messages) == 1
        assert logger.messages[0]['status'] == 'E'
        assert "Execution failed" in logger.messages[0]['message']


if __name__ == '__main__':
    pytest.main([__file__, '-v'])