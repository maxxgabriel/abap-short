"""
Custom ETL Exception Hierarchy
Provides specialized exception classes for Extract, Transform, and Load operations
"""

from typing import Optional, Dict, Any
from datetime import datetime


class ETLError(Exception):
    """
    Base exception class for all ETL errors.
    
    Attributes:
        message: Error message
        error_step: ETL step where error occurred (EXTRACT, TRANSFORM, LOAD, etc.)
        record_id: ID of the record that caused the error
        timestamp: When the error occurred
        context: Additional context information
    """
    
    def __init__(
        self,
        message: str,
        error_step: Optional[str] = None,
        record_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize ETL error.
        
        Args:
            message: Error message
            error_step: ETL step where error occurred
            record_id: ID of the record that caused the error
            context: Additional context information
        """
        self.message = message
        self.error_step = error_step or "UNKNOWN"
        self.record_id = record_id
        self.timestamp = datetime.now()
        self.context = context or {}
        
        # Build detailed error message
        error_parts = [f"[{self.error_step}] {message}"]
        
        if record_id:
            error_parts.append(f"Record ID: {record_id}")
        
        if context:
            context_str = ", ".join([f"{k}={v}" for k, v in context.items()])
            error_parts.append(f"Context: {context_str}")
        
        super().__init__(" | ".join(error_parts))
    
    def get_error_details(self) -> Dict[str, Any]:
        """
        Get structured error details.
        
        Returns:
            Dictionary with error details
        """
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "error_step": self.error_step,
            "record_id": self.record_id,
            "timestamp": self.timestamp.isoformat(),
            "context": self.context
        }
    
    def __str__(self) -> str:
        """String representation of the error."""
        return f"{self.__class__.__name__}: {self.message}"


class ExtractError(ETLError):
    """
    Exception raised during data extraction phase.
    
    This exception is raised when:
    - Source data cannot be read
    - Database connection fails
    - Query execution fails
    - Data validation fails during extraction
    - Source file format is invalid
    """
    
    def __init__(
        self,
        message: str,
        source: Optional[str] = None,
        query: Optional[str] = None,
        record_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize extraction error.
        
        Args:
            message: Error message
            source: Data source identifier (table name, file path, etc.)
            query: Query that failed (if applicable)
            record_id: ID of the record that caused the error
            context: Additional context information
        """
        error_context = context or {}
        
        if source:
            error_context["source"] = source
        
        if query:
            error_context["query"] = query
        
        super().__init__(
            message=message,
            error_step="EXTRACT",
            record_id=record_id,
            context=error_context
        )


class TransformError(ETLError):
    """
    Exception raised during data transformation phase.
    
    This exception is raised when:
    - Data type conversion fails
    - Business rule validation fails
    - Calculated field computation fails
    - Data enrichment fails
    - Data quality checks fail
    """
    
    def __init__(
        self,
        message: str,
        transformation: Optional[str] = None,
        field_name: Optional[str] = None,
        field_value: Optional[Any] = None,
        record_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize transformation error.
        
        Args:
            message: Error message
            transformation: Name of the transformation that failed
            field_name: Field that caused the error
            field_value: Value that caused the error
            record_id: ID of the record that caused the error
            context: Additional context information
        """
        error_context = context or {}
        
        if transformation:
            error_context["transformation"] = transformation
        
        if field_name:
            error_context["field_name"] = field_name
        
        if field_value is not None:
            error_context["field_value"] = str(field_value)
        
        super().__init__(
            message=message,
            error_step="TRANSFORM",
            record_id=record_id,
            context=error_context
        )


class LoadError(ETLError):
    """
    Exception raised during data loading phase.
    
    This exception is raised when:
    - Target database write fails
    - Data validation fails before load
    - Duplicate key violations occur
    - Transaction commit fails
    - Target file write fails
    """
    
    def __init__(
        self,
        message: str,
        target: Optional[str] = None,
        operation: Optional[str] = None,
        record_id: Optional[str] = None,
        records_processed: Optional[int] = None,
        records_failed: Optional[int] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize load error.
        
        Args:
            message: Error message
            target: Target identifier (table name, file path, etc.)
            operation: Type of load operation (INSERT, UPDATE, UPSERT)
            record_id: ID of the record that caused the error
            records_processed: Number of records processed before error
            records_failed: Number of records that failed
            context: Additional context information
        """
        error_context = context or {}
        
        if target:
            error_context["target"] = target
        
        if operation:
            error_context["operation"] = operation
        
        if records_processed is not None:
            error_context["records_processed"] = records_processed
        
        if records_failed is not None:
            error_context["records_failed"] = records_failed
        
        super().__init__(
            message=message,
            error_step="LOAD",
            record_id=record_id,
            context=error_context
        )


class ValidationError(ETLError):
    """
    Exception raised during data validation.
    
    This exception is raised when:
    - Required fields are missing
    - Data types are invalid
    - Business rules are violated
    - Data quality checks fail
    """
    
    def __init__(
        self,
        message: str,
        validation_rule: Optional[str] = None,
        field_name: Optional[str] = None,
        field_value: Optional[Any] = None,
        expected_value: Optional[Any] = None,
        record_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize validation error.
        
        Args:
            message: Error message
            validation_rule: Validation rule that failed
            field_name: Field that failed validation
            field_value: Actual value
            expected_value: Expected value or format
            record_id: ID of the record that caused the error
            context: Additional context information
        """
        error_context = context or {}
        
        if validation_rule:
            error_context["validation_rule"] = validation_rule
        
        if field_name:
            error_context["field_name"] = field_name
        
        if field_value is not None:
            error_context["field_value"] = str(field_value)
        
        if expected_value is not None:
            error_context["expected_value"] = str(expected_value)
        
        super().__init__(
            message=message,
            error_step="VALIDATE",
            record_id=record_id,
            context=error_context
        )


class ConfigurationError(ETLError):
    """
    Exception raised for configuration errors.
    
    This exception is raised when:
    - Configuration file is missing or invalid
    - Required configuration parameters are missing
    - Configuration values are invalid
    """
    
    def __init__(
        self,
        message: str,
        config_key: Optional[str] = None,
        config_value: Optional[Any] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize configuration error.
        
        Args:
            message: Error message
            config_key: Configuration key that caused the error
            config_value: Invalid configuration value
            context: Additional context information
        """
        error_context = context or {}
        
        if config_key:
            error_context["config_key"] = config_key
        
        if config_value is not None:
            error_context["config_value"] = str(config_value)
        
        super().__init__(
            message=message,
            error_step="CONFIG",
            context=error_context
        )


class OrchestrationError(ETLError):
    """
    Exception raised during ETL orchestration.
    
    This exception is raised when:
    - ETL workflow initialization fails
    - Component coordination fails
    - Resource allocation fails
    - Workflow state is invalid
    """
    
    def __init__(
        self,
        message: str,
        workflow_step: Optional[str] = None,
        failed_component: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize orchestration error.
        
        Args:
            message: Error message
            workflow_step: Workflow step that failed
            failed_component: Component that caused the failure
            context: Additional context information
        """
        error_context = context or {}
        
        if workflow_step:
            error_context["workflow_step"] = workflow_step
        
        if failed_component:
            error_context["failed_component"] = failed_component
        
        super().__init__(
            message=message,
            error_step="ORCHESTRATION",
            context=error_context
        )