"""
ETL Exception Hierarchy Module

This module defines a custom exception hierarchy for ETL errors,
replacing the ABAP ZCX_ETL_ERROR exception class and if_t100_message interface.
"""

from typing import Optional, Dict, Any
from datetime import datetime


class ETLError(Exception):
    """
    Base exception class for all ETL errors.
    
    Replaces ABAP ZCX_ETL_ERROR base exception with custom message formatting
    instead of if_t100_message interface.
    """
    
    def __init__(
        self,
        message: str,
        error_step: Optional[str] = None,
        record_id: Optional[str] = None,
        previous: Optional[Exception] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize ETL error with context.
        
        Args:
            message: Human-readable error message
            error_step: ETL step where error occurred (EXTRACT/TRANSFORM/LOAD)
            record_id: ID of record being processed when error occurred
            previous: Previous exception that caused this error (chained exception)
            context: Additional context information as key-value pairs
        """
        super().__init__(message)
        self.message = message
        self.error_step = error_step or "UNKNOWN"
        self.record_id = record_id
        self.previous = previous
        self.context = context or {}
        self.timestamp = datetime.now()
        
    def get_formatted_message(self) -> str:
        """
        Get formatted error message with context.
        
        Replaces ABAP if_t100_message interface formatting.
        
        Returns:
            Formatted error message string
        """
        parts = [f"[{self.error_step}]"]
        
        if self.record_id:
            parts.append(f"Record: {self.record_id}")
            
        parts.append(self.message)
        
        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            parts.append(f"Context: {context_str}")
            
        if self.previous:
            parts.append(f"Caused by: {str(self.previous)}")
            
        return " | ".join(parts)
    
    def __str__(self) -> str:
        """String representation of the error."""
        return self.get_formatted_message()
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert exception to dictionary for logging/serialization.
        
        Returns:
            Dictionary representation of exception
        """
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "error_step": self.error_step,
            "record_id": self.record_id,
            "timestamp": self.timestamp.isoformat(),
            "context": self.context,
            "previous_error": str(self.previous) if self.previous else None
        }


class ExtractError(ETLError):
    """
    Exception for data extraction errors.
    
    Replaces ABAP ZCX_ETL_ERROR=>EXTRACT_ERROR constant.
    Raised when errors occur during the extract phase.
    """
    
    def __init__(
        self,
        message: str,
        source_table: Optional[str] = None,
        record_id: Optional[str] = None,
        previous: Optional[Exception] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize extraction error.
        
        Args:
            message: Error message
            source_table: Name of source table/dataset
            record_id: ID of record causing error
            previous: Previous exception
            context: Additional context
        """
        context = context or {}
        if source_table:
            context["source_table"] = source_table
            
        super().__init__(
            message=message,
            error_step="EXTRACT",
            record_id=record_id,
            previous=previous,
            context=context
        )
        self.source_table = source_table


class TransformError(ETLError):
    """
    Exception for data transformation errors.
    
    Replaces ABAP ZCX_ETL_ERROR=>TRANSFORM_ERROR constant.
    Raised when errors occur during the transform phase.
    """
    
    def __init__(
        self,
        message: str,
        transformation_rule: Optional[str] = None,
        record_id: Optional[str] = None,
        field_name: Optional[str] = None,
        field_value: Optional[Any] = None,
        previous: Optional[Exception] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize transformation error.
        
        Args:
            message: Error message
            transformation_rule: Name of transformation rule that failed
            record_id: ID of record causing error
            field_name: Name of field being transformed
            field_value: Value that caused error
            previous: Previous exception
            context: Additional context
        """
        context = context or {}
        if transformation_rule:
            context["transformation_rule"] = transformation_rule
        if field_name:
            context["field_name"] = field_name
        if field_value is not None:
            context["field_value"] = str(field_value)
            
        super().__init__(
            message=message,
            error_step="TRANSFORM",
            record_id=record_id,
            previous=previous,
            context=context
        )
        self.transformation_rule = transformation_rule
        self.field_name = field_name
        self.field_value = field_value


class LoadError(ETLError):
    """
    Exception for data loading errors.
    
    Replaces ABAP ZCX_ETL_ERROR=>LOAD_ERROR constant.
    Raised when errors occur during the load phase.
    """
    
    def __init__(
        self,
        message: str,
        target_table: Optional[str] = None,
        record_id: Optional[str] = None,
        operation: Optional[str] = None,
        previous: Optional[Exception] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize load error.
        
        Args:
            message: Error message
            target_table: Name of target table/dataset
            record_id: ID of record causing error
            operation: Database operation (INSERT/UPDATE/DELETE)
            previous: Previous exception
            context: Additional context
        """
        context = context or {}
        if target_table:
            context["target_table"] = target_table
        if operation:
            context["operation"] = operation
            
        super().__init__(
            message=message,
            error_step="LOAD",
            record_id=record_id,
            previous=previous,
            context=context
        )
        self.target_table = target_table
        self.operation = operation


class ValidationError(ETLError):
    """
    Exception for data validation errors.
    
    Raised when data fails validation rules.
    """
    
    def __init__(
        self,
        message: str,
        validation_rule: Optional[str] = None,
        record_id: Optional[str] = None,
        field_name: Optional[str] = None,
        expected_value: Optional[Any] = None,
        actual_value: Optional[Any] = None,
        previous: Optional[Exception] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize validation error.
        
        Args:
            message: Error message
            validation_rule: Name of validation rule that failed
            record_id: ID of record causing error
            field_name: Name of field that failed validation
            expected_value: Expected value or pattern
            actual_value: Actual value received
            previous: Previous exception
            context: Additional context
        """
        context = context or {}
        if validation_rule:
            context["validation_rule"] = validation_rule
        if field_name:
            context["field_name"] = field_name
        if expected_value is not None:
            context["expected_value"] = str(expected_value)
        if actual_value is not None:
            context["actual_value"] = str(actual_value)
            
        super().__init__(
            message=message,
            error_step="VALIDATE",
            record_id=record_id,
            previous=previous,
            context=context
        )
        self.validation_rule = validation_rule
        self.field_name = field_name
        self.expected_value = expected_value
        self.actual_value = actual_value


class ConfigurationError(ETLError):
    """
    Exception for ETL configuration errors.
    
    Raised when ETL configuration is invalid or missing.
    """
    
    def __init__(
        self,
        message: str,
        config_key: Optional[str] = None,
        previous: Optional[Exception] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize configuration error.
        
        Args:
            message: Error message
            config_key: Configuration key that is invalid/missing
            previous: Previous exception
            context: Additional context
        """
        context = context or {}
        if config_key:
            context["config_key"] = config_key
            
        super().__init__(
            message=message,
            error_step="INIT",
            previous=previous,
            context=context
        )
        self.config_key = config_key


class ConnectionError(ETLError):
    """
    Exception for database/data source connection errors.
    
    Raised when connection to data sources fails.
    """
    
    def __init__(
        self,
        message: str,
        connection_type: Optional[str] = None,
        host: Optional[str] = None,
        previous: Optional[Exception] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize connection error.
        
        Args:
            message: Error message
            connection_type: Type of connection (database/file/api)
            host: Host or connection string
            previous: Previous exception
            context: Additional context
        """
        context = context or {}
        if connection_type:
            context["connection_type"] = connection_type
        if host:
            context["host"] = host
            
        super().__init__(
            message=message,
            error_step="INIT",
            previous=previous,
            context=context
        )
        self.connection_type = connection_type
        self.host = host


# Convenience function for error handling
def handle_etl_error(
    operation: str,
    error: Exception,
    record_id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None
) -> ETLError:
    """
    Convert standard Python exceptions to ETL exceptions.
    
    Args:
        operation: Operation being performed (extract/transform/load)
        error: Original exception
        record_id: Record ID if applicable
        context: Additional context
        
    Returns:
        Appropriate ETLError subclass
    """
    operation_lower = operation.lower()
    message = f"Error during {operation}: {str(error)}"
    
    if operation_lower == "extract":
        return ExtractError(
            message=message,
            record_id=record_id,
            previous=error,
            context=context
        )
    elif operation_lower == "transform":
        return TransformError(
            message=message,
            record_id=record_id,
            previous=error,
            context=context
        )
    elif operation_lower == "load":
        return LoadError(
            message=message,
            record_id=record_id,
            previous=error,
            context=context
        )
    else:
        return ETLError(
            message=message,
            error_step=operation,
            record_id=record_id,
            previous=error,
            context=context
        )