"""
Custom ETL Exception Hierarchy
Specialized exception classes for Extract, Transform, and Load operations.
"""

from typing import Optional, Dict, Any
from datetime import datetime


class ETLError(Exception):
    """
    Base exception class for all ETL errors.
    Provides common functionality for error handling and logging.
    """
    
    def __init__(
        self,
        message: str,
        error_step: Optional[str] = None,
        record_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize ETL error.
        
        Args:
            message: Error description
            error_step: ETL step where error occurred (INIT, EXTRACT, TRANSFORM, LOAD)
            record_id: ID of record being processed when error occurred
            details: Additional error context and metadata
        """
        self.message = message
        self.error_step = error_step or "UNKNOWN"
        self.record_id = record_id
        self.details = details or {}
        self.timestamp = datetime.now()
        
        # Build full error message
        full_message = self._build_message()
        super().__init__(full_message)
    
    def _build_message(self) -> str:
        """Build comprehensive error message."""
        parts = [f"[{self.error_step}]"]
        
        if self.record_id:
            parts.append(f"Record {self.record_id}")
        
        parts.append(self.message)
        
        if self.details:
            details_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            parts.append(f"({details_str})")
        
        return " - ".join(parts)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert exception to dictionary for logging/serialization.
        
        Returns:
            Dictionary representation of the exception
        """
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "error_step": self.error_step,
            "record_id": self.record_id,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
            "full_message": str(self)
        }


class ExtractError(ETLError):
    """
    Exception raised during data extraction phase.
    Used for source data reading, connection, and query errors.
    """
    
    def __init__(
        self,
        message: str,
        source_table: Optional[str] = None,
        query: Optional[str] = None,
        record_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize extraction error.
        
        Args:
            message: Error description
            source_table: Source table/dataset name
            query: Query/filter that failed
            record_id: Record ID if applicable
            details: Additional error context
        """
        details = details or {}
        if source_table:
            details["source_table"] = source_table
        if query:
            details["query"] = query
        
        super().__init__(
            message=message,
            error_step="EXTRACT",
            record_id=record_id,
            details=details
        )
        
        self.source_table = source_table
        self.query = query


class TransformError(ETLError):
    """
    Exception raised during data transformation phase.
    Used for calculation errors, data validation failures, and business rule violations.
    """
    
    def __init__(
        self,
        message: str,
        record_id: Optional[str] = None,
        field_name: Optional[str] = None,
        field_value: Optional[Any] = None,
        transformation_rule: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize transformation error.
        
        Args:
            message: Error description
            record_id: Record ID being transformed
            field_name: Field that caused the error
            field_value: Value that caused the error
            transformation_rule: Business rule or transformation that failed
            details: Additional error context
        """
        details = details or {}
        if field_name:
            details["field_name"] = field_name
        if field_value is not None:
            details["field_value"] = str(field_value)
        if transformation_rule:
            details["transformation_rule"] = transformation_rule
        
        super().__init__(
            message=message,
            error_step="TRANSFORM",
            record_id=record_id,
            details=details
        )
        
        self.field_name = field_name
        self.field_value = field_value
        self.transformation_rule = transformation_rule


class LoadError(ETLError):
    """
    Exception raised during data loading phase.
    Used for target database errors, validation failures, and write errors.
    """
    
    def __init__(
        self,
        message: str,
        target_table: Optional[str] = None,
        record_id: Optional[str] = None,
        operation: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize load error.
        
        Args:
            message: Error description
            target_table: Target table/dataset name
            record_id: Record ID being loaded
            operation: Operation that failed (INSERT, UPDATE, UPSERT)
            details: Additional error context
        """
        details = details or {}
        if target_table:
            details["target_table"] = target_table
        if operation:
            details["operation"] = operation
        
        super().__init__(
            message=message,
            error_step="LOAD",
            record_id=record_id,
            details=details
        )
        
        self.target_table = target_table
        self.operation = operation


class ValidationError(ETLError):
    """
    Exception raised during data validation.
    Used for schema validation, data quality checks, and constraint violations.
    """
    
    def __init__(
        self,
        message: str,
        validation_type: str,
        record_id: Optional[str] = None,
        expected_value: Optional[Any] = None,
        actual_value: Optional[Any] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize validation error.
        
        Args:
            message: Error description
            validation_type: Type of validation that failed
            record_id: Record ID being validated
            expected_value: Expected value or format
            actual_value: Actual value that failed validation
            details: Additional error context
        """
        details = details or {}
        details["validation_type"] = validation_type
        if expected_value is not None:
            details["expected_value"] = str(expected_value)
        if actual_value is not None:
            details["actual_value"] = str(actual_value)
        
        super().__init__(
            message=message,
            error_step="VALIDATE",
            record_id=record_id,
            details=details
        )
        
        self.validation_type = validation_type
        self.expected_value = expected_value
        self.actual_value = actual_value


class ConfigurationError(ETLError):
    """
    Exception raised for configuration errors.
    Used for invalid settings, missing parameters, and setup issues.
    """
    
    def __init__(
        self,
        message: str,
        config_key: Optional[str] = None,
        config_value: Optional[Any] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize configuration error.
        
        Args:
            message: Error description
            config_key: Configuration key that caused the error
            config_value: Invalid configuration value
            details: Additional error context
        """
        details = details or {}
        if config_key:
            details["config_key"] = config_key
        if config_value is not None:
            details["config_value"] = str(config_value)
        
        super().__init__(
            message=message,
            error_step="INIT",
            record_id=None,
            details=details
        )
        
        self.config_key = config_key
        self.config_value = config_value


class ETLTimeoutError(ETLError):
    """
    Exception raised when ETL operation times out.
    """
    
    def __init__(
        self,
        message: str,
        timeout_seconds: int,
        error_step: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize timeout error.
        
        Args:
            message: Error description
            timeout_seconds: Timeout threshold that was exceeded
            error_step: ETL step that timed out
            details: Additional error context
        """
        details = details or {}
        details["timeout_seconds"] = timeout_seconds
        
        super().__init__(
            message=message,
            error_step=error_step or "TIMEOUT",
            record_id=None,
            details=details
        )
        
        self.timeout_seconds = timeout_seconds


class ETLRetryableError(ETLError):
    """
    Exception that indicates the operation can be retried.
    Used for transient errors like network issues or temporary resource unavailability.
    """
    
    def __init__(
        self,
        message: str,
        retry_after_seconds: Optional[int] = None,
        max_retries: Optional[int] = None,
        error_step: Optional[str] = None,
        record_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize retryable error.
        
        Args:
            message: Error description
            retry_after_seconds: Recommended wait time before retry
            max_retries: Maximum number of retries allowed
            error_step: ETL step where error occurred
            record_id: Record ID if applicable
            details: Additional error context
        """
        details = details or {}
        if retry_after_seconds:
            details["retry_after_seconds"] = retry_after_seconds
        if max_retries:
            details["max_retries"] = max_retries
        
        super().__init__(
            message=message,
            error_step=error_step,
            record_id=record_id,
            details=details
        )
        
        self.retry_after_seconds = retry_after_seconds
        self.max_retries = max_retries


# Exception hierarchy summary for reference
__all__ = [
    "ETLError",              # Base exception
    "ExtractError",          # Data extraction errors
    "TransformError",        # Data transformation errors
    "LoadError",             # Data loading errors
    "ValidationError",       # Data validation errors
    "ConfigurationError",    # Configuration errors
    "ETLTimeoutError",       # Timeout errors
    "ETLRetryableError",     # Retryable errors
]