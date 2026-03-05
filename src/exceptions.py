"""
Custom exception hierarchy for ETL errors.

This module defines a comprehensive exception hierarchy for the ETL system,
replacing the ABAP ZCX_ETL_ERROR exception class with Python-based custom
exceptions. Each exception type supports custom message formatting and
contextual information.
"""

from typing import Optional, Dict, Any
from datetime import datetime


class ETLError(Exception):
    """
    Base exception class for all ETL-related errors.
    
    Replaces ABAP class: ZCX_ETL_ERROR
    
    This base exception provides common functionality for all ETL exceptions:
    - Custom message formatting
    - Error context tracking
    - Step and record identification
    - Timestamp tracking
    
    Attributes:
        message: Human-readable error message
        error_step: ETL step where error occurred (EXTRACT, TRANSFORM, LOAD)
        record_id: Optional identifier of the record that caused the error
        context: Additional context information as key-value pairs
        timestamp: When the error occurred
    """
    
    def __init__(
        self,
        message: str,
        error_step: Optional[str] = None,
        record_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize ETL error with message and context.
        
        Args:
            message: Description of the error
            error_step: ETL step identifier (INIT, EXTRACT, TRANSFORM, LOAD, etc.)
            record_id: Record identifier that caused the error
            context: Additional contextual information
        """
        super().__init__(message)
        self.message = message
        self.error_step = error_step
        self.record_id = record_id
        self.context = context or {}
        self.timestamp = datetime.now()
        
    def __str__(self) -> str:
        """Format exception as string with full context."""
        parts = [f"ETLError: {self.message}"]
        
        if self.error_step:
            parts.append(f"Step: {self.error_step}")
            
        if self.record_id:
            parts.append(f"Record ID: {self.record_id}")
            
        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            parts.append(f"Context: {context_str}")
            
        parts.append(f"Timestamp: {self.timestamp.isoformat()}")
        
        return " | ".join(parts)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert exception to dictionary for logging/serialization.
        
        Returns:
            Dictionary with error details
        """
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "error_step": self.error_step,
            "record_id": self.record_id,
            "context": self.context,
            "timestamp": self.timestamp.isoformat()
        }


class ExtractError(ETLError):
    """
    Exception raised during data extraction phase.
    
    Replaces ABAP constant: zcx_etl_error=>extract_error
    
    This exception is raised when errors occur during:
    - Database connection failures
    - Query execution errors
    - Data retrieval issues
    - Source data validation failures
    
    Example:
        raise ExtractError(
            message="Failed to connect to source database",
            error_step="EXTRACT",
            context={"host": "localhost", "port": 5432}
        )
    """
    
    def __init__(
        self,
        message: str,
        record_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize extraction error.
        
        Args:
            message: Description of the extraction error
            record_id: Optional record identifier
            context: Additional context (e.g., connection details, query)
        """
        super().__init__(
            message=message,
            error_step="EXTRACT",
            record_id=record_id,
            context=context
        )


class TransformError(ETLError):
    """
    Exception raised during data transformation phase.
    
    Replaces ABAP constant: zcx_etl_error=>transform_error
    
    This exception is raised when errors occur during:
    - Data type conversion failures
    - Business rule validation errors
    - Calculation errors
    - Data quality issues
    
    Example:
        raise TransformError(
            message="Invalid quantity value for discount calculation",
            record_id="T000001",
            context={"quantity": -5, "unit_price": 99.99}
        )
    """
    
    def __init__(
        self,
        message: str,
        record_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize transformation error.
        
        Args:
            message: Description of the transformation error
            record_id: Record identifier being transformed
            context: Additional context (e.g., field values, validation rules)
        """
        super().__init__(
            message=message,
            error_step="TRANSFORM",
            record_id=record_id,
            context=context
        )


class LoadError(ETLError):
    """
    Exception raised during data loading phase.
    
    Replaces ABAP constant: zcx_etl_error=>load_error
    
    This exception is raised when errors occur during:
    - Database insert/update failures
    - Constraint violations
    - Target data validation failures
    - Transaction commit errors
    
    Example:
        raise LoadError(
            message="Duplicate key violation on analytics_id",
            record_id="ANL20240115123456",
            context={"table": "sales_analytics", "constraint": "pk_analytics"}
        )
    """
    
    def __init__(
        self,
        message: str,
        record_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize load error.
        
        Args:
            message: Description of the load error
            record_id: Record identifier being loaded
            context: Additional context (e.g., table name, constraint info)
        """
        super().__init__(
            message=message,
            error_step="LOAD",
            record_id=record_id,
            context=context
        )


class ValidationError(ETLError):
    """
    Exception raised during data validation.
    
    This exception is raised when data fails validation rules:
    - Required field missing
    - Invalid data format
    - Business rule violations
    - Data consistency checks
    
    Example:
        raise ValidationError(
            message="Required field 'customer_id' is missing",
            error_step="VALIDATE",
            record_id="T000001",
            context={"field": "customer_id", "value": None}
        )
    """
    
    def __init__(
        self,
        message: str,
        error_step: str = "VALIDATE",
        record_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize validation error.
        
        Args:
            message: Description of the validation error
            error_step: Step where validation occurred
            record_id: Record identifier being validated
            context: Additional context (e.g., field name, expected vs actual)
        """
        super().__init__(
            message=message,
            error_step=error_step,
            record_id=record_id,
            context=context
        )


class ConfigurationError(ETLError):
    """
    Exception raised for ETL configuration issues.
    
    This exception is raised when:
    - Configuration file is missing or invalid
    - Required configuration parameters are missing
    - Configuration values are out of acceptable range
    
    Example:
        raise ConfigurationError(
            message="Invalid batch_size configuration",
            error_step="INIT",
            context={"batch_size": -1, "expected_range": "1-10000"}
        )
    """
    
    def __init__(
        self,
        message: str,
        error_step: str = "INIT",
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize configuration error.
        
        Args:
            message: Description of the configuration error
            error_step: Step where configuration error occurred
            context: Additional context (e.g., parameter name, value)
        """
        super().__init__(
            message=message,
            error_step=error_step,
            record_id=None,
            context=context
        )


class ConnectionError(ETLError):
    """
    Exception raised for database/external system connection issues.
    
    This exception is raised when:
    - Unable to establish database connection
    - Connection timeout
    - Authentication failures
    - Network connectivity issues
    
    Example:
        raise ConnectionError(
            message="Failed to connect to Spark cluster",
            error_step="INIT",
            context={"master": "spark://localhost:7077", "timeout": 30}
        )
    """
    
    def __init__(
        self,
        message: str,
        error_step: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize connection error.
        
        Args:
            message: Description of the connection error
            error_step: Step where connection error occurred
            context: Additional context (e.g., host, port, credentials)
        """
        super().__init__(
            message=message,
            error_step=error_step or "INIT",
            record_id=None,
            context=context
        )


# Exception hierarchy mapping for backwards compatibility
# Maps ABAP exception message numbers to Python exception classes
EXCEPTION_MAP = {
    "001": ETLError,      # General ETL error
    "002": ExtractError,  # Extraction error
    "003": TransformError,  # Transformation error
    "004": LoadError,     # Load error
}


def create_exception_from_code(
    error_code: str,
    message: str,
    **kwargs
) -> ETLError:
    """
    Factory function to create exception from error code.
    
    Provides backwards compatibility with ABAP message number system.
    
    Args:
        error_code: ABAP-style error code (e.g., "002" for extract error)
        message: Error message
        **kwargs: Additional arguments for exception constructor
        
    Returns:
        Instance of appropriate exception class
        
    Example:
        exc = create_exception_from_code(
            "002",
            "Database query failed",
            context={"table": "sales_raw"}
        )
    """
    exception_class = EXCEPTION_MAP.get(error_code, ETLError)
    return exception_class(message=message, **kwargs)