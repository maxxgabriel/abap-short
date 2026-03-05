"""
Custom ETL Exception Hierarchy

This module defines a specialized exception hierarchy for ETL operations,
providing granular error handling for Extract, Transform, and Load phases.
"""

from typing import Optional, Dict, Any
from datetime import datetime


class ETLError(Exception):
    """
    Base exception class for all ETL errors.
    
    This is the root exception that all ETL-specific exceptions inherit from.
    Provides common functionality for error tracking, logging, and context.
    
    Attributes:
        error_text: Human-readable error message
        error_step: ETL step where error occurred (EXTRACT, TRANSFORM, LOAD, etc.)
        record_id: Optional identifier of the record that caused the error
        timestamp: When the error occurred
        context: Additional context information
    """
    
    def __init__(
        self,
        error_text: str,
        error_step: Optional[str] = None,
        record_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize ETL error with context information.
        
        Args:
            error_text: Description of the error
            error_step: ETL phase where error occurred
            record_id: ID of problematic record
            context: Additional context data
        """
        super().__init__(error_text)
        self.error_text = error_text
        self.error_step = error_step or "UNKNOWN"
        self.record_id = record_id
        self.timestamp = datetime.now()
        self.context = context or {}
        
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert exception to dictionary for logging/serialization.
        
        Returns:
            Dictionary representation of the exception
        """
        return {
            "error_type": self.__class__.__name__,
            "error_text": self.error_text,
            "error_step": self.error_step,
            "record_id": self.record_id,
            "timestamp": self.timestamp.isoformat(),
            "context": self.context
        }
    
    def __str__(self) -> str:
        """String representation of the error."""
        parts = [f"[{self.error_step}]", self.error_text]
        if self.record_id:
            parts.append(f"(Record: {self.record_id})")
        return " ".join(parts)


class ExtractError(ETLError):
    """
    Exception raised during data extraction phase.
    
    This exception is raised when issues occur while reading data from source
    systems, databases, or files during the EXTRACT phase.
    
    Common scenarios:
        - Source system unavailable
        - Authentication failures
        - Invalid query/filter parameters
        - Missing source tables/files
        - Data retrieval timeouts
    """
    
    def __init__(
        self,
        error_text: str,
        record_id: Optional[str] = None,
        source: Optional[str] = None,
        query: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize extraction error.
        
        Args:
            error_text: Description of extraction error
            record_id: ID of problematic record
            source: Source system/table/file name
            query: Query or filter that failed
            context: Additional context
        """
        context = context or {}
        if source:
            context["source"] = source
        if query:
            context["query"] = query
            
        super().__init__(
            error_text=error_text,
            error_step="EXTRACT",
            record_id=record_id,
            context=context
        )
        self.source = source
        self.query = query


class TransformError(ETLError):
    """
    Exception raised during data transformation phase.
    
    This exception is raised when business logic, calculations, or data
    transformations fail during the TRANSFORM phase.
    
    Common scenarios:
        - Invalid data format
        - Calculation errors (division by zero, overflow)
        - Failed business rule validation
        - Type conversion failures
        - Missing required fields
        - Data quality issues
    """
    
    def __init__(
        self,
        error_text: str,
        record_id: Optional[str] = None,
        field_name: Optional[str] = None,
        field_value: Optional[Any] = None,
        transformation: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize transformation error.
        
        Args:
            error_text: Description of transformation error
            record_id: ID of problematic record
            field_name: Name of field that caused error
            field_value: Value that caused error
            transformation: Name of transformation that failed
            context: Additional context
        """
        context = context or {}
        if field_name:
            context["field_name"] = field_name
        if field_value is not None:
            context["field_value"] = str(field_value)
        if transformation:
            context["transformation"] = transformation
            
        super().__init__(
            error_text=error_text,
            error_step="TRANSFORM",
            record_id=record_id,
            context=context
        )
        self.field_name = field_name
        self.field_value = field_value
        self.transformation = transformation


class LoadError(ETLError):
    """
    Exception raised during data loading phase.
    
    This exception is raised when issues occur while writing transformed data
    to target systems, databases, or files during the LOAD phase.
    
    Common scenarios:
        - Target system unavailable
        - Write permission issues
        - Constraint violations (primary key, foreign key, unique)
        - Schema mismatches
        - Insufficient storage space
        - Transaction commit failures
        - Duplicate records
    """
    
    def __init__(
        self,
        error_text: str,
        record_id: Optional[str] = None,
        target: Optional[str] = None,
        operation: Optional[str] = None,
        constraint: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize load error.
        
        Args:
            error_text: Description of load error
            record_id: ID of problematic record
            target: Target table/file name
            operation: Database operation (INSERT, UPDATE, etc.)
            constraint: Violated constraint name
            context: Additional context
        """
        context = context or {}
        if target:
            context["target"] = target
        if operation:
            context["operation"] = operation
        if constraint:
            context["constraint"] = constraint
            
        super().__init__(
            error_text=error_text,
            error_step="LOAD",
            record_id=record_id,
            context=context
        )
        self.target = target
        self.operation = operation
        self.constraint = constraint


class ValidationError(ETLError):
    """
    Exception raised during data validation phase.
    
    This exception is raised when data fails validation rules before or after
    processing phases.
    
    Common scenarios:
        - Missing mandatory fields
        - Invalid data formats
        - Out of range values
        - Failed business rule checks
        - Referential integrity violations
    """
    
    def __init__(
        self,
        error_text: str,
        record_id: Optional[str] = None,
        validation_rule: Optional[str] = None,
        expected_value: Optional[Any] = None,
        actual_value: Optional[Any] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize validation error.
        
        Args:
            error_text: Description of validation error
            record_id: ID of problematic record
            validation_rule: Name of failed validation rule
            expected_value: Expected value
            actual_value: Actual value
            context: Additional context
        """
        context = context or {}
        if validation_rule:
            context["validation_rule"] = validation_rule
        if expected_value is not None:
            context["expected_value"] = str(expected_value)
        if actual_value is not None:
            context["actual_value"] = str(actual_value)
            
        super().__init__(
            error_text=error_text,
            error_step="VALIDATE",
            record_id=record_id,
            context=context
        )
        self.validation_rule = validation_rule
        self.expected_value = expected_value
        self.actual_value = actual_value


class ConfigurationError(ETLError):
    """
    Exception raised for ETL configuration issues.
    
    This exception is raised when there are problems with ETL configuration,
    initialization, or setup.
    
    Common scenarios:
        - Missing configuration files
        - Invalid configuration values
        - Missing required parameters
        - Invalid connection strings
    """
    
    def __init__(
        self,
        error_text: str,
        config_key: Optional[str] = None,
        config_value: Optional[Any] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize configuration error.
        
        Args:
            error_text: Description of configuration error
            config_key: Configuration key that caused error
            config_value: Invalid configuration value
            context: Additional context
        """
        context = context or {}
        if config_key:
            context["config_key"] = config_key
        if config_value is not None:
            context["config_value"] = str(config_value)
            
        super().__init__(
            error_text=error_text,
            error_step="INIT",
            context=context
        )
        self.config_key = config_key
        self.config_value = config_value


class ETLTimeoutError(ETLError):
    """
    Exception raised when ETL operation times out.
    
    This exception is raised when an ETL operation exceeds its allowed
    execution time.
    """
    
    def __init__(
        self,
        error_text: str,
        timeout_seconds: Optional[int] = None,
        error_step: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize timeout error.
        
        Args:
            error_text: Description of timeout error
            timeout_seconds: Timeout threshold in seconds
            error_step: ETL step that timed out
            context: Additional context
        """
        context = context or {}
        if timeout_seconds:
            context["timeout_seconds"] = timeout_seconds
            
        super().__init__(
            error_text=error_text,
            error_step=error_step or "TIMEOUT",
            context=context
        )
        self.timeout_seconds = timeout_seconds