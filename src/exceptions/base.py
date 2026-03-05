"""
Base exception hierarchy for ETL processes.

This module defines the custom exception hierarchy that replaces the ABAP
ZCX_ETL_ERROR exception class with specialized Python exceptions for each
ETL phase.
"""

from typing import Optional, Dict, Any
from datetime import datetime


class ETLError(Exception):
    """
    Base exception class for all ETL-related errors.
    
    This is the Python equivalent of the ABAP ZCX_ETL_ERROR class.
    All ETL exceptions inherit from this base class to provide a unified
    exception hierarchy for error handling throughout the ETL pipeline.
    
    Attributes:
        error_text: Human-readable error message
        error_step: ETL step where the error occurred (EXTRACT, TRANSFORM, LOAD)
        record_id: Identifier of the record that caused the error (if applicable)
        timestamp: When the error occurred
        context: Additional context information as key-value pairs
    """
    
    def __init__(
        self,
        error_text: str,
        error_step: Optional[str] = None,
        record_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the ETL error.
        
        Args:
            error_text: Description of the error
            error_step: ETL step identifier (INIT, EXTRACT, TRANSFORM, LOAD, etc.)
            record_id: ID of the record being processed when error occurred
            context: Additional contextual information
        """
        super().__init__(error_text)
        self.error_text = error_text
        self.error_step = error_step or "UNKNOWN"
        self.record_id = record_id
        self.timestamp = datetime.now()
        self.context = context or {}
        
    def __str__(self) -> str:
        """Return formatted error message."""
        parts = [f"[{self.error_step}]"]
        
        if self.record_id:
            parts.append(f"Record {self.record_id}:")
            
        parts.append(self.error_text)
        
        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            parts.append(f"({context_str})")
            
        return " ".join(parts)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert exception to dictionary format.
        
        Returns:
            Dictionary containing all error details
        """
        return {
            "error_type": self.__class__.__name__,
            "error_text": self.error_text,
            "error_step": self.error_step,
            "record_id": self.record_id,
            "timestamp": self.timestamp.isoformat(),
            "context": self.context
        }


class ExtractError(ETLError):
    """
    Exception raised during data extraction phase.
    
    This exception should be raised when errors occur while reading data
    from source systems, including:
    - Database connection failures
    - Query execution errors
    - Data format issues in source
    - Missing or inaccessible data sources
    - Authentication/authorization failures
    
    Corresponds to ABAP ZCX_ETL_ERROR=>EXTRACT_ERROR constant.
    """
    
    def __init__(
        self,
        error_text: str,
        record_id: Optional[str] = None,
        source: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize extraction error.
        
        Args:
            error_text: Description of the extraction error
            record_id: ID of record that failed to extract
            source: Source system or table name
            context: Additional context (e.g., query parameters)
        """
        context = context or {}
        if source:
            context["source"] = source
            
        super().__init__(
            error_text=error_text,
            error_step="EXTRACT",
            record_id=record_id,
            context=context
        )
        self.source = source


class TransformError(ETLError):
    """
    Exception raised during data transformation phase.
    
    This exception should be raised when errors occur during data
    transformation logic, including:
    - Business rule validation failures
    - Data type conversion errors
    - Calculation errors
    - Invalid field values
    - Missing required fields
    
    Corresponds to ABAP ZCX_ETL_ERROR=>TRANSFORM_ERROR constant.
    """
    
    def __init__(
        self,
        error_text: str,
        record_id: Optional[str] = None,
        field_name: Optional[str] = None,
        field_value: Optional[Any] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize transformation error.
        
        Args:
            error_text: Description of the transformation error
            record_id: ID of record that failed transformation
            field_name: Name of field that caused the error
            field_value: Value that caused the error
            context: Additional context (e.g., validation rules)
        """
        context = context or {}
        if field_name:
            context["field_name"] = field_name
        if field_value is not None:
            context["field_value"] = str(field_value)
            
        super().__init__(
            error_text=error_text,
            error_step="TRANSFORM",
            record_id=record_id,
            context=context
        )
        self.field_name = field_name
        self.field_value = field_value


class LoadError(ETLError):
    """
    Exception raised during data loading phase.
    
    This exception should be raised when errors occur while writing
    transformed data to target systems, including:
    - Database write failures
    - Constraint violations
    - Duplicate key errors
    - Target system unavailability
    - Insufficient permissions
    - Transaction failures
    
    Corresponds to ABAP ZCX_ETL_ERROR=>LOAD_ERROR constant.
    """
    
    def __init__(
        self,
        error_text: str,
        record_id: Optional[str] = None,
        target: Optional[str] = None,
        operation: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize load error.
        
        Args:
            error_text: Description of the load error
            record_id: ID of record that failed to load
            target: Target table or system name
            operation: Database operation (INSERT, UPDATE, etc.)
            context: Additional context (e.g., constraint violations)
        """
        context = context or {}
        if target:
            context["target"] = target
        if operation:
            context["operation"] = operation
            
        super().__init__(
            error_text=error_text,
            error_step="LOAD",
            record_id=record_id,
            context=context
        )
        self.target = target
        self.operation = operation


class ValidationError(ETLError):
    """
    Exception raised during data validation phase.
    
    This exception should be raised when data fails validation checks
    before or after transformation, including:
    - Schema validation failures
    - Data quality checks
    - Business rule violations
    - Referential integrity issues
    """
    
    def __init__(
        self,
        error_text: str,
        record_id: Optional[str] = None,
        validation_rule: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize validation error.
        
        Args:
            error_text: Description of the validation error
            record_id: ID of record that failed validation
            validation_rule: Name of the validation rule that failed
            context: Additional context
        """
        context = context or {}
        if validation_rule:
            context["validation_rule"] = validation_rule
            
        super().__init__(
            error_text=error_text,
            error_step="VALIDATE",
            record_id=record_id,
            context=context
        )
        self.validation_rule = validation_rule


class ConfigurationError(ETLError):
    """
    Exception raised for ETL configuration issues.
    
    This exception should be raised when there are problems with
    ETL configuration or setup, including:
    - Missing configuration files
    - Invalid configuration values
    - Missing required parameters
    - Environment setup issues
    """
    
    def __init__(
        self,
        error_text: str,
        config_key: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize configuration error.
        
        Args:
            error_text: Description of the configuration error
            config_key: Configuration key that caused the error
            context: Additional context
        """
        context = context or {}
        if config_key:
            context["config_key"] = config_key
            
        super().__init__(
            error_text=error_text,
            error_step="INIT",
            context=context
        )
        self.config_key = config_key


class OrchestrationError(ETLError):
    """
    Exception raised during ETL orchestration.
    
    This exception should be raised when there are errors in the
    overall ETL workflow coordination, including:
    - Step sequencing failures
    - Dependency issues
    - Resource allocation problems
    - Process timeout
    """
    
    def __init__(
        self,
        error_text: str,
        failed_step: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize orchestration error.
        
        Args:
            error_text: Description of the orchestration error
            failed_step: Step that failed
            context: Additional context
        """
        context = context or {}
        if failed_step:
            context["failed_step"] = failed_step
            
        super().__init__(
            error_text=error_text,
            error_step="ORCHESTRATE",
            context=context
        )
        self.failed_step = failed_step