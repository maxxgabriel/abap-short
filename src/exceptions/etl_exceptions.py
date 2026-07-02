"""
Custom exception hierarchy for ETL error handling.

This module defines custom exception classes for ETL operations, replacing
the ABAP ZCX_ETL_ERROR class and T100 message constants with Python exception
templates that include context attributes.
"""

from datetime import datetime
from typing import Optional, Dict, Any


class ETLError(Exception):
    """
    Base exception class for all ETL errors.
    
    Replaces ABAP ZCX_ETL_ERROR base exception class.
    Provides context attributes for error tracking and logging.
    
    Attributes:
        step: The ETL process step where the error occurred
        status: Error status code (E=Error, W=Warning, F=Fatal)
        record_count: Number of records processed before error
        record_id: Identifier of the record that caused the error
        error_code: Unique error code for categorization
        timestamp: When the error occurred
        context: Additional context information
    """
    
    # Error code constants (replaces T100 message constants)
    ERROR_CODE_BASE = "ETL001"
    MESSAGE_ID = "ZETL"
    
    def __init__(
        self,
        message: str,
        step: Optional[str] = None,
        status: str = "E",
        record_count: int = 0,
        record_id: Optional[str] = None,
        error_code: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        previous: Optional[Exception] = None
    ):
        """
        Initialize ETL error with context attributes.
        
        Args:
            message: Human-readable error message
            step: ETL step where error occurred (INIT, EXTRACT, TRANSFORM, LOAD)
            status: Error severity (E=Error, W=Warning, F=Fatal)
            record_count: Number of records processed
            record_id: ID of problematic record
            error_code: Unique error identifier
            context: Additional context dictionary
            previous: Previous exception for chaining
        """
        super().__init__(message)
        self.message = message
        self.step = step or "UNKNOWN"
        self.status = status
        self.record_count = record_count
        self.record_id = record_id
        self.error_code = error_code or self.ERROR_CODE_BASE
        self.timestamp = datetime.now()
        self.context = context or {}
        self.previous = previous
        
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert exception to dictionary for logging and serialization.
        
        Returns:
            Dictionary containing all error attributes
        """
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "step": self.step,
            "status": self.status,
            "record_count": self.record_count,
            "record_id": self.record_id,
            "error_code": self.error_code,
            "timestamp": self.timestamp.isoformat(),
            "context": self.context,
            "previous_error": str(self.previous) if self.previous else None
        }
    
    def __str__(self) -> str:
        """String representation with context."""
        parts = [f"[{self.error_code}] {self.message}"]
        if self.step:
            parts.append(f"Step: {self.step}")
        if self.record_id:
            parts.append(f"Record: {self.record_id}")
        if self.record_count > 0:
            parts.append(f"Processed: {self.record_count} records")
        return " | ".join(parts)
    
    def get_log_entry(self) -> Dict[str, Any]:
        """
        Generate log entry format compatible with ETL logging system.
        
        Returns:
            Dictionary formatted for ETL log table
        """
        return {
            "process_step": self.step,
            "status": self.status,
            "records_processed": self.record_count,
            "message": str(self),
            "error_code": self.error_code,
            "timestamp": self.timestamp
        }


class ExtractionError(ETLError):
    """
    Exception for errors during data extraction phase.
    
    Replaces ABAP extract_error constant (msgno='002').
    Raised when data cannot be read from source systems.
    
    Common scenarios:
        - Database connection failures
        - Invalid query syntax
        - Missing source tables
        - Access permission errors
        - Data format issues in source
    """
    
    ERROR_CODE_BASE = "ETL002"
    
    def __init__(
        self,
        message: str,
        source_system: Optional[str] = None,
        query: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize extraction error with source context.
        
        Args:
            message: Error description
            source_system: Name of source system/database
            query: SQL or query that failed
            **kwargs: Additional ETLError arguments
        """
        context = kwargs.pop("context", {})
        context.update({
            "source_system": source_system,
            "query": query
        })
        
        super().__init__(
            message=message,
            step="EXTRACT",
            error_code=self.ERROR_CODE_BASE,
            context=context,
            **kwargs
        )
        
        self.source_system = source_system
        self.query = query


class TransformationError(ETLError):
    """
    Exception for errors during data transformation phase.
    
    Replaces ABAP transform_error constant (msgno='003').
    Raised when data transformation logic fails.
    
    Common scenarios:
        - Data type conversion errors
        - Invalid business rule application
        - Calculation errors (divide by zero, overflow)
        - Schema validation failures
        - Missing required fields
    """
    
    ERROR_CODE_BASE = "ETL003"
    
    def __init__(
        self,
        message: str,
        transformation_rule: Optional[str] = None,
        input_value: Optional[Any] = None,
        expected_type: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize transformation error with rule context.
        
        Args:
            message: Error description
            transformation_rule: Name of failed transformation rule
            input_value: The value that caused the error
            expected_type: Expected data type or format
            **kwargs: Additional ETLError arguments
        """
        context = kwargs.pop("context", {})
        context.update({
            "transformation_rule": transformation_rule,
            "input_value": str(input_value) if input_value is not None else None,
            "expected_type": expected_type
        })
        
        super().__init__(
            message=message,
            step="TRANSFORM",
            error_code=self.ERROR_CODE_BASE,
            context=context,
            **kwargs
        )
        
        self.transformation_rule = transformation_rule
        self.input_value = input_value
        self.expected_type = expected_type


class LoadError(ETLError):
    """
    Exception for errors during data loading phase.
    
    Replaces ABAP load_error constant (msgno='004').
    Raised when data cannot be written to target system.
    
    Common scenarios:
        - Database constraint violations
        - Duplicate key errors
        - Target table unavailable
        - Insufficient storage space
        - Transaction commit failures
        - Write permission errors
    """
    
    ERROR_CODE_BASE = "ETL004"
    
    def __init__(
        self,
        message: str,
        target_table: Optional[str] = None,
        constraint_violated: Optional[str] = None,
        failed_records: int = 0,
        **kwargs
    ):
        """
        Initialize load error with target context.
        
        Args:
            message: Error description
            target_table: Name of target table/destination
            constraint_violated: Database constraint that was violated
            failed_records: Number of records that failed to load
            **kwargs: Additional ETLError arguments
        """
        context = kwargs.pop("context", {})
        context.update({
            "target_table": target_table,
            "constraint_violated": constraint_violated,
            "failed_records": failed_records
        })
        
        super().__init__(
            message=message,
            step="LOAD",
            error_code=self.ERROR_CODE_BASE,
            context=context,
            **kwargs
        )
        
        self.target_table = target_table
        self.constraint_violated = constraint_violated
        self.failed_records = failed_records


class ValidationError(ETLError):
    """
    Exception for data validation failures.
    
    Raised when data fails quality or business rule validation.
    
    Common scenarios:
        - Missing mandatory fields
        - Invalid data formats
        - Business rule violations
        - Data quality thresholds not met
        - Referential integrity errors
    """
    
    ERROR_CODE_BASE = "ETL005"
    
    def __init__(
        self,
        message: str,
        validation_rule: Optional[str] = None,
        field_name: Optional[str] = None,
        invalid_value: Optional[Any] = None,
        **kwargs
    ):
        """
        Initialize validation error with rule context.
        
        Args:
            message: Error description
            validation_rule: Name of validation rule that failed
            field_name: Field that failed validation
            invalid_value: The invalid value
            **kwargs: Additional ETLError arguments
        """
        context = kwargs.pop("context", {})
        context.update({
            "validation_rule": validation_rule,
            "field_name": field_name,
            "invalid_value": str(invalid_value) if invalid_value is not None else None
        })
        
        super().__init__(
            message=message,
            step="VALIDATE",
            error_code=self.ERROR_CODE_BASE,
            context=context,
            **kwargs
        )
        
        self.validation_rule = validation_rule
        self.field_name = field_name
        self.invalid_value = invalid_value


class ConfigurationError(ETLError):
    """
    Exception for ETL configuration errors.
    
    Raised when ETL process configuration is invalid or missing.
    
    Common scenarios:
        - Missing configuration files
        - Invalid configuration parameters
        - Missing required environment variables
        - Invalid connection strings
    """
    
    ERROR_CODE_BASE = "ETL006"
    
    def __init__(
        self,
        message: str,
        config_parameter: Optional[str] = None,
        config_file: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize configuration error.
        
        Args:
            message: Error description
            config_parameter: Name of problematic configuration parameter
            config_file: Path to configuration file
            **kwargs: Additional ETLError arguments
        """
        context = kwargs.pop("context", {})
        context.update({
            "config_parameter": config_parameter,
            "config_file": config_file
        })
        
        super().__init__(
            message=message,
            step="INIT",
            error_code=self.ERROR_CODE_BASE,
            context=context,
            **kwargs
        )
        
        self.config_parameter = config_parameter
        self.config_file = config_file


# T100-style message templates (replaces ABAP T100 message constants)
ERROR_MESSAGES = {
    "ETL001": "General ETL error: {message}",
    "ETL002": "Data extraction failed from {source_system}: {message}",
    "ETL003": "Data transformation failed for rule '{transformation_rule}': {message}",
    "ETL004": "Data load failed to table '{target_table}': {message}",
    "ETL005": "Validation failed for field '{field_name}': {message}",
    "ETL006": "Configuration error for parameter '{config_parameter}': {message}",
    "ETL101": "Database connection failed: {message}",
    "ETL102": "Query execution timeout after {timeout} seconds",
    "ETL103": "No data found for date range {start_date} to {end_date}",
    "ETL201": "Invalid data type conversion from {from_type} to {to_type}",
    "ETL202": "Calculation error: {operation} failed for value {value}",
    "ETL203": "Business rule violation: {rule_name}",
    "ETL301": "Duplicate key violation for {key_field}={key_value}",
    "ETL302": "Foreign key constraint failed: {constraint_name}",
    "ETL303": "Transaction commit failed after {retry_count} retries"
}


def format_error_message(error_code: str, **kwargs) -> str:
    """
    Format error message using T100-style templates.
    
    Args:
        error_code: Error code to look up
        **kwargs: Values to substitute in template
        
    Returns:
        Formatted error message
    """
    template = ERROR_MESSAGES.get(error_code, "Unknown error code: {error_code}")
    return template.format(error_code=error_code, **kwargs)


def raise_extraction_error(
    message: str,
    source_system: Optional[str] = None,
    query: Optional[str] = None,
    record_count: int = 0,
    previous: Optional[Exception] = None
) -> None:
    """
    Helper function to raise ExtractionError with standard formatting.
    
    Args:
        message: Error description
        source_system: Source system name
        query: Failed query
        record_count: Records processed before error
        previous: Previous exception
        
    Raises:
        ExtractionError
    """
    raise ExtractionError(
        message=message,
        source_system=source_system,
        query=query,
        record_count=record_count,
        previous=previous
    )


def raise_transformation_error(
    message: str,
    transformation_rule: Optional[str] = None,
    record_id: Optional[str] = None,
    record_count: int = 0,
    previous: Optional[Exception] = None
) -> None:
    """
    Helper function to raise TransformationError with standard formatting.
    
    Args:
        message: Error description
        transformation_rule: Rule that failed
        record_id: ID of problematic record
        record_count: Records processed before error
        previous: Previous exception
        
    Raises:
        TransformationError
    """
    raise TransformationError(
        message=message,
        transformation_rule=transformation_rule,
        record_id=record_id,
        record_count=record_count,
        previous=previous
    )


def raise_load_error(
    message: str,
    target_table: Optional[str] = None,
    record_id: Optional[str] = None,
    record_count: int = 0,
    failed_records: int = 0,
    previous: Optional[Exception] = None
) -> None:
    """
    Helper function to raise LoadError with standard formatting.
    
    Args:
        message: Error description
        target_table: Target table name
        record_id: ID of problematic record
        record_count: Records processed before error
        failed_records: Number of failed records
        previous: Previous exception
        
    Raises:
        LoadError
    """
    raise LoadError(
        message=message,
        target_table=target_table,
        record_id=record_id,
        record_count=record_count,
        failed_records=failed_records,
        previous=previous
    )