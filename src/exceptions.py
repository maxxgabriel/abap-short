"""
ETL Exception Hierarchy
Mirrors ZCX_ETL_ERROR functionality with base ETLException and phase-specific subclasses.
"""
from typing import Optional, Dict, Any
from datetime import datetime


class ETLException(Exception):
    """
    Base exception class for all ETL errors.
    Mirrors ABAP ZCX_ETL_ERROR functionality.
    
    Attributes:
        error_text: Human-readable error message
        error_step: ETL phase where error occurred (INIT, EXTRACT, TRANSFORM, LOAD)
        record_id: ID of the record that caused the error (if applicable)
        error_code: Numeric error code for categorization
        timestamp: When the error occurred
        context: Additional context information as dictionary
    """
    
    ERROR_CODE = "001"
    ERROR_PHASE = "GENERAL"
    
    def __init__(
        self,
        error_text: str,
        error_step: Optional[str] = None,
        record_id: Optional[str] = None,
        error_code: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        previous: Optional[Exception] = None
    ):
        """
        Initialize ETL exception.
        
        Args:
            error_text: Description of the error
            error_step: ETL phase (INIT, EXTRACT, TRANSFORM, LOAD, VALIDATE, COMPLETE)
            record_id: ID of affected record
            error_code: Error code override
            context: Additional context information
            previous: Previous exception in the chain
        """
        super().__init__(error_text)
        
        self.error_text = error_text
        self.error_step = error_step or self.ERROR_PHASE
        self.record_id = record_id
        self.error_code = error_code or self.ERROR_CODE
        self.timestamp = datetime.now()
        self.context = context or {}
        self.previous = previous
        
    def __str__(self) -> str:
        """Format exception as string."""
        msg = f"[{self.error_code}] {self.error_step}: {self.error_text}"
        if self.record_id:
            msg += f" (Record: {self.record_id})"
        return msg
    
    def __repr__(self) -> str:
        """Detailed representation of exception."""
        return (
            f"{self.__class__.__name__}("
            f"error_code='{self.error_code}', "
            f"error_step='{self.error_step}', "
            f"error_text='{self.error_text}', "
            f"record_id='{self.record_id}', "
            f"timestamp={self.timestamp.isoformat()})"
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert exception to dictionary for logging/serialization.
        
        Returns:
            Dictionary representation of the exception
        """
        return {
            "exception_class": self.__class__.__name__,
            "error_code": self.error_code,
            "error_step": self.error_step,
            "error_text": self.error_text,
            "record_id": self.record_id,
            "timestamp": self.timestamp.isoformat(),
            "context": self.context,
            "previous_error": str(self.previous) if self.previous else None
        }
    
    def get_message(self) -> str:
        """
        Get formatted error message.
        Mirrors ABAP get_text() method.
        
        Returns:
            Formatted error message
        """
        return self.error_text
    
    def get_context_value(self, key: str, default: Any = None) -> Any:
        """
        Get value from error context.
        
        Args:
            key: Context key
            default: Default value if key not found
            
        Returns:
            Context value or default
        """
        return self.context.get(key, default)


class ExtractError(ETLException):
    """
    Exception raised during data extraction phase.
    Mirrors ABAP extract_error constant in ZCX_ETL_ERROR.
    
    Use this for:
    - Database connection failures
    - Source data access errors
    - Query execution failures
    - Data retrieval timeouts
    """
    
    ERROR_CODE = "002"
    ERROR_PHASE = "EXTRACT"
    
    def __init__(
        self,
        error_text: str,
        record_id: Optional[str] = None,
        source_table: Optional[str] = None,
        query: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        previous: Optional[Exception] = None
    ):
        """
        Initialize extraction error.
        
        Args:
            error_text: Description of the error
            record_id: ID of affected record
            source_table: Source table name
            query: Query that failed
            context: Additional context information
            previous: Previous exception in the chain
        """
        # Merge extraction-specific context
        extraction_context = context or {}
        if source_table:
            extraction_context["source_table"] = source_table
        if query:
            extraction_context["query"] = query
            
        super().__init__(
            error_text=error_text,
            error_step=self.ERROR_PHASE,
            record_id=record_id,
            error_code=self.ERROR_CODE,
            context=extraction_context,
            previous=previous
        )
        
        self.source_table = source_table
        self.query = query


class TransformError(ETLException):
    """
    Exception raised during data transformation phase.
    Mirrors ABAP transform_error constant in ZCX_ETL_ERROR.
    
    Use this for:
    - Data type conversion failures
    - Business rule validation errors
    - Calculation errors
    - Data quality issues
    """
    
    ERROR_CODE = "003"
    ERROR_PHASE = "TRANSFORM"
    
    def __init__(
        self,
        error_text: str,
        record_id: Optional[str] = None,
        field_name: Optional[str] = None,
        field_value: Optional[Any] = None,
        transformation_rule: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        previous: Optional[Exception] = None
    ):
        """
        Initialize transformation error.
        
        Args:
            error_text: Description of the error
            record_id: ID of affected record
            field_name: Field that caused the error
            field_value: Value that caused the error
            transformation_rule: Rule that failed
            context: Additional context information
            previous: Previous exception in the chain
        """
        # Merge transformation-specific context
        transform_context = context or {}
        if field_name:
            transform_context["field_name"] = field_name
        if field_value is not None:
            transform_context["field_value"] = str(field_value)
        if transformation_rule:
            transform_context["transformation_rule"] = transformation_rule
            
        super().__init__(
            error_text=error_text,
            error_step=self.ERROR_PHASE,
            record_id=record_id,
            error_code=self.ERROR_CODE,
            context=transform_context,
            previous=previous
        )
        
        self.field_name = field_name
        self.field_value = field_value
        self.transformation_rule = transformation_rule


class LoadError(ETLException):
    """
    Exception raised during data loading phase.
    Mirrors ABAP load_error constant in ZCX_ETL_ERROR.
    
    Use this for:
    - Database insert/update failures
    - Target table access errors
    - Constraint violations
    - Transaction commit failures
    """
    
    ERROR_CODE = "004"
    ERROR_PHASE = "LOAD"
    
    def __init__(
        self,
        error_text: str,
        record_id: Optional[str] = None,
        target_table: Optional[str] = None,
        operation: Optional[str] = None,
        constraint_violated: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        previous: Optional[Exception] = None
    ):
        """
        Initialize load error.
        
        Args:
            error_text: Description of the error
            record_id: ID of affected record
            target_table: Target table name
            operation: Operation that failed (INSERT, UPDATE, DELETE)
            constraint_violated: Constraint that was violated
            context: Additional context information
            previous: Previous exception in the chain
        """
        # Merge load-specific context
        load_context = context or {}
        if target_table:
            load_context["target_table"] = target_table
        if operation:
            load_context["operation"] = operation
        if constraint_violated:
            load_context["constraint_violated"] = constraint_violated
            
        super().__init__(
            error_text=error_text,
            error_step=self.ERROR_PHASE,
            record_id=record_id,
            error_code=self.ERROR_CODE,
            context=load_context,
            previous=previous
        )
        
        self.target_table = target_table
        self.operation = operation
        self.constraint_violated = constraint_violated


class ValidationError(ETLException):
    """
    Exception raised during data validation phase.
    Additional exception class for validation-specific errors.
    
    Use this for:
    - Schema validation failures
    - Data quality checks
    - Business rule validations
    - Prerequisite checks
    """
    
    ERROR_CODE = "005"
    ERROR_PHASE = "VALIDATE"
    
    def __init__(
        self,
        error_text: str,
        record_id: Optional[str] = None,
        validation_rule: Optional[str] = None,
        expected_value: Optional[Any] = None,
        actual_value: Optional[Any] = None,
        context: Optional[Dict[str, Any]] = None,
        previous: Optional[Exception] = None
    ):
        """
        Initialize validation error.
        
        Args:
            error_text: Description of the error
            record_id: ID of affected record
            validation_rule: Rule that failed
            expected_value: Expected value
            actual_value: Actual value
            context: Additional context information
            previous: Previous exception in the chain
        """
        # Merge validation-specific context
        validation_context = context or {}
        if validation_rule:
            validation_context["validation_rule"] = validation_rule
        if expected_value is not None:
            validation_context["expected_value"] = str(expected_value)
        if actual_value is not None:
            validation_context["actual_value"] = str(actual_value)
            
        super().__init__(
            error_text=error_text,
            error_step=self.ERROR_PHASE,
            record_id=record_id,
            error_code=self.ERROR_CODE,
            context=validation_context,
            previous=previous
        )
        
        self.validation_rule = validation_rule
        self.expected_value = expected_value
        self.actual_value = actual_value


class ConfigurationError(ETLException):
    """
    Exception raised for configuration-related errors.
    
    Use this for:
    - Missing configuration parameters
    - Invalid configuration values
    - Configuration file access errors
    """
    
    ERROR_CODE = "006"
    ERROR_PHASE = "INIT"
    
    def __init__(
        self,
        error_text: str,
        config_key: Optional[str] = None,
        config_file: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        previous: Optional[Exception] = None
    ):
        """
        Initialize configuration error.
        
        Args:
            error_text: Description of the error
            config_key: Configuration key that caused the error
            config_file: Configuration file path
            context: Additional context information
            previous: Previous exception in the chain
        """
        config_context = context or {}
        if config_key:
            config_context["config_key"] = config_key
        if config_file:
            config_context["config_file"] = config_file
            
        super().__init__(
            error_text=error_text,
            error_step=self.ERROR_PHASE,
            error_code=self.ERROR_CODE,
            context=config_context,
            previous=previous
        )
        
        self.config_key = config_key
        self.config_file = config_file


# Helper functions for exception handling

def wrap_exception(
    exc: Exception,
    error_step: str,
    record_id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None
) -> ETLException:
    """
    Wrap a generic exception in an appropriate ETL exception.
    
    Args:
        exc: Original exception
        error_step: ETL phase where error occurred
        record_id: ID of affected record
        context: Additional context information
        
    Returns:
        Wrapped ETL exception
    """
    error_text = str(exc)
    
    if error_step == "EXTRACT":
        return ExtractError(
            error_text=error_text,
            record_id=record_id,
            context=context,
            previous=exc
        )
    elif error_step == "TRANSFORM":
        return TransformError(
            error_text=error_text,
            record_id=record_id,
            context=context,
            previous=exc
        )
    elif error_step == "LOAD":
        return LoadError(
            error_text=error_text,
            record_id=record_id,
            context=context,
            previous=exc
        )
    elif error_step == "VALIDATE":
        return ValidationError(
            error_text=error_text,
            record_id=record_id,
            context=context,
            previous=exc
        )
    else:
        return ETLException(
            error_text=error_text,
            error_step=error_step,
            record_id=record_id,
            context=context,
            previous=exc
        )


def format_exception_chain(exc: ETLException) -> str:
    """
    Format exception chain for logging.
    
    Args:
        exc: ETL exception
        
    Returns:
        Formatted exception chain
    """
    messages = [str(exc)]
    current = exc.previous
    
    while current:
        messages.append(f"  Caused by: {current}")
        if isinstance(current, ETLException):
            current = current.previous
        else:
            break
            
    return "\n".join(messages)