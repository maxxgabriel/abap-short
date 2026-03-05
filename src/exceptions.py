"""
ETL exception hierarchy.

This module defines custom exception classes for different ETL error scenarios,
providing a structured approach to error handling.
"""


class ETLError(Exception):
    """
    Base exception class for all ETL errors.
    
    Attributes:
        error_text: Descriptive error message
        error_step: ETL step where error occurred
        record_id: ID of the record that caused the error (if applicable)
    """
    
    def __init__(
        self,
        error_text: str = "",
        error_step: str = "",
        record_id: str = ""
    ):
        """
        Initialize ETL error.
        
        Args:
            error_text: Descriptive error message
            error_step: ETL step where error occurred
            record_id: ID of the record that caused the error
        """
        self.error_text = error_text
        self.error_step = error_step
        self.record_id = record_id
        
        message = f"ETL Error: {error_text}"
        if error_step:
            message += f" (Step: {error_step})"
        if record_id:
            message += f" (Record: {record_id})"
            
        super().__init__(message)


class ExtractError(ETLError):
    """Exception raised during data extraction phase."""
    
    def __init__(
        self,
        error_text: str = "",
        record_id: str = ""
    ):
        super().__init__(
            error_text=error_text,
            error_step="EXTRACT",
            record_id=record_id
        )


class TransformError(ETLError):
    """Exception raised during data transformation phase."""
    
    def __init__(
        self,
        error_text: str = "",
        record_id: str = ""
    ):
        super().__init__(
            error_text=error_text,
            error_step="TRANSFORM",
            record_id=record_id
        )


class LoadError(ETLError):
    """Exception raised during data loading phase."""
    
    def __init__(
        self,
        error_text: str = "",
        record_id: str = ""
    ):
        super().__init__(
            error_text=error_text,
            error_step="LOAD",
            record_id=record_id
        )


class ValidationError(ETLError):
    """Exception raised during data validation."""
    
    def __init__(
        self,
        error_text: str = "",
        record_id: str = ""
    ):
        super().__init__(
            error_text=error_text,
            error_step="VALIDATE",
            record_id=record_id
        )


class ConfigurationError(ETLError):
    """Exception raised for configuration errors."""
    
    def __init__(self, error_text: str = ""):
        super().__init__(
            error_text=error_text,
            error_step="CONFIG"
        )