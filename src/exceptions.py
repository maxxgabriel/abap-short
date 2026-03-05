"""
ETL Exception Classes
Custom exceptions for ETL error handling.
"""


class ETLError(Exception):
    """Base exception for ETL errors."""
    
    def __init__(self, message: str, step: str = None, record_id: str = None):
        """
        Initialize ETL error.

        Args:
            message: Error message
            step: ETL step where error occurred
            record_id: Record ID if applicable
        """
        self.message = message
        self.step = step
        self.record_id = record_id
        super().__init__(self.message)

    def __str__(self):
        error_str = f"ETLError: {self.message}"
        if self.step:
            error_str += f" [Step: {self.step}]"
        if self.record_id:
            error_str += f" [Record: {self.record_id}]"
        return error_str


class ExtractError(ETLError):
    """Exception raised during extraction phase."""
    
    def __init__(self, message: str, record_id: str = None):
        super().__init__(message, step="EXTRACT", record_id=record_id)


class TransformError(ETLError):
    """Exception raised during transformation phase."""
    
    def __init__(self, message: str, record_id: str = None):
        super().__init__(message, step="TRANSFORM", record_id=record_id)


class LoadError(ETLError):
    """Exception raised during load phase."""
    
    def __init__(self, message: str, record_id: str = None):
        super().__init__(message, step="LOAD", record_id=record_id)


class ValidationError(ETLError):
    """Exception raised during validation."""
    
    def __init__(self, message: str, record_id: str = None):
        super().__init__(message, step="VALIDATE", record_id=record_id)