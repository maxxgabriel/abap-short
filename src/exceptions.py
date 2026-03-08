"""
Exception Handling Module
Custom exceptions for ETL error handling.
"""


class ETLError(Exception):
    """Base exception for all ETL errors."""
    
    def __init__(
        self,
        message: str,
        error_step: str = "",
        record_id: str = "",
        previous: Exception = None
    ):
        self.message = message
        self.error_step = error_step
        self.record_id = record_id
        self.previous = previous
        super().__init__(self.message)
    
    def __str__(self):
        details = [f"ETLError: {self.message}"]
        if self.error_step:
            details.append(f"Step: {self.error_step}")
        if self.record_id:
            details.append(f"Record ID: {self.record_id}")
        if self.previous:
            details.append(f"Caused by: {str(self.previous)}")
        return " | ".join(details)


class ExtractError(ETLError):
    """Exception raised during data extraction."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, error_step="EXTRACT", **kwargs)


class TransformError(ETLError):
    """Exception raised during data transformation."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, error_step="TRANSFORM", **kwargs)


class LoadError(ETLError):
    """Exception raised during data loading."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, error_step="LOAD", **kwargs)


class ValidationError(ETLError):
    """Exception raised during data validation."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(message, error_step="VALIDATE", **kwargs)