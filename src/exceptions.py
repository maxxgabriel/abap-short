"""
Custom exception classes for ETL processing.
"""


class ETLError(Exception):
    """Base exception for ETL errors."""
    
    def __init__(self, message: str, step: str = None, record_id: str = None):
        self.message = message
        self.step = step
        self.record_id = record_id
        super().__init__(self.message)
    
    def __str__(self):
        error_parts = [self.message]
        if self.step:
            error_parts.append(f"Step: {self.step}")
        if self.record_id:
            error_parts.append(f"Record ID: {self.record_id}")
        return " | ".join(error_parts)


class ExtractionError(ETLError):
    """Exception raised during data extraction."""
    pass


class TransformationError(ETLError):
    """Exception raised during data transformation."""
    pass


class LoadError(ETLError):
    """Exception raised during data loading."""
    pass


class ValidationError(ETLError):
    """Exception raised during data validation."""
    pass


class ConfigurationError(ETLError):
    """Exception raised for configuration issues."""
    pass