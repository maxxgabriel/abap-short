"""
Custom exceptions for ETL processes.
"""


class ETLError(Exception):
    """Base exception for ETL errors."""
    
    def __init__(self, message: str, step: str = None, record_id: str = None):
        self.message = message
        self.step = step
        self.record_id = record_id
        super().__init__(self.message)


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