"""
ETL Exception Classes
Custom exceptions for ETL error handling.
"""


class ETLError(Exception):
    """Base exception class for ETL errors."""
    
    def __init__(
        self,
        message: str,
        error_step: str = "",
        record_id: str = ""
    ):
        """
        Initialize ETL error.
        
        Args:
            message: Error message
            error_step: Processing step where error occurred
            record_id: Record identifier if applicable
        """
        self.message = message
        self.error_step = error_step
        self.record_id = record_id
        super().__init__(self.format_message())
    
    def format_message(self) -> str:
        """Format error message with context."""
        msg = self.message
        if self.error_step:
            msg = f"[{self.error_step}] {msg}"
        if self.record_id:
            msg = f"{msg} (Record: {self.record_id})"
        return msg


class ExtractError(ETLError):
    """Exception raised during data extraction."""
    pass


class TransformError(ETLError):
    """Exception raised during data transformation."""
    pass


class LoadError(ETLError):
    """Exception raised during data loading."""
    pass


class ValidationError(ETLError):
    """Exception raised during data validation."""
    pass