"""
ETL Exception Classes
Custom exceptions for ETL operations.
"""


class ETLError(Exception):
    """Base exception class for ETL errors."""
    
    def __init__(self, message: str, step: str = None, record_id: str = None):
        """
        Initialize ETL error.
        
        Args:
            message: Error message
            step: ETL step where error occurred
            record_id: Record identifier (if applicable)
        """
        self.message = message
        self.step = step
        self.record_id = record_id
        super().__init__(self.message)


class ETLExtractError(ETLError):
    """Exception raised during extraction phase."""
    pass


class ETLTransformError(ETLError):
    """Exception raised during transformation phase."""
    pass


class ETLLoadError(ETLError):
    """Exception raised during load phase."""
    pass


class ETLValidationError(ETLError):
    """Exception raised during validation."""
    pass