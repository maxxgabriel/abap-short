"""
ETL Exception Classes
Custom exceptions for ETL error handling
"""


class ETLError(Exception):
    """Base exception for ETL errors"""
    
    def __init__(self, message: str, error_step: str = None, record_id: str = None):
        self.message = message
        self.error_step = error_step
        self.record_id = record_id
        super().__init__(self.message)


class ETLExtractError(ETLError):
    """Exception raised during data extraction"""
    pass


class ETLTransformError(ETLError):
    """Exception raised during data transformation"""
    pass


class ETLLoadError(ETLError):
    """Exception raised during data loading"""
    pass


class ETLValidationError(ETLError):
    """Exception raised during data validation"""
    pass