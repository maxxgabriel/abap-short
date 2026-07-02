"""
ETL Exception Classes
Custom exceptions for ETL error handling
"""
from typing import Optional


class ETLError(Exception):
    """Base exception class for ETL errors"""
    
    def __init__(
        self,
        message: str,
        error_step: Optional[str] = None,
        record_id: Optional[str] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize ETL error
        
        Args:
            message: Error message
            error_step: ETL step where error occurred
            record_id: ID of record that caused error
            original_exception: Original exception if wrapping
        """
        self.message = message
        self.error_step = error_step
        self.record_id = record_id
        self.original_exception = original_exception
        
        super().__init__(self._format_message())
    
    def _format_message(self) -> str:
        """Format comprehensive error message"""
        parts = [self.message]
        
        if self.error_step:
            parts.append(f"Step: {self.error_step}")
        
        if self.record_id:
            parts.append(f"Record: {self.record_id}")
        
        if self.original_exception:
            parts.append(f"Cause: {str(self.original_exception)}")
        
        return " | ".join(parts)


class ETLExtractionError(ETLError):
    """Exception raised during data extraction"""
    
    def __init__(
        self,
        message: str = "Data extraction failed",
        error_step: str = "EXTRACT",
        record_id: Optional[str] = None,
        original_exception: Optional[Exception] = None
    ):
        super().__init__(message, error_step, record_id, original_exception)


class ETLTransformationError(ETLError):
    """Exception raised during data transformation"""
    
    def __init__(
        self,
        message: str = "Data transformation failed",
        error_step: str = "TRANSFORM",
        record_id: Optional[str] = None,
        original_exception: Optional[Exception] = None
    ):
        super().__init__(message, error_step, record_id, original_exception)


class ETLLoadError(ETLError):
    """Exception raised during data loading"""
    
    def __init__(
        self,
        message: str = "Data load failed",
        error_step: str = "LOAD",
        record_id: Optional[str] = None,
        original_exception: Optional[Exception] = None
    ):
        super().__init__(message, error_step, record_id, original_exception)


class ETLValidationError(ETLError):
    """Exception raised during data validation"""
    
    def __init__(
        self,
        message: str = "Data validation failed",
        error_step: str = "VALIDATE",
        record_id: Optional[str] = None,
        original_exception: Optional[Exception] = None
    ):
        super().__init__(message, error_step, record_id, original_exception)


class ETLConfigurationError(ETLError):
    """Exception raised for configuration issues"""
    
    def __init__(
        self,
        message: str = "ETL configuration error",
        error_step: str = "INIT",
        record_id: Optional[str] = None,
        original_exception: Optional[Exception] = None
    ):
        super().__init__(message, error_step, record_id, original_exception)