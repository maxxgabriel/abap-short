"""
Module: etl_error
Description: ETL exception classes
Converted from: ZCX_ETL_ERROR ABAP exception class
"""

from typing import Optional


class ETLError(Exception):
    """
    Base exception class for ETL errors.
    Converted from: ZCX_ETL_ERROR ABAP exception
    """

    def __init__(
        self,
        error_text: Optional[str] = None,
        error_step: Optional[str] = None,
        record_id: Optional[str] = None,
        previous: Optional[Exception] = None
    ):
        """
        Initialize ETL error.
        
        Args:
            error_text: Error description
            error_step: ETL step where error occurred
            record_id: ID of record being processed when error occurred
            previous: Previous exception in chain
        """
        self.error_text = error_text or "ETL error occurred"
        self.error_step = error_step
        self.record_id = record_id
        self.previous = previous
        
        message = self.error_text
        if self.error_step:
            message = f"[{self.error_step}] {message}"
        if self.record_id:
            message = f"{message} (Record: {self.record_id})"
            
        super().__init__(message)


class ExtractError(ETLError):
    """Exception raised during data extraction."""
    
    def __init__(self, error_text: Optional[str] = None, **kwargs):
        super().__init__(
            error_text=error_text or "Data extraction failed",
            error_step="EXTRACT",
            **kwargs
        )


class TransformError(ETLError):
    """Exception raised during data transformation."""
    
    def __init__(self, error_text: Optional[str] = None, **kwargs):
        super().__init__(
            error_text=error_text or "Data transformation failed",
            error_step="TRANSFORM",
            **kwargs
        )


class LoadError(ETLError):
    """Exception raised during data loading."""
    
    def __init__(self, error_text: Optional[str] = None, **kwargs):
        super().__init__(
            error_text=error_text or "Data load failed",
            error_step="LOAD",
            **kwargs
        )