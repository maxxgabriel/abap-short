"""
ETL Exception Classes Module

Custom exception hierarchy for ETL process error handling,
migrated from ABAP ZCX_ETL_ERROR.
"""

from typing import Optional


class ETLError(Exception):
    """Base exception class for all ETL errors"""
    
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
            record_id: ID of record being processed when error occurred
            original_exception: Original exception if wrapped
        """
        self.message = message
        self.error_step = error_step
        self.record_id = record_id
        self.original_exception = original_exception
        
        # Build detailed message
        detailed_msg = f"ETL Error: {message}"
        if error_step:
            detailed_msg += f" | Step: {error_step}"
        if record_id:
            detailed_msg += f" | Record: {record_id}"
        if original_exception:
            detailed_msg += f" | Cause: {str(original_exception)}"
            
        super().__init__(detailed_msg)
    
    def to_dict(self) -> dict:
        """Convert exception to dictionary for logging"""
        return {
            'error_type': self.__class__.__name__,
            'message': self.message,
            'error_step': self.error_step,
            'record_id': self.record_id,
            'original_error': str(self.original_exception) if self.original_exception else None
        }


class ExtractError(ETLError):
    """Exception raised during data extraction phase"""
    
    def __init__(
        self,
        message: str,
        record_id: Optional[str] = None,
        original_exception: Optional[Exception] = None
    ):
        super().__init__(
            message=message,
            error_step='EXTRACT',
            record_id=record_id,
            original_exception=original_exception
        )


class TransformError(ETLError):
    """Exception raised during data transformation phase"""
    
    def __init__(
        self,
        message: str,
        record_id: Optional[str] = None,
        original_exception: Optional[Exception] = None
    ):
        super().__init__(
            message=message,
            error_step='TRANSFORM',
            record_id=record_id,
            original_exception=original_exception
        )


class LoadError(ETLError):
    """Exception raised during data loading phase"""
    
    def __init__(
        self,
        message: str,
        record_id: Optional[str] = None,
        original_exception: Optional[Exception] = None
    ):
        super().__init__(
            message=message,
            error_step='LOAD',
            record_id=record_id,
            original_exception=original_exception
        )


class ValidationError(ETLError):
    """Exception raised during data validation"""
    
    def __init__(
        self,
        message: str,
        record_id: Optional[str] = None,
        validation_field: Optional[str] = None,
        original_exception: Optional[Exception] = None
    ):
        self.validation_field = validation_field
        
        full_message = message
        if validation_field:
            full_message += f" | Field: {validation_field}"
            
        super().__init__(
            message=full_message,
            error_step='VALIDATE',
            record_id=record_id,
            original_exception=original_exception
        )


class ConfigurationError(ETLError):
    """Exception raised for configuration-related errors"""
    
    def __init__(
        self,
        message: str,
        config_key: Optional[str] = None,
        original_exception: Optional[Exception] = None
    ):
        self.config_key = config_key
        
        full_message = message
        if config_key:
            full_message += f" | Config Key: {config_key}"
            
        super().__init__(
            message=full_message,
            error_step='INIT',
            original_exception=original_exception
        )