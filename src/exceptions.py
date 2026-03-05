"""
ETL Exception Classes

This module defines custom exception classes for ETL error handling,
migrated from ABAP ZCX_ETL_ERROR.
"""


class ETLError(Exception):
    """
    Base exception class for ETL errors.
    Migrated from ABAP ZCX_ETL_ERROR.
    """
    
    def __init__(
        self,
        message: str,
        error_step: str = "",
        record_id: str = "",
        original_exception: Exception = None
    ):
        """
        Initialize ETL error
        
        Args:
            message: Error message
            error_step: ETL step where error occurred
            record_id: ID of record that caused error (if applicable)
            original_exception: Original exception that was caught
        """
        self.message = message
        self.error_step = error_step
        self.record_id = record_id
        self.original_exception = original_exception
        
        super().__init__(self._format_message())
    
    def _format_message(self) -> str:
        """Format complete error message"""
        parts = [self.message]
        
        if self.error_step:
            parts.append(f"Step: {self.error_step}")
        
        if self.record_id:
            parts.append(f"Record ID: {self.record_id}")
        
        if self.original_exception:
            parts.append(f"Original error: {str(self.original_exception)}")
        
        return " | ".join(parts)
    
    def to_dict(self) -> dict:
        """Convert exception to dictionary"""
        return {
            'error_type': self.__class__.__name__,
            'message': self.message,
            'error_step': self.error_step,
            'record_id': self.record_id,
            'original_exception': str(self.original_exception) if self.original_exception else None
        }


class ExtractError(ETLError):
    """Exception for extraction phase errors"""
    
    def __init__(self, message: str, **kwargs):
        kwargs['error_step'] = kwargs.get('error_step', 'EXTRACT')
        super().__init__(message, **kwargs)


class TransformError(ETLError):
    """Exception for transformation phase errors"""
    
    def __init__(self, message: str, **kwargs):
        kwargs['error_step'] = kwargs.get('error_step', 'TRANSFORM')
        super().__init__(message, **kwargs)


class LoadError(ETLError):
    """Exception for load phase errors"""
    
    def __init__(self, message: str, **kwargs):
        kwargs['error_step'] = kwargs.get('error_step', 'LOAD')
        super().__init__(message, **kwargs)


class ValidationError(ETLError):
    """Exception for validation errors"""
    
    def __init__(self, message: str, **kwargs):
        kwargs['error_step'] = kwargs.get('error_step', 'VALIDATE')
        super().__init__(message, **kwargs)


class ConfigurationError(ETLError):
    """Exception for configuration errors"""
    
    def __init__(self, message: str, **kwargs):
        kwargs['error_step'] = kwargs.get('error_step', 'INIT')
        super().__init__(message, **kwargs)