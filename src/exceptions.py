"""
Custom Exception Classes
Migrated from ZCX_ETL_ERROR
"""


class ETLError(Exception):
    """Base exception class for ETL errors"""
    
    def __init__(
        self,
        message: str,
        error_step: str = None,
        record_id: str = None,
        original_exception: Exception = None
    ):
        self.message = message
        self.error_step = error_step
        self.record_id = record_id
        self.original_exception = original_exception
        super().__init__(self.message)
    
    def __str__(self):
        error_parts = [f"ETL Error: {self.message}"]
        if self.error_step:
            error_parts.append(f"Step: {self.error_step}")
        if self.record_id:
            error_parts.append(f"Record ID: {self.record_id}")
        if self.original_exception:
            error_parts.append(f"Original: {str(self.original_exception)}")
        return " | ".join(error_parts)


class ExtractError(ETLError):
    """Exception raised during data extraction"""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(f"Extract Error: {message}", error_step="EXTRACT", **kwargs)


class TransformError(ETLError):
    """Exception raised during data transformation"""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(f"Transform Error: {message}", error_step="TRANSFORM", **kwargs)


class LoadError(ETLError):
    """Exception raised during data loading"""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(f"Load Error: {message}", error_step="LOAD", **kwargs)


class ValidationError(ETLError):
    """Exception raised during data validation"""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(f"Validation Error: {message}", error_step="VALIDATE", **kwargs)