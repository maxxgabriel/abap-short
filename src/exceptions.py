"""
ETL Exception Hierarchy
Custom exceptions for ETL error handling
"""


class ETLError(Exception):
    """Base exception class for all ETL errors"""
    
    def __init__(self, message: str, error_step: str = None, record_id: str = None):
        """
        Initialize ETL error
        
        Args:
            message: Error message
            error_step: ETL step where error occurred (EXTRACT, TRANSFORM, LOAD)
            record_id: ID of the record that caused the error
        """
        self.message = message
        self.error_step = error_step
        self.record_id = record_id
        super().__init__(self.message)
    
    def __str__(self):
        parts = [self.message]
        if self.error_step:
            parts.append(f"Step: {self.error_step}")
        if self.record_id:
            parts.append(f"Record ID: {self.record_id}")
        return " | ".join(parts)


class ETLExtractionError(ETLError):
    """Exception raised during data extraction"""
    
    def __init__(self, message: str, record_id: str = None):
        super().__init__(message, error_step="EXTRACT", record_id=record_id)


class ETLTransformationError(ETLError):
    """Exception raised during data transformation"""
    
    def __init__(self, message: str, record_id: str = None):
        super().__init__(message, error_step="TRANSFORM", record_id=record_id)


class ETLLoadError(ETLError):
    """Exception raised during data loading"""
    
    def __init__(self, message: str, record_id: str = None):
        super().__init__(message, error_step="LOAD", record_id=record_id)


class ETLValidationError(ETLError):
    """Exception raised during data validation"""
    
    def __init__(self, message: str, record_id: str = None):
        super().__init__(message, error_step="VALIDATE", record_id=record_id)


class ETLConfigurationError(ETLError):
    """Exception raised for configuration errors"""
    
    def __init__(self, message: str):
        super().__init__(message, error_step="CONFIG")


class ETLPrerequisiteError(ETLError):
    """Exception raised when prerequisites are not met"""
    
    def __init__(self, message: str, component_name: str = None):
        msg = f"{component_name}: {message}" if component_name else message
        super().__init__(msg, error_step="PREREQUISITE")