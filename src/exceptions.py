"""
Custom exception classes for ETL error handling.

This module defines all custom exceptions used throughout the ETL system,
providing structured error handling and detailed error information.
"""

from typing import Optional


class ETLError(Exception):
    """Base exception class for ETL errors"""
    
    def __init__(
        self,
        message: str,
        error_step: Optional[str] = None,
        record_id: Optional[str] = None
    ):
        """
        Initialize ETL error.
        
        Args:
            message: Error message
            error_step: ETL step where error occurred
            record_id: ID of record being processed when error occurred
        """
        super().__init__(message)
        self.message = message
        self.error_step = error_step
        self.record_id = record_id
    
    def __str__(self) -> str:
        """String representation of error"""
        parts = [self.message]
        if self.error_step:
            parts.append(f"Step: {self.error_step}")
        if self.record_id:
            parts.append(f"Record: {self.record_id}")
        return " | ".join(parts)


class ExtractError(ETLError):
    """Exception raised during data extraction"""
    
    def __init__(
        self,
        message: str,
        source: Optional[str] = None,
        record_id: Optional[str] = None
    ):
        """
        Initialize extraction error.
        
        Args:
            message: Error message
            source: Source of extraction (table, file, etc.)
            record_id: ID of record being extracted
        """
        super().__init__(message, error_step="EXTRACT", record_id=record_id)
        self.source = source


class TransformError(ETLError):
    """Exception raised during data transformation"""
    
    def __init__(
        self,
        message: str,
        transformation: Optional[str] = None,
        record_id: Optional[str] = None
    ):
        """
        Initialize transformation error.
        
        Args:
            message: Error message
            transformation: Name of transformation that failed
            record_id: ID of record being transformed
        """
        super().__init__(message, error_step="TRANSFORM", record_id=record_id)
        self.transformation = transformation


class LoadError(ETLError):
    """Exception raised during data loading"""
    
    def __init__(
        self,
        message: str,
        target: Optional[str] = None,
        record_id: Optional[str] = None
    ):
        """
        Initialize load error.
        
        Args:
            message: Error message
            target: Target of load operation (table, file, etc.)
            record_id: ID of record being loaded
        """
        super().__init__(message, error_step="LOAD", record_id=record_id)
        self.target = target


class ValidationError(ETLError):
    """Exception raised during data validation"""
    
    def __init__(
        self,
        message: str,
        field: Optional[str] = None,
        record_id: Optional[str] = None
    ):
        """
        Initialize validation error.
        
        Args:
            message: Error message
            field: Field that failed validation
            record_id: ID of record being validated
        """
        super().__init__(message, error_step="VALIDATE", record_id=record_id)
        self.field = field


class ConfigurationError(ETLError):
    """Exception raised for configuration errors"""
    
    def __init__(
        self,
        message: str,
        config_key: Optional[str] = None
    ):
        """
        Initialize configuration error.
        
        Args:
            message: Error message
            config_key: Configuration key that caused error
        """
        super().__init__(message, error_step="INIT")
        self.config_key = config_key