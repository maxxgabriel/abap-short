"""
Custom exceptions for ETL processes.
"""


class ETLError(Exception):
    """Base exception for ETL errors."""
    pass


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


class ConfigurationError(ETLError):
    """Exception raised for configuration issues."""
    pass