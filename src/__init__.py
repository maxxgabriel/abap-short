"""
ETL Package Initialization

This module initializes the ETL package and provides
convenient imports for core components.
"""

from src.constants import ETLConstants, CONSTANTS
from src.config import ETLConfig, ConfigLoader
from src.logger import ETLLogger, LogEntry
from src.exceptions import (
    ETLError,
    ExtractError,
    TransformError,
    LoadError,
    ValidationError,
    ConfigurationError
)

__version__ = "1.0.0"
__author__ = "ETL Team"

__all__ = [
    # Constants
    'ETLConstants',
    'CONSTANTS',
    
    # Configuration
    'ETLConfig',
    'ConfigLoader',
    
    # Logging
    'ETLLogger',
    'LogEntry',
    
    # Exceptions
    'ETLError',
    'ExtractError',
    'TransformError',
    'LoadError',
    'ValidationError',
    'ConfigurationError',
    
    # Metadata
    '__version__',
    '__author__'
]


def get_version() -> str:
    """Get package version"""
    return __version__


def get_package_info() -> dict:
    """Get package information"""
    return {
        'version': __version__,
        'author': __author__,
        'components': __all__
    }