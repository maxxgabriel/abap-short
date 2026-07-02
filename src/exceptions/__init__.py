"""
ETL Exception Hierarchy Package.

This package provides a comprehensive exception hierarchy for handling
errors throughout the ETL pipeline. It replaces the ABAP ZCX_ETL_ERROR
exception class with Python-native exceptions.

Main exception classes:
    - ETLError: Base exception for all ETL errors
    - ExtractError: Errors during data extraction
    - TransformError: Errors during data transformation
    - LoadError: Errors during data loading
    - ValidationError: Data validation failures
    - ConfigurationError: Configuration issues
    - OrchestrationError: Workflow coordination errors
"""

from src.exceptions.base import (
    ETLError,
    ExtractError,
    TransformError,
    LoadError,
    ValidationError,
    ConfigurationError,
    OrchestrationError,
)

__all__ = [
    "ETLError",
    "ExtractError",
    "TransformError",
    "LoadError",
    "ValidationError",
    "ConfigurationError",
    "OrchestrationError",
]

# Version information
__version__ = "1.0.0"

# Exception hierarchy mapping (for documentation)
EXCEPTION_HIERARCHY = {
    "ETLError": {
        "description": "Base exception for all ETL errors",
        "abap_equivalent": "ZCX_ETL_ERROR",
        "subclasses": {
            "ExtractError": {
                "description": "Data extraction errors",
                "step": "EXTRACT",
                "abap_constant": "EXTRACT_ERROR"
            },
            "TransformError": {
                "description": "Data transformation errors",
                "step": "TRANSFORM",
                "abap_constant": "TRANSFORM_ERROR"
            },
            "LoadError": {
                "description": "Data loading errors",
                "step": "LOAD",
                "abap_constant": "LOAD_ERROR"
            },
            "ValidationError": {
                "description": "Data validation errors",
                "step": "VALIDATE"
            },
            "ConfigurationError": {
                "description": "Configuration errors",
                "step": "INIT"
            },
            "OrchestrationError": {
                "description": "Workflow orchestration errors",
                "step": "ORCHESTRATE"
            }
        }
    }
}