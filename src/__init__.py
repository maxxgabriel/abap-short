"""
Sales ETL System - Python/PySpark Implementation

A complete ETL system for sales data processing, migrated from SAP ABAP.
This package provides extraction, transformation, and loading capabilities
for sales analytics data.
"""

__version__ = "1.0.0"
__author__ = "Senior PySpark Team"
__license__ = "MIT"

from src.orchestrator import ETLOrchestrator
from src.logger import ETLLogger
from src.exceptions import ETLError

__all__ = [
    "ETLOrchestrator",
    "ETLLogger",
    "ETLError",
]