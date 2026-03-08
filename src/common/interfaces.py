"""
Interface definitions for ETL components.
Converted from ZIF_ETL_COMPONENT and ZIF_ETL_LOGGER.
"""

from abc import ABC, abstractmethod
from typing import Optional
from src.common.types import ExecutionResult


class ETLComponent(ABC):
    """
    Interface for all ETL components.
    Converted from ZIF_ETL_COMPONENT.
    """
    
    @abstractmethod
    def execute(self) -> ExecutionResult:
        """Execute the ETL component."""
        pass
    
    @abstractmethod
    def get_component_name(self) -> str:
        """Get the component name."""
        pass
    
    @abstractmethod
    def validate_prerequisites(self) -> bool:
        """Validate prerequisites before execution."""
        pass


class ETLLogger(ABC):
    """
    Interface for ETL logging.
    Converted from ZIF_ETL_LOGGER.
    """
    
    @abstractmethod
    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ):
        """Log a message."""
        pass
    
    @abstractmethod
    def get_etl_run_id(self) -> str:
        """Get the ETL run ID."""
        pass