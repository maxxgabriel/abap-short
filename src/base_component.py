"""
Abstract base class for ETL components.

This module defines the base interface that all ETL components must implement,
including execution contracts, validation, and result structures.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ExecutionResult:
    """Result structure for ETL component execution."""
    
    success: bool
    records_total: int = 0
    records_success: int = 0
    records_error: int = 0
    message: str = ""
    
    @property
    def records_failed(self) -> int:
        """Calculate failed records."""
        return self.records_total - self.records_success
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage."""
        if self.records_total == 0:
            return 0.0
        return (self.records_success / self.records_total) * 100


class ETLComponentInterface(ABC):
    """
    Abstract base class for all ETL components.
    
    All ETL components (Extract, Transform, Load) must inherit from this
    class and implement its abstract methods.
    """
    
    @abstractmethod
    def execute(self) -> ExecutionResult:
        """
        Execute the ETL component logic.
        
        Returns:
            ExecutionResult: Execution status and statistics
            
        Raises:
            ETLError: If component execution fails
        """
        pass
    
    @abstractmethod
    def get_component_name(self) -> str:
        """
        Get the component name.
        
        Returns:
            str: Name of the component
        """
        pass
    
    @abstractmethod
    def validate_prerequisites(self) -> bool:
        """
        Validate component prerequisites before execution.
        
        Returns:
            bool: True if prerequisites are met, False otherwise
        """
        pass