"""
Module: etl_component
Description: Abstract Base Class for ETL Component Interface
Converted from: ZIF_ETL_COMPONENT ABAP interface
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class ExecutionResult:
    """
    Execution result data structure.
    Converted from: ty_execution_result structure
    """
    success: bool
    records_total: int
    records_success: int
    records_error: int
    message: str

    def __post_init__(self):
        """Validate execution result fields."""
        if self.records_total < 0:
            raise ValueError("records_total cannot be negative")
        if self.records_success < 0:
            raise ValueError("records_success cannot be negative")
        if self.records_error < 0:
            raise ValueError("records_error cannot be negative")
        if self.records_success + self.records_error > self.records_total:
            raise ValueError("Sum of success and error records exceeds total")


class ETLComponentInterface(ABC):
    """
    Abstract Base Class for ETL components.
    Converted from: ZIF_ETL_COMPONENT ABAP interface
    
    All ETL components (Extract, Transform, Load) must implement this interface
    to ensure consistent behavior and error handling across the ETL pipeline.
    """

    @abstractmethod
    def execute(self) -> ExecutionResult:
        """
        Execute the ETL component logic.
        
        Returns:
            ExecutionResult: Result of execution with statistics and status
            
        Raises:
            ETLError: When execution fails
        """
        pass

    @abstractmethod
    def get_component_name(self) -> str:
        """
        Get the component name for logging and identification.
        
        Returns:
            str: Component name (e.g., 'Extractor', 'Transformer', 'Loader')
        """
        pass

    @abstractmethod
    def validate_prerequisites(self) -> bool:
        """
        Validate that all prerequisites are met before execution.
        
        Returns:
            bool: True if all prerequisites are valid, False otherwise
        """
        pass