"""
ETL Component Interface Module

Defines the abstract base class for all ETL components with standard
execute() method contract and execution result dataclass.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime


@dataclass
class ExecutionResult:
    """
    Standard execution result dataclass for consistent component behavior.
    
    Attributes:
        success: Boolean indicating if execution was successful
        records_total: Total number of records processed
        records_success: Number of successfully processed records
        records_error: Number of records that failed processing
        message: Human-readable message describing the result
        start_time: Execution start timestamp
        end_time: Execution end timestamp
        component_name: Name of the component that produced this result
        metadata: Additional metadata specific to the component
    """
    success: bool = False
    records_total: int = 0
    records_success: int = 0
    records_error: int = 0
    message: str = ""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    component_name: str = ""
    metadata: dict = field(default_factory=dict)
    
    @property
    def duration_seconds(self) -> float:
        """Calculate execution duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.records_total > 0:
            return (self.records_success / self.records_total) * 100.0
        return 0.0
    
    @property
    def error_rate(self) -> float:
        """Calculate error rate as percentage."""
        if self.records_total > 0:
            return (self.records_error / self.records_total) * 100.0
        return 0.0
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            'success': self.success,
            'records_total': self.records_total,
            'records_success': self.records_success,
            'records_error': self.records_error,
            'message': self.message,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.duration_seconds,
            'component_name': self.component_name,
            'success_rate': self.success_rate,
            'error_rate': self.error_rate,
            'metadata': self.metadata
        }
    
    def __str__(self) -> str:
        """String representation of execution result."""
        status = "SUCCESS" if self.success else "FAILED"
        return (
            f"ExecutionResult({status}): {self.component_name} - "
            f"{self.records_success}/{self.records_total} records processed "
            f"in {self.duration_seconds:.2f}s"
        )


class ETLComponent(ABC):
    """
    Abstract base class defining standard interface for ETL components.
    
    All ETL components (Extract, Transform, Load) must inherit from this
    class and implement the execute() method contract.
    """
    
    def __init__(self, component_name: str, config: Optional[dict] = None):
        """
        Initialize ETL component.
        
        Args:
            component_name: Name identifier for the component
            config: Optional configuration dictionary
        """
        self._component_name = component_name
        self._config = config or {}
        self._initialized = False
    
    @abstractmethod
    def execute(self, **kwargs) -> ExecutionResult:
        """
        Execute the ETL component operation.
        
        This method must be implemented by all concrete ETL components.
        It should perform the component's primary operation and return
        a standardized ExecutionResult.
        
        Args:
            **kwargs: Component-specific execution parameters
            
        Returns:
            ExecutionResult: Standardized execution result
            
        Raises:
            ETLComponentError: If execution fails critically
        """
        pass
    
    @abstractmethod
    def validate_prerequisites(self) -> bool:
        """
        Validate that all prerequisites for execution are met.
        
        Returns:
            bool: True if prerequisites are satisfied, False otherwise
        """
        pass
    
    def get_component_name(self) -> str:
        """
        Get the component name.
        
        Returns:
            str: Component name identifier
        """
        return self._component_name
    
    def get_config(self) -> dict:
        """
        Get the component configuration.
        
        Returns:
            dict: Configuration dictionary
        """
        return self._config.copy()
    
    def is_initialized(self) -> bool:
        """
        Check if component is initialized.
        
        Returns:
            bool: True if initialized, False otherwise
        """
        return self._initialized
    
    def _create_result(
        self,
        success: bool,
        records_total: int = 0,
        records_success: int = 0,
        records_error: int = 0,
        message: str = "",
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        **metadata
    ) -> ExecutionResult:
        """
        Helper method to create standardized ExecutionResult.
        
        Args:
            success: Execution success status
            records_total: Total records processed
            records_success: Successfully processed records
            records_error: Failed records
            message: Result message
            start_time: Execution start time
            end_time: Execution end time
            **metadata: Additional metadata
            
        Returns:
            ExecutionResult: Populated execution result
        """
        return ExecutionResult(
            success=success,
            records_total=records_total,
            records_success=records_success,
            records_error=records_error,
            message=message,
            start_time=start_time,
            end_time=end_time,
            component_name=self._component_name,
            metadata=metadata
        )
    
    def __repr__(self) -> str:
        """String representation of component."""
        return f"{self.__class__.__name__}(name='{self._component_name}')"


class ETLComponentError(Exception):
    """
    Base exception class for ETL component errors.
    
    Attributes:
        message: Error message
        component_name: Name of component where error occurred
        error_step: Processing step where error occurred
        record_id: Optional identifier of record that caused error
    """
    
    def __init__(
        self,
        message: str,
        component_name: str = "",
        error_step: str = "",
        record_id: str = "",
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize ETL component error.
        
        Args:
            message: Error message
            component_name: Component where error occurred
            error_step: Processing step
            record_id: Record identifier if applicable
            original_exception: Original exception if wrapping
        """
        self.message = message
        self.component_name = component_name
        self.error_step = error_step
        self.record_id = record_id
        self.original_exception = original_exception
        
        super().__init__(self._format_message())
    
    def _format_message(self) -> str:
        """Format comprehensive error message."""
        parts = []
        
        if self.component_name:
            parts.append(f"Component: {self.component_name}")
        
        if self.error_step:
            parts.append(f"Step: {self.error_step}")
        
        if self.record_id:
            parts.append(f"Record: {self.record_id}")
        
        parts.append(f"Error: {self.message}")
        
        if self.original_exception:
            parts.append(f"Caused by: {str(self.original_exception)}")
        
        return " | ".join(parts)


class ExtractError(ETLComponentError):
    """Exception raised during extraction phase."""
    pass


class TransformError(ETLComponentError):
    """Exception raised during transformation phase."""
    pass


class LoadError(ETLComponentError):
    """Exception raised during load phase."""
    pass


class ValidationError(ETLComponentError):
    """Exception raised during validation."""
    pass