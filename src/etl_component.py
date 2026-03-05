"""
ETL Component Abstract Base Class
Defines the interface for all ETL components (Extract, Transform, Load)
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime


@dataclass
class ExecutionResult:
    """Result structure for ETL component execution"""
    success: bool
    records_total: int = 0
    records_success: int = 0
    records_error: int = 0
    message: str = ""
    execution_time: Optional[float] = None
    
    @property
    def records_failed(self) -> int:
        """Calculate failed records"""
        return self.records_total - self.records_success
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage"""
        if self.records_total == 0:
            return 0.0
        return (self.records_success / self.records_total) * 100


class ETLComponent(ABC):
    """
    Abstract base class for ETL components.
    All ETL components (Extractor, Transformer, Loader) must inherit from this.
    """
    
    def __init__(self, logger=None):
        """
        Initialize ETL component
        
        Args:
            logger: Logger instance for recording execution details
        """
        self._logger = logger
        self._start_time: Optional[datetime] = None
        self._end_time: Optional[datetime] = None
    
    @abstractmethod
    def execute(self, *args, **kwargs) -> ExecutionResult:
        """
        Execute the ETL component operation
        
        Returns:
            ExecutionResult: Result of the execution
            
        Raises:
            ETLError: If execution fails
        """
        pass
    
    @abstractmethod
    def get_component_name(self) -> str:
        """
        Get the name of the component
        
        Returns:
            str: Component name
        """
        pass
    
    @abstractmethod
    def validate_prerequisites(self) -> bool:
        """
        Validate that all prerequisites for execution are met
        
        Returns:
            bool: True if all prerequisites are met, False otherwise
        """
        pass
    
    def _start_execution(self):
        """Record execution start time"""
        self._start_time = datetime.now()
        if self._logger:
            self._logger.log_message(
                step=self.get_component_name(),
                status='I',
                message=f"Starting {self.get_component_name()} execution"
            )
    
    def _end_execution(self, success: bool, message: str = ""):
        """
        Record execution end time and log result
        
        Args:
            success: Whether execution was successful
            message: Optional message to log
        """
        self._end_time = datetime.now()
        if self._logger:
            status = 'S' if success else 'E'
            duration = (self._end_time - self._start_time).total_seconds() if self._start_time else 0
            log_msg = f"{message} (Duration: {duration:.2f}s)" if message else f"Duration: {duration:.2f}s"
            self._logger.log_message(
                step=self.get_component_name(),
                status=status,
                message=log_msg
            )
    
    def get_execution_duration(self) -> Optional[float]:
        """
        Get execution duration in seconds
        
        Returns:
            Optional[float]: Duration in seconds, or None if not yet executed
        """
        if self._start_time and self._end_time:
            return (self._end_time - self._start_time).total_seconds()
        return None