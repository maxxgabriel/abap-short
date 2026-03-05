"""
Abstract base class for ETL components.

This module defines the interface that all ETL components (Extract, Transform, Load)
must implement. It provides a contract for component execution, validation, and monitoring.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime


@dataclass
class ExecutionResult:
    """
    Result of an ETL component execution.
    
    Attributes:
        success: Whether the execution was successful
        records_total: Total number of records processed
        records_success: Number of successfully processed records
        records_error: Number of records that failed processing
        message: Human-readable message about the execution
        execution_time: Timestamp when execution completed
    """
    success: bool
    records_total: int = 0
    records_success: int = 0
    records_error: int = 0
    message: str = ""
    execution_time: datetime = field(default_factory=datetime.now)
    
    @property
    def success_rate(self) -> float:
        """Calculate the success rate as a percentage."""
        if self.records_total == 0:
            return 0.0
        return (self.records_success / self.records_total) * 100.0
    
    @property
    def error_rate(self) -> float:
        """Calculate the error rate as a percentage."""
        if self.records_total == 0:
            return 0.0
        return (self.records_error / self.records_total) * 100.0


class ETLComponentInterface(ABC):
    """
    Abstract base class for all ETL components.
    
    All concrete ETL components (Extractor, Transformer, Loader) must inherit
    from this class and implement its abstract methods.
    """
    
    @abstractmethod
    def execute(self) -> ExecutionResult:
        """
        Execute the ETL component.
        
        Returns:
            ExecutionResult: The result of the execution
            
        Raises:
            ETLError: If the component execution fails
        """
        pass
    
    @abstractmethod
    def get_component_name(self) -> str:
        """
        Get the name of the component.
        
        Returns:
            str: The component name (e.g., "Extractor", "Transformer", "Loader")
        """
        pass
    
    @abstractmethod
    def validate_prerequisites(self) -> bool:
        """
        Validate that all prerequisites for component execution are met.
        
        This should check things like:
        - Required configuration is present
        - Database connections are available
        - Input data is in the expected format
        
        Returns:
            bool: True if all prerequisites are met, False otherwise
        """
        pass
    
    def get_execution_metadata(self) -> dict:
        """
        Get metadata about the component for logging and monitoring.
        
        Returns:
            dict: Metadata about the component
        """
        return {
            "component_name": self.get_component_name(),
            "prerequisites_valid": self.validate_prerequisites(),
            "timestamp": datetime.now().isoformat()
        }


class LoggingInterface(ABC):
    """
    Abstract base class for ETL logging components.
    
    Defines the interface for logging ETL operations and events.
    """
    
    # Status constants
    STATUS_SUCCESS = "S"
    STATUS_ERROR = "E"
    STATUS_WARNING = "W"
    STATUS_INFO = "I"
    
    # Process step constants
    STEP_INIT = "INIT"
    STEP_EXTRACT = "EXTRACT"
    STEP_TRANSFORM = "TRANSFORM"
    STEP_LOAD = "LOAD"
    STEP_VALIDATE = "VALIDATE"
    STEP_COMPLETE = "COMPLETE"
    STEP_ERROR = "ERROR"
    
    @abstractmethod
    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ) -> None:
        """
        Log a message about ETL execution.
        
        Args:
            step: The ETL step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: The status code (S, E, W, I)
            message: The log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        pass
    
    @abstractmethod
    def get_etl_run_id(self) -> str:
        """
        Get the unique identifier for the current ETL run.
        
        Returns:
            str: The ETL run ID
        """
        pass


@dataclass
class ETLConfig:
    """
    Configuration for ETL process execution.
    
    Attributes:
        batch_size: Number of records to process in each batch
        commit_interval: Number of records between commits
        parallel_jobs: Number of parallel processing jobs
        retry_attempts: Number of retry attempts on failure
        timeout_seconds: Maximum execution time in seconds
    """
    batch_size: int = 1000
    commit_interval: int = 500
    parallel_jobs: int = 1
    retry_attempts: int = 3
    timeout_seconds: int = 3600


@dataclass
class ETLStatistics:
    """
    Statistics about ETL process execution.
    
    Attributes:
        total_records: Total number of records processed
        success_records: Number of successfully processed records
        error_records: Number of records that failed
        warning_records: Number of records with warnings
        start_time: When processing started
        end_time: When processing completed
    """
    total_records: int = 0
    success_records: int = 0
    error_records: int = 0
    warning_records: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    @property
    def duration_seconds(self) -> float:
        """Calculate duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_records == 0:
            return 0.0
        return (self.success_records / self.total_records) * 100.0
    
    def to_dict(self) -> dict:
        """Convert statistics to dictionary."""
        return {
            "total_records": self.total_records,
            "success_records": self.success_records,
            "error_records": self.error_records,
            "warning_records": self.warning_records,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.duration_seconds,
            "success_rate": self.success_rate
        }