"""
Abstract Base Class for ETL Components
Converted from ABAP interface ZIF_ETL_COMPONENT
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class ExecutionResult:
    """
    Execution result data structure
    Mapped from ABAP ty_execution_result structure
    """
    success: bool
    records_total: int
    records_success: int
    records_error: int
    message: str


class ETLComponentError(Exception):
    """
    Base exception for ETL component errors
    Replaces ABAP RAISING zcx_etl_error clause
    """
    def __init__(self, message: str, error_step: Optional[str] = None, 
                 record_id: Optional[str] = None):
        self.message = message
        self.error_step = error_step
        self.record_id = record_id
        super().__init__(self.message)


class ExtractError(ETLComponentError):
    """Exception raised during extraction phase"""
    pass


class TransformError(ETLComponentError):
    """Exception raised during transformation phase"""
    pass


class LoadError(ETLComponentError):
    """Exception raised during load phase"""
    pass


class ETLComponent(ABC):
    """
    Abstract Base Class for all ETL components
    Converted from ABAP interface ZIF_ETL_COMPONENT
    
    All concrete ETL components must inherit from this class
    and implement the abstract methods.
    """
    
    @abstractmethod
    def execute(self) -> ExecutionResult:
        """
        Execute the ETL component processing
        
        Returns:
            ExecutionResult: Processing result with statistics
            
        Raises:
            ETLComponentError: If execution fails
        """
        pass
    
    @abstractmethod
    def get_component_name(self) -> str:
        """
        Get the name of this ETL component
        
        Returns:
            str: Component name for logging/identification
        """
        pass
    
    @abstractmethod
    def validate_prerequisites(self) -> bool:
        """
        Validate that all prerequisites for execution are met
        
        Returns:
            bool: True if prerequisites are satisfied, False otherwise
        """
        pass


class ETLLogger(ABC):
    """
    Abstract Base Class for ETL logging
    Converted from ABAP interface ZIF_ETL_LOGGER
    """
    
    # Status constants (mapped from ABAP gc_status)
    STATUS_SUCCESS = 'S'
    STATUS_ERROR = 'E'
    STATUS_WARNING = 'W'
    STATUS_INFO = 'I'
    
    # Step constants (mapped from ABAP gc_step)
    STEP_INIT = 'INIT'
    STEP_EXTRACT = 'EXTRACT'
    STEP_TRANSFORM = 'TRANSFORM'
    STEP_LOAD = 'LOAD'
    STEP_VALIDATE = 'VALIDATE'
    STEP_COMPLETE = 'COMPLETE'
    STEP_ERROR = 'ERROR'
    
    @abstractmethod
    def log_message(self, step: str, status: str, message: str,
                    records_processed: int = 0, records_success: int = 0,
                    records_error: int = 0) -> None:
        """
        Log a message with ETL context
        
        Args:
            step: Process step identifier
            status: Status code (S/E/W/I)
            message: Log message text
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        pass
    
    @abstractmethod
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run identifier
        
        Returns:
            str: ETL run ID
        """
        pass