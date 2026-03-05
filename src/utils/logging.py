"""
ETL Logging Utility Module
Provides logging functionality for ETL processes with structured message handling,
statistics tracking, and unique run ID generation.
"""

import logging
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field


@dataclass
class LogEntry:
    """Structured log entry for ETL operations."""
    log_id: str
    etl_run_id: str
    execution_date: str
    execution_time: str
    process_step: str
    status: str
    records_processed: int = 0
    records_success: int = 0
    records_error: int = 0
    message: str = ""
    
    def to_dict(self):
        """Convert log entry to dictionary format."""
        return {
            "log_id": self.log_id,
            "etl_run_id": self.etl_run_id,
            "execution_date": self.execution_date,
            "execution_time": self.execution_time,
            "process_step": self.process_step,
            "status": self.status,
            "records_processed": self.records_processed,
            "records_success": self.records_success,
            "records_error": self.records_error,
            "message": self.message
        }


class ETLLogger:
    """
    ETL Logger class for structured logging with statistics tracking.
    Provides utilities equivalent to ABAP ZETL_MACROS functionality.
    """
    
    # Status constants (equivalent to ABAP constants)
    STATUS_SUCCESS = 'S'
    STATUS_ERROR = 'E'
    STATUS_WARNING = 'W'
    STATUS_INFO = 'I'
    
    # Step constants
    STEP_INIT = 'INIT'
    STEP_EXTRACT = 'EXTRACT'
    STEP_TRANSFORM = 'TRANSFORM'
    STEP_LOAD = 'LOAD'
    STEP_VALIDATE = 'VALIDATE'
    STEP_COMPLETE = 'COMPLETE'
    STEP_ERROR = 'ERROR'
    
    def __init__(self, etl_run_id: str, logger_name: str = "etl"):
        """
        Initialize ETL Logger.
        
        Args:
            etl_run_id: Unique identifier for the ETL run
            logger_name: Name for the Python logger instance
        """
        self.etl_run_id = etl_run_id
        self.logger = logging.getLogger(logger_name)
        self.log_entries = []
        
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
        Log a message with step, status and optional statistics.
        Equivalent to log_etl_message and log_etl_statistics macros.
        
        Args:
            step: ETL process step
            status: Log status (S/E/W/I)
            message: Log message text
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        now = datetime.now()
        log_id = self._generate_log_id()
        
        log_entry = LogEntry(
            log_id=log_id,
            etl_run_id=self.etl_run_id,
            execution_date=now.strftime('%Y-%m-%d'),
            execution_time=now.strftime('%H:%M:%S'),
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message
        )
        
        self.log_entries.append(log_entry)
        
        # Log to Python logger based on status
        log_text = f"[{step}] {message}"
        if records_processed > 0:
            log_text += f" (Processed: {records_processed}, Success: {records_success}, Error: {records_error})"
        
        if status == self.STATUS_ERROR:
            self.logger.error(log_text)
        elif status == self.STATUS_WARNING:
            self.logger.warning(log_text)
        elif status == self.STATUS_INFO:
            self.logger.info(log_text)
        else:
            self.logger.info(log_text)
    
    def log_statistics(
        self,
        step: str,
        status: str,
        records_processed: int,
        records_success: int,
        records_error: int,
        message: str
    ) -> None:
        """
        Log statistics with record counts.
        Equivalent to log_etl_statistics macro.
        
        Args:
            step: ETL process step
            status: Log status
            records_processed: Total records processed
            records_success: Successful records
            records_error: Error records
            message: Additional message
        """
        self.log_message(
            step=step,
            status=status,
            message=message,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error
        )
    
    def get_etl_run_id(self) -> str:
        """Get the ETL run ID."""
        return self.etl_run_id
    
    def get_log_entries(self) -> list:
        """Get all log entries for this ETL run."""
        return self.log_entries
    
    def _generate_log_id(self) -> str:
        """
        Generate unique log ID.
        Equivalent to generate_unique_id macro.
        
        Returns:
            Unique log identifier
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f"LOG{timestamp[:14]}"
    
    @staticmethod
    def generate_etl_run_id() -> str:
        """
        Generate unique ETL run ID.
        
        Returns:
            Unique ETL run identifier
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f"ETL{timestamp[:14]}"


def validate_field(value, field_name: str = "field") -> bool:
    """
    Validate that a field is not empty/null.
    Equivalent to validate_field macro.
    
    Args:
        value: Value to validate
        field_name: Name of the field for error messages
        
    Returns:
        True if valid, False otherwise
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        return False
    return True


def calculate_percentage(numerator: float, denominator: float) -> float:
    """
    Calculate percentage safely.
    Equivalent to calculate_percentage macro.
    
    Args:
        numerator: Numerator value
        denominator: Denominator value
        
    Returns:
        Percentage value (0 if denominator is 0)
    """
    if denominator > 0:
        return (numerator / denominator) * 100
    return 0.0


def format_currency(amount: float, currency: str = "USD", decimals: int = 2) -> str:
    """
    Format currency value.
    Equivalent to format_currency macro.
    
    Args:
        amount: Amount to format
        currency: Currency code
        decimals: Number of decimal places
        
    Returns:
        Formatted currency string
    """
    return f"{currency} {amount:,.{decimals}f}"


def generate_unique_id(prefix: str = "") -> str:
    """
    Generate a unique ID with optional prefix.
    Equivalent to generate_unique_id macro.
    
    Args:
        prefix: Prefix for the ID
        
    Returns:
        Unique identifier
    """
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
    return f"{prefix}{timestamp[:14]}"


class ETLError(Exception):
    """
    Base exception class for ETL errors.
    Equivalent to ZCX_ETL_ERROR exception class.
    """
    
    def __init__(
        self,
        message: str,
        error_step: Optional[str] = None,
        record_id: Optional[str] = None
    ):
        """
        Initialize ETL error.
        
        Args:
            message: Error message
            error_step: ETL step where error occurred
            record_id: Record identifier related to error
        """
        self.message = message
        self.error_step = error_step
        self.record_id = record_id
        super().__init__(self.message)
    
    def __str__(self):
        """String representation of error."""
        error_text = f"ETL Error: {self.message}"
        if self.error_step:
            error_text += f" (Step: {self.error_step})"
        if self.record_id:
            error_text += f" (Record: {self.record_id})"
        return error_text


class ExtractError(ETLError):
    """Exception for extraction errors."""
    pass


class TransformError(ETLError):
    """Exception for transformation errors."""
    pass


class LoadError(ETLError):
    """Exception for load errors."""
    pass


def handle_etl_error(logger: ETLLogger, step: str, error: Exception) -> None:
    """
    Handle ETL errors with logging.
    Equivalent to handle_etl_error macro.
    
    Args:
        logger: ETL logger instance
        step: ETL step where error occurred
        error: Exception that was raised
    """
    error_message = f"{step} failed: {str(error)}"
    logger.log_message(
        step=step,
        status=ETLLogger.STATUS_ERROR,
        message=error_message
    )


# Configure logging format
def configure_logging(log_level: str = "INFO", log_file: Optional[str] = None):
    """
    Configure logging for ETL processes.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional log file path
    """
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=handlers
    )