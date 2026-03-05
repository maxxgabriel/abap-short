"""
Logger module for Sales ETL System.
Provides logging functionality across all ETL components.
"""
import logging
from datetime import datetime
from typing import Optional
from enum import Enum


class LogStatus(Enum):
    """Log status codes."""
    SUCCESS = "S"
    ERROR = "E"
    WARNING = "W"
    INFO = "I"


class LogStep(Enum):
    """ETL process step identifiers."""
    INIT = "INIT"
    EXTRACT = "EXTRACT"
    TRANSFORM = "TRANSFORM"
    LOAD = "LOAD"
    VALIDATE = "VALIDATE"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"


class ETLLogger:
    """Custom logger for ETL processes."""
    
    def __init__(self, etl_run_id: str, log_level: int = logging.INFO):
        """
        Initialize ETL logger.
        
        Args:
            etl_run_id: ETL run identifier
            log_level: Logging level (default INFO)
        """
        self.etl_run_id = etl_run_id
        self.logger = self._setup_logger(log_level)
    
    def _setup_logger(self, log_level: int) -> logging.Logger:
        """
        Setup and configure logger.
        
        Args:
            log_level: Logging level
        
        Returns:
            Configured logger instance
        """
        logger = logging.getLogger(f"ETL_{self.etl_run_id}")
        logger.setLevel(log_level)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - [%(levelname)s] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        
        logger.addHandler(console_handler)
        
        return logger
    
    def log_message(self, step: str, status: str, message: str,
                   records_processed: int = 0, records_success: int = 0, 
                   records_error: int = 0):
        """
        Log a message with ETL context.
        
        Args:
            step: Process step
            status: Status code
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        log_entry = {
            "etl_run_id": self.etl_run_id,
            "step": step,
            "status": status,
            "message": message,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "timestamp": datetime.now().isoformat()
        }
        
        # Log at appropriate level based on status
        if status == LogStatus.ERROR.value:
            self.logger.error(f"[{step}] {message} | Processed: {records_processed}, Success: {records_success}, Errors: {records_error}")
        elif status == LogStatus.WARNING.value:
            self.logger.warning(f"[{step}] {message} | Processed: {records_processed}, Success: {records_success}, Errors: {records_error}")
        else:
            self.logger.info(f"[{step}] {message} | Processed: {records_processed}, Success: {records_success}, Errors: {records_error}")
    
    def get_etl_run_id(self) -> str:
        """Get the ETL run identifier."""
        return self.etl_run_id
    
    def info(self, message: str):
        """Log info level message."""
        self.logger.info(message)
    
    def warning(self, message: str):
        """Log warning level message."""
        self.logger.warning(message)
    
    def error(self, message: str, exc_info: bool = False):
        """Log error level message."""
        self.logger.error(message, exc_info=exc_info)
    
    def debug(self, message: str):
        """Log debug level message."""
        self.logger.debug(message)