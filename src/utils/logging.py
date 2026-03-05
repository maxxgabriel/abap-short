"""
ETL Logging Utility Module

Provides logging functionality for ETL processes including message logging,
statistics tracking, and run ID management.
"""

import logging
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession


class ETLLogger:
    """
    Logger class for ETL operations.
    
    Handles logging of ETL process steps, statistics, and messages
    with support for structured logging and metrics tracking.
    """
    
    def __init__(self, etl_run_id: str, spark: Optional[SparkSession] = None):
        """
        Initialize ETL logger.
        
        Args:
            etl_run_id: Unique identifier for the ETL run
            spark: Optional SparkSession for distributed logging
        """
        self.etl_run_id = etl_run_id
        self.spark = spark
        
        # Setup Python logging
        self.logger = logging.getLogger(f"ETL_{etl_run_id}")
        self.logger.setLevel(logging.INFO)
        
        # Create console handler if not exists
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
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
        Log a message with ETL context.
        
        Args:
            step: ETL process step (INIT, EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message text
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Records with errors
        """
        log_entry = {
            'etl_run_id': self.etl_run_id,
            'timestamp': datetime.now().isoformat(),
            'step': step,
            'status': status,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error,
            'message': message
        }
        
        # Log to Python logger
        log_level = self._get_log_level(status)
        self.logger.log(
            log_level,
            f"[{step}] {message} | Processed: {records_processed}, "
            f"Success: {records_success}, Errors: {records_error}"
        )
        
        # TODO: Write to database/log table if needed
        # self._write_to_log_table(log_entry)
    
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
        Log statistics for an ETL step.
        
        Args:
            step: ETL process step
            status: Status code
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Records with errors
            message: Statistics message
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
    
    @staticmethod
    def generate_log_id() -> str:
        """Generate a unique log ID."""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f"LOG{timestamp}"
    
    @staticmethod
    def generate_etl_run_id() -> str:
        """Generate a unique ETL run ID."""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"ETL{timestamp}"
    
    def _get_log_level(self, status: str) -> int:
        """
        Map status code to logging level.
        
        Args:
            status: Status code (S, E, W, I)
            
        Returns:
            Logging level constant
        """
        status_map = {
            'S': logging.INFO,
            'E': logging.ERROR,
            'W': logging.WARNING,
            'I': logging.INFO
        }
        return status_map.get(status, logging.INFO)
    
    def _write_to_log_table(self, log_entry: dict) -> None:
        """
        Write log entry to persistent storage.
        
        Args:
            log_entry: Log entry dictionary
        """
        # TODO: Implement database/Delta Lake logging if needed
        pass


def log_etl_message(logger: ETLLogger, step: str, status: str, message: str) -> None:
    """
    Utility function to log ETL message.
    
    Args:
        logger: ETLLogger instance
        step: Process step
        status: Status code
        message: Log message
    """
    logger.log_message(step=step, status=status, message=message)


def log_etl_statistics(
    logger: ETLLogger,
    step: str,
    status: str,
    records_processed: int,
    records_success: int,
    records_error: int,
    message: str
) -> None:
    """
    Utility function to log ETL statistics.
    
    Args:
        logger: ETLLogger instance
        step: Process step
        status: Status code
        records_processed: Total records processed
        records_success: Successfully processed records
        records_error: Records with errors
        message: Statistics message
    """
    logger.log_statistics(
        step=step,
        status=status,
        records_processed=records_processed,
        records_success=records_success,
        records_error=records_error,
        message=message
    )


def generate_unique_id(prefix: str) -> str:
    """
    Generate a unique ID with prefix.
    
    Args:
        prefix: ID prefix (e.g., 'ETL', 'LOG', 'ANL')
        
    Returns:
        Unique ID string
    """
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
    return f"{prefix}{timestamp}"


def calculate_percentage(numerator: float, denominator: float) -> float:
    """
    Calculate percentage safely.
    
    Args:
        numerator: Numerator value
        denominator: Denominator value
        
    Returns:
        Percentage value (0.0 if denominator is 0)
    """
    if denominator > 0:
        return (numerator / denominator) * 100
    return 0.0