"""
ETL Logger Module
Provides logging functionality for ETL processes.
"""

from datetime import datetime
from typing import Optional
import logging
from pyspark.sql import SparkSession


class ETLLogger:
    """Logger for ETL operations with database persistence."""
    
    def __init__(self, spark: SparkSession, etl_run_id: str):
        """
        Initialize the ETL Logger.
        
        Args:
            spark: Active SparkSession
            etl_run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.etl_run_id = etl_run_id
        
        # Setup Python logging
        self.logger = logging.getLogger(f'ETL_{etl_run_id}')
        self.logger.setLevel(logging.INFO)
        
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
    ):
        """
        Log a message with ETL context.
        
        Args:
            step: ETL step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Records with errors
        """
        log_entry = {
            'log_id': self._generate_log_id(),
            'etl_run_id': self.etl_run_id,
            'execution_date': datetime.now().strftime('%Y-%m-%d'),
            'execution_time': datetime.now().strftime('%H:%M:%S'),
            'process_step': step,
            'status': status,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error,
            'message': message
        }
        
        # Log to console
        log_level = self._get_log_level(status)
        log_msg = (
            f"[{step}] {message} | "
            f"Processed: {records_processed}, "
            f"Success: {records_success}, "
            f"Error: {records_error}"
        )
        self.logger.log(log_level, log_msg)
        
        # In production, persist to database table
        # self._persist_log_entry(log_entry)
    
    def get_etl_run_id(self) -> str:
        """Get the current ETL run ID."""
        return self.etl_run_id
    
    def _generate_log_id(self) -> str:
        """Generate unique log ID."""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f'LOG{timestamp}'
    
    def _get_log_level(self, status: str) -> int:
        """Map status code to logging level."""
        status_map = {
            'S': logging.INFO,
            'E': logging.ERROR,
            'W': logging.WARNING,
            'I': logging.INFO
        }
        return status_map.get(status, logging.INFO)
    
    def _persist_log_entry(self, log_entry: dict):
        """
        Persist log entry to database table.
        
        Args:
            log_entry: Dictionary containing log information
        """
        # In production, write to ETL log table
        pass