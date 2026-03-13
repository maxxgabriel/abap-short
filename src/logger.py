"""
ETL Logger Module
Provides logging functionality for ETL processes.
"""

from datetime import datetime
from typing import Dict, Optional
import logging


class ETLLogger:
    """
    Logger for ETL processes with structured logging.
    """
    
    def __init__(self, etl_run_id: str, log_level: str = 'INFO'):
        """
        Initialize the ETL logger.
        
        Args:
            etl_run_id: Unique identifier for the ETL run
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.etl_run_id = etl_run_id
        
        # Configure Python logger
        self.logger = logging.getLogger(f'ETL_{etl_run_id}')
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Create console handler
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
        # Log entries collection
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
        Log a message with metadata.
        
        Args:
            step: ETL step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        log_entry = {
            'log_id': self._generate_log_id(),
            'etl_run_id': self.etl_run_id,
            'timestamp': datetime.now().isoformat(),
            'step': step,
            'status': status,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error,
            'message': message
        }
        
        self.log_entries.append(log_entry)
        
        # Log to console
        log_msg = (f"[{step}] {message} "
                  f"(Processed: {records_processed}, "
                  f"Success: {records_success}, "
                  f"Errors: {records_error})")
        
        if status == 'E':
            self.logger.error(log_msg)
        elif status == 'W':
            self.logger.warning(log_msg)
        elif status == 'I':
            self.logger.info(log_msg)
        else:
            self.logger.info(log_msg)
    
    def _generate_log_id(self) -> str:
        """Generate unique log ID."""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f'LOG_{timestamp}'
    
    def get_etl_run_id(self) -> str:
        """Get the ETL run ID."""
        return self.etl_run_id
    
    def get_log_entries(self) -> list:
        """Get all log entries."""
        return self.log_entries