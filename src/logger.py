"""
ETL Logger - Logging utility for ETL process
Handles logging to console, file, and database
"""

from datetime import datetime
from typing import Optional
import logging


class ETLLogger:
    """ETL logging utility"""
    
    def __init__(self, etl_run_id: str, config):
        """
        Initialize logger
        
        Args:
            etl_run_id: Unique ETL run identifier
            config: Configuration object
        """
        self.etl_run_id = etl_run_id
        self.config = config
        self.log_counter = 0
        
        # Setup Python logging
        self.log = logging.getLogger(self.__class__.__name__)
    
    def log_message(self,
                   step: str,
                   status: str,
                   message: str,
                   records_processed: int = 0,
                   records_success: int = 0,
                   records_error: int = 0) -> None:
        """
        Log ETL message
        
        Args:
            step: ETL process step
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        log_id = self._generate_log_id()
        execution_date = datetime.now().date()
        execution_time = datetime.now().time()
        
        log_entry = {
            'log_id': log_id,
            'etl_run_id': self.etl_run_id,
            'execution_date': execution_date,
            'execution_time': execution_time,
            'process_step': step,
            'status': status,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error,
            'message': message
        }
        
        # Console logging
        log_level = self._get_log_level(status)
        self.log.log(
            log_level,
            f"[{step}] {message} (Processed: {records_processed}, Success: {records_success}, Error: {records_error})"
        )
        
        # Store log entry (could be extended to write to database)
        if self.config.enable_db_logging:
            self._write_to_database(log_entry)
    
    def _generate_log_id(self) -> str:
        """Generate unique log ID"""
        self.log_counter += 1
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"LOG{timestamp}{self.log_counter:06d}"
    
    def _get_log_level(self, status: str) -> int:
        """Convert status to logging level"""
        status_map = {
            'S': logging.INFO,
            'I': logging.INFO,
            'W': logging.WARNING,
            'E': logging.ERROR
        }
        return status_map.get(status, logging.INFO)
    
    def _write_to_database(self, log_entry: dict) -> None:
        """Write log entry to database (placeholder)"""
        # This would write to a logging table in production
        pass
    
    def get_etl_run_id(self) -> str:
        """Get current ETL run ID"""
        return self.etl_run_id