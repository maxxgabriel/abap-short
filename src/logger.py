"""
ETL Logger Module
Handles logging for the ETL process.
"""

import logging
from datetime import datetime
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class ETLLogger:
    """Custom logger for ETL operations."""
    
    def __init__(self, run_id: str, config: Dict):
        """
        Initialize ETL logger.
        
        Args:
            run_id: ETL run identifier
            config: Configuration dictionary
        """
        self.run_id = run_id
        self.config = config
        self.log_entries = []
    
    def log_step(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ):
        """
        Log an ETL step.
        
        Args:
            step: Process step name
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Failed records
        """
        log_entry = {
            'log_id': self._generate_log_id(),
            'etl_run_id': self.run_id,
            'timestamp': datetime.now().isoformat(),
            'process_step': step,
            'status': status,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error,
            'message': message
        }
        
        self.log_entries.append(log_entry)
        
        # Log to standard logger
        log_level = {
            'S': logging.INFO,
            'I': logging.INFO,
            'W': logging.WARNING,
            'E': logging.ERROR
        }.get(status, logging.INFO)
        
        logger.log(
            log_level,
            f"[{self.run_id}] [{step}] {message} "
            f"(Processed: {records_processed}, Success: {records_success}, Error: {records_error})"
        )
    
    def _generate_log_id(self) -> str:
        """Generate unique log ID."""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f"LOG{timestamp}"
    
    def get_log_entries(self):
        """Return all log entries."""
        return self.log_entries