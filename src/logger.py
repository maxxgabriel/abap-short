"""
ETL Logger Module
Provides logging functionality for ETL processes.
"""

from datetime import datetime
from typing import Optional
import logging
import uuid


class ETLLogger:
    """
    Logger for ETL processes.
    Tracks execution steps, status, and statistics.
    """
    
    def __init__(self, etl_run_id: str):
        """
        Initialize the ETL logger.
        
        Args:
            etl_run_id: Unique identifier for this ETL run
        """
        self.etl_run_id = etl_run_id
        self.log_entries = []
        
        # Configure Python logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)
    
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
            step: ETL process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message text
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        log_entry = {
            "log_id": self._generate_log_id(),
            "etl_run_id": self.etl_run_id,
            "execution_date": datetime.now().strftime("%Y-%m-%d"),
            "execution_time": datetime.now().strftime("%H:%M:%S"),
            "process_step": step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message
        }
        
        self.log_entries.append(log_entry)
        
        # Map status to Python logging level
        level_map = {
            "S": logging.INFO,
            "I": logging.INFO,
            "W": logging.WARNING,
            "E": logging.ERROR
        }
        
        level = level_map.get(status, logging.INFO)
        
        # Format log message
        log_msg = f"[{step}] {message}"
        if records_processed > 0:
            log_msg += f" (Processed: {records_processed}, Success: {records_success}, Error: {records_error})"
        
        self.logger.log(level, log_msg)
    
    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID.
        
        Returns:
            ETL run identifier
        """
        return self.etl_run_id
    
    def get_log_entries(self) -> list:
        """
        Get all log entries for this ETL run.
        
        Returns:
            List of log entry dictionaries
        """
        return self.log_entries
    
    def _generate_log_id(self) -> str:
        """
        Generate a unique log ID.
        
        Returns:
            Unique log identifier
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"LOG{timestamp}{str(uuid.uuid4())[:6]}"


def generate_etl_run_id() -> str:
    """
    Generate a unique ETL run ID.
    
    Returns:
        Unique ETL run identifier
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"ETL{timestamp}"