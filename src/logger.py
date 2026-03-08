"""
ETL Logger Module
Provides logging functionality for ETL processes.
"""

from datetime import datetime
from typing import Optional
import logging


class ETLLogger:
    """
    Logger class for ETL operations.
    Logs to console and optionally to database/file.
    """
    
    def __init__(self, etl_run_id: str, log_level: str = 'INFO'):
        """
        Initialize logger with ETL run ID.
        
        Args:
            etl_run_id: Unique identifier for ETL run
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.etl_run_id = etl_run_id
        self.log_entries = []
        
        # Configure Python logging
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.python_logger = logging.getLogger(f'ETL_{etl_run_id}')
        
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
        Log a message for an ETL step.
        
        Args:
            step: ETL process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        log_entry = {
            'log_id': self._generate_log_id(),
            'etl_run_id': self.etl_run_id,
            'execution_date': datetime.now().date(),
            'execution_time': datetime.now().time(),
            'process_step': step,
            'status': status,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error,
            'message': message,
            'timestamp': datetime.now()
        }
        
        self.log_entries.append(log_entry)
        
        # Log to Python logger
        log_level_map = {
            'S': logging.INFO,
            'I': logging.INFO,
            'W': logging.WARNING,
            'E': logging.ERROR
        }
        
        level = log_level_map.get(status, logging.INFO)
        log_msg = f"[{step}] {message}"
        if records_processed > 0:
            log_msg += f" (Processed: {records_processed}, Success: {records_success}, Error: {records_error})"
        
        self.python_logger.log(level, log_msg)
        
    def _generate_log_id(self) -> str:
        """
        Generate unique log ID.
        
        Returns:
            str: Unique log identifier
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f"LOG{timestamp}"
    
    def get_etl_run_id(self) -> str:
        """
        Get ETL run ID.
        
        Returns:
            str: ETL run identifier
        """
        return self.etl_run_id
    
    def get_log_entries(self) -> list:
        """
        Get all log entries for this ETL run.
        
        Returns:
            list: List of log entry dictionaries
        """
        return self.log_entries
    
    def display_summary(self):
        """
        Display summary of ETL execution.
        """
        print("\n" + "=" * 60)
        print("ETL Process Summary")
        print("=" * 60)
        print(f"ETL Run ID: {self.etl_run_id}")
        
        if self.log_entries:
            first_entry = self.log_entries[0]
            last_entry = self.log_entries[-1]
            print(f"Start Time: {first_entry['timestamp']}")
            print(f"End Time: {last_entry['timestamp']}")
            
            duration = (last_entry['timestamp'] - first_entry['timestamp']).total_seconds()
            print(f"Duration: {duration:.2f} seconds")
            
            # Count by status
            status_counts = {}
            for entry in self.log_entries:
                status = entry['status']
                status_counts[status] = status_counts.get(status, 0) + 1
            
            print(f"\nLog Statistics:")
            print(f"  Success: {status_counts.get('S', 0)}")
            print(f"  Info: {status_counts.get('I', 0)}")
            print(f"  Warning: {status_counts.get('W', 0)}")
            print(f"  Error: {status_counts.get('E', 0)}")
        
        print("=" * 60)