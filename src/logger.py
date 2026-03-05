"""
PySpark ETL Logger Module

Migrated from ABAP ZCL_ETL_LOGGER class.
Provides logging utilities for ETL processes.
"""

import logging
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field
import uuid


@dataclass
class LogEntry:
    """
    Log entry structure.
    Migrated from ABAP ty_log_entry type.
    """
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


class ETLLogger:
    """
    Logger for ETL processes.
    Migrated from ABAP ZCL_ETL_LOGGER class.
    """
    
    # Status constants (migrated from ABAP)
    STATUS_SUCCESS = 'S'
    STATUS_ERROR = 'E'
    STATUS_WARNING = 'W'
    STATUS_INFO = 'I'
    
    # Step constants (migrated from ABAP)
    STEP_INIT = 'INIT'
    STEP_EXTRACT = 'EXTRACT'
    STEP_TRANSFORM = 'TRANSFORM'
    STEP_LOAD = 'LOAD'
    STEP_VALIDATE = 'VALIDATE'
    STEP_COMPLETE = 'COMPLETE'
    STEP_ERROR = 'ERROR'
    
    def __init__(self, etl_run_id: str = None):
        """
        Initialize logger with ETL run ID.
        
        Args:
            etl_run_id: Optional ETL run ID. Generated if not provided.
        """
        self.etl_run_id = etl_run_id or self._generate_etl_run_id()
        self.log_entries = []
        self.log = logging.getLogger(__name__)
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        self.log.info(f"ETL Logger initialized with run_id: {self.etl_run_id}")
    
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
        
        Migrated from ABAP log_message method.
        
        Args:
            step: ETL process step
            status: Status code (S/E/W/I)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        now = datetime.now()
        
        log_entry = LogEntry(
            log_id=self._generate_log_id(),
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
        
        # Log to standard logger
        log_level = self._get_log_level(status)
        self.log.log(
            log_level,
            f"[{step}] {message} "
            f"(processed={records_processed}, success={records_success}, "
            f"error={records_error})"
        )
    
    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID.
        
        Returns:
            ETL run ID string
        """
        return self.etl_run_id
    
    def get_log_entries(self) -> list:
        """
        Get all log entries for this run.
        
        Returns:
            List of LogEntry objects
        """
        return self.log_entries
    
    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run ID.
        
        Migrated from ABAP generate_etl_run_id method.
        
        Returns:
            Unique run ID
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"ETL{timestamp}"
    
    def _generate_log_id(self) -> str:
        """
        Generate unique log entry ID.
        
        Migrated from ABAP generate_log_id method.
        
        Returns:
            Unique log ID
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        return f"LOG{timestamp}{unique_id}"
    
    def _get_log_level(self, status: str) -> int:
        """
        Map status code to logging level.
        
        Args:
            status: Status code
            
        Returns:
            Logging level constant
        """
        mapping = {
            self.STATUS_SUCCESS: logging.INFO,
            self.STATUS_ERROR: logging.ERROR,
            self.STATUS_WARNING: logging.WARNING,
            self.STATUS_INFO: logging.INFO
        }
        return mapping.get(status, logging.INFO)