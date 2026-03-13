"""
ETL Logger Module
Provides logging functionality for ETL processes
"""
import logging
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field

from pyspark.sql import SparkSession

from src.constants import constants


@dataclass
class LogEntry:
    """Represents a single log entry"""
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
    
    def to_dict(self) -> dict:
        """Convert log entry to dictionary"""
        return {
            'log_id': self.log_id,
            'etl_run_id': self.etl_run_id,
            'execution_date': self.execution_date,
            'execution_time': self.execution_time,
            'process_step': self.process_step,
            'status': self.status,
            'records_processed': self.records_processed,
            'records_success': self.records_success,
            'records_error': self.records_error,
            'message': self.message
        }


class ETLLogger:
    """Logger for ETL processes"""
    
    def __init__(self, etl_run_id: str, spark: Optional[SparkSession] = None):
        """
        Initialize the logger
        
        Args:
            etl_run_id: Unique identifier for this ETL run
            spark: Optional SparkSession for database logging
        """
        self.etl_run_id = etl_run_id
        self.spark = spark
        self.log_entries: list[LogEntry] = []
        
        # Configure Python logger
        self.logger = logging.getLogger(f'ETL.{etl_run_id}')
        self.logger.setLevel(logging.INFO)
        
        # Create console handler if not already present
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def _generate_log_id(self) -> str:
        """Generate unique log ID"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f"{constants.PREFIX.LOG_ID}{timestamp}"
    
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
        Log a message
        
        Args:
            step: Process step identifier
            status: Status code (S/E/W/I)
            message: Log message text
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        now = datetime.now()
        
        log_entry = LogEntry(
            log_id=self._generate_log_id(),
            etl_run_id=self.etl_run_id,
            execution_date=now.strftime('%Y%m%d'),
            execution_time=now.strftime('%H%M%S'),
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message
        )
        
        self.log_entries.append(log_entry)
        
        # Log to Python logger
        log_level = {
            constants.STATUS.SUCCESS: logging.INFO,
            constants.STATUS.ERROR: logging.ERROR,
            constants.STATUS.WARNING: logging.WARNING,
            constants.STATUS.INFO: logging.INFO
        }.get(status, logging.INFO)
        
        log_msg = (
            f"[{step}] {message} "
            f"(Processed: {records_processed}, "
            f"Success: {records_success}, "
            f"Error: {records_error})"
        )
        self.logger.log(log_level, log_msg)
    
    def get_etl_run_id(self) -> str:
        """Return the ETL run ID"""
        return self.etl_run_id
    
    def get_log_entries(self) -> list[LogEntry]:
        """Return all log entries for this run"""
        return self.log_entries
    
    def persist_logs(self, table_name: str = "etl_log") -> bool:
        """
        Persist logs to database table
        
        Args:
            table_name: Target table name
            
        Returns:
            True if successful, False otherwise
        """
        if not self.spark or not self.log_entries:
            return False
        
        try:
            log_data = [entry.to_dict() for entry in self.log_entries]
            df = self.spark.createDataFrame(log_data)
            df.write.mode("append").saveAsTable(table_name)
            return True
        except Exception as e:
            self.logger.error(f"Failed to persist logs: {str(e)}")
            return False
    
    def get_summary(self) -> dict:
        """Return summary statistics for this ETL run"""
        total_processed = sum(e.records_processed for e in self.log_entries)
        total_success = sum(e.records_success for e in self.log_entries)
        total_error = sum(e.records_error for e in self.log_entries)
        
        error_count = sum(1 for e in self.log_entries if e.status == constants.STATUS.ERROR)
        warning_count = sum(1 for e in self.log_entries if e.status == constants.STATUS.WARNING)
        
        return {
            'etl_run_id': self.etl_run_id,
            'total_log_entries': len(self.log_entries),
            'total_records_processed': total_processed,
            'total_records_success': total_success,
            'total_records_error': total_error,
            'error_log_count': error_count,
            'warning_log_count': warning_count
        }