"""
PySpark ETL Logger Module
Provides logging functionality with dependency injection support.
"""

from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field
from pyspark.sql import SparkSession


@dataclass
class LogEntry:
    """Data class representing a single log entry."""
    log_id: str
    etl_run_id: str
    execution_date: datetime
    execution_time: datetime
    process_step: str
    status: str
    records_processed: int = 0
    records_success: int = 0
    records_error: int = 0
    message: str = ""


class ETLLogger:
    """
    Logger for ETL operations with structured logging support.
    Supports both console and structured data logging.
    """
    
    def __init__(self, etl_run_id: str, spark: Optional[SparkSession] = None):
        """
        Initialize logger with ETL run ID.
        
        Args:
            etl_run_id: Unique identifier for the ETL run
            spark: Optional SparkSession for DataFrame-based logging
        """
        self.etl_run_id = etl_run_id
        self.spark = spark
        self.log_entries = []
        self._log_counter = 0
    
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
            step: ETL process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message text
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        now = datetime.now()
        log_id = self._generate_log_id()
        
        log_entry = LogEntry(
            log_id=log_id,
            etl_run_id=self.etl_run_id,
            execution_date=now,
            execution_time=now,
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message
        )
        
        self.log_entries.append(log_entry)
        
        # Console output
        status_symbol = self._get_status_symbol(status)
        timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] [{status_symbol}] {step}: {message}")
        
        if records_processed > 0:
            print(f"  Records - Processed: {records_processed}, "
                  f"Success: {records_success}, Error: {records_error}")
    
    def _generate_log_id(self) -> str:
        """
        Generate unique log ID.
        
        Returns:
            Unique log identifier
        """
        self._log_counter += 1
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"LOG{timestamp}{self._log_counter:04d}"
    
    def _get_status_symbol(self, status: str) -> str:
        """
        Get display symbol for status code.
        
        Args:
            status: Status code
            
        Returns:
            Display symbol
        """
        symbols = {
            "S": "✓",
            "E": "✗",
            "W": "⚠",
            "I": "ℹ"
        }
        return symbols.get(status, "•")
    
    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID.
        
        Returns:
            ETL run identifier
        """
        return self.etl_run_id
    
    def get_log_entries(self) -> list:
        """
        Get all log entries.
        
        Returns:
            List of LogEntry objects
        """
        return self.log_entries.copy()
    
    def export_logs_to_dataframe(self) -> Optional['DataFrame']:
        """
        Export logs to Spark DataFrame.
        
        Returns:
            DataFrame containing log entries, or None if Spark not available
        """
        if not self.spark or not self.log_entries:
            return None
        
        log_dicts = [
            {
                "log_id": entry.log_id,
                "etl_run_id": entry.etl_run_id,
                "execution_date": entry.execution_date.date(),
                "execution_time": entry.execution_time.time(),
                "process_step": entry.process_step,
                "status": entry.status,
                "records_processed": entry.records_processed,
                "records_success": entry.records_success,
                "records_error": entry.records_error,
                "message": entry.message
            }
            for entry in self.log_entries
        ]
        
        return self.spark.createDataFrame(log_dicts)