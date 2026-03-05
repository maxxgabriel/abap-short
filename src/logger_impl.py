"""
Concrete implementation of ETL Logger using Python logging framework and Spark.
"""

import logging
from datetime import datetime
from typing import Optional, List
from pyspark.sql import SparkSession

from src.logger_interface import (
    ETLLoggerInterface,
    LogStatus,
    ProcessStep,
    LogEntry
)


class SparkETLLogger(ETLLoggerInterface):
    """
    PySpark-based implementation of ETL logger.
    
    This implementation writes logs to both Python logging framework
    and optionally to a Spark DataFrame/Delta table for persistence.
    """
    
    def __init__(
        self,
        etl_run_id: str,
        spark: Optional[SparkSession] = None,
        log_table_path: Optional[str] = None
    ) -> None:
        """
        Initialize the Spark ETL logger.
        
        Args:
            etl_run_id: Unique identifier for the ETL run
            spark: Optional SparkSession for DataFrame logging
            log_table_path: Optional path to log table/Delta table
        """
        self._etl_run_id = etl_run_id
        self._spark = spark
        self._log_table_path = log_table_path
        self._log_entries: List[LogEntry] = []
        
        # Setup Python logger
        self._logger = logging.getLogger(f"ETL.{etl_run_id}")
        self._logger.setLevel(logging.INFO)
        
        # Add console handler if not already present
        if not self._logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(formatter)
            self._logger.addHandler(console_handler)
        
        # Log initialization
        self._logger.info(f"ETL Logger initialized with run ID: {etl_run_id}")
    
    def log_message(
        self,
        step: ProcessStep,
        status: LogStatus,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0,
        exception: Optional[Exception] = None
    ) -> None:
        """
        Log a message for an ETL process step.
        
        Args:
            step: The ETL process step
            status: The log status
            message: The log message text
            records_processed: Total number of records processed
            records_success: Number of successfully processed records
            records_error: Number of records with errors
            exception: Optional exception object for error logging
        """
        now = datetime.now()
        log_id = self._generate_log_id()
        
        # Create structured log entry
        log_entry = LogEntry(
            log_id=log_id,
            etl_run_id=self._etl_run_id,
            execution_date=now,
            execution_time=now,
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message
        )
        
        # Store log entry
        self._log_entries.append(log_entry)
        
        # Format message with metrics
        formatted_message = (
            f"[{step.value}] {message}"
        )
        if records_processed > 0:
            formatted_message += (
                f" | Processed: {records_processed}, "
                f"Success: {records_success}, "
                f"Errors: {records_error}"
            )
        
        # Log to Python logger with appropriate level
        if status == LogStatus.ERROR:
            self._logger.error(formatted_message, exc_info=exception)
        elif status == LogStatus.WARNING:
            self._logger.warning(formatted_message)
        elif status == LogStatus.INFO:
            self._logger.info(formatted_message)
        else:  # SUCCESS
            self._logger.info(formatted_message)
    
    def get_etl_run_id(self) -> str:
        """Get the current ETL run identifier."""
        return self._etl_run_id
    
    def get_log_entries(self) -> List[dict]:
        """
        Retrieve all log entries for the current ETL run.
        
        Returns:
            List of log entry dictionaries
        """
        return [entry.to_dict() for entry in self._log_entries]
    
    def flush(self) -> None:
        """
        Flush log entries to Spark DataFrame/Delta table if configured.
        """
        if not self._spark or not self._log_table_path or not self._log_entries:
            return
        
        try:
            # Convert log entries to DataFrame
            log_data = self.get_log_entries()
            log_df = self._spark.createDataFrame(log_data)
            
            # Write to Delta table (append mode)
            log_df.write.format("delta").mode("append").save(self._log_table_path)
            
            self._logger.info(
                f"Flushed {len(self._log_entries)} log entries to {self._log_table_path}"
            )
        except Exception as e:
            self._logger.error(f"Failed to flush logs to Delta table: {str(e)}")
    
    def _generate_log_id(self) -> str:
        """
        Generate a unique log ID.
        
        Returns:
            Unique log identifier
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f"LOG{timestamp}"
    
    def __del__(self):
        """Destructor to ensure logs are flushed."""
        try:
            self.flush()
        except Exception:
            pass  # Ignore errors during cleanup


class ConsoleETLLogger(ETLLoggerInterface):
    """
    Simple console-only implementation of ETL logger for testing.
    """
    
    def __init__(self, etl_run_id: str) -> None:
        """
        Initialize console logger.
        
        Args:
            etl_run_id: Unique identifier for the ETL run
        """
        self._etl_run_id = etl_run_id
        self._log_entries: List[LogEntry] = []
        self._logger = logging.getLogger(f"ETL.Console.{etl_run_id}")
        self._logger.setLevel(logging.INFO)
        
        if not self._logger.handlers:
            console_handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(formatter)
            self._logger.addHandler(console_handler)
    
    def log_message(
        self,
        step: ProcessStep,
        status: LogStatus,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0,
        exception: Optional[Exception] = None
    ) -> None:
        """Log message to console."""
        now = datetime.now()
        log_id = f"LOG{now.strftime('%Y%m%d%H%M%S')}"
        
        log_entry = LogEntry(
            log_id=log_id,
            etl_run_id=self._etl_run_id,
            execution_date=now,
            execution_time=now,
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message
        )
        
        self._log_entries.append(log_entry)
        
        formatted_message = f"[{step.value}][{status.value}] {message}"
        if records_processed > 0:
            formatted_message += (
                f" (Processed: {records_processed}, "
                f"Success: {records_success}, "
                f"Errors: {records_error})"
            )
        
        if status == LogStatus.ERROR:
            self._logger.error(formatted_message, exc_info=exception)
        elif status == LogStatus.WARNING:
            self._logger.warning(formatted_message)
        else:
            self._logger.info(formatted_message)
    
    def get_etl_run_id(self) -> str:
        """Get ETL run ID."""
        return self._etl_run_id
    
    def get_log_entries(self) -> List[dict]:
        """Get all log entries."""
        return [entry.to_dict() for entry in self._log_entries]
    
    def flush(self) -> None:
        """Console logger doesn't need flushing."""
        pass