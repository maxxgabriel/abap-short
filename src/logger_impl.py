"""
ETL Logger Implementation
Concrete implementation of IETLLogger integrated with Python logging framework.
"""

import logging
from datetime import datetime
from typing import List, Optional
from pyspark.sql import SparkSession

from src.logger_interface import IETLLogger, LogStatus, ProcessStep, LogEntry


class ETLLogger(IETLLogger):
    """
    Production ETL logger implementation.
    Integrates with Python logging and optionally persists to database/Delta Lake.
    """

    def __init__(
        self,
        etl_run_id: str,
        log_level: str = "INFO",
        spark: Optional[SparkSession] = None,
        persist_to_storage: bool = True
    ):
        """
        Initialize ETL logger.
        
        Args:
            etl_run_id: Unique identifier for the ETL run
            log_level: Python logging level (DEBUG, INFO, WARNING, ERROR)
            spark: Optional SparkSession for database persistence
            persist_to_storage: Whether to persist logs to storage
        """
        self._etl_run_id = etl_run_id
        self._log_entries: List[LogEntry] = []
        self._spark = spark
        self._persist_to_storage = persist_to_storage
        
        # Configure Python logger
        self._logger = logging.getLogger(f"ETL.{etl_run_id}")
        self._logger.setLevel(getattr(logging, log_level.upper()))
        
        # Add console handler if not already present
        if not self._logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(getattr(logging, log_level.upper()))
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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
        records_error: int = 0
    ) -> None:
        """
        Log a message for an ETL process step.
        
        Args:
            step: The ETL process step
            status: Log status level
            message: Log message content
            records_processed: Total number of records processed
            records_success: Number of successfully processed records
            records_error: Number of records with errors
        """
        # Generate unique log ID
        log_id = self._generate_log_id()
        execution_datetime = datetime.now()
        
        # Create log entry
        entry = LogEntry(
            log_id=log_id,
            etl_run_id=self._etl_run_id,
            execution_datetime=execution_datetime,
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message
        )
        
        # Store log entry
        self._log_entries.append(entry)
        
        # Log to Python logger
        log_msg = self._format_log_message(entry)
        self._log_to_python_logger(status, log_msg)
        
        # Persist to storage if enabled
        if self._persist_to_storage and self._spark:
            self._persist_log_entry(entry)

    def get_etl_run_id(self) -> str:
        """Get the ETL run identifier"""
        return self._etl_run_id

    def get_log_entries(self) -> List[dict]:
        """
        Retrieve all log entries for this ETL run.
        
        Returns:
            List of log entry dictionaries
        """
        return [entry.to_dict() for entry in self._log_entries]

    def _generate_log_id(self) -> str:
        """Generate unique log ID with timestamp"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f"LOG{timestamp}"

    def _format_log_message(self, entry: LogEntry) -> str:
        """Format log entry as human-readable message"""
        msg_parts = [
            f"[{entry.process_step.value}]",
            f"Status: {entry.status.value}",
        ]
        
        if entry.records_processed > 0:
            msg_parts.append(
                f"Records: {entry.records_success}/{entry.records_processed}"
            )
            if entry.records_error > 0:
                msg_parts.append(f"Errors: {entry.records_error}")
        
        msg_parts.append(f"- {entry.message}")
        
        return " ".join(msg_parts)

    def _log_to_python_logger(self, status: LogStatus, message: str) -> None:
        """Log message using Python logging framework"""
        if status == LogStatus.ERROR:
            self._logger.error(message)
        elif status == LogStatus.WARNING:
            self._logger.warning(message)
        elif status == LogStatus.INFO:
            self._logger.info(message)
        else:  # SUCCESS
            self._logger.info(message)

    def _persist_log_entry(self, entry: LogEntry) -> None:
        """
        Persist log entry to database/Delta Lake.
        
        Args:
            entry: Log entry to persist
        """
        try:
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
            
            # Define schema
            schema = StructType([
                StructField("log_id", StringType(), False),
                StructField("etl_run_id", StringType(), False),
                StructField("execution_datetime", TimestampType(), False),
                StructField("process_step", StringType(), False),
                StructField("status", StringType(), False),
                StructField("records_processed", IntegerType(), False),
                StructField("records_success", IntegerType(), False),
                StructField("records_error", IntegerType(), False),
                StructField("message", StringType(), True)
            ])
            
            # Create DataFrame
            log_data = [(
                entry.log_id,
                entry.etl_run_id,
                entry.execution_datetime,
                entry.process_step.value,
                entry.status.value,
                entry.records_processed,
                entry.records_success,
                entry.records_error,
                entry.message
            )]
            
            df = self._spark.createDataFrame(log_data, schema)
            
            # Write to Delta Lake (append mode)
            df.write.format("delta").mode("append").save("/mnt/etl/logs")
            
        except Exception as e:
            self._logger.error(f"Failed to persist log entry: {str(e)}")


class ConsoleLogger(IETLLogger):
    """
    Simple console-only logger for testing/development.
    Does not persist to storage.
    """

    def __init__(self, etl_run_id: str):
        """Initialize console logger"""
        self._etl_run_id = etl_run_id
        self._log_entries: List[LogEntry] = []

    def log_message(
        self,
        step: ProcessStep,
        status: LogStatus,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ) -> None:
        """Log message to console"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_id = f"LOG{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
        
        entry = LogEntry(
            log_id=log_id,
            etl_run_id=self._etl_run_id,
            execution_datetime=datetime.now(),
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message
        )
        
        self._log_entries.append(entry)
        
        # Print to console
        status_symbol = {
            LogStatus.SUCCESS: '✓',
            LogStatus.ERROR: '✗',
            LogStatus.WARNING: '⚠',
            LogStatus.INFO: 'ℹ'
        }.get(status, '•')
        
        print(f"{timestamp} {status_symbol} [{step.value}] {message}")
        
        if records_processed > 0:
            print(f"  Records: {records_success}/{records_processed} "
                  f"(Errors: {records_error})")

    def get_etl_run_id(self) -> str:
        """Get the ETL run identifier"""
        return self._etl_run_id

    def get_log_entries(self) -> List[dict]:
        """Retrieve all log entries"""
        return [entry.to_dict() for entry in self._log_entries]