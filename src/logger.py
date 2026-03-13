"""
ETL Logger implementation.

Provides logging functionality for ETL processes with support for
both console and structured logging to Delta tables.
"""

import logging
from datetime import datetime
from typing import List, Optional
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from src.protocols import (
    ETLLoggerProtocol,
    LogEntry,
    StatusCode,
    ProcessStep
)


class ETLLogger(ETLLoggerProtocol):
    """
    Logger implementation for ETL processes.
    
    Logs messages to console and optionally to a Delta table.
    """

    def __init__(
        self,
        etl_run_id: str,
        spark: Optional[SparkSession] = None,
        log_path: Optional[str] = None,
        console_enabled: bool = True
    ):
        """
        Initialize the ETL logger.

        Args:
            etl_run_id: Unique identifier for this ETL run
            spark: SparkSession for logging to Delta tables
            log_path: Path to Delta table for structured logs
            console_enabled: Whether to enable console logging
        """
        self._etl_run_id = etl_run_id
        self._spark = spark
        self._log_path = log_path
        self._console_enabled = console_enabled
        self._log_entries: List[LogEntry] = []
        
        # Set up Python logging
        self._setup_logging()

    def _setup_logging(self) -> None:
        """Configure Python logging."""
        self._logger = logging.getLogger(f"ETL.{self._etl_run_id}")
        self._logger.setLevel(logging.DEBUG)
        
        if self._console_enabled and not self._logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)

    @property
    def etl_run_id(self) -> str:
        """Get the ETL run identifier."""
        return self._etl_run_id

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
        Log a message for the ETL process.

        Args:
            step: Process step identifier
            status: Status code (S/E/W/I)
            message: Log message text
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        now = datetime.now()
        
        # Generate unique log ID
        log_id = f"LOG{now.strftime('%Y%m%d%H%M%S%f')}"
        
        # Create log entry
        entry = LogEntry(
            log_id=log_id,
            etl_run_id=self._etl_run_id,
            execution_date=now,
            execution_time=now.strftime("%H:%M:%S"),
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message,
            created_at=now
        )
        
        self._log_entries.append(entry)
        
        # Log to console
        if self._console_enabled:
            log_level = self._get_log_level(status)
            self._logger.log(
                log_level,
                f"[{step}] {message} "
                f"(Processed: {records_processed}, "
                f"Success: {records_success}, "
                f"Error: {records_error})"
            )
        
        # Log to Delta table if configured
        if self._spark and self._log_path:
            self._write_to_delta(entry)

    def _get_log_level(self, status: str) -> int:
        """Map status code to Python logging level."""
        mapping = {
            StatusCode.SUCCESS.value: logging.INFO,
            StatusCode.INFO.value: logging.INFO,
            StatusCode.WARNING.value: logging.WARNING,
            StatusCode.ERROR.value: logging.ERROR
        }
        return mapping.get(status, logging.INFO)

    def _write_to_delta(self, entry: LogEntry) -> None:
        """Write log entry to Delta table."""
        try:
            schema = StructType([
                StructField("log_id", StringType(), False),
                StructField("etl_run_id", StringType(), False),
                StructField("execution_date", TimestampType(), False),
                StructField("execution_time", StringType(), False),
                StructField("process_step", StringType(), False),
                StructField("status", StringType(), False),
                StructField("records_processed", IntegerType(), False),
                StructField("records_success", IntegerType(), False),
                StructField("records_error", IntegerType(), False),
                StructField("message", StringType(), False),
                StructField("created_at", TimestampType(), False)
            ])
            
            row = Row(
                log_id=entry.log_id,
                etl_run_id=entry.etl_run_id,
                execution_date=entry.execution_date,
                execution_time=entry.execution_time,
                process_step=entry.process_step,
                status=entry.status,
                records_processed=entry.records_processed,
                records_success=entry.records_success,
                records_error=entry.records_error,
                message=entry.message,
                created_at=entry.created_at
            )
            
            df = self._spark.createDataFrame([row], schema)
            df.write.format("delta").mode("append").save(self._log_path)
            
        except Exception as e:
            self._logger.error(f"Failed to write log to Delta: {str(e)}")

    def get_logs(self) -> List[LogEntry]:
        """Retrieve all log entries for this ETL run."""
        return self._log_entries.copy()

    def get_summary(self) -> dict:
        """Get summary statistics from logs."""
        total_processed = sum(e.records_processed for e in self._log_entries)
        total_success = sum(e.records_success for e in self._log_entries)
        total_error = sum(e.records_error for e in self._log_entries)
        
        error_count = sum(
            1 for e in self._log_entries 
            if e.status == StatusCode.ERROR.value
        )
        warning_count = sum(
            1 for e in self._log_entries 
            if e.status == StatusCode.WARNING.value
        )
        
        return {
            "etl_run_id": self._etl_run_id,
            "total_processed": total_processed,
            "total_success": total_success,
            "total_error": total_error,
            "error_count": error_count,
            "warning_count": warning_count,
            "log_entries": len(self._log_entries)
        }