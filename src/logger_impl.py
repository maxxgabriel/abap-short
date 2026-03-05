"""
Concrete Implementation of ETL Logger

Implements the ETL Logger interface with Python logging framework integration,
database persistence, and console output.
"""

import logging
import uuid
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)

from src.logger import (
    ETLLoggerInterface, LogEntry, LogStatus, ProcessStep
)


class ETLLogger(ETLLoggerInterface):
    """
    Concrete implementation of ETL Logger
    
    Provides logging functionality with:
    - Python logging framework integration
    - Console output
    - DataFrame-based log storage
    - Database persistence support
    """
    
    LOG_SCHEMA = StructType([
        StructField("log_id", StringType(), False),
        StructField("etl_run_id", StringType(), False),
        StructField("execution_date", TimestampType(), False),
        StructField("process_step", StringType(), False),
        StructField("status", StringType(), False),
        StructField("records_processed", IntegerType(), False),
        StructField("records_success", IntegerType(), False),
        StructField("records_error", IntegerType(), False),
        StructField("message", StringType(), True)
    ])
    
    def __init__(
        self,
        etl_run_id: str,
        spark: Optional[SparkSession] = None,
        log_table: Optional[str] = None
    ):
        """
        Initialize logger with ETL run ID and optional persistence
        
        Args:
            etl_run_id: Unique identifier for ETL run
            spark: SparkSession for DataFrame operations
            log_table: Optional database table for log persistence
        """
        self._etl_run_id = etl_run_id
        self._spark = spark
        self._log_table = log_table
        self._log_entries: list[LogEntry] = []
        
        # Configure Python logger
        self._logger = logging.getLogger(f"ETL.{etl_run_id}")
        self._logger.setLevel(logging.INFO)
        
        # Add console handler if not already configured
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            handler.setLevel(logging.INFO)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)
    
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
        Log a message with ETL context
        
        Args:
            step: ETL process step
            status: Log status
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successfully processed records
            records_error: Number of records with errors
        """
        # Create log entry
        log_entry = LogEntry(
            log_id=self.generate_log_id(),
            etl_run_id=self._etl_run_id,
            execution_date=datetime.now(),
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message
        )
        
        # Store log entry
        self._log_entries.append(log_entry)
        
        # Log to Python logger
        log_msg = (
            f"[{step.value}] [{status.value}] {message} "
            f"(Processed: {records_processed}, Success: {records_success}, "
            f"Error: {records_error})"
        )
        
        if status == LogStatus.ERROR:
            self._logger.error(log_msg)
        elif status == LogStatus.WARNING:
            self._logger.warning(log_msg)
        elif status == LogStatus.INFO:
            self._logger.info(log_msg)
        else:
            self._logger.info(log_msg)
        
        # Persist to database if configured
        if self._spark and self._log_table:
            self._persist_log_entry(log_entry)
    
    def get_etl_run_id(self) -> str:
        """Get the current ETL run ID"""
        return self._etl_run_id
    
    def get_log_entries(self) -> list[LogEntry]:
        """Get all log entries for this ETL run"""
        return self._log_entries.copy()
    
    def generate_log_id(self) -> str:
        """Generate a unique log entry ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"LOG{timestamp}{unique_id}"
    
    def _persist_log_entry(self, log_entry: LogEntry) -> None:
        """
        Persist log entry to database table
        
        Args:
            log_entry: Log entry to persist
        """
        try:
            # Convert log entry to DataFrame row
            log_data = [(
                log_entry.log_id,
                log_entry.etl_run_id,
                log_entry.execution_date,
                log_entry.process_step.value,
                log_entry.status.value,
                log_entry.records_processed,
                log_entry.records_success,
                log_entry.records_error,
                log_entry.message
            )]
            
            # Create DataFrame
            df = self._spark.createDataFrame(log_data, schema=self.LOG_SCHEMA)
            
            # Write to table
            df.write.mode("append").saveAsTable(self._log_table)
            
        except Exception as e:
            self._logger.error(f"Failed to persist log entry: {str(e)}")
    
    def get_logs_as_dataframe(self):
        """
        Get all log entries as a Spark DataFrame
        
        Returns:
            DataFrame containing all log entries
        """
        if not self._spark:
            raise RuntimeError("SparkSession not configured for this logger")
        
        if not self._log_entries:
            return self._spark.createDataFrame([], schema=self.LOG_SCHEMA)
        
        log_data = [
            (
                entry.log_id,
                entry.etl_run_id,
                entry.execution_date,
                entry.process_step.value,
                entry.status.value,
                entry.records_processed,
                entry.records_success,
                entry.records_error,
                entry.message
            )
            for entry in self._log_entries
        ]
        
        return self._spark.createDataFrame(log_data, schema=self.LOG_SCHEMA)