"""
ETL Logger Module
Implements logging functionality with DataFrame persistence for ETL processes.
"""
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType, DateType
)


@dataclass
class LogEntry:
    """
    Dataclass representing a single ETL log entry.
    Mirrors the ABAP ty_log_entry structure.
    """
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
    created_at: datetime = field(default_factory=datetime.now)
    created_by: str = field(default_factory=lambda: "system")

    def to_dict(self):
        """Convert log entry to dictionary for DataFrame creation."""
        return {
            "log_id": self.log_id,
            "etl_run_id": self.etl_run_id,
            "execution_date": self.execution_date.date(),
            "execution_time": self.execution_time,
            "process_step": self.process_step,
            "status": self.status,
            "records_processed": self.records_processed,
            "records_success": self.records_success,
            "records_error": self.records_error,
            "message": self.message,
            "created_at": self.created_at,
            "created_by": self.created_by
        }


class ETLLogger:
    """
    ETL Logger class with DataFrame persistence.
    Converts ZCL_ETL_LOGGER functionality to Python.
    """

    # Log status constants
    STATUS_SUCCESS = "S"
    STATUS_ERROR = "E"
    STATUS_WARNING = "W"
    STATUS_INFO = "I"

    # Process step constants
    STEP_INIT = "INIT"
    STEP_EXTRACT = "EXTRACT"
    STEP_TRANSFORM = "TRANSFORM"
    STEP_LOAD = "LOAD"
    STEP_VALIDATE = "VALIDATE"
    STEP_COMPLETE = "COMPLETE"
    STEP_ERROR = "ERROR"

    LOG_SCHEMA = StructType([
        StructField("log_id", StringType(), False),
        StructField("etl_run_id", StringType(), False),
        StructField("execution_date", DateType(), False),
        StructField("execution_time", TimestampType(), False),
        StructField("process_step", StringType(), False),
        StructField("status", StringType(), False),
        StructField("records_processed", IntegerType(), True),
        StructField("records_success", IntegerType(), True),
        StructField("records_error", IntegerType(), True),
        StructField("message", StringType(), True),
        StructField("created_at", TimestampType(), False),
        StructField("created_by", StringType(), False)
    ])

    def __init__(
        self,
        spark: SparkSession,
        etl_run_id: str,
        log_table_path: Optional[str] = None,
        enable_console: bool = True
    ):
        """
        Initialize ETL Logger.

        Args:
            spark: SparkSession instance
            etl_run_id: Unique identifier for the ETL run
            log_table_path: Path to persist log DataFrame (optional)
            enable_console: Enable console logging (default: True)
        """
        self.spark = spark
        self.etl_run_id = etl_run_id
        self.log_table_path = log_table_path
        self.enable_console = enable_console
        
        # In-memory log entries
        self._log_entries: List[LogEntry] = []
        
        # Setup Python logging
        self._setup_python_logger()
        
        # Log counter for unique IDs
        self._log_counter = 0

    def _setup_python_logger(self):
        """Setup standard Python logger for console output."""
        self.logger = logging.getLogger(f"ETLLogger_{self.etl_run_id}")
        self.logger.setLevel(logging.INFO)
        
        if self.enable_console and not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - [%(levelname)s] - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def _generate_log_id(self) -> str:
        """
        Generate unique log ID.
        
        Returns:
            Unique log identifier
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        self._log_counter += 1
        return f"LOG{timestamp}{self._log_counter:04d}"

    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ) -> LogEntry:
        """
        Log a message with statistics.

        Args:
            step: ETL process step
            status: Log status (S/E/W/I)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records

        Returns:
            LogEntry object
        """
        now = datetime.now()
        
        log_entry = LogEntry(
            log_id=self._generate_log_id(),
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
        
        # Store log entry
        self._log_entries.append(log_entry)
        
        # Console logging
        if self.enable_console:
            log_level = self._map_status_to_level(status)
            log_msg = (
                f"[{step}] {message}"
                f" (Processed: {records_processed}, "
                f"Success: {records_success}, "
                f"Errors: {records_error})"
            )
            self.logger.log(log_level, log_msg)
        
        return log_entry

    def _map_status_to_level(self, status: str) -> int:
        """Map ETL status to Python logging level."""
        mapping = {
            self.STATUS_SUCCESS: logging.INFO,
            self.STATUS_ERROR: logging.ERROR,
            self.STATUS_WARNING: logging.WARNING,
            self.STATUS_INFO: logging.INFO
        }
        return mapping.get(status, logging.INFO)

    def get_log_dataframe(self) -> DataFrame:
        """
        Convert log entries to Spark DataFrame.

        Returns:
            DataFrame containing all log entries
        """
        if not self._log_entries:
            # Return empty DataFrame with schema
            return self.spark.createDataFrame([], schema=self.LOG_SCHEMA)
        
        # Convert log entries to list of dicts
        log_data = [entry.to_dict() for entry in self._log_entries]
        
        # Create DataFrame
        df = self.spark.createDataFrame(log_data, schema=self.LOG_SCHEMA)
        
        return df

    def persist_logs(
        self,
        mode: str = "append",
        format: str = "parquet",
        partition_by: Optional[List[str]] = None
    ):
        """
        Persist log entries to storage as DataFrame.

        Args:
            mode: Write mode (append/overwrite)
            format: Output format (parquet/delta/csv)
            partition_by: List of columns to partition by
        """
        if not self.log_table_path:
            self.logger.warning("No log table path configured. Skipping persistence.")
            return
        
        if not self._log_entries:
            self.logger.info("No log entries to persist.")
            return
        
        df = self.get_log_dataframe()
        
        writer = df.write.mode(mode).format(format)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        writer.save(self.log_table_path)
        
        self.logger.info(
            f"Persisted {len(self._log_entries)} log entries to {self.log_table_path}"
        )

    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run ID.

        Returns:
            ETL run identifier
        """
        return self.etl_run_id

    def get_log_summary(self) -> dict:
        """
        Get summary statistics of logged entries.

        Returns:
            Dictionary with summary statistics
        """
        if not self._log_entries:
            return {
                "total_logs": 0,
                "success_count": 0,
                "error_count": 0,
                "warning_count": 0,
                "info_count": 0,
                "total_records_processed": 0,
                "total_records_success": 0,
                "total_records_error": 0
            }
        
        summary = {
            "total_logs": len(self._log_entries),
            "success_count": sum(1 for e in self._log_entries if e.status == self.STATUS_SUCCESS),
            "error_count": sum(1 for e in self._log_entries if e.status == self.STATUS_ERROR),
            "warning_count": sum(1 for e in self._log_entries if e.status == self.STATUS_WARNING),
            "info_count": sum(1 for e in self._log_entries if e.status == self.STATUS_INFO),
            "total_records_processed": sum(e.records_processed for e in self._log_entries),
            "total_records_success": sum(e.records_success for e in self._log_entries),
            "total_records_error": sum(e.records_error for e in self._log_entries)
        }
        
        return summary

    def clear_logs(self):
        """Clear in-memory log entries."""
        self._log_entries.clear()
        self._log_counter = 0

    def read_persisted_logs(
        self,
        filter_expression: Optional[str] = None
    ) -> DataFrame:
        """
        Read persisted logs from storage.

        Args:
            filter_expression: Optional SQL filter expression

        Returns:
            DataFrame with persisted logs
        """
        if not self.log_table_path:
            raise ValueError("No log table path configured")
        
        df = self.spark.read.format("parquet").load(self.log_table_path)
        
        if filter_expression:
            df = df.filter(filter_expression)
        
        return df

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with automatic persistence."""
        if exc_type is not None:
            self.log_message(
                step=self.STEP_ERROR,
                status=self.STATUS_ERROR,
                message=f"ETL process failed with error: {exc_val}"
            )
        
        # Auto-persist logs on exit
        if self.log_table_path:
            try:
                self.persist_logs()
            except Exception as e:
                self.logger.error(f"Failed to persist logs: {e}")


def generate_etl_run_id(prefix: str = "ETL") -> str:
    """
    Generate unique ETL run ID.

    Args:
        prefix: Prefix for the run ID (default: "ETL")

    Returns:
        Unique ETL run identifier
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"{prefix}{timestamp}"