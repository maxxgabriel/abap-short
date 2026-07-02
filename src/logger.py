"""
ETL Logging Framework
Provides structured logging with DataFrame-based log persistence for PySpark ETL processes.
"""

import logging
import sys
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, asdict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType, DateType
)


@dataclass
class LogEntry:
    """Structured log entry matching ABAP ZETL_LOG table structure."""
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
    created_at: Optional[str] = None
    created_by: Optional[str] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()
        if self.created_by is None:
            self.created_by = "etl_system"


class ETLLogger:
    """
    Production-grade ETL logger with structured logging and DataFrame persistence.
    
    Features:
    - Structured log entries compatible with ABAP log format
    - DataFrame-based log persistence
    - Console and file logging
    - Log level management
    - Batch log writing for performance
    """
    
    # Status codes matching ABAP constants
    STATUS_SUCCESS = 'S'
    STATUS_ERROR = 'E'
    STATUS_WARNING = 'W'
    STATUS_INFO = 'I'
    
    # Process steps matching ABAP constants
    STEP_INIT = 'INIT'
    STEP_EXTRACT = 'EXTRACT'
    STEP_TRANSFORM = 'TRANSFORM'
    STEP_LOAD = 'LOAD'
    STEP_VALIDATE = 'VALIDATE'
    STEP_COMPLETE = 'COMPLETE'
    STEP_ERROR = 'ERROR'
    
    LOG_SCHEMA = StructType([
        StructField("log_id", StringType(), False),
        StructField("etl_run_id", StringType(), False),
        StructField("execution_date", DateType(), False),
        StructField("execution_time", StringType(), False),
        StructField("process_step", StringType(), False),
        StructField("status", StringType(), False),
        StructField("records_processed", IntegerType(), True),
        StructField("records_success", IntegerType(), True),
        StructField("records_error", IntegerType(), True),
        StructField("message", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("created_by", StringType(), True)
    ])
    
    def __init__(
        self,
        etl_run_id: str,
        spark: SparkSession,
        log_table_path: Optional[str] = None,
        console_level: str = "INFO",
        enable_file_logging: bool = False,
        log_file_path: Optional[str] = None
    ):
        """
        Initialize ETL Logger.
        
        Args:
            etl_run_id: Unique identifier for this ETL run
            spark: Active SparkSession
            log_table_path: Path to persist log DataFrame (Delta/Parquet)
            console_level: Console logging level (DEBUG, INFO, WARNING, ERROR)
            enable_file_logging: Enable file-based logging
            log_file_path: Path to log file if file logging enabled
        """
        self.etl_run_id = etl_run_id
        self.spark = spark
        self.log_table_path = log_table_path
        self._log_buffer: List[LogEntry] = []
        self._log_counter = 0
        
        # Setup Python logging
        self._setup_python_logger(console_level, enable_file_logging, log_file_path)
        
        # Log initialization
        self.log_message(
            step=self.STEP_INIT,
            status=self.STATUS_SUCCESS,
            message=f"ETL Logger initialized with run ID: {etl_run_id}"
        )
    
    def _setup_python_logger(
        self,
        console_level: str,
        enable_file_logging: bool,
        log_file_path: Optional[str]
    ):
        """Setup Python standard logging."""
        self.logger = logging.getLogger(f"ETL_{self.etl_run_id}")
        self.logger.setLevel(logging.DEBUG)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, console_level.upper()))
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # File handler
        if enable_file_logging and log_file_path:
            file_handler = logging.FileHandler(log_file_path)
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - [%(process)d] - %(message)s'
            )
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)
    
    def _generate_log_id(self) -> str:
        """Generate unique log ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        self._log_counter += 1
        return f"LOG{timestamp}{self._log_counter:06d}"
    
    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0,
        extra_data: Optional[Dict[str, Any]] = None
    ):
        """
        Log a structured message.
        
        Args:
            step: ETL process step
            status: Log status (S/E/W/I)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
            extra_data: Additional data to include in log
        """
        now = datetime.now()
        
        # Create structured log entry
        log_entry = LogEntry(
            log_id=self._generate_log_id(),
            etl_run_id=self.etl_run_id,
            execution_date=now.strftime("%Y-%m-%d"),
            execution_time=now.strftime("%H:%M:%S"),
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message
        )
        
        # Add to buffer
        self._log_buffer.append(log_entry)
        
        # Python logging
        log_msg = self._format_log_message(log_entry, extra_data)
        log_level = self._map_status_to_level(status)
        self.logger.log(log_level, log_msg)
    
    def _format_log_message(
        self,
        log_entry: LogEntry,
        extra_data: Optional[Dict[str, Any]]
    ) -> str:
        """Format log message for console output."""
        msg = f"[{log_entry.process_step}] {log_entry.message}"
        
        if log_entry.records_processed > 0:
            msg += f" | Processed: {log_entry.records_processed}"
            msg += f" | Success: {log_entry.records_success}"
            msg += f" | Errors: {log_entry.records_error}"
        
        if extra_data:
            msg += f" | Extra: {extra_data}"
        
        return msg
    
    def _map_status_to_level(self, status: str) -> int:
        """Map ABAP status codes to Python logging levels."""
        mapping = {
            self.STATUS_SUCCESS: logging.INFO,
            self.STATUS_INFO: logging.INFO,
            self.STATUS_WARNING: logging.WARNING,
            self.STATUS_ERROR: logging.ERROR
        }
        return mapping.get(status, logging.INFO)
    
    def flush_logs(self):
        """Flush buffered logs to DataFrame storage."""
        if not self._log_buffer:
            self.logger.debug("No logs to flush")
            return
        
        if not self.log_table_path:
            self.logger.warning("No log table path configured, skipping flush")
            return
        
        try:
            # Convert log entries to dictionaries
            log_dicts = [asdict(entry) for entry in self._log_buffer]
            
            # Create DataFrame
            log_df = self.spark.createDataFrame(log_dicts, schema=self.LOG_SCHEMA)
            
            # Write to storage
            log_df.write \
                .mode("append") \
                .format("delta") \
                .save(self.log_table_path)
            
            self.logger.info(
                f"Flushed {len(self._log_buffer)} log entries to {self.log_table_path}"
            )
            
            # Clear buffer
            self._log_buffer.clear()
            
        except Exception as e:
            self.logger.error(f"Failed to flush logs: {str(e)}", exc_info=True)
    
    def get_logs_dataframe(self) -> Optional[DataFrame]:
        """
        Retrieve logs as DataFrame from storage.
        
        Returns:
            DataFrame containing all logs for this ETL run, or None if no logs exist
        """
        if not self.log_table_path:
            self.logger.warning("No log table path configured")
            return None
        
        try:
            log_df = self.spark.read \
                .format("delta") \
                .load(self.log_table_path) \
                .filter(f"etl_run_id = '{self.etl_run_id}'")
            
            return log_df
            
        except Exception as e:
            self.logger.error(f"Failed to read logs: {str(e)}")
            return None
    
    def get_etl_run_id(self) -> str:
        """Get the ETL run ID."""
        return self.etl_run_id
    
    def log_statistics(
        self,
        step: str,
        total: int,
        success: int,
        errors: int,
        warnings: int = 0
    ):
        """
        Log statistics for an ETL step.
        
        Args:
            step: ETL process step
            total: Total records
            success: Successful records
            errors: Error records
            warnings: Warning records
        """
        status = self.STATUS_SUCCESS if errors == 0 else self.STATUS_WARNING
        
        message = (
            f"Statistics - Total: {total}, Success: {success}, "
            f"Errors: {errors}, Warnings: {warnings}"
        )
        
        self.log_message(
            step=step,
            status=status,
            message=message,
            records_processed=total,
            records_success=success,
            records_error=errors
        )
    
    def log_error(self, step: str, error: Exception, context: Optional[str] = None):
        """
        Log an error with exception details.
        
        Args:
            step: ETL process step where error occurred
            error: Exception object
            context: Additional context about the error
        """
        message = f"Error: {str(error)}"
        if context:
            message = f"{context} - {message}"
        
        self.log_message(
            step=step,
            status=self.STATUS_ERROR,
            message=message,
            extra_data={"exception_type": type(error).__name__}
        )
        
        # Log full traceback to Python logger
        self.logger.error(message, exc_info=True)
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - flush logs."""
        if exc_type is not None:
            self.log_error(
                step=self.STEP_ERROR,
                error=exc_val,
                context="ETL process failed"
            )
        
        self.flush_logs()
        return False


def generate_etl_run_id(prefix: str = "ETL") -> str:
    """
    Generate unique ETL run ID.
    
    Args:
        prefix: Prefix for the run ID
        
    Returns:
        Unique ETL run ID
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"{prefix}{timestamp}"