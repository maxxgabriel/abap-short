"""
Concrete ETL Logger Implementation
Converted from ABAP class ZCL_ETL_LOGGER
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass, field, asdict

from pyspark.sql import SparkSession

from src.logger_interface import ETLLoggerInterface, LogStatus, ProcessStep


@dataclass
class LogEntry:
    """
    Data class representing a single log entry.
    Corresponds to ABAP structure ty_log_entry
    """
    log_id: str
    etl_run_id: str
    execution_date: str  # Format: YYYY-MM-DD
    execution_time: str  # Format: HH:MM:SS
    process_step: str
    status: str
    records_processed: int = 0
    records_success: int = 0
    records_error: int = 0
    message: str = ""
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())


class ETLLogger(ETLLoggerInterface):
    """
    Production ETL logger implementation with PySpark integration.
    Logs to both standard logging and optional database table.
    
    Converted from: ZCL_ETL_LOGGER
    """

    def __init__(
        self,
        etl_run_id: str,
        spark: Optional[SparkSession] = None,
        log_table: Optional[str] = None,
        console_level: int = logging.INFO
    ):
        """
        Initialize the ETL logger.
        
        Args:
            etl_run_id: Unique identifier for this ETL run
            spark: Optional SparkSession for database logging
            log_table: Optional table name for persisting logs
            console_level: Logging level for console output
        """
        self._etl_run_id = etl_run_id
        self._spark = spark
        self._log_table = log_table
        self._log_entries: List[LogEntry] = []
        self._log_counter = 0

        # Configure standard Python logging
        self._logger = logging.getLogger(f"ETL.{etl_run_id}")
        self._logger.setLevel(console_level)
        
        # Console handler with formatting
        if not self._logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(console_level)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - [%(levelname)s] - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
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
        Log an ETL process message.
        Implements abstract method from ETLLoggerInterface.
        """
        # Generate unique log ID
        log_id = self._generate_log_id()
        
        # Get current timestamp
        now = datetime.now()
        execution_date = now.strftime('%Y-%m-%d')
        execution_time = now.strftime('%H:%M:%S')

        # Create log entry
        log_entry = LogEntry(
            log_id=log_id,
            etl_run_id=self._etl_run_id,
            execution_date=execution_date,
            execution_time=execution_time,
            process_step=step.value,
            status=status.value,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message[:255]  # Truncate to 255 chars
        )

        # Store in memory
        self._log_entries.append(log_entry)

        # Log to console using Python logging
        log_msg = (
            f"[{step.value}] {message} "
            f"(Processed: {records_processed}, "
            f"Success: {records_success}, "
            f"Errors: {records_error})"
        )
        
        if status == LogStatus.ERROR:
            self._logger.error(log_msg)
        elif status == LogStatus.WARNING:
            self._logger.warning(log_msg)
        elif status == LogStatus.INFO:
            self._logger.info(log_msg)
        else:  # SUCCESS
            self._logger.info(log_msg)

        # Optionally persist to database
        if self._spark and self._log_table:
            self._persist_to_database(log_entry)

    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run identifier.
        Implements abstract method from ETLLoggerInterface.
        """
        return self._etl_run_id

    def get_log_entries(self) -> List[LogEntry]:
        """
        Get all log entries for this ETL run.
        
        Returns:
            List of LogEntry objects
        """
        return self._log_entries.copy()

    def get_statistics(self) -> Dict[str, int]:
        """
        Get aggregated statistics from log entries.
        
        Returns:
            Dictionary with counts by status
        """
        stats = {
            'total': len(self._log_entries),
            'success': 0,
            'error': 0,
            'warning': 0,
            'info': 0
        }
        
        for entry in self._log_entries:
            if entry.status == LogStatus.SUCCESS.value:
                stats['success'] += 1
            elif entry.status == LogStatus.ERROR.value:
                stats['error'] += 1
            elif entry.status == LogStatus.WARNING.value:
                stats['warning'] += 1
            elif entry.status == LogStatus.INFO.value:
                stats['info'] += 1
        
        return stats

    def flush_to_database(self) -> None:
        """
        Flush all in-memory log entries to database.
        Only works if SparkSession and log_table are configured.
        """
        if not self._spark or not self._log_table or not self._log_entries:
            return

        try:
            # Convert log entries to DataFrame
            log_dicts = [asdict(entry) for entry in self._log_entries]
            df = self._spark.createDataFrame(log_dicts)
            
            # Write to table
            df.write.mode("append").saveAsTable(self._log_table)
            
            self._logger.info(
                f"Flushed {len(self._log_entries)} log entries to {self._log_table}"
            )
            
        except Exception as e:
            self._logger.error(f"Failed to flush logs to database: {str(e)}")

    def _generate_log_id(self) -> str:
        """
        Generate a unique log ID.
        Corresponds to ABAP method generate_log_id.
        
        Returns:
            Unique log ID string (format: LOG<timestamp><counter>)
        """
        self._log_counter += 1
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"LOG{timestamp}{self._log_counter:04d}"

    def _persist_to_database(self, log_entry: LogEntry) -> None:
        """
        Persist a single log entry to database immediately.
        
        Args:
            log_entry: LogEntry to persist
        """
        if not self._spark or not self._log_table:
            return

        try:
            df = self._spark.createDataFrame([asdict(log_entry)])
            df.write.mode("append").saveAsTable(self._log_table)
        except Exception as e:
            # Log to console but don't fail the ETL process
            self._logger.warning(
                f"Failed to persist log entry to database: {str(e)}"
            )