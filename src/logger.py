"""
ETL Logger Component
Generates unique log IDs from timestamps, captures execution metrics,
and structures log entries with step/status/duration information.
"""

from datetime import datetime
from typing import Optional, Dict, Any
from dataclasses import dataclass, asdict
from pyspark.sql import SparkSession
import logging


@dataclass
class LogEntry:
    """Structure for a log entry"""
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
    duration_ms: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert log entry to dictionary"""
        return asdict(self)


class ETLLogger:
    """
    ETL Logger component that generates unique log IDs from timestamps,
    captures execution metrics, and structures log entries.
    """
    
    # Status constants
    STATUS_SUCCESS = 'S'
    STATUS_ERROR = 'E'
    STATUS_WARNING = 'W'
    STATUS_INFO = 'I'
    
    # Step constants
    STEP_INIT = 'INIT'
    STEP_EXTRACT = 'EXTRACT'
    STEP_TRANSFORM = 'TRANSFORM'
    STEP_LOAD = 'LOAD'
    STEP_VALIDATE = 'VALIDATE'
    STEP_COMPLETE = 'COMPLETE'
    STEP_ERROR = 'ERROR'
    
    def __init__(self, etl_run_id: str, spark: Optional[SparkSession] = None):
        """
        Initialize ETL Logger
        
        Args:
            etl_run_id: Unique identifier for the ETL run
            spark: Optional SparkSession for DataFrame operations
        """
        self.etl_run_id = etl_run_id
        self.spark = spark
        self.log_entries = []
        self._step_start_times: Dict[str, datetime] = {}
        
        # Configure Python logging
        self._logger = logging.getLogger(__name__)
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)
            self._logger.setLevel(logging.INFO)
    
    @staticmethod
    def generate_log_id() -> str:
        """
        Generate unique log ID from timestamp
        
        Returns:
            Unique log ID in format LOG<timestamp>
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f"LOG{timestamp[:14]}"
    
    @staticmethod
    def generate_etl_run_id() -> str:
        """
        Generate unique ETL run ID from timestamp
        
        Returns:
            Unique ETL run ID in format ETL<timestamp>
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f"ETL{timestamp[:14]}"
    
    def start_step(self, step: str) -> None:
        """
        Mark the start of an ETL step for duration tracking
        
        Args:
            step: Name of the ETL step
        """
        self._step_start_times[step] = datetime.now()
    
    def get_step_duration_ms(self, step: str) -> Optional[int]:
        """
        Get duration of a step in milliseconds
        
        Args:
            step: Name of the ETL step
            
        Returns:
            Duration in milliseconds or None if step not started
        """
        if step not in self._step_start_times:
            return None
        
        start_time = self._step_start_times[step]
        duration = datetime.now() - start_time
        return int(duration.total_seconds() * 1000)
    
    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0,
        include_duration: bool = True
    ) -> LogEntry:
        """
        Log a message with execution metrics
        
        Args:
            step: ETL process step
            status: Status code (S/E/W/I)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
            include_duration: Whether to include step duration
            
        Returns:
            Created LogEntry object
        """
        now = datetime.now()
        
        # Calculate duration if step was started
        duration_ms = None
        if include_duration:
            duration_ms = self.get_step_duration_ms(step)
        
        # Create log entry
        log_entry = LogEntry(
            log_id=self.generate_log_id(),
            etl_run_id=self.etl_run_id,
            execution_date=now.strftime('%Y-%m-%d'),
            execution_time=now.strftime('%H:%M:%S'),
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message,
            duration_ms=duration_ms
        )
        
        # Store log entry
        self.log_entries.append(log_entry)
        
        # Output to console/logging system
        self._write_log(log_entry)
        
        return log_entry
    
    def _write_log(self, log_entry: LogEntry) -> None:
        """
        Write log entry to console/logging system
        
        Args:
            log_entry: LogEntry to write
        """
        # Format log message
        duration_str = ""
        if log_entry.duration_ms is not None:
            duration_str = f" (Duration: {log_entry.duration_ms}ms)"
        
        log_line = (
            f"{log_entry.execution_time} | "
            f"{log_entry.process_step:12} | "
            f"{log_entry.status} | "
            f"Processed: {log_entry.records_processed:6} | "
            f"Success: {log_entry.records_success:6} | "
            f"Error: {log_entry.records_error:6} | "
            f"{log_entry.message}{duration_str}"
        )
        
        # Log to appropriate level
        if log_entry.status == self.STATUS_ERROR:
            self._logger.error(log_line)
        elif log_entry.status == self.STATUS_WARNING:
            self._logger.warning(log_line)
        else:
            self._logger.info(log_line)
    
    def get_etl_run_id(self) -> str:
        """Get the ETL run ID"""
        return self.etl_run_id
    
    def get_log_entries(self) -> list:
        """Get all log entries"""
        return self.log_entries
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get aggregated statistics from log entries
        
        Returns:
            Dictionary with aggregated statistics
        """
        total_processed = sum(entry.records_processed for entry in self.log_entries)
        total_success = sum(entry.records_success for entry in self.log_entries)
        total_error = sum(entry.records_error for entry in self.log_entries)
        
        error_count = sum(1 for entry in self.log_entries if entry.status == self.STATUS_ERROR)
        warning_count = sum(1 for entry in self.log_entries if entry.status == self.STATUS_WARNING)
        
        total_duration_ms = sum(
            entry.duration_ms for entry in self.log_entries 
            if entry.duration_ms is not None
        )
        
        return {
            'etl_run_id': self.etl_run_id,
            'total_records_processed': total_processed,
            'total_records_success': total_success,
            'total_records_error': total_error,
            'error_count': error_count,
            'warning_count': warning_count,
            'total_duration_ms': total_duration_ms,
            'total_duration_seconds': total_duration_ms / 1000 if total_duration_ms else 0,
            'total_log_entries': len(self.log_entries)
        }
    
    def save_logs_to_dataframe(self):
        """
        Save log entries to Spark DataFrame
        
        Returns:
            Spark DataFrame with log entries
        
        Raises:
            ValueError: If SparkSession not provided
        """
        if not self.spark:
            raise ValueError("SparkSession required for DataFrame operations")
        
        # Convert log entries to list of dicts
        log_dicts = [entry.to_dict() for entry in self.log_entries]
        
        # Create DataFrame
        df = self.spark.createDataFrame(log_dicts)
        
        return df
    
    def save_logs_to_table(self, table_name: str, mode: str = 'append') -> None:
        """
        Save log entries to a Spark table
        
        Args:
            table_name: Name of the target table
            mode: Write mode (append/overwrite)
            
        Raises:
            ValueError: If SparkSession not provided
        """
        df = self.save_logs_to_dataframe()
        df.write.mode(mode).saveAsTable(table_name)
        
        self._logger.info(f"Saved {len(self.log_entries)} log entries to table {table_name}")
    
    def display_summary(self) -> None:
        """Display formatted summary of ETL execution"""
        stats = self.get_statistics()
        
        print("=" * 70)
        print("ETL Process Summary")
        print("=" * 70)
        print(f"ETL Run ID:         {stats['etl_run_id']}")
        print(f"Total Records:      {stats['total_records_processed']}")
        print(f"Success Records:    {stats['total_records_success']}")
        print(f"Error Records:      {stats['total_records_error']}")
        print(f"Errors:             {stats['error_count']}")
        print(f"Warnings:           {stats['warning_count']}")
        print(f"Total Duration:     {stats['total_duration_seconds']:.2f} seconds")
        print(f"Log Entries:        {stats['total_log_entries']}")
        print("=" * 70)