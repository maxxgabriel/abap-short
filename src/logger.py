"""
ETL Logger Module - Implements logging with DataFrame persistence
Migrated from ZCL_ETL_LOGGER ABAP class
"""
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import List, Optional
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DateType, TimestampType
)


@dataclass
class LogEntry:
    """
    Log entry structure matching ABAP ty_log_entry
    Represents a single ETL log record
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
    created_by: str = "etl_system"

    def to_dict(self):
        """Convert to dictionary for DataFrame creation"""
        return {
            'log_id': self.log_id,
            'etl_run_id': self.etl_run_id,
            'execution_date': self.execution_date.date(),
            'execution_time': self.execution_time,
            'process_step': self.process_step,
            'status': self.status,
            'records_processed': self.records_processed,
            'records_success': self.records_success,
            'records_error': self.records_error,
            'message': self.message,
            'created_at': self.created_at,
            'created_by': self.created_by
        }


class ETLLogger:
    """
    ETL Logger with DataFrame persistence
    Migrated from ZCL_ETL_LOGGER ABAP class
    
    Provides logging functionality with:
    - Structured log entries using dataclasses
    - DataFrame-based log persistence
    - Python logging integration
    - Unique log ID generation
    """
    
    # Status constants (matching ABAP gc_status)
    STATUS_SUCCESS = 'S'
    STATUS_ERROR = 'E'
    STATUS_WARNING = 'W'
    STATUS_INFO = 'I'
    STATUS_NEW = 'N'
    STATUS_PROCESSED = 'P'
    
    # Process step constants (matching ABAP gc_step)
    STEP_INIT = 'INIT'
    STEP_EXTRACT = 'EXTRACT'
    STEP_TRANSFORM = 'TRANSFORM'
    STEP_LOAD = 'LOAD'
    STEP_VALIDATE = 'VALIDATE'
    STEP_COMPLETE = 'COMPLETE'
    STEP_ERROR = 'ERROR'
    
    def __init__(self, etl_run_id: str, spark: SparkSession, config: dict):
        """
        Initialize logger with ETL run ID
        
        Args:
            etl_run_id: Unique identifier for this ETL run
            spark: SparkSession for DataFrame operations
            config: Configuration dictionary with logging settings
        """
        self.etl_run_id = etl_run_id
        self.spark = spark
        self.config = config
        self.log_entries: List[LogEntry] = []
        self._log_counter = 0
        
        # Setup Python logging
        self._setup_python_logger()
        
        # Define schema for log DataFrame
        self.log_schema = StructType([
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
        
        self.logger.info(f"ETL Logger initialized with run ID: {etl_run_id}")
    
    def _setup_python_logger(self):
        """Setup standard Python logging"""
        log_level = self.config.get('log_level', 'INFO')
        log_format = self.config.get(
            'log_format',
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        self.logger = logging.getLogger(f'ETLLogger_{self.etl_run_id}')
        self.logger.setLevel(getattr(logging, log_level))
        
        # Console handler
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setLevel(getattr(logging, log_level))
            formatter = logging.Formatter(log_format)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def _generate_log_id(self) -> str:
        """
        Generate unique log ID
        Matches ABAP generate_log_id method
        
        Returns:
            Unique log ID string
        """
        self._log_counter += 1
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"LOG{timestamp}{self._log_counter:04d}"
    
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
        Log a message with metadata
        Matches ABAP log_message method
        
        Args:
            step: ETL process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S, E, W, I)
            message: Log message text
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        now = datetime.now()
        
        # Create log entry
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
        
        # Store in memory
        self.log_entries.append(log_entry)
        
        # Log to Python logger
        log_msg = (
            f"[{step}] [{status}] {message} "
            f"(Processed: {records_processed}, "
            f"Success: {records_success}, "
            f"Errors: {records_error})"
        )
        
        if status == self.STATUS_ERROR:
            self.logger.error(log_msg)
        elif status == self.STATUS_WARNING:
            self.logger.warning(log_msg)
        elif status == self.STATUS_INFO:
            self.logger.info(log_msg)
        else:
            self.logger.info(log_msg)
    
    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID
        Matches ABAP get_etl_run_id method
        
        Returns:
            ETL run ID string
        """
        return self.etl_run_id
    
    def get_log_entries_as_dataframe(self) -> DataFrame:
        """
        Convert log entries to DataFrame
        
        Returns:
            DataFrame containing all log entries
        """
        if not self.log_entries:
            # Return empty DataFrame with schema
            return self.spark.createDataFrame([], schema=self.log_schema)
        
        # Convert log entries to dictionaries
        log_dicts = [entry.to_dict() for entry in self.log_entries]
        
        # Create DataFrame
        df = self.spark.createDataFrame(log_dicts, schema=self.log_schema)
        
        return df
    
    def persist_logs(self, output_path: Optional[str] = None) -> None:
        """
        Persist logs to storage using DataFrame operations
        
        Args:
            output_path: Optional path override for log output
        """
        if not self.log_entries:
            self.logger.warning("No log entries to persist")
            return
        
        # Get DataFrame
        log_df = self.get_log_entries_as_dataframe()
        
        # Determine output path
        if output_path is None:
            output_path = self.config.get('log_output_path', '/tmp/etl_logs')
        
        # Add partition columns
        log_df = log_df.withColumn(
            "partition_date",
            log_df.execution_date
        )
        
        try:
            # Write to storage (partitioned by date)
            log_df.write \
                .mode(self.config.get('log_write_mode', 'append')) \
                .partitionBy('partition_date') \
                .parquet(output_path)
            
            self.logger.info(
                f"Persisted {len(self.log_entries)} log entries to {output_path}"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to persist logs: {str(e)}")
            raise
    
    def get_summary_statistics(self) -> dict:
        """
        Get summary statistics from log entries
        
        Returns:
            Dictionary with summary statistics
        """
        if not self.log_entries:
            return {
                'total_entries': 0,
                'success_count': 0,
                'error_count': 0,
                'warning_count': 0,
                'total_records_processed': 0,
                'total_records_success': 0,
                'total_records_error': 0
            }
        
        return {
            'total_entries': len(self.log_entries),
            'success_count': sum(
                1 for e in self.log_entries if e.status == self.STATUS_SUCCESS
            ),
            'error_count': sum(
                1 for e in self.log_entries if e.status == self.STATUS_ERROR
            ),
            'warning_count': sum(
                1 for e in self.log_entries if e.status == self.STATUS_WARNING
            ),
            'total_records_processed': sum(
                e.records_processed for e in self.log_entries
            ),
            'total_records_success': sum(
                e.records_success for e in self.log_entries
            ),
            'total_records_error': sum(
                e.records_error for e in self.log_entries
            )
        }
    
    def display_summary(self) -> None:
        """Display summary of logs to console"""
        stats = self.get_summary_statistics()
        
        print("=" * 70)
        print("ETL Logging Summary")
        print("=" * 70)
        print(f"ETL Run ID:              {self.etl_run_id}")
        print(f"Total Log Entries:       {stats['total_entries']}")
        print(f"Success Count:           {stats['success_count']}")
        print(f"Error Count:             {stats['error_count']}")
        print(f"Warning Count:           {stats['warning_count']}")
        print(f"Total Records Processed: {stats['total_records_processed']}")
        print(f"Total Records Success:   {stats['total_records_success']}")
        print(f"Total Records Error:     {stats['total_records_error']}")
        print("=" * 70)
    
    def clear_logs(self) -> None:
        """Clear in-memory log entries"""
        self.log_entries.clear()
        self._log_counter = 0
        self.logger.info("Log entries cleared")


def create_logger(etl_run_id: str, spark: SparkSession, config: dict) -> ETLLogger:
    """
    Factory function to create ETL logger instance
    
    Args:
        etl_run_id: Unique ETL run identifier
        spark: SparkSession instance
        config: Configuration dictionary
    
    Returns:
        Configured ETLLogger instance
    """
    return ETLLogger(etl_run_id, spark, config)