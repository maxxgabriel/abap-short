"""
ETL Logger Module
Migrated from ZCL_ETL_LOGGER ABAP class
Provides logging functionality for ETL processes
"""
import logging
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from src.constants import ETLConstants


@dataclass
class LogEntry:
    """Data class for log entries"""
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
    timestamp: datetime = field(default_factory=datetime.now)


class ETLLogger:
    """
    ETL Logger class for structured logging
    Migrated from ABAP ZCL_ETL_LOGGER
    """
    
    # Schema for log table
    LOG_SCHEMA = StructType([
        StructField("log_id", StringType(), False),
        StructField("etl_run_id", StringType(), False),
        StructField("execution_date", StringType(), False),
        StructField("execution_time", StringType(), False),
        StructField("process_step", StringType(), False),
        StructField("status", StringType(), False),
        StructField("records_processed", IntegerType(), True),
        StructField("records_success", IntegerType(), True),
        StructField("records_error", IntegerType(), True),
        StructField("message", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])
    
    def __init__(self, etl_run_id: str, spark: Optional[SparkSession] = None):
        """
        Initialize ETL Logger
        
        Args:
            etl_run_id: Unique ETL run identifier
            spark: SparkSession for logging to tables
        """
        self.etl_run_id = etl_run_id
        self.spark = spark
        self.log_entries = []
        
        # Setup Python logging
        self._setup_logging()
        
        self.logger.info(f"ETL Logger initialized with run ID: {etl_run_id}")
    
    def _setup_logging(self):
        """Setup Python logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/etl_process.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(f'ETL_{self.etl_run_id}')
    
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
        Log a message with statistics
        
        Args:
            step: Process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S, E, W, I)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        now = datetime.now()
        
        # Generate unique log ID
        log_id = self._generate_log_id()
        
        # Create log entry
        entry = LogEntry(
            log_id=log_id,
            etl_run_id=self.etl_run_id,
            execution_date=now.strftime('%Y%m%d'),
            execution_time=now.strftime('%H%M%S'),
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message,
            timestamp=now
        )
        
        self.log_entries.append(entry)
        
        # Log to Python logger
        log_level = self._get_log_level(status)
        log_message = (
            f"[{step}] {message} | "
            f"Processed: {records_processed}, "
            f"Success: {records_success}, "
            f"Errors: {records_error}"
        )
        self.logger.log(log_level, log_message)
        
        # Print to console (mimics ABAP WRITE statement)
        print(f"{entry.execution_time} {entry.process_step} {entry.status} {entry.message}")
    
    def _generate_log_id(self) -> str:
        """
        Generate unique log ID
        
        Returns:
            Unique log identifier
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]
        return f"{ETLConstants.PREFIXES.LOG_ID}{timestamp}"
    
    def _get_log_level(self, status: str) -> int:
        """
        Map status code to Python logging level
        
        Args:
            status: Status code (S, E, W, I)
            
        Returns:
            Python logging level
        """
        mapping = {
            ETLConstants.STATUS.SUCCESS: logging.INFO,
            ETLConstants.STATUS.ERROR: logging.ERROR,
            ETLConstants.STATUS.WARNING: logging.WARNING,
            ETLConstants.STATUS.INFO: logging.INFO
        }
        return mapping.get(status, logging.INFO)
    
    def get_etl_run_id(self) -> str:
        """
        Get ETL run ID
        
        Returns:
            ETL run identifier
        """
        return self.etl_run_id
    
    def persist_logs(self, table_name: str = "zetl_log") -> bool:
        """
        Persist logs to database table using Spark
        
        Args:
            table_name: Target table name
            
        Returns:
            True if successful, False otherwise
        """
        if not self.spark or not self.log_entries:
            return False
        
        try:
            # Convert log entries to list of tuples
            log_data = [
                (
                    entry.log_id,
                    entry.etl_run_id,
                    entry.execution_date,
                    entry.execution_time,
                    entry.process_step,
                    entry.status,
                    entry.records_processed,
                    entry.records_success,
                    entry.records_error,
                    entry.message,
                    entry.timestamp
                )
                for entry in self.log_entries
            ]
            
            # Create DataFrame
            log_df = self.spark.createDataFrame(log_data, schema=self.LOG_SCHEMA)
            
            # Write to table
            log_df.write.mode("append").saveAsTable(table_name)
            
            self.logger.info(f"Persisted {len(self.log_entries)} log entries to {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to persist logs: {str(e)}")
            return False
    
    def get_summary(self) -> dict:
        """
        Get summary of log entries
        
        Returns:
            Dictionary with log statistics
        """
        total_processed = sum(e.records_processed for e in self.log_entries)
        total_success = sum(e.records_success for e in self.log_entries)
        total_errors = sum(e.records_error for e in self.log_entries)
        
        error_count = sum(1 for e in self.log_entries if e.status == ETLConstants.STATUS.ERROR)
        warning_count = sum(1 for e in self.log_entries if e.status == ETLConstants.STATUS.WARNING)
        
        return {
            'etl_run_id': self.etl_run_id,
            'total_log_entries': len(self.log_entries),
            'total_processed': total_processed,
            'total_success': total_success,
            'total_errors': total_errors,
            'error_count': error_count,
            'warning_count': warning_count
        }