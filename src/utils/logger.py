"""
ETL Logger Utility Module
Provides logging functionality for ETL processes with statistics tracking.
"""

from datetime import datetime
from typing import Optional
import logging
from pyspark.sql import SparkSession


class ETLLogger:
    """Logger class for ETL operations with message and statistics tracking."""
    
    def __init__(self, etl_run_id: str, spark: Optional[SparkSession] = None):
        """
        Initialize ETL logger.
        
        Args:
            etl_run_id: Unique identifier for ETL run
            spark: SparkSession for database logging (optional)
        """
        self.etl_run_id = etl_run_id
        self.spark = spark
        self._setup_logger()
        
    def _setup_logger(self):
        """Configure Python logging."""
        self.logger = logging.getLogger(f"ETL_{self.etl_run_id}")
        self.logger.setLevel(logging.INFO)
        
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
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
        Log ETL message with optional statistics.
        
        Args:
            step: ETL process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message text
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Error records count
        """
        log_entry = {
            'log_id': self._generate_log_id(),
            'etl_run_id': self.etl_run_id,
            'execution_date': datetime.now().date(),
            'execution_time': datetime.now().time(),
            'process_step': step,
            'status': status,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error,
            'message': message,
            'timestamp': datetime.now()
        }
        
        # Console logging
        log_level = self._get_log_level(status)
        self.logger.log(
            log_level,
            f"[{step}] {message} | Processed: {records_processed}, "
            f"Success: {records_success}, Error: {records_error}"
        )
        
        # Database logging (if Spark session available)
        if self.spark:
            self._persist_log(log_entry)
    
    def _generate_log_id(self) -> str:
        """Generate unique log ID."""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f"LOG{timestamp[:14]}"
    
    def _get_log_level(self, status: str) -> int:
        """Map status code to logging level."""
        status_map = {
            'S': logging.INFO,
            'E': logging.ERROR,
            'W': logging.WARNING,
            'I': logging.INFO
        }
        return status_map.get(status, logging.INFO)
    
    def _persist_log(self, log_entry: dict) -> None:
        """Persist log entry to database (stub for implementation)."""
        # In production, write to ETL_LOG table
        # self.spark.createDataFrame([log_entry]).write.mode("append").saveAsTable("etl_log")
        pass
    
    def get_etl_run_id(self) -> str:
        """Get current ETL run ID."""
        return self.etl_run_id


def log_etl_message(logger: ETLLogger, step: str, status: str, message: str) -> None:
    """
    Utility function to log ETL message.
    
    Args:
        logger: ETLLogger instance
        step: Process step
        status: Status code
        message: Message text
    """
    logger.log_message(step=step, status=status, message=message)


def log_etl_statistics(
    logger: ETLLogger,
    step: str,
    status: str,
    records_processed: int,
    records_success: int,
    records_error: int,
    message: str
) -> None:
    """
    Utility function to log ETL message with statistics.
    
    Args:
        logger: ETLLogger instance
        step: Process step
        status: Status code
        records_processed: Total records processed
        records_success: Success count
        records_error: Error count
        message: Message text
    """
    logger.log_message(
        step=step,
        status=status,
        message=message,
        records_processed=records_processed,
        records_success=records_success,
        records_error=records_error
    )