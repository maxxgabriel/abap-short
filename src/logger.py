"""
Logger Module
Provides centralized logging for ETL operations.
"""
import logging
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession

from src.types import ETLLog
from src.constants import IDPrefixes


class ETLLogger:
    """Logger for ETL operations with database persistence."""
    
    def __init__(self, etl_run_id: str, spark: Optional[SparkSession] = None):
        """
        Initialize logger.
        
        Args:
            etl_run_id: Unique identifier for this ETL run
            spark: Optional SparkSession for database logging
        """
        self.etl_run_id = etl_run_id
        self.spark = spark
        self.logger = logging.getLogger(f"ETL.{etl_run_id}")
        
        # Configure console logging
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
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
        Log a message with ETL context.
        
        Args:
            step: Process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S, E, W, I)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        log_entry = ETLLog(
            log_id=self._generate_log_id(),
            etl_run_id=self.etl_run_id,
            execution_date=datetime.now().date(),
            execution_time=datetime.now().strftime('%H:%M:%S'),
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message,
            created_at=datetime.now()
        )
        
        # Console logging
        log_level = self._get_log_level(status)
        self.logger.log(
            log_level,
            f"[{step}] {message} | "
            f"Processed: {records_processed}, "
            f"Success: {records_success}, "
            f"Errors: {records_error}"
        )
        
        # Database logging (if Spark session available)
        if self.spark:
            self._persist_log(log_entry)
    
    def get_etl_run_id(self) -> str:
        """Get the current ETL run ID."""
        return self.etl_run_id
    
    def _generate_log_id(self) -> str:
        """Generate a unique log ID."""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f"{IDPrefixes.LOG_ID}{timestamp}"
    
    def _get_log_level(self, status: str) -> int:
        """Map status code to logging level."""
        status_map = {
            'S': logging.INFO,
            'I': logging.INFO,
            'W': logging.WARNING,
            'E': logging.ERROR
        }
        return status_map.get(status, logging.INFO)
    
    def _persist_log(self, log_entry: ETLLog) -> None:
        """
        Persist log entry to database.
        
        Args:
            log_entry: Log entry to persist
        """
        try:
            from pyspark.sql import Row
            
            row = Row(
                log_id=log_entry.log_id,
                etl_run_id=log_entry.etl_run_id,
                execution_date=str(log_entry.execution_date),
                execution_time=log_entry.execution_time,
                process_step=log_entry.process_step,
                status=log_entry.status,
                records_processed=log_entry.records_processed,
                records_success=log_entry.records_success,
                records_error=log_entry.records_error,
                message=log_entry.message,
                created_at=log_entry.created_at.isoformat() if log_entry.created_at else None
            )
            
            df = self.spark.createDataFrame([row])
            df.write.mode("append").saveAsTable("zetl_log")
            
        except Exception as e:
            self.logger.warning(f"Failed to persist log to database: {str(e)}")