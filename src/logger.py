"""
ETL Logging Framework.
Replaces ABAP WRITE statements with Python logging and creates structured ETL log records.
"""

import logging
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DateType, TimestampType
)


class ETLLogger:
    """
    ETL Logger that creates structured log records conforming to ty_etl_log schema.
    Persists logs to ETL log table after each step.
    """
    
    def __init__(self, spark: SparkSession, etl_run_id: str, config: dict):
        """
        Initialize ETL Logger.
        
        Args:
            spark: SparkSession instance
            etl_run_id: Unique identifier for ETL run
            config: Configuration dictionary
        """
        self.spark = spark
        self.etl_run_id = etl_run_id
        self.config = config
        self.log_table = config.get("log_table", "zetl_log")
        self.log_buffer = []
        
        # Set up Python logger
        self.logger = logging.getLogger(__name__)
        
    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ):
        """
        Log a message with ETL metadata.
        
        Args:
            step: ETL process step (INIT, EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        log_id = self._generate_log_id()
        execution_date = datetime.now().date()
        execution_time = datetime.now().time()
        timestamp = datetime.now()
        
        # Create log entry
        log_entry = {
            "log_id": log_id,
            "etl_run_id": self.etl_run_id,
            "execution_date": execution_date,
            "execution_time": execution_time.strftime("%H:%M:%S"),
            "process_step": step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message[:255],  # Truncate to max length
            "created_at": timestamp,
            "created_by": "ETL_SYSTEM"
        }
        
        # Add to buffer
        self.log_buffer.append(log_entry)
        
        # Write to Python log
        log_level = self._get_log_level(status)
        self.logger.log(
            log_level,
            f"[{step}] [{status}] {message} "
            f"(Processed: {records_processed}, Success: {records_success}, Error: {records_error})"
        )
        
        # Persist to table if buffer is full or it's a critical step
        if len(self.log_buffer) >= self.config.get("log_buffer_size", 10) or status == "E":
            self.flush_logs()
            
    def flush_logs(self):
        """Flush buffered logs to the ETL log table."""
        if not self.log_buffer:
            return
            
        try:
            # Create DataFrame from buffer
            schema = self._get_log_schema()
            df_logs = self.spark.createDataFrame(self.log_buffer, schema=schema)
            
            # Write to log table
            df_logs.write.mode("append").saveAsTable(self.log_table)
            
            self.logger.info(f"Flushed {len(self.log_buffer)} log entries to {self.log_table}")
            
            # Clear buffer
            self.log_buffer = []
            
        except Exception as e:
            self.logger.error(f"Failed to flush logs: {str(e)}", exc_info=True)
            
    def get_etl_run_id(self) -> str:
        """Get the current ETL run ID."""
        return self.etl_run_id
        
    def _generate_log_id(self) -> str:
        """Generate unique log ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"LOG{timestamp}"
        
    def _get_log_level(self, status: str) -> int:
        """
        Map ETL status to Python log level.
        
        Args:
            status: ETL status code
            
        Returns:
            Python logging level
        """
        status_map = {
            "S": logging.INFO,
            "E": logging.ERROR,
            "W": logging.WARNING,
            "I": logging.INFO
        }
        return status_map.get(status, logging.INFO)
        
    def _get_log_schema(self) -> StructType:
        """
        Get schema for ETL log records (ty_etl_log).
        
        Returns:
            StructType schema
        """
        return StructType([
            StructField("log_id", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("execution_date", DateType(), False),
            StructField("execution_time", StringType(), False),
            StructField("process_step", StringType(), False),
            StructField("status", StringType(), False),
            StructField("records_processed", IntegerType(), False),
            StructField("records_success", IntegerType(), False),
            StructField("records_error", IntegerType(), False),
            StructField("message", StringType(), True),
            StructField("created_at", TimestampType(), False),
            StructField("created_by", StringType(), False)
        ])
        
    def log_statistics(
        self,
        step: str,
        total_records: int,
        success_records: int,
        error_records: int,
        duration_seconds: Optional[float] = None
    ):
        """
        Log statistics for an ETL step.
        
        Args:
            step: ETL process step
            total_records: Total records processed
            success_records: Successfully processed records
            error_records: Error records
            duration_seconds: Optional duration in seconds
        """
        duration_msg = f" in {duration_seconds:.2f}s" if duration_seconds else ""
        message = (
            f"Step completed{duration_msg}: "
            f"{success_records}/{total_records} successful, "
            f"{error_records} errors"
        )
        
        self.log_message(
            step=step,
            status="S" if error_records == 0 else "W",
            message=message,
            records_processed=total_records,
            records_success=success_records,
            records_error=error_records
        )
        
    def __del__(self):
        """Ensure logs are flushed on cleanup."""
        try:
            self.flush_logs()
        except:
            pass