"""
ETL Logger module.
Converted from ABAP ZCL_ETL_LOGGER class.
"""

import logging
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession

from src.config import CONSTANTS, ETLStatus, ETLStep
from src.types import ETLLogEntry


class ETLLogger:
    """
    ETL logging utility class.
    Handles logging to console, file, and database.
    """
    
    def __init__(self, etl_run_id: str, spark: Optional[SparkSession] = None):
        """
        Initialize ETL logger.
        
        Args:
            etl_run_id: Unique identifier for this ETL run
            spark: Optional SparkSession for database logging
        """
        self.etl_run_id = etl_run_id
        self.spark = spark
        
        # Setup Python logger
        self.logger = logging.getLogger(f"ETL_{etl_run_id}")
        self.logger.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # Log initialization
        self.log_message(
            step=ETLStep.INIT,
            status=ETLStatus.SUCCESS,
            message=f"Logger initialized for ETL run: {etl_run_id}"
        )
    
    def log_message(
        self,
        step: ETLStep,
        status: ETLStatus,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ) -> None:
        """
        Log a message with ETL context.
        
        Args:
            step: ETL process step
            status: Status code
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        log_id = self._generate_log_id()
        now = datetime.now()
        
        # Create log entry
        log_entry = ETLLogEntry(
            log_id=log_id,
            etl_run_id=self.etl_run_id,
            execution_date=now.date(),
            execution_time=now.strftime("%H:%M:%S"),
            process_step=step.value,
            status=status.value,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message,
            created_at=now,
            created_by="ETL_SYSTEM"
        )
        
        # Log to console
        log_msg = (
            f"[{step.value}] [{status.value}] {message}"
            f" (Processed: {records_processed}, Success: {records_success}, "
            f"Error: {records_error})"
        )
        
        if status == ETLStatus.ERROR:
            self.logger.error(log_msg)
        elif status == ETLStatus.WARNING:
            self.logger.warning(log_msg)
        else:
            self.logger.info(log_msg)
        
        # Log to database if Spark session available
        if self.spark:
            self._log_to_database(log_entry)
    
    def _generate_log_id(self) -> str:
        """
        Generate unique log ID.
        
        Returns:
            Unique log identifier
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"{CONSTANTS.config.PREFIX_LOG_ID}{timestamp}"
    
    def _log_to_database(self, log_entry: ETLLogEntry) -> None:
        """
        Write log entry to database.
        
        Args:
            log_entry: Log entry to persist
        """
        try:
            # Convert to DataFrame and write
            from pyspark.sql import Row
            
            row = Row(
                log_id=log_entry.log_id,
                etl_run_id=log_entry.etl_run_id,
                execution_date=log_entry.execution_date,
                execution_time=log_entry.execution_time,
                process_step=log_entry.process_step,
                status=log_entry.status,
                records_processed=log_entry.records_processed,
                records_success=log_entry.records_success,
                records_error=log_entry.records_error,
                message=log_entry.message,
                created_at=log_entry.created_at,
                created_by=log_entry.created_by
            )
            
            df = self.spark.createDataFrame([row])
            
            # In production, write to actual database
            # df.write.mode("append").saveAsTable("ZETL_LOG")
            
        except Exception as e:
            self.logger.error(f"Failed to write log to database: {str(e)}")
    
    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID.
        
        Returns:
            ETL run identifier
        """
        return self.etl_run_id