"""
ETL logging utility.
"""
import logging
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from src.schemas import ETLSchemas
from src.constants import ETLConstants


class ETLLogger:
    """Logger for ETL processes with DataFrame-based log storage."""
    
    def __init__(self, etl_run_id: str, spark: SparkSession):
        self.etl_run_id = etl_run_id
        self.spark = spark
        self.logger = logging.getLogger(f"ETL.{etl_run_id}")
        self._log_entries = []
    
    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ):
        """Log a message with context."""
        now = datetime.now()
        
        log_entry = {
            "log_id": self._generate_log_id(),
            "etl_run_id": self.etl_run_id,
            "execution_date": now.date(),
            "execution_time": now.strftime("%H:%M:%S"),
            "process_step": step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message,
            "created_at": now,
            "created_by": "ETL_SYSTEM"
        }
        
        self._log_entries.append(log_entry)
        
        # Console logging
        log_level = self._get_log_level(status)
        self.logger.log(
            log_level,
            f"[{step}] {message} (Processed: {records_processed}, "
            f"Success: {records_success}, Error: {records_error})"
        )
    
    def get_logs_dataframe(self) -> Optional[DataFrame]:
        """Get all logs as a DataFrame."""
        if not self._log_entries:
            return None
        
        return self.spark.createDataFrame(
            self._log_entries,
            schema=ETLSchemas.etl_log_schema()
        )
    
    def _generate_log_id(self) -> str:
        """Generate unique log ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"{ETLConstants.PREFIX_LOG_ID}{timestamp}"
    
    @staticmethod
    def _get_log_level(status: str) -> int:
        """Map ETL status to logging level."""
        mapping = {
            ETLConstants.STATUS.SUCCESS: logging.INFO,
            ETLConstants.STATUS.INFO: logging.INFO,
            ETLConstants.STATUS.WARNING: logging.WARNING,
            ETLConstants.STATUS.ERROR: logging.ERROR
        }
        return mapping.get(status, logging.INFO)