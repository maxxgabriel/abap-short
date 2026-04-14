"""
ETL logging utility.
Migrated from ABAP ZCL_ETL_LOGGER class.
"""
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import Row

from src.constants import StatusCode, ProcessStep
from src.schemas import get_etl_log_schema
from src.utils import generate_log_id


class ETLLogger:
    """
    Logger for ETL process execution.
    Provides structured logging with database persistence.
    """
    
    def __init__(self, etl_run_id: str, spark: SparkSession):
        """
        Initialize logger.
        
        Args:
            etl_run_id: Unique ETL run identifier
            spark: Active SparkSession
        """
        self.etl_run_id = etl_run_id
        self.spark = spark
        self.log_entries = []
    
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
        Log a message with execution details.
        
        Args:
            step: Process step identifier
            status: Status code
            message: Log message text
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Records with errors
        """
        now = datetime.now()
        
        log_entry = {
            "log_id": generate_log_id(),
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
        
        self.log_entries.append(log_entry)
        
        # Console output
        status_symbol = self._get_status_symbol(status)
        print(f"[{now.strftime('%H:%M:%S')}] {status_symbol} {step}: {message}")
        
        if records_processed > 0:
            print(f"  Records: {records_processed} processed, "
                  f"{records_success} success, {records_error} errors")
    
    def get_etl_run_id(self) -> str:
        """Get the current ETL run ID."""
        return self.etl_run_id
    
    def get_logs_dataframe(self) -> DataFrame:
        """
        Get all log entries as a DataFrame.
        
        Returns:
            DataFrame with log entries
        """
        if not self.log_entries:
            return self.spark.createDataFrame([], schema=get_etl_log_schema())
        
        return self.spark.createDataFrame(
            [Row(**entry) for entry in self.log_entries],
            schema=get_etl_log_schema()
        )
    
    def persist_logs(self, target_path: str, mode: str = "append") -> None:
        """
        Persist log entries to storage.
        
        Args:
            target_path: Target path for log storage
            mode: Write mode (append, overwrite, etc.)
        """
        if self.log_entries:
            df = self.get_logs_dataframe()
            df.write.mode(mode).parquet(target_path)
            print(f"Persisted {len(self.log_entries)} log entries to {target_path}")
    
    @staticmethod
    def _get_status_symbol(status: str) -> str:
        """Get visual symbol for status code."""
        symbols = {
            StatusCode.SUCCESS.value: "✓",
            StatusCode.ERROR.value: "✗",
            StatusCode.WARNING.value: "⚠",
            StatusCode.INFO.value: "ℹ"
        }
        return symbols.get(status, "•")