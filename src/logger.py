"""
Logging module for ETL system.
Provides structured logging for ETL operations.
"""
from typing import Optional
from datetime import datetime
from pyspark.sql import SparkSession


class ETLLogger:
    """Logger for ETL operations."""
    
    def __init__(self, spark: SparkSession, etl_run_id: str):
        """
        Initialize the ETL logger.
        
        Args:
            spark: SparkSession instance
            etl_run_id: Unique ETL run identifier
        """
        self.spark = spark
        self.etl_run_id = etl_run_id
        self._log_entries = []
    
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
            step: ETL process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        log_entry = {
            "log_id": self._generate_log_id(),
            "etl_run_id": self.etl_run_id,
            "execution_date": datetime.now().date().isoformat(),
            "execution_time": datetime.now().time().strftime("%H:%M:%S"),
            "process_step": step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message,
            "timestamp": datetime.now().isoformat()
        }
        
        self._log_entries.append(log_entry)
        
        # Print to console
        status_symbol = {
            "S": "✓",
            "E": "✗",
            "W": "⚠",
            "I": "ℹ"
        }.get(status, "•")
        
        print(f"[{log_entry['execution_time']}] {status_symbol} {step}: {message}")
        
        if records_processed > 0:
            print(f"  └─ Processed: {records_processed}, Success: {records_success}, Error: {records_error}")
    
    def get_etl_run_id(self) -> str:
        """Get the current ETL run ID."""
        return self.etl_run_id
    
    def _generate_log_id(self) -> str:
        """Generate a unique log ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"LOG{timestamp}"
    
    def get_log_entries(self) -> list:
        """Get all log entries for this ETL run."""
        return self._log_entries
    
    def persist_logs(self, target_table: str = "zetl_log") -> None:
        """
        Persist log entries to database table.
        
        Args:
            target_table: Target log table name
        """
        if not self._log_entries:
            return
        
        try:
            df = self.spark.createDataFrame(self._log_entries)
            df.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(target_table)
            
            print(f"Persisted {len(self._log_entries)} log entries to {target_table}")
        except Exception as e:
            print(f"Failed to persist logs: {str(e)}")