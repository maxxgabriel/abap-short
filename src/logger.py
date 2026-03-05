"""
ETL Logger Module
Utility class for ETL logging
Migrated from ZCL_ETL_LOGGER ABAP class
"""

from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession


class ETLLogger:
    """
    ETL logging utility for tracking process execution.
    Logs messages to console and optionally to persistent storage.
    """

    def __init__(self, etl_run_id: str, spark: SparkSession):
        """
        Initialize logger with ETL run ID.
        
        Args:
            etl_run_id: Unique identifier for this ETL run
            spark: SparkSession for database operations
        """
        self.etl_run_id = etl_run_id
        self.spark = spark

    def log_message(
        self,
        step: str,
        status: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0,
        message: str = ""
    ) -> None:
        """
        Log ETL process message.
        Migrates ABAP log_message method.
        
        Args:
            step: Process step (INIT, EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
            message: Log message text
        """
        # Generate log entry
        log_entry = {
            "log_id": self._generate_log_id(),
            "etl_run_id": self.etl_run_id,
            "execution_date": datetime.now().strftime("%Y-%m-%d"),
            "execution_time": datetime.now().strftime("%H:%M:%S"),
            "process_step": step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message
        }

        # Console output
        status_symbol = {
            "S": "✓",
            "E": "✗",
            "W": "⚠",
            "I": "ℹ"
        }.get(status, "•")

        print(f"[{log_entry['execution_time']}] {status_symbol} {step}: {message}")

        if records_processed > 0:
            print(f"  → Processed: {records_processed}, Success: {records_success}, Errors: {records_error}")

        # In production: Write to log table
        # log_df = self.spark.createDataFrame([log_entry])
        # log_df.write.jdbc(
        #     url="jdbc:...",
        #     table="zetl_log",
        #     mode="append",
        #     properties={...}
        # )

    def _generate_log_id(self) -> str:
        """
        Generate unique log ID based on timestamp.
        Migrates ABAP generate_log_id method.
        
        Returns:
            Unique log ID string
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"LOG{timestamp}"