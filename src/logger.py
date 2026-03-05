"""
ETL logging utility.
Converted from ABAP ZCL_ETL_LOGGER.
"""

from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession

from src.schemas import ETLSchemas, ProcessSteps, StatusCodes


class ETLLogger:
    """Utility class for ETL logging."""

    def __init__(self, spark: SparkSession, etl_run_id: str):
        """
        Initialize logger.
        
        Args:
            spark: SparkSession instance
            etl_run_id: Unique ETL run identifier
        """
        self.spark = spark
        self.etl_run_id = etl_run_id
        self.log_entries = []

    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0,
    ) -> None:
        """
        Log ETL message.
        
        Args:
            step: Process step
            status: Status code
            message: Log message
            records_processed: Total records processed
            records_success: Successful records
            records_error: Error records
        """
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
            "created_by": "SYSTEM",
        }
        
        self.log_entries.append(log_entry)
        
        # Console output
        print(
            f"[{log_entry['execution_time']}] "
            f"{log_entry['process_step']} - "
            f"{log_entry['status']} - "
            f"{log_entry['message']}"
        )

    def save_logs(self, target_path: Optional[str] = None) -> None:
        """
        Persist logs to storage.
        
        Args:
            target_path: Optional path for log storage
        """
        if not self.log_entries:
            return
        
        schema = ETLSchemas.etl_log_schema()
        df_logs = self.spark.createDataFrame(self.log_entries, schema)
        
        if target_path:
            df_logs.write.mode("append").parquet(target_path)
        else:
            # Write to default log table
            df_logs.write.mode("append").saveAsTable("zetl_log")

    def get_etl_run_id(self) -> str:
        """
        Get current ETL run ID.
        
        Returns:
            ETL run identifier
        """
        return self.etl_run_id

    def _generate_log_id(self) -> str:
        """
        Generate unique log ID.
        
        Returns:
            Unique log identifier
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"LOG{timestamp}"