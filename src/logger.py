"""
PySpark Logger Module
Migrated from ZCL_ETL_LOGGER ABAP class
Utility class for ETL logging
"""

from typing import Optional
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import logging


class ETLLogger:
    """
    ETL logging utility.
    Equivalent to ZCL_ETL_LOGGER ABAP class.
    """

    def __init__(self, etl_run_id: str, spark: Optional[SparkSession] = None):
        """
        Initialize logger with ETL run ID.
        
        Args:
            etl_run_id: Unique ETL run identifier
            spark: Optional SparkSession for distributed logging
        """
        self.etl_run_id = etl_run_id
        self.spark = spark or SparkSession.getActiveSession()
        
        # Setup Python logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Setup console handler if not already configured
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        self.log_entries = []

    def get_log_schema(self) -> StructType:
        """
        Define schema for log entries.
        Equivalent to ty_log_entry structure in ABAP.
        
        Returns:
            StructType schema for log DataFrame
        """
        return StructType([
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
            StructField("timestamp", TimestampType(), False)
        ])

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
        Equivalent to log_message method in ABAP.
        
        Args:
            step: Process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S, E, W, I)
            message: Log message text
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        log_id = self._generate_log_id()
        now = datetime.now()
        execution_date = now.strftime("%Y-%m-%d")
        execution_time = now.strftime("%H:%M:%S")

        log_entry = {
            "log_id": log_id,
            "etl_run_id": self.etl_run_id,
            "execution_date": execution_date,
            "execution_time": execution_time,
            "process_step": step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message,
            "timestamp": now
        }

        self.log_entries.append(log_entry)

        # Console output (equivalent to WRITE in ABAP)
        log_line = (
            f"{execution_time} | {step:12} | {status} | "
            f"Records: {records_processed}/{records_success}/{records_error} | "
            f"{message}"
        )
        
        # Use appropriate logging level
        if status == 'E':
            self.logger.error(log_line)
        elif status == 'W':
            self.logger.warning(log_line)
        else:
            self.logger.info(log_line)

    def _generate_log_id(self) -> str:
        """
        Generate unique log ID.
        Equivalent to generate_log_id method in ABAP.
        
        Returns:
            Unique log identifier
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"LOG{timestamp[:14]}"

    def get_etl_run_id(self) -> str:
        """
        Get current ETL run ID.
        Equivalent to get_etl_run_id method in ABAP.
        
        Returns:
            ETL run identifier
        """
        return self.etl_run_id

    def save_logs(self, target_path: str) -> None:
        """
        Save collected logs to target storage.
        Equivalent to INSERT zetl_log in ABAP.
        
        Args:
            target_path: Path to save log data
        """
        if not self.log_entries:
            return

        if self.spark:
            log_df = self.spark.createDataFrame(self.log_entries, schema=self.get_log_schema())
            log_df.write.mode("append").parquet(target_path)
            self.logger.info(f"Saved {len(self.log_entries)} log entries to {target_path}")

    def get_logs_dataframe(self):
        """
        Get logs as DataFrame for analysis.
        
        Returns:
            DataFrame containing all log entries
        """
        if not self.log_entries or not self.spark:
            return None

        return self.spark.createDataFrame(self.log_entries, schema=self.get_log_schema())