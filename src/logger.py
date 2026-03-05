"""
ETL Logger Component

Handles logging of ETL process steps and statistics.
Converted from ABAP ZCL_ETL_LOGGER class.
"""

import logging
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp

from src.schemas import ETLSchemas
from src.constants import ETLConstants


class ETLLogger:
    """Logger for ETL operations with database persistence"""

    def __init__(self, spark: SparkSession, etl_run_id: str, log_table: str):
        """
        Initialize ETL logger
        
        Args:
            spark: SparkSession instance
            etl_run_id: Unique ETL run identifier
            log_table: Target log table path/name
        """
        self.spark = spark
        self.etl_run_id = etl_run_id
        self.log_table = log_table
        self.schema = ETLSchemas.etl_log_schema()
        
        # Set up Python logging
        self.logger = logging.getLogger(__name__)
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
        Log a message to both console and database
        
        Args:
            step: Process step name
            status: Status code (S/E/W/I)
            message: Log message text
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        now = datetime.now()
        log_id = self._generate_log_id()
        
        # Console logging
        log_level = self._get_log_level(status)
        self.logger.log(
            log_level,
            f"[{step}] {message} | Processed: {records_processed}, "
            f"Success: {records_success}, Error: {records_error}"
        )
        
        # Create log entry DataFrame
        log_data = [(
            log_id,
            self.etl_run_id,
            now.date(),
            now.strftime("%H:%M:%S"),
            step,
            status,
            records_processed,
            records_success,
            records_error,
            message,
            now,
            "etl_system"
        )]
        
        log_df = self.spark.createDataFrame(log_data, schema=self.schema)
        
        # Persist to database/storage
        try:
            log_df.write.mode("append").format("delta").saveAsTable(self.log_table)
        except Exception as e:
            self.logger.error(f"Failed to persist log entry: {str(e)}")

    def _generate_log_id(self) -> str:
        """Generate unique log ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"{ETLConstants.PREFIX_LOG_ID}{timestamp}"

    def _get_log_level(self, status: str) -> int:
        """Map status code to Python logging level"""
        mapping = {
            ETLConstants.Status.ERROR: logging.ERROR,
            ETLConstants.Status.WARNING: logging.WARNING,
            ETLConstants.Status.SUCCESS: logging.INFO,
            ETLConstants.Status.INFO: logging.INFO,
        }
        return mapping.get(status, logging.INFO)

    def get_etl_run_id(self) -> str:
        """Get the current ETL run ID"""
        return self.etl_run_id

    def get_logs(self) -> DataFrame:
        """Retrieve logs for current ETL run"""
        return (
            self.spark.table(self.log_table)
            .filter(f"etl_run_id = '{self.etl_run_id}'")
            .orderBy("created_at")
        )

    def get_statistics(self) -> dict:
        """Get aggregated statistics for current ETL run"""
        stats_df = (
            self.spark.table(self.log_table)
            .filter(f"etl_run_id = '{self.etl_run_id}'")
            .agg({
                "records_processed": "sum",
                "records_success": "sum",
                "records_error": "sum"
            })
        ).collect()
        
        if stats_df:
            row = stats_df[0]
            return {
                "total_processed": row[0] or 0,
                "total_success": row[1] or 0,
                "total_error": row[2] or 0
            }
        return {"total_processed": 0, "total_success": 0, "total_error": 0}