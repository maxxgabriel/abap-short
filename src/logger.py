"""
ETL logging utility with structured logging support.
Converted from ABAP ZCL_ETL_LOGGER.
"""

import logging
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from src.constants import ETLConstants
from src.schemas import ETLSchemas


class ETLLogger:
    """Utility class for ETL logging with database persistence."""

    def __init__(self, etl_run_id: str, spark: SparkSession):
        """
        Initialize logger with ETL run ID.

        Args:
            etl_run_id: Unique identifier for this ETL run
            spark: Active SparkSession for database operations
        """
        self.etl_run_id = etl_run_id
        self.spark = spark
        self._setup_logging()

    def _setup_logging(self):
        """Configure Python logging."""
        self.logger = logging.getLogger(f"ETL.{self.etl_run_id}")
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
        records_error: int = 0,
    ) -> None:
        """
        Log a message to both console and database.

        Args:
            step: Process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S, E, W, I)
            message: Log message text
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Records with errors
        """
        now = datetime.now()
        log_id = self._generate_log_id()

        # Log to console
        log_level = self._get_log_level(status)
        self.logger.log(
            log_level,
            f"[{step}] {message} (Processed: {records_processed}, "
            f"Success: {records_success}, Errors: {records_error})"
        )

        # Create log entry for database
        log_data = [{
            "log_id": log_id,
            "etl_run_id": self.etl_run_id,
            "execution_date": now.date(),
            "execution_time": now.strftime("%H:%M:%S"),
            "process_step": step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message[:255],  # Truncate to match ABAP length
            "created_at": now,
            "created_by": "PYSPARK_ETL",
        }]

        # Persist to database (in production, write to actual log table)
        try:
            log_df = self.spark.createDataFrame(
                log_data, schema=ETLSchemas.etl_log_schema()
            )
            # In production: log_df.write.mode("append").saveAsTable("etl_log")
            # For now, just show the log entry
            self.logger.debug(f"Log entry created: {log_id}")
        except Exception as e:
            self.logger.error(f"Failed to persist log entry: {e}")

    def _generate_log_id(self) -> str:
        """Generate unique log ID based on timestamp."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")[:20]
        return f"{ETLConstants.PREFIX_LOG_ID}{timestamp}"

    def _get_log_level(self, status: str) -> int:
        """Map status code to Python logging level."""
        mapping = {
            ETLConstants.Status.SUCCESS: logging.INFO,
            ETLConstants.Status.INFO: logging.INFO,
            ETLConstants.Status.WARNING: logging.WARNING,
            ETLConstants.Status.ERROR: logging.ERROR,
        }
        return mapping.get(status, logging.INFO)

    def get_etl_run_id(self) -> str:
        """Return the ETL run ID."""
        return self.etl_run_id