"""
ETL Logging Module
Provides comprehensive logging for ETL processes.
"""

import logging
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession

from src.utils import generate_run_id


class ETLLogger:
    """Logger for ETL processes with database persistence."""

    def __init__(self, spark: SparkSession, run_id: str, config: dict):
        """
        Initialize the ETL logger.

        Args:
            spark: Active SparkSession
            run_id: Unique ETL run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.run_id = run_id
        self.config = config
        
        # Set up Python logging
        self._setup_logging()

    def _setup_logging(self):
        """Configure Python logging."""
        log_level = self.config.get("log_level", "INFO")
        
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        self.logger = logging.getLogger("ETLLogger")

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
        Log an ETL message.

        Args:
            step: Process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        log_entry = {
            "log_id": generate_run_id("LOG"),
            "etl_run_id": self.run_id,
            "execution_timestamp": datetime.now().isoformat(),
            "process_step": step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message
        }

        # Log to console/file
        log_level_map = {
            "S": logging.INFO,
            "I": logging.INFO,
            "W": logging.WARNING,
            "E": logging.ERROR
        }
        
        log_level = log_level_map.get(status, logging.INFO)
        self.logger.log(
            log_level,
            f"[{step}] {message} (Processed: {records_processed}, "
            f"Success: {records_success}, Errors: {records_error})"
        )

        # Persist to database/storage if configured
        if self.config.get("persist_logs", False):
            self._persist_log(log_entry)

    def _persist_log(self, log_entry: dict):
        """
        Persist log entry to database/storage.

        Args:
            log_entry: Log entry dictionary
        """
        try:
            log_path = self.config.get("log_data_path")
            if log_path:
                # Convert to DataFrame and append
                log_df = self.spark.createDataFrame([log_entry])
                log_df.write \
                    .format("parquet") \
                    .mode("append") \
                    .save(log_path)
        except Exception as e:
            self.logger.warning(f"Failed to persist log: {str(e)}")

    def get_run_id(self) -> str:
        """Get the current ETL run ID."""
        return self.run_id