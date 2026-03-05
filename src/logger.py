"""
Logging utility for Sales ETL System.
Provides structured logging for ETL processes.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, IntegerType
from datetime import datetime
from typing import Optional
import logging


class ETLLogger:
    """Manages ETL process logging."""
    
    def __init__(self, spark: SparkSession, etl_run_id: str):
        """
        Initialize the ETL logger.
        
        Args:
            spark: Active SparkSession
            etl_run_id: Unique identifier for this ETL run
        """
        self.spark = spark
        self.etl_run_id = etl_run_id
        self.log_entries = []
        
        # Setup standard Python logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    @staticmethod
    def get_log_schema() -> StructType:
        """
        Define schema for log entries.
        
        Returns:
            StructType schema definition
        """
        return StructType([
            StructField("log_id", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("execution_date", DateType(), False),
            StructField("execution_time", TimestampType(), False),
            StructField("process_step", StringType(), False),
            StructField("status", StringType(), False),
            StructField("records_processed", IntegerType(), True),
            StructField("records_success", IntegerType(), True),
            StructField("records_error", IntegerType(), True),
            StructField("message", StringType(), True)
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
        
        Args:
            step: ETL process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S, E, W, I)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        now = datetime.now()
        log_id = self._generate_log_id()
        
        log_entry = {
            "log_id": log_id,
            "etl_run_id": self.etl_run_id,
            "execution_date": now.date(),
            "execution_time": now,
            "process_step": step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message
        }
        
        self.log_entries.append(log_entry)
        
        # Also log to standard output
        log_level = self._get_log_level(status)
        self.logger.log(
            log_level,
            f"[{self.etl_run_id}] [{step}] {message} "
            f"(Processed: {records_processed}, Success: {records_success}, Error: {records_error})"
        )
    
    def _generate_log_id(self) -> str:
        """
        Generate unique log ID.
        
        Returns:
            Unique log identifier
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"LOG{timestamp}"
    
    def _get_log_level(self, status: str) -> int:
        """
        Map status code to Python logging level.
        
        Args:
            status: Status code (S, E, W, I)
        
        Returns:
            Python logging level constant
        """
        mapping = {
            'S': logging.INFO,
            'I': logging.INFO,
            'W': logging.WARNING,
            'E': logging.ERROR
        }
        return mapping.get(status, logging.INFO)
    
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run ID.
        
        Returns:
            ETL run identifier
        """
        return self.etl_run_id
    
    def get_logs_dataframe(self):
        """
        Get all log entries as a DataFrame.
        
        Returns:
            DataFrame with log entries
        """
        if not self.log_entries:
            return self.spark.createDataFrame([], self.get_log_schema())
        
        return self.spark.createDataFrame(self.log_entries, self.get_log_schema())
    
    def persist_logs(self, target_path: Optional[str] = None) -> None:
        """
        Persist log entries to storage.
        
        Args:
            target_path: Optional path to write logs
        """
        logs_df = self.get_logs_dataframe()
        
        if target_path:
            logs_df.write.mode("append").parquet(target_path)
            self.logger.info(f"Logs persisted to {target_path}")
        else:
            self.logger.info("Logs not persisted (no target path specified)")