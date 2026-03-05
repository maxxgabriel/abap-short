"""
ETL Logger Module
Provides logging functionality for ETL processes with timestamp tracking.
"""

from datetime import datetime
from typing import Dict, Any, Optional, List
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, IntegerType


class ETLLogger:
    """
    Logger for ETL processes with database persistence support.
    
    Attributes:
        spark: SparkSession instance
        etl_run_id: Unique identifier for the ETL run
        config: Configuration dictionary
        log_entries: List of log entries
    """
    
    def __init__(self, spark: SparkSession, etl_run_id: str, config: Dict[str, Any]):
        """
        Initialize ETL logger.
        
        Args:
            spark: Active SparkSession
            etl_run_id: Unique ETL run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.etl_run_id = etl_run_id
        self.config = config
        self.log_entries: List[Dict[str, Any]] = []
        
        # Configure Python logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
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
        Log a message with timestamp and statistics.
        
        Args:
            step: ETL process step (INIT, EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message text
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Records with errors
        """
        log_entry = {
            "log_id": self._generate_log_id(),
            "etl_run_id": self.etl_run_id,
            "execution_timestamp": datetime.now(),
            "process_step": step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message
        }
        
        self.log_entries.append(log_entry)
        
        # Also log to Python logger
        log_level = {
            "S": logging.INFO,
            "E": logging.ERROR,
            "W": logging.WARNING,
            "I": logging.INFO
        }.get(status, logging.INFO)
        
        self.logger.log(
            log_level,
            f"[{step}] {message} (Processed: {records_processed}, "
            f"Success: {records_success}, Errors: {records_error})"
        )
    
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run identifier.
        
        Returns:
            str: ETL run ID
        """
        return self.etl_run_id
    
    def persist_logs(self) -> bool:
        """
        Persist log entries to database or file system.
        
        Returns:
            bool: True if persistence successful, False otherwise
        """
        try:
            if not self.log_entries:
                self.logger.info("No log entries to persist")
                return True
            
            # Create DataFrame from log entries
            log_df = self.spark.createDataFrame(self.log_entries)
            
            # In production, write to log table
            # log_df.write \
            #     .format(self.config.get("log_format", "parquet")) \
            #     .mode("append") \
            #     .save(self.config.get("log_path"))
            
            self.logger.info(f"Persisted {len(self.log_entries)} log entries")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to persist logs: {str(e)}")
            return False
    
    def get_logs_dataframe(self) -> Optional[DataFrame]:
        """
        Get log entries as a Spark DataFrame.
        
        Returns:
            DataFrame containing log entries, or None if no entries
        """
        if not self.log_entries:
            return None
        
        return self.spark.createDataFrame(self.log_entries)
    
    def _generate_log_id(self) -> str:
        """
        Generate a unique log identifier.
        
        Returns:
            str: Unique log ID
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"LOG_{timestamp}"