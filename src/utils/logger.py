"""
ETL Logging Utility
Migrated from ZCL_ETL_LOGGER
"""

import logging
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from src.constants import ETLConstants
from src.schemas import ETLSchemas
from src.utils.id_generator import IDGenerator


class ETLLogger:
    """ETL logging utility with DataFrame-based log storage"""
    
    def __init__(self, etl_run_id: str, spark: SparkSession):
        """
        Initialize ETL logger
        
        Args:
            etl_run_id: Unique ETL run identifier
            spark: SparkSession instance
        """
        self.etl_run_id = etl_run_id
        self.spark = spark
        self.id_generator = IDGenerator()
        
        # Setup Python logging
        self.logger = logging.getLogger(f"ETL_{etl_run_id}")
        self._setup_logging()
        
        # Initialize empty log DataFrame
        self.log_records = []
    
    def _setup_logging(self):
        """Setup Python logging configuration"""
        if not self.logger.handlers:
            self.logger.setLevel(logging.INFO)
            
            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
    
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
        Log a message and store in DataFrame
        
        Args:
            step: Process step name
            status: Status code (S/E/W/I)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        now = datetime.now()
        log_id = self.id_generator.generate_log_id()
        
        # Create log record
        log_record = {
            "log_id": log_id,
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
            "created_by": "etl_system"
        }
        
        self.log_records.append(log_record)
        
        # Also log to Python logger
        log_level = {
            ETLConstants.Status.ERROR: logging.ERROR,
            ETLConstants.Status.WARNING: logging.WARNING,
            ETLConstants.Status.INFO: logging.INFO,
            ETLConstants.Status.SUCCESS: logging.INFO,
        }.get(status, logging.INFO)
        
        self.logger.log(
            log_level,
            f"[{step}] {message} | "
            f"Processed: {records_processed}, Success: {records_success}, Error: {records_error}"
        )
    
    def get_log_dataframe(self) -> DataFrame:
        """
        Get all log records as a DataFrame
        
        Returns:
            DataFrame with log records
        """
        if not self.log_records:
            # Return empty DataFrame with proper schema
            return self.spark.createDataFrame([], ETLSchemas.etl_log_schema())
        
        return self.spark.createDataFrame(self.log_records, ETLSchemas.etl_log_schema())
    
    def save_logs(self, target_path: str, mode: str = "append"):
        """
        Save log records to storage
        
        Args:
            target_path: Path to save logs
            mode: Write mode (append/overwrite)
        """
        log_df = self.get_log_dataframe()
        if log_df.count() > 0:
            log_df.write.mode(mode).parquet(target_path)
            self.logger.info(f"Saved {log_df.count()} log records to {target_path}")
    
    def get_etl_run_id(self) -> str:
        """Get the ETL run ID"""
        return self.etl_run_id
    
    def get_statistics(self) -> dict:
        """
        Get aggregated statistics from logs
        
        Returns:
            Dictionary with statistics
        """
        log_df = self.get_log_dataframe()
        
        if log_df.count() == 0:
            return {
                "total_processed": 0,
                "total_success": 0,
                "total_error": 0,
                "error_count": 0,
                "warning_count": 0
            }
        
        stats = log_df.agg({
            "records_processed": "sum",
            "records_success": "sum",
            "records_error": "sum"
        }).collect()[0]
        
        error_count = log_df.filter(log_df.status == ETLConstants.Status.ERROR).count()
        warning_count = log_df.filter(log_df.status == ETLConstants.Status.WARNING).count()
        
        return {
            "total_processed": stats[0] or 0,
            "total_success": stats[1] or 0,
            "total_error": stats[2] or 0,
            "error_count": error_count,
            "warning_count": warning_count
        }