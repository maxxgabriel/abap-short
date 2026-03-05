"""
ETL logging utility.
Migrated from ABAP ZCL_ETL_LOGGER class.
"""

import logging
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from src.config_loader import ETLConfig
from src.schemas import ETLSchemas


class ETLLogger:
    """
    Logger for ETL processes.
    Migrated from ABAP ZCL_ETL_LOGGER.
    """
    
    def __init__(self, spark: SparkSession, etl_run_id: str, config: ETLConfig):
        """
        Initialize ETL logger.
        
        Args:
            spark: SparkSession instance
            etl_run_id: Unique identifier for ETL run
            config: ETL configuration
        """
        self.spark = spark
        self.etl_run_id = etl_run_id
        self.config = config
        
        # Set up Python logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(f"ETL.{etl_run_id}")
        
        # Log buffer for batch writing
        self.log_buffer = []
    
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
        Log ETL message (replaces ABAP log_message method).
        
        Args:
            step: Process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S, E, W, I)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        log_entry = {
            "log_id": self._generate_log_id(),
            "etl_run_id": self.etl_run_id,
            "execution_date": datetime.now().date(),
            "execution_time": datetime.now().strftime("%H:%M:%S"),
            "process_step": step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message,
            "created_at": datetime.now(),
            "created_by": "PYSPARK_ETL"
        }
        
        self.log_buffer.append(log_entry)
        
        # Also log to Python logger
        log_level = self._map_status_to_log_level(status)
        self.logger.log(
            log_level,
            f"[{step}] {message} "
            f"(Processed: {records_processed}, "
            f"Success: {records_success}, "
            f"Error: {records_error})"
        )
    
    def flush_logs(self) -> None:
        """
        Write buffered logs to storage.
        """
        if not self.log_buffer:
            return
        
        try:
            log_df = self.spark.createDataFrame(
                self.log_buffer,
                schema=ETLSchemas.etl_log_schema()
            )
            
            # Write logs to storage
            output_path = f"{self.config.log_output_path}/{self.etl_run_id}"
            log_df.write.mode("append").parquet(output_path)
            
            self.logger.info(f"Flushed {len(self.log_buffer)} log entries")
            self.log_buffer.clear()
            
        except Exception as e:
            self.logger.error(f"Failed to flush logs: {str(e)}")
    
    def get_etl_run_id(self) -> str:
        """
        Get ETL run ID (replaces ABAP get_etl_run_id method).
        
        Returns:
            ETL run ID
        """
        return self.etl_run_id
    
    def _generate_log_id(self) -> str:
        """
        Generate unique log ID (replaces ABAP generate_log_id method).
        
        Returns:
            Unique log ID
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"{self.config.prefix_log_id}{timestamp}"
    
    @staticmethod
    def _map_status_to_log_level(status: str) -> int:
        """
        Map ETL status code to Python logging level.
        
        Args:
            status: ETL status code
            
        Returns:
            Python logging level
        """
        mapping = {
            'S': logging.INFO,
            'E': logging.ERROR,
            'W': logging.WARNING,
            'I': logging.INFO
        }
        return mapping.get(status, logging.INFO)