"""Logging utilities for ETL pipeline."""
import logging
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp
import uuid


class ETLLogger:
    """Logger for ETL pipeline with PySpark integration."""
    
    def __init__(self, spark: SparkSession, etl_run_id: str, config: dict):
        """Initialize ETL logger.
        
        Args:
            spark: SparkSession instance
            etl_run_id: Unique identifier for ETL run
            config: Logger configuration
        """
        self.spark = spark
        self.etl_run_id = etl_run_id
        self.config = config
        self.log_entries = []
        
        # Setup Python logger
        self._setup_python_logger()
    
    def _setup_python_logger(self) -> None:
        """Setup Python logging."""
        log_config = self.config.get('logging', {})
        log_level = getattr(logging, log_config.get('level', 'INFO'))
        log_format = log_config.get('format', 
                                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=[
                logging.StreamHandler(),
            ]
        )
        
        if log_config.get('output', {}).get('file', False):
            file_handler = logging.FileHandler(
                log_config.get('output', {}).get('file_path', 'logs/etl_pipeline.log')
            )
            file_handler.setFormatter(logging.Formatter(log_format))
            logging.getLogger().addHandler(file_handler)
        
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0,
        error_details: Optional[str] = None
    ) -> None:
        """Log a message to both Python logger and Spark DataFrame.
        
        Args:
            step: ETL process step
            status: Status code (S, E, W, I)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
            error_details: Detailed error information
        """
        log_id = f"LOG_{uuid.uuid4().hex[:12]}"
        
        # Log to Python logger
        log_method = {
            'S': self.logger.info,
            'E': self.logger.error,
            'W': self.logger.warning,
            'I': self.logger.info
        }.get(status, self.logger.info)
        
        log_method(f"[{step}] {message} - Processed: {records_processed}, "
                  f"Success: {records_success}, Errors: {records_error}")
        
        # Store for Spark logging
        self.log_entries.append({
            'log_id': log_id,
            'etl_run_id': self.etl_run_id,
            'execution_timestamp': datetime.now(),
            'process_step': step,
            'status': status,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error,
            'message': message,
            'error_details': error_details
        })
    
    def flush_logs(self) -> DataFrame:
        """Flush accumulated logs to Spark DataFrame.
        
        Returns:
            DataFrame containing log entries
        """
        if not self.log_entries:
            return self.spark.createDataFrame([], schema="log_id string")
        
        log_df = self.spark.createDataFrame(self.log_entries)
        self.log_entries = []
        return log_df
    
    def save_logs(self, output_path: str) -> None:
        """Save logs to storage.
        
        Args:
            output_path: Path to save logs
        """
        log_df = self.flush_logs()
        if log_df.count() > 0:
            log_df.write.mode('append').parquet(output_path)
            self.logger.info(f"Logs saved to {output_path}")