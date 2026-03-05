"""
ETL Logger Module - PySpark Implementation
Handles logging and metrics tracking for ETL processes.
"""

from typing import Optional, Dict, Any
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import logging


class ETLLogger:
    """
    Logger for ETL processes with structured logging to table and console.
    """
    
    def __init__(self, spark: SparkSession, etl_run_id: str, config: Dict[str, Any]):
        """
        Initialize ETL Logger.
        
        Args:
            spark: Active SparkSession
            etl_run_id: Unique identifier for this ETL run
            config: Configuration dictionary
        """
        self.spark = spark
        self.etl_run_id = etl_run_id
        self.config = config
        self.log_table = config.get('logging', {}).get('log_table', 'etl_log')
        self.log_entries = []
        
        # Configure Python logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        logging.info(f"ETLLogger initialized with run_id: {etl_run_id}")
    
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
        Log a message with structured metadata.
        
        Args:
            step: ETL process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message text
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Failed records
        """
        log_entry = {
            'log_id': self._generate_log_id(),
            'etl_run_id': self.etl_run_id,
            'execution_timestamp': datetime.now(),
            'process_step': step,
            'status': status,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error,
            'message': message
        }
        
        self.log_entries.append(log_entry)
        
        # Console logging
        level_map = {'S': logging.INFO, 'E': logging.ERROR, 
                    'W': logging.WARNING, 'I': logging.INFO}
        log_level = level_map.get(status, logging.INFO)
        
        logging.log(
            log_level,
            f"[{step}] {message} | Processed: {records_processed}, "
            f"Success: {records_success}, Errors: {records_error}"
        )
    
    def get_etl_run_id(self) -> str:
        """Get the current ETL run ID."""
        return self.etl_run_id
    
    def flush_logs(self) -> None:
        """Write accumulated logs to log table."""
        if not self.log_entries:
            return
        
        try:
            log_df = self.spark.createDataFrame(self.log_entries, schema=self._get_log_schema())
            
            log_format = self.config.get('logging', {}).get('format', 'parquet')
            log_path = self.config.get('logging', {}).get('path')
            
            if log_path:
                log_df.write.mode('append').format(log_format).save(log_path)
                logging.info(f"Flushed {len(self.log_entries)} log entries")
            
            self.log_entries.clear()
            
        except Exception as e:
            logging.error(f"Failed to flush logs: {str(e)}")
    
    def _generate_log_id(self) -> str:
        """Generate unique log entry ID."""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f"LOG{timestamp}"
    
    def _get_log_schema(self) -> StructType:
        """Get schema for log entries."""
        return StructType([
            StructField("log_id", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("execution_timestamp", TimestampType(), False),
            StructField("process_step", StringType(), False),
            StructField("status", StringType(), False),
            StructField("records_processed", IntegerType(), True),
            StructField("records_success", IntegerType(), True),
            StructField("records_error", IntegerType(), True),
            StructField("message", StringType(), True)
        ])