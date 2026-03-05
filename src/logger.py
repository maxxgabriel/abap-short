"""
ETL Logger Module - PySpark Implementation
Provides structured logging for ETL operations.
"""

from pyspark.sql import SparkSession
from datetime import datetime
from typing import Optional
import uuid
import logging


class ETLLogger:
    """
    Logger for ETL operations with structured output.
    
    Tracks:
    - Process steps
    - Status codes
    - Record counts
    - Messages and errors
    """
    
    def __init__(self, spark: SparkSession, etl_run_id: str, config: dict):
        """
        Initialize the logger.
        
        Args:
            spark: Active SparkSession
            etl_run_id: Unique ETL run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.etl_run_id = etl_run_id
        self.config = config
        self.log_path = config.get('logging', {}).get('log_path', '/tmp/etl_logs')
        
        # Setup Python logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('ETL')
    
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
            step: Process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        log_entry = {
            'log_id': self._generate_log_id(),
            'etl_run_id': self.etl_run_id,
            'execution_date': datetime.now().strftime('%Y-%m-%d'),
            'execution_time': datetime.now().strftime('%H:%M:%S'),
            'process_step': step,
            'status': status,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error,
            'message': message,
            'created_at': datetime.now().isoformat()
        }
        
        # Log to Python logger
        log_level = self._get_log_level(status)
        self.logger.log(
            log_level,
            f"[{step}] {message} | Processed: {records_processed}, "
            f"Success: {records_success}, Errors: {records_error}"
        )
        
        # Optionally persist to Spark DataFrame/Table
        if self.config.get('logging', {}).get('persist_logs', False):
            self._persist_log_entry(log_entry)
    
    def _generate_log_id(self) -> str:
        """Generate unique log entry ID."""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"LOG{timestamp}{str(uuid.uuid4())[:6]}"
    
    def _get_log_level(self, status: str) -> int:
        """Map status code to Python logging level."""
        mapping = {
            'S': logging.INFO,
            'E': logging.ERROR,
            'W': logging.WARNING,
            'I': logging.INFO
        }
        return mapping.get(status, logging.INFO)
    
    def _persist_log_entry(self, log_entry: dict) -> None:
        """Persist log entry to storage."""
        try:
            df = self.spark.createDataFrame([log_entry])
            df.write.mode('append').format('parquet').save(self.log_path)
        except Exception as e:
            self.logger.error(f"Failed to persist log entry: {str(e)}")
    
    def get_etl_run_id(self) -> str:
        """Get the current ETL run ID."""
        return self.etl_run_id