"""
ETL Logger Implementation
Migrated from ZCL_ETL_LOGGER
"""

from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import Row
import logging

from src.constants import ETLConstants
from src.types import ETLSchemas


class ETLLogger:
    """
    Logger for ETL process
    Migrated from ZCL_ETL_LOGGER
    """
    
    def __init__(self, etl_run_id: str, spark: Optional[SparkSession] = None):
        """
        Initialize ETL Logger
        
        Args:
            etl_run_id: Unique identifier for this ETL run
            spark: SparkSession instance
        """
        self.etl_run_id = etl_run_id
        self.spark = spark
        self.constants = ETLConstants()
        
        # Setup Python logging
        self._setup_logging()
        
    def _setup_logging(self):
        """Configure Python logging"""
        log_format = self.constants.get('logging.format', 
                                        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log_level = self.constants.get('logging.level', 'INFO')
        
        logging.basicConfig(
            level=getattr(logging, log_level),
            format=log_format
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
        Log ETL message
        
        Args:
            step: Process step name
            status: Status code
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        now = datetime.now()
        log_id = self._generate_log_id()
        
        log_entry = {
            'log_id': log_id,
            'etl_run_id': self.etl_run_id,
            'execution_date': now.date(),
            'execution_time': now.strftime('%H:%M:%S'),
            'process_step': step,
            'status': status,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error,
            'message': message,
            'created_at': now,
            'created_by': 'ETL_SYSTEM',
        }
        
        # Log to console
        log_level = self._get_log_level(status)
        self.logger.log(
            log_level,
            f"[{step}] {message} | Processed: {records_processed}, "
            f"Success: {records_success}, Errors: {records_error}"
        )
        
        # Persist to database if Spark session available
        if self.spark:
            self._persist_log_entry(log_entry)
            
    def _generate_log_id(self) -> str:
        """Generate unique log ID"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f"LOG{timestamp}"
        
    def _get_log_level(self, status: str) -> int:
        """Map status code to logging level"""
        status_map = {
            ETLConstants.STATUS.ERROR: logging.ERROR,
            ETLConstants.STATUS.WARNING: logging.WARNING,
            ETLConstants.STATUS.INFO: logging.INFO,
            ETLConstants.STATUS.SUCCESS: logging.INFO,
        }
        return status_map.get(status, logging.INFO)
        
    def _persist_log_entry(self, log_entry: dict):
        """Persist log entry to database"""
        try:
            log_df = self.spark.createDataFrame(
                [Row(**log_entry)],
                schema=ETLSchemas.etl_log_schema()
            )
            
            # In production, write to actual log table
            table_name = self.constants.get('database.log.table_name', 'zetl_log')
            log_df.write.mode('append').saveAsTable(table_name)
            
        except Exception as e:
            self.logger.error(f"Failed to persist log entry: {e}")
            
    def get_etl_run_id(self) -> str:
        """Get the ETL run ID"""
        return self.etl_run_id