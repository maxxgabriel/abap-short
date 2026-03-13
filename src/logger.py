"""
ETL Logger utility
Migrated from ABAP ZCL_ETL_LOGGER
"""

import logging
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import Row

from src.config import get_config
from src.schemas import ETLSchemas
from src.utils import generate_unique_id


class ETLLogger:
    """
    Logger for ETL processes
    Corresponds to ZCL_ETL_LOGGER
    """
    
    def __init__(self, etl_run_id: str, spark: SparkSession = None):
        """
        Initialize logger
        
        Args:
            etl_run_id: Unique identifier for this ETL run
            spark: SparkSession instance (optional, for database logging)
        """
        self.etl_run_id = etl_run_id
        self.spark = spark
        self.config = get_config()
        
        # Setup Python logger
        self._setup_python_logger()
        
        # Storage for log entries
        self.log_entries = []
    
    def _setup_python_logger(self):
        """Setup Python logging"""
        logging_config = self.config._config_data.get('logging', {})
        level = logging_config.get('level', 'INFO')
        format_str = logging_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        self.logger = logging.getLogger(f'ETL.{self.etl_run_id}')
        self.logger.setLevel(getattr(logging, level))
        
        # Console handler
        if logging_config.get('handlers', {}).get('console', {}).get('enabled', True):
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter(format_str))
            self.logger.addHandler(console_handler)
        
        # File handler
        file_config = logging_config.get('handlers', {}).get('file', {})
        if file_config.get('enabled', True):
            from logging.handlers import RotatingFileHandler
            file_handler = RotatingFileHandler(
                file_config.get('path', 'logs/etl.log'),
                maxBytes=file_config.get('max_bytes', 10485760),
                backupCount=file_config.get('backup_count', 5)
            )
            file_handler.setFormatter(logging.Formatter(format_str))
            self.logger.addHandler(file_handler)
    
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
        Log a message
        
        Args:
            step: Process step
            status: Status code (S, E, W, I)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        now = datetime.now()
        
        # Create log entry
        log_entry = {
            'log_id': generate_unique_id(self.config.id_prefixes.LOG),
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
            'created_by': 'ETL_SYSTEM'
        }
        
        self.log_entries.append(log_entry)
        
        # Log to Python logger
        log_msg = f"[{step}] [{status}] {message}"
        if records_processed > 0:
            log_msg += f" (Processed: {records_processed}, Success: {records_success}, Error: {records_error})"
        
        if status == self.config.status.ERROR:
            self.logger.error(log_msg)
        elif status == self.config.status.WARNING:
            self.logger.warning(log_msg)
        else:
            self.logger.info(log_msg)
    
    def get_etl_run_id(self) -> str:
        """Get ETL run ID"""
        return self.etl_run_id
    
    def persist_logs(self) -> bool:
        """
        Persist log entries to database/file
        
        Returns:
            True if successful, False otherwise
        """
        if not self.log_entries:
            return True
        
        try:
            if self.spark is not None:
                # Create DataFrame from log entries
                log_df = self.spark.createDataFrame(
                    [Row(**entry) for entry in self.log_entries],
                    schema=ETLSchemas.etl_log_schema()
                )
                
                # Get log data source configuration
                log_config = self.config.get_data_source_config('etl_log')
                
                # Write to configured destination
                if log_config.get('format') == 'jdbc':
                    jdbc_config = log_config['jdbc']
                    log_df.write \
                        .format('jdbc') \
                        .option('url', jdbc_config['url']) \
                        .option('dbtable', jdbc_config['table']) \
                        .option('user', jdbc_config['user']) \
                        .option('password', jdbc_config['password']) \
                        .option('driver', jdbc_config['driver']) \
                        .mode('append') \
                        .save()
                else:
                    log_df.write \
                        .format(log_config['format']) \
                        .mode('append') \
                        .save(log_config['path'])
                
                self.logger.info(f"Persisted {len(self.log_entries)} log entries")
            
            return True
        
        except Exception as e:
            self.logger.error(f"Failed to persist logs: {str(e)}")
            return False
    
    def get_log_entries(self) -> list:
        """Get all log entries"""
        return self.log_entries