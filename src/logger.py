"""
Centralized Logging Setup for ETL Operations
Provides structured logging with multiple handlers (console, file, database)
"""

import logging
import sys
from typing import Optional, Dict, Any
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from src.config_manager import get_config


class ETLLogger:
    """
    Centralized logger for ETL operations with multiple output handlers.
    Supports console, file, and database logging.
    """
    
    def __init__(self, etl_run_id: str, spark: Optional[SparkSession] = None):
        """
        Initialize ETL logger
        
        Args:
            etl_run_id: Unique identifier for this ETL run
            spark: SparkSession for database logging
        """
        self.etl_run_id = etl_run_id
        self.spark = spark
        self.config = get_config()
        self.log_config = self.config.get_logging_config()
        
        # Initialize Python logger
        self.logger = self._setup_logger()
        
        # In-memory log buffer for batch database writes
        self._log_buffer = []
        self._log_batch_size = self.log_config.get('database', {}).get('batch_size', 100)
        
    def _setup_logger(self) -> logging.Logger:
        """Setup logging with configured handlers"""
        logger = logging.getLogger(f"ETL_{self.etl_run_id}")
        logger.setLevel(logging.DEBUG)
        logger.handlers.clear()  # Clear existing handlers
        
        formatter = logging.Formatter(
            fmt=self.log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            datefmt=self.log_config.get('date_format', '%Y-%m-%d %H:%M:%S')
        )
        
        # Console handler
        if self.log_config.get('handlers', {}).get('console', {}).get('enabled', True):
            console_handler = logging.StreamHandler(sys.stdout)
            console_level = self.log_config['handlers']['console'].get('level', 'INFO')
            console_handler.setLevel(getattr(logging, console_level))
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        
        # File handler
        if self.log_config.get('handlers', {}).get('file', {}).get('enabled', True):
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)
            
            log_filename = self.log_config['handlers']['file'].get(
                'filename', f'logs/etl_{self.etl_run_id}.log'
            ).format(run_id=self.etl_run_id)
            
            file_handler = RotatingFileHandler(
                log_filename,
                maxBytes=self.log_config['handlers']['file'].get('max_bytes', 10485760),
                backupCount=self.log_config['handlers']['file'].get('backup_count', 5)
            )
            file_level = self.log_config['handlers']['file'].get('level', 'DEBUG')
            file_handler.setLevel(getattr(logging, file_level))
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        return logger
    
    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0,
        extra_data: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log a message with ETL context
        
        Args:
            step: ETL process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
            extra_data: Additional context data
        """
        # Log to Python logger
        log_level = self._get_log_level(status)
        formatted_message = (
            f"[{step}] {message} | "
            f"Processed: {records_processed}, "
            f"Success: {records_success}, "
            f"Errors: {records_error}"
        )
        self.logger.log(log_level, formatted_message, extra=extra_data or {})
        
        # Add to database log buffer
        if self.log_config.get('handlers', {}).get('database', {}).get('enabled', True):
            log_entry = self._create_log_entry(
                step, status, message,
                records_processed, records_success, records_error
            )
            self._log_buffer.append(log_entry)
            
            # Flush if buffer is full
            if len(self._log_buffer) >= self._log_batch_size:
                self.flush_logs()
    
    def _get_log_level(self, status: str) -> int:
        """Map status code to logging level"""
        status_map = {
            'S': logging.INFO,
            'I': logging.INFO,
            'W': logging.WARNING,
            'E': logging.ERROR
        }
        return status_map.get(status, logging.INFO)
    
    def _create_log_entry(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int,
        records_success: int,
        records_error: int
    ) -> Dict[str, Any]:
        """Create log entry dictionary"""
        timestamp = datetime.now()
        log_id = f"LOG{self.etl_run_id}_{timestamp.strftime('%Y%m%d%H%M%S%f')}"
        
        return {
            'log_id': log_id,
            'etl_run_id': self.etl_run_id,
            'execution_date': timestamp.date(),
            'execution_time': timestamp.time(),
            'process_step': step,
            'status': status,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error,
            'message': message[:255],  # Truncate to match ABAP char255
            'created_at': timestamp,
            'created_by': 'pyspark_etl'
        }
    
    def flush_logs(self) -> None:
        """Flush log buffer to database"""
        if not self._log_buffer:
            return
        
        if self.spark is None:
            self.logger.warning("SparkSession not available, cannot flush logs to database")
            self._log_buffer.clear()
            return
        
        try:
            log_schema = StructType([
                StructField("log_id", StringType(), False),
                StructField("etl_run_id", StringType(), False),
                StructField("execution_date", StringType(), False),
                StructField("execution_time", StringType(), False),
                StructField("process_step", StringType(), False),
                StructField("status", StringType(), False),
                StructField("records_processed", IntegerType(), False),
                StructField("records_success", IntegerType(), False),
                StructField("records_error", IntegerType(), False),
                StructField("message", StringType(), True),
                StructField("created_at", TimestampType(), False),
                StructField("created_by", StringType(), False)
            ])
            
            # Convert log entries to DataFrame
            log_data = []
            for entry in self._log_buffer:
                log_data.append({
                    **entry,
                    'execution_date': entry['execution_date'].isoformat(),
                    'execution_time': entry['execution_time'].isoformat()
                })
            
            log_df = self.spark.createDataFrame(log_data, schema=log_schema)
            
            # Write to database
            db_config = self.config.get_database_config('target')
            log_table = self.log_config['handlers']['database'].get('table', 'etl_log')
            
            log_df.write \
                .format("jdbc") \
                .option("url", db_config['jdbc_url']) \
                .option("dbtable", log_table) \
                .option("user", db_config['user']) \
                .option("password", db_config['password']) \
                .option("driver", db_config['driver']) \
                .mode("append") \
                .save()
            
            self.logger.debug(f"Flushed {len(self._log_buffer)} log entries to database")
            self._log_buffer.clear()
            
        except Exception as e:
            self.logger.error(f"Failed to flush logs to database: {e}")
            self._log_buffer.clear()  # Clear to prevent memory buildup
    
    def log_init(self, message: str = "ETL process initialized") -> None:
        """Log initialization message"""
        self.log_message('INIT', 'S', message)
    
    def log_extract(self, message: str, records: int, success: int = 0, errors: int = 0) -> None:
        """Log extraction step"""
        status = 'S' if errors == 0 else 'W' if success > 0 else 'E'
        self.log_message('EXTRACT', status, message, records, success, errors)
    
    def log_transform(self, message: str, records: int, success: int = 0, errors: int = 0) -> None:
        """Log transformation step"""
        status = 'S' if errors == 0 else 'W' if success > 0 else 'E'
        self.log_message('TRANSFORM', status, message, records, success, errors)
    
    def log_load(self, message: str, records: int, success: int = 0, errors: int = 0) -> None:
        """Log load step"""
        status = 'S' if errors == 0 else 'W' if success > 0 else 'E'
        self.log_message('LOAD', status, message, records, success, errors)
    
    def log_error(self, step: str, message: str, exception: Optional[Exception] = None) -> None:
        """Log error message"""
        error_msg = message
        if exception:
            error_msg = f"{message}: {str(exception)}"
        self.log_message(step, 'E', error_msg)
    
    def log_complete(self, message: str = "ETL process completed successfully") -> None:
        """Log completion message"""
        self.log_message('COMPLETE', 'S', message)
        self.flush_logs()  # Ensure all logs are written
    
    def close(self) -> None:
        """Close logger and flush remaining logs"""
        self.flush_logs()
        for handler in self.logger.handlers:
            handler.close()
            self.logger.removeHandler(handler)


def create_logger(etl_run_id: str, spark: Optional[SparkSession] = None) -> ETLLogger:
    """
    Factory function to create ETL logger
    
    Args:
        etl_run_id: Unique ETL run identifier
        spark: Optional SparkSession for database logging
        
    Returns:
        Configured ETLLogger instance
    """
    return ETLLogger(etl_run_id, spark)