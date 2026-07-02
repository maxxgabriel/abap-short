"""
Centralized logging setup for ETL framework.
Provides consistent logging configuration across all modules.
"""

import logging
import logging.handlers
from typing import Optional
from pathlib import Path
from datetime import datetime
import sys


class ETLLogger:
    """
    Centralized logger for ETL operations.
    Configures logging with file and console handlers.
    """
    
    _instance: Optional['ETLLogger'] = None
    _initialized: bool = False
    
    def __new__(cls):
        """Implement singleton pattern."""
        if cls._instance is None:
            cls._instance = super(ETLLogger, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize logger (only once)."""
        if not self._initialized:
            self.logger = logging.getLogger('etl')
            self.logger.setLevel(logging.DEBUG)
            self.logger.propagate = False
            self._initialized = True
    
    def setup_logging(
        self,
        log_level: str = 'INFO',
        log_dir: Optional[str] = None,
        enable_console: bool = True,
        enable_file: bool = True,
        max_bytes: int = 10485760,  # 10MB
        backup_count: int = 5
    ) -> None:
        """
        Configure logging with file and console handlers.
        
        Args:
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_dir: Directory for log files
            enable_console: Enable console output
            enable_file: Enable file output
            max_bytes: Maximum log file size before rotation
            backup_count: Number of backup files to keep
        """
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # Set log level
        level = getattr(logging, log_level.upper(), logging.INFO)
        self.logger.setLevel(level)
        
        # Create formatter
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console handler
        if enable_console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(level)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        
        # File handler with rotation
        if enable_file:
            if log_dir is None:
                log_dir = Path(__file__).parent.parent / "logs"
            else:
                log_dir = Path(log_dir)
            
            log_dir.mkdir(parents=True, exist_ok=True)
            
            # Create log file with timestamp
            timestamp = datetime.now().strftime('%Y%m%d')
            log_file = log_dir / f"etl_{timestamp}.log"
            
            file_handler = logging.handlers.RotatingFileHandler(
                filename=log_file,
                maxBytes=max_bytes,
                backupCount=backup_count
            )
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            
            self.logger.info(f"Logging to file: {log_file}")
    
    def get_logger(self, name: Optional[str] = None) -> logging.Logger:
        """
        Get logger instance.
        
        Args:
            name: Optional logger name for submodules
            
        Returns:
            Logger instance
        """
        if name:
            return logging.getLogger(f'etl.{name}')
        return self.logger


class ETLLogManager:
    """
    Database-backed logging manager for ETL execution tracking.
    Records ETL run statistics and step-level details.
    """
    
    def __init__(self, etl_run_id: str):
        """
        Initialize log manager.
        
        Args:
            etl_run_id: Unique identifier for this ETL run
        """
        self.etl_run_id = etl_run_id
        self.logger = logging.getLogger('etl.log_manager')
        self.log_entries = []
    
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
        Log ETL step message.
        
        Args:
            step: ETL step name (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        log_entry = {
            'log_id': self._generate_log_id(),
            'etl_run_id': self.etl_run_id,
            'execution_date': datetime.now().date(),
            'execution_time': datetime.now().time(),
            'process_step': step,
            'status': status,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error,
            'message': message,
            'created_at': datetime.now()
        }
        
        self.log_entries.append(log_entry)
        
        # Also log to standard logger
        log_level = {
            'S': logging.INFO,
            'E': logging.ERROR,
            'W': logging.WARNING,
            'I': logging.INFO
        }.get(status, logging.INFO)
        
        self.logger.log(
            log_level,
            f"[{step}] {message} - Processed: {records_processed}, "
            f"Success: {records_success}, Error: {records_error}"
        )
    
    def _generate_log_id(self) -> str:
        """Generate unique log ID."""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f"LOG{timestamp}"
    
    def get_log_entries(self):
        """Get all log entries as list of dictionaries."""
        return self.log_entries
    
    def save_to_database(self, spark_session) -> None:
        """
        Save log entries to database.
        
        Args:
            spark_session: Active Spark session
        """
        if not self.log_entries:
            self.logger.warning("No log entries to save")
            return
        
        try:
            from pyspark.sql import Row
            
            # Convert log entries to DataFrame
            log_rows = [Row(**entry) for entry in self.log_entries]
            log_df = spark_session.createDataFrame(log_rows)
            
            # Write to database (implementation depends on target database)
            # log_df.write.jdbc(url, table="etl_log", mode="append", properties=props)
            
            self.logger.info(f"Saved {len(self.log_entries)} log entries to database")
            
        except Exception as e:
            self.logger.error(f"Failed to save log entries: {e}")


def setup_logging(config_manager=None):
    """
    Setup logging using configuration.
    
    Args:
        config_manager: ConfigManager instance (optional)
    """
    etl_logger = ETLLogger()
    
    if config_manager:
        log_config = config_manager.get_logging_config()
        etl_logger.setup_logging(
            log_level=log_config.get('level', 'INFO'),
            log_dir=log_config.get('directory'),
            enable_console=log_config.get('enable_console', True),
            enable_file=log_config.get('enable_file', True)
        )
    else:
        # Default configuration
        etl_logger.setup_logging()
    
    return etl_logger.get_logger()


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get logger instance.
    
    Args:
        name: Optional logger name
        
    Returns:
        Logger instance
    """
    etl_logger = ETLLogger()
    return etl_logger.get_logger(name)