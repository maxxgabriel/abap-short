"""
Logging Utilities Module
Provides standardized logging for ETL processes with structured output.
"""

import logging
import sys
from typing import Optional, Dict, Any
from datetime import datetime
from pathlib import Path
import json


class ETLLogger:
    """
    Standardized logger for ETL processes with structured logging support.
    Handles console and file logging with configurable levels.
    """
    
    # Status constants
    STATUS_SUCCESS = 'S'
    STATUS_ERROR = 'E'
    STATUS_WARNING = 'W'
    STATUS_INFO = 'I'
    
    # Step constants
    STEP_INIT = 'INIT'
    STEP_EXTRACT = 'EXTRACT'
    STEP_TRANSFORM = 'TRANSFORM'
    STEP_LOAD = 'LOAD'
    STEP_VALIDATE = 'VALIDATE'
    STEP_COMPLETE = 'COMPLETE'
    STEP_ERROR = 'ERROR'
    
    def __init__(self, etl_run_id: str, log_level: str = 'INFO', log_dir: str = 'logs'):
        """
        Initialize ETL logger.
        
        Args:
            etl_run_id: Unique identifier for the ETL run
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_dir: Directory for log files
        """
        self.etl_run_id = etl_run_id
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create logger
        self.logger = logging.getLogger(f'etl.{etl_run_id}')
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Remove existing handlers
        self.logger.handlers.clear()
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # File handler
        log_file = self.log_dir / f'etl_{etl_run_id}_{datetime.now():%Y%m%d_%H%M%S}.log'
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)
        
        # Structured log file (JSON)
        self.structured_log_file = self.log_dir / f'etl_{etl_run_id}_structured.jsonl'
        
        self.logger.info(f"Logger initialized for ETL run: {etl_run_id}")
    
    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0,
        additional_info: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log a structured ETL message.
        
        Args:
            step: ETL process step
            status: Status code (S, E, W, I)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
            additional_info: Additional information to log
        """
        # Create structured log entry
        log_entry = {
            'etl_run_id': self.etl_run_id,
            'timestamp': datetime.now().isoformat(),
            'step': step,
            'status': status,
            'message': message,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error
        }
        
        if additional_info:
            log_entry['additional_info'] = additional_info
        
        # Write structured log
        with open(self.structured_log_file, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')
        
        # Log to standard logger
        log_level = self._get_log_level(status)
        log_msg = self._format_log_message(step, message, records_processed, records_success, records_error)
        self.logger.log(log_level, log_msg)
    
    def _get_log_level(self, status: str) -> int:
        """Convert status code to logging level."""
        status_map = {
            self.STATUS_SUCCESS: logging.INFO,
            self.STATUS_INFO: logging.INFO,
            self.STATUS_WARNING: logging.WARNING,
            self.STATUS_ERROR: logging.ERROR
        }
        return status_map.get(status, logging.INFO)
    
    def _format_log_message(
        self,
        step: str,
        message: str,
        records_processed: int,
        records_success: int,
        records_error: int
    ) -> str:
        """Format log message with statistics."""
        if records_processed > 0:
            return (f"[{step}] {message} - "
                   f"Processed: {records_processed}, "
                   f"Success: {records_success}, "
                   f"Error: {records_error}")
        return f"[{step}] {message}"
    
    def info(self, message: str, step: str = STEP_INFO) -> None:
        """Log info message."""
        self.log_message(step, self.STATUS_INFO, message)
    
    def warning(self, message: str, step: str = STEP_INFO) -> None:
        """Log warning message."""
        self.log_message(step, self.STATUS_WARNING, message)
    
    def error(self, message: str, step: str = STEP_ERROR, exception: Optional[Exception] = None) -> None:
        """Log error message."""
        if exception:
            message = f"{message}: {str(exception)}"
        self.log_message(step, self.STATUS_ERROR, message)
    
    def success(self, message: str, step: str) -> None:
        """Log success message."""
        self.log_message(step, self.STATUS_SUCCESS, message)
    
    def log_statistics(
        self,
        step: str,
        total: int,
        success: int,
        error: int,
        duration_seconds: Optional[float] = None
    ) -> None:
        """
        Log processing statistics.
        
        Args:
            step: ETL process step
            total: Total records
            success: Successful records
            error: Error records
            duration_seconds: Processing duration
        """
        message = f"Statistics - Total: {total}, Success: {success}, Error: {error}"
        if duration_seconds:
            message += f", Duration: {duration_seconds:.2f}s"
        
        status = self.STATUS_SUCCESS if error == 0 else self.STATUS_WARNING
        self.log_message(
            step=step,
            status=status,
            message=message,
            records_processed=total,
            records_success=success,
            records_error=error
        )
    
    def get_etl_run_id(self) -> str:
        """Get the ETL run identifier."""
        return self.etl_run_id


class LoggerFactory:
    """Factory for creating ETL loggers."""
    
    @staticmethod
    def create_logger(etl_run_id: str, config: Optional[Dict[str, Any]] = None) -> ETLLogger:
        """
        Create a new ETL logger instance.
        
        Args:
            etl_run_id: Unique ETL run identifier
            config: Optional configuration dictionary
            
        Returns:
            ETLLogger instance
        """
        if config is None:
            from src.config_manager import config as cfg
            config = cfg.get_logging_config()
        
        log_level = config.get('level', 'INFO')
        log_dir = config.get('log_dir', 'logs')
        
        return ETLLogger(etl_run_id, log_level, log_dir)