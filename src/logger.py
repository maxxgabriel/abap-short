"""
Centralized Logging Module
Provides structured logging for ETL operations
"""
import logging
import sys
from datetime import datetime
from typing import Optional, Dict, Any
from logging.handlers import RotatingFileHandler
from pathlib import Path
from dataclasses import dataclass
from enum import Enum

from pyspark.sql import SparkSession


class LogLevel(Enum):
    """Log level enumeration"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class ProcessStep(Enum):
    """ETL process step enumeration"""
    INIT = "INIT"
    EXTRACT = "EXTRACT"
    TRANSFORM = "TRANSFORM"
    LOAD = "LOAD"
    VALIDATE = "VALIDATE"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"


class Status(Enum):
    """Process status enumeration"""
    SUCCESS = "S"
    ERROR = "E"
    WARNING = "W"
    INFO = "I"
    NEW = "N"
    PROCESSED = "P"


@dataclass
class LogEntry:
    """Structured log entry"""
    log_id: str
    etl_run_id: str
    execution_timestamp: datetime
    process_step: str
    status: str
    records_processed: int = 0
    records_success: int = 0
    records_error: int = 0
    message: str = ""
    error_details: Optional[str] = None


class ETLLogger:
    """Centralized ETL logging utility"""
    
    def __init__(
        self,
        etl_run_id: str,
        log_file_path: Optional[str] = None,
        log_level: str = "INFO",
        console_output: bool = True,
        spark: Optional[SparkSession] = None
    ):
        """
        Initialize ETL logger
        
        Args:
            etl_run_id: Unique ETL run identifier
            log_file_path: Path to log file (optional)
            log_level: Logging level
            console_output: Whether to output to console
            spark: SparkSession for distributed logging
        """
        self.etl_run_id = etl_run_id
        self.spark = spark
        self.log_entries = []
        
        # Create logger
        self.logger = logging.getLogger(f"ETL-{etl_run_id}")
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(process_step)s] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Add console handler
        if console_output:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        
        # Add file handler
        if log_file_path:
            log_path = Path(log_file_path)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            file_handler = RotatingFileHandler(
                log_file_path,
                maxBytes=10485760,  # 10MB
                backupCount=5
            )
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
        
        # Set Spark log level if available
        if self.spark:
            self.spark.sparkContext.setLogLevel("WARN")
    
    def _generate_log_id(self) -> str:
        """Generate unique log entry ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"LOG{timestamp}"
    
    def log_message(
        self,
        step: ProcessStep,
        status: Status,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0,
        error_details: Optional[str] = None,
        extra_data: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log a message with structured data
        
        Args:
            step: Process step
            status: Status code
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
            error_details: Detailed error information
            extra_data: Additional data to log
        """
        # Create log entry
        log_entry = LogEntry(
            log_id=self._generate_log_id(),
            etl_run_id=self.etl_run_id,
            execution_timestamp=datetime.now(),
            process_step=step.value,
            status=status.value,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message,
            error_details=error_details
        )
        
        self.log_entries.append(log_entry)
        
        # Prepare extra context
        extra = {
            'process_step': step.value,
            'etl_run_id': self.etl_run_id,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error
        }
        
        if extra_data:
            extra.update(extra_data)
        
        # Log to appropriate level
        if status == Status.ERROR:
            self.logger.error(message, extra=extra, exc_info=error_details is not None)
        elif status == Status.WARNING:
            self.logger.warning(message, extra=extra)
        elif status == Status.INFO:
            self.logger.info(message, extra=extra)
        else:
            self.logger.info(message, extra=extra)
    
    def log_extract_start(self, from_date: str, to_date: str) -> None:
        """Log extraction start"""
        self.log_message(
            step=ProcessStep.EXTRACT,
            status=Status.INFO,
            message=f"Starting extraction from {from_date} to {to_date}"
        )
    
    def log_extract_complete(self, record_count: int) -> None:
        """Log extraction completion"""
        self.log_message(
            step=ProcessStep.EXTRACT,
            status=Status.SUCCESS,
            message=f"Extraction completed successfully",
            records_processed=record_count,
            records_success=record_count
        )
    
    def log_transform_start(self, record_count: int) -> None:
        """Log transformation start"""
        self.log_message(
            step=ProcessStep.TRANSFORM,
            status=Status.INFO,
            message=f"Starting transformation of {record_count} records"
        )
    
    def log_transform_complete(self, success: int, errors: int) -> None:
        """Log transformation completion"""
        self.log_message(
            step=ProcessStep.TRANSFORM,
            status=Status.SUCCESS,
            message="Transformation completed",
            records_processed=success + errors,
            records_success=success,
            records_error=errors
        )
    
    def log_load_start(self, record_count: int) -> None:
        """Log load start"""
        self.log_message(
            step=ProcessStep.LOAD,
            status=Status.INFO,
            message=f"Starting load of {record_count} records"
        )
    
    def log_load_complete(self, success: int, errors: int) -> None:
        """Log load completion"""
        self.log_message(
            step=ProcessStep.LOAD,
            status=Status.SUCCESS,
            message="Load completed",
            records_processed=success + errors,
            records_success=success,
            records_error=errors
        )
    
    def log_error(self, step: ProcessStep, error: Exception) -> None:
        """Log error with exception details"""
        self.log_message(
            step=step,
            status=Status.ERROR,
            message=f"Error in {step.value}: {str(error)}",
            error_details=str(error)
        )
    
    def get_etl_run_id(self) -> str:
        """Get ETL run ID"""
        return self.etl_run_id
    
    def get_log_entries(self) -> list:
        """Get all log entries"""
        return self.log_entries
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get logging statistics"""
        total_processed = sum(entry.records_processed for entry in self.log_entries)
        total_success = sum(entry.records_success for entry in self.log_entries)
        total_errors = sum(entry.records_error for entry in self.log_entries)
        
        error_count = sum(1 for entry in self.log_entries if entry.status == Status.ERROR.value)
        warning_count = sum(1 for entry in self.log_entries if entry.status == Status.WARNING.value)
        
        return {
            'total_log_entries': len(self.log_entries),
            'total_records_processed': total_processed,
            'total_records_success': total_success,
            'total_records_error': total_errors,
            'error_count': error_count,
            'warning_count': warning_count
        }


class LoggerFactory:
    """Factory for creating logger instances"""
    
    @staticmethod
    def create_logger(
        etl_run_id: str,
        config: Optional[Dict[str, Any]] = None,
        spark: Optional[SparkSession] = None
    ) -> ETLLogger:
        """
        Create logger instance with configuration
        
        Args:
            etl_run_id: Unique ETL run identifier
            config: Logging configuration dictionary
            spark: SparkSession instance
            
        Returns:
            ETLLogger instance
        """
        if config is None:
            config = {}
        
        return ETLLogger(
            etl_run_id=etl_run_id,
            log_file_path=config.get('file_path', 'logs/etl.log'),
            log_level=config.get('level', 'INFO'),
            console_output=config.get('console_output', True),
            spark=spark
        )