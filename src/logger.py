"""
Logger Module
Centralized logging utilities for ETL system.
"""

import logging
import sys
from datetime import datetime
from typing import Optional, Dict, Any
from pathlib import Path
import json


class ETLLogger:
    """Enhanced logger for ETL processes with structured logging."""
    
    def __init__(
        self,
        name: str,
        etl_run_id: str,
        log_level: str = "INFO",
        log_file: Optional[str] = None
    ):
        """
        Initialize ETL logger.
        
        Args:
            name: Logger name
            etl_run_id: Unique ETL run identifier
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_file: Optional log file path
        """
        self.etl_run_id = etl_run_id
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Remove existing handlers
        self.logger.handlers.clear()
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - [%(etl_run_id)s] - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # File handler if specified
        if log_file:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
        
        # Statistics tracking
        self._stats: Dict[str, Any] = {
            "total_records": 0,
            "success_records": 0,
            "error_records": 0,
            "warning_records": 0,
            "start_time": None,
            "end_time": None
        }
    
    def _log_with_context(self, level: int, message: str, extra: Optional[Dict] = None) -> None:
        """Internal method to log with ETL context."""
        context = {"etl_run_id": self.etl_run_id}
        if extra:
            context.update(extra)
        
        self.logger.log(level, message, extra=context)
    
    def info(self, message: str, **kwargs) -> None:
        """Log info message."""
        self._log_with_context(logging.INFO, message, kwargs)
    
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message."""
        self._log_with_context(logging.DEBUG, message, kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message."""
        self._log_with_context(logging.WARNING, message, kwargs)
        self._stats["warning_records"] += 1
    
    def error(self, message: str, **kwargs) -> None:
        """Log error message."""
        self._log_with_context(logging.ERROR, message, kwargs)
        self._stats["error_records"] += 1
    
    def critical(self, message: str, **kwargs) -> None:
        """Log critical message."""
        self._log_with_context(logging.CRITICAL, message, kwargs)
    
    def log_step(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ) -> None:
        """
        Log ETL step with statistics.
        
        Args:
            step: ETL step name (EXTRACT, TRANSFORM, LOAD)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Records with errors
        """
        log_entry = {
            "step": step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "timestamp": datetime.now().isoformat()
        }
        
        # Update statistics
        self._stats["total_records"] += records_processed
        self._stats["success_records"] += records_success
        self._stats["error_records"] += records_error
        
        # Choose log level based on status
        if status == 'E':
            self.error(f"[{step}] {message}", **log_entry)
        elif status == 'W':
            self.warning(f"[{step}] {message}", **log_entry)
        else:
            self.info(f"[{step}] {message}", **log_entry)
    
    def start_etl(self) -> None:
        """Mark ETL process start."""
        self._stats["start_time"] = datetime.now()
        self.info(f"ETL process started - Run ID: {self.etl_run_id}")
    
    def end_etl(self, success: bool = True) -> None:
        """
        Mark ETL process end.
        
        Args:
            success: Whether ETL completed successfully
        """
        self._stats["end_time"] = datetime.now()
        
        duration = None
        if self._stats["start_time"] and self._stats["end_time"]:
            duration = (self._stats["end_time"] - self._stats["start_time"]).total_seconds()
        
        status_msg = "completed successfully" if success else "failed"
        
        self.info(
            f"ETL process {status_msg}",
            duration_seconds=duration,
            **self._stats
        )
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get ETL execution statistics."""
        return self._stats.copy()
    
    def log_dataframe_info(self, df, name: str) -> None:
        """
        Log DataFrame information.
        
        Args:
            df: PySpark DataFrame
            name: Name/description of the DataFrame
        """
        count = df.count()
        columns = df.columns
        
        self.info(
            f"DataFrame [{name}] - Rows: {count}, Columns: {len(columns)}",
            dataframe_name=name,
            row_count=count,
            column_count=len(columns),
            columns=columns
        )


class LoggerFactory:
    """Factory for creating ETL loggers."""
    
    @staticmethod
    def create_logger(
        etl_run_id: str,
        module_name: str,
        config: Optional[Dict[str, Any]] = None
    ) -> ETLLogger:
        """
        Create ETL logger instance.
        
        Args:
            etl_run_id: Unique ETL run identifier
            module_name: Name of the module using the logger
            config: Optional logging configuration
            
        Returns:
            ETLLogger instance
        """
        if config is None:
            config = {}
        
        log_level = config.get("level", "INFO")
        log_dir = config.get("log_dir", "logs")
        
        # Create log file path with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"{log_dir}/etl_{module_name}_{timestamp}.log"
        
        return ETLLogger(
            name=module_name,
            etl_run_id=etl_run_id,
            log_level=log_level,
            log_file=log_file
        )
    
    @staticmethod
    def generate_run_id() -> str:
        """Generate unique ETL run ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"ETL{timestamp}"