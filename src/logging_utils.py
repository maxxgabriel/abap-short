"""
Standardized logging utilities for the ETL framework.
Provides centralized logging with multiple handlers and formatters.
"""

import logging
import logging.handlers
from pathlib import Path
from typing import Optional
from datetime import datetime
from pyspark.sql import SparkSession

from src.config import LoggingConfig, get_config_manager


class ETLLogger:
    """
    Centralized logging utility for ETL processes.
    Supports console and file logging with rotation.
    """

    def __init__(
        self,
        name: str,
        etl_run_id: str,
        config: Optional[LoggingConfig] = None
    ):
        """
        Initialize ETL logger.

        Args:
            name: Logger name
            etl_run_id: Unique ETL run identifier
            config: Logging configuration
        """
        self.name = name
        self.etl_run_id = etl_run_id
        self.config = config or get_config_manager().get_logging_config()
        self.logger = self._setup_logger()
        self.log_entries = []

    def _setup_logger(self) -> logging.Logger:
        """Setup and configure logger with handlers."""
        logger = logging.getLogger(self.name)
        logger.setLevel(getattr(logging, self.config.level))

        # Clear existing handlers
        logger.handlers.clear()

        # Create formatter
        formatter = logging.Formatter(self.config.format)

        # Console handler
        if self.config.console_output:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        # File handler with rotation
        log_dir = Path(self.config.log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)

        log_file = log_dir / f"{self.etl_run_id}_{self.config.log_file}"
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=self.config.max_bytes,
            backupCount=self.config.backup_count
        )
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
        extra: Optional[dict] = None
    ) -> None:
        """
        Log ETL message with structured data.

        Args:
            step: ETL process step
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Error records
            extra: Additional metadata
        """
        log_entry = {
            'etl_run_id': self.etl_run_id,
            'timestamp': datetime.now().isoformat(),
            'step': step,
            'status': status,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error,
            'message': message
        }

        if extra:
            log_entry.update(extra)

        self.log_entries.append(log_entry)

        # Log to appropriate level
        log_msg = (
            f"[{self.etl_run_id}] [{step}] [{status}] "
            f"Processed: {records_processed}, Success: {records_success}, "
            f"Errors: {records_error} - {message}"
        )

        if status == 'E':
            self.logger.error(log_msg)
        elif status == 'W':
            self.logger.warning(log_msg)
        elif status == 'S':
            self.logger.info(log_msg)
        else:
            self.logger.debug(log_msg)

    def info(self, step: str, message: str, **kwargs) -> None:
        """Log info message."""
        self.log_message(step, 'I', message, **kwargs)

    def success(self, step: str, message: str, **kwargs) -> None:
        """Log success message."""
        self.log_message(step, 'S', message, **kwargs)

    def warning(self, step: str, message: str, **kwargs) -> None:
        """Log warning message."""
        self.log_message(step, 'W', message, **kwargs)

    def error(self, step: str, message: str, **kwargs) -> None:
        """Log error message."""
        self.log_message(step, 'E', message, **kwargs)

    def log_exception(self, step: str, exception: Exception, **kwargs) -> None:
        """
        Log exception with traceback.

        Args:
            step: ETL process step
            exception: Exception object
            **kwargs: Additional logging parameters
        """
        self.logger.exception(f"[{self.etl_run_id}] [{step}] Exception occurred")
        self.log_message(
            step=step,
            status='E',
            message=f"Exception: {str(exception)}",
            **kwargs
        )

    def get_log_entries(self) -> list:
        """Get all log entries for this run."""
        return self.log_entries.copy()

    def get_etl_run_id(self) -> str:
        """Get ETL run ID."""
        return self.etl_run_id

    def create_child_logger(self, name: str) -> 'ETLLogger':
        """
        Create child logger with same ETL run ID.

        Args:
            name: Child logger name

        Returns:
            New ETLLogger instance
        """
        child_name = f"{self.name}.{name}"
        return ETLLogger(child_name, self.etl_run_id, self.config)


def setup_spark_logging(spark: SparkSession, log_level: str = "WARN") -> None:
    """
    Configure Spark logging level.

    Args:
        spark: SparkSession instance
        log_level: Logging level (ERROR, WARN, INFO, DEBUG)
    """
    spark.sparkContext.setLogLevel(log_level)


def generate_etl_run_id(prefix: str = "ETL") -> str:
    """
    Generate unique ETL run ID.

    Args:
        prefix: ID prefix

    Returns:
        Unique run ID
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
    return f"{prefix}_{timestamp}"