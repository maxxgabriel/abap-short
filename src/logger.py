"""
ETL Logger Module
Custom logger for ETL operations
"""
import logging
import sys
from datetime import datetime
from typing import Optional


class ETLLogger:
    """Custom logger for ETL operations with structured logging"""
    
    def __init__(self, etl_run_id: str, log_level: str = "INFO"):
        """
        Initialize the ETL logger
        
        Args:
            etl_run_id: Unique ETL run identifier
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        self.etl_run_id = etl_run_id
        self.logger = self._setup_logger(log_level)
    
    def _setup_logger(self, log_level: str) -> logging.Logger:
        """
        Set up the logger with custom formatting
        
        Args:
            log_level: Logging level
            
        Returns:
            Configured logger instance
        """
        logger = logging.getLogger(f"ETL_{self.etl_run_id}")
        logger.setLevel(getattr(logging, log_level.upper()))
        
        # Remove existing handlers
        logger.handlers = []
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, log_level.upper()))
        
        # Custom formatter
        formatter = logging.Formatter(
            fmt='%(asctime)s | %(etl_run_id)s | %(step)s | %(status)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        
        logger.addHandler(console_handler)
        
        return logger
    
    def get_logger(self) -> logging.Logger:
        """Get the configured logger instance"""
        return logging.LoggerAdapter(
            self.logger,
            {'etl_run_id': self.etl_run_id, 'step': 'UNKNOWN', 'status': 'I'}
        )