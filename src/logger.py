"""
ETL Logger
Centralized logging infrastructure with multiple outputs
"""

import logging
import sys
from typing import Optional, Dict, Any
from datetime import datetime
from pathlib import Path
import json
from logging.handlers import RotatingFileHandler

from src.config_manager import config


class ETLLogger:
    """Enhanced logging with file, console, and database output"""
    
    def __init__(self, name: str, run_id: str):
        """
        Initialize ETL logger
        
        Args:
            name: Logger name (usually module name)
            run_id: ETL run identifier
        """
        self.name = name
        self.run_id = run_id
        self.logger = logging.getLogger(name)
        
        # Prevent duplicate handlers
        if not self.logger.handlers:
            self._setup_logger()
        
        self.log_buffer = []
        self.buffer_size = config.get('logging.database.batch_size', 100)
    
    def _setup_logger(self) -> None:
        """Setup logger with configured handlers"""
        log_config = config.get_logging_config()
        
        # Set log level
        level_str = log_config.get('level', 'INFO')
        level = getattr(logging, level_str.upper(), logging.INFO)
        self.logger.setLevel(level)
        
        # Create formatter
        log_format = log_config.get('format', 
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        date_format = log_config.get('date_format', '%Y-%m-%d %H:%M:%S')
        formatter = logging.Formatter(log_format, date_format)
        
        # Console handler
        if log_config.get('console.enabled', True):
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            
            # Add color if configured
            if log_config.get('console.colored', True):
                try:
                    import colorlog
                    color_formatter = colorlog.ColoredFormatter(
                        '%(log_color)s' + log_format,
                        datefmt=date_format,
                        log_colors={
                            'DEBUG': 'cyan',
                            'INFO': 'green',
                            'WARNING': 'yellow',
                            'ERROR': 'red',
                            'CRITICAL': 'red,bg_white',
                        }
                    )
                    console_handler.setFormatter(color_formatter)
                except ImportError:
                    pass  # colorlog not available, use standard formatter
            
            self.logger.addHandler(console_handler)
        
        # File handler
        if log_config.get('file.enabled', True):
            log_path_template = log_config.get('file.path', 'logs/etl_{run_id}.log')
            log_path = log_path_template.format(run_id=self.run_id)
            
            # Create logs directory if needed
            Path(log_path).parent.mkdir(parents=True, exist_ok=True)
            
            max_bytes = log_config.get('file.max_bytes', 10485760)  # 10MB
            backup_count = log_config.get('file.backup_count', 5)
            
            file_handler = RotatingFileHandler(
                log_path,
                maxBytes=max_bytes,
                backupCount=backup_count
            )
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
    
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
            status: Status code (S, E, W, I)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
            extra_data: Additional data to log
        """
        # Create structured log entry
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'run_id': self.run_id,
            'step': step,
            'status': status,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error,
            'message': message
        }
        
        if extra_data:
            log_entry['extra'] = extra_data
        
        # Add to buffer for database logging
        self.log_buffer.append(log_entry)
        
        # Log to file/console based on status
        log_msg = (f"[{step}] {message} "
                  f"(Processed: {records_processed}, "
                  f"Success: {records_success}, "
                  f"Errors: {records_error})")
        
        if status == 'E':
            self.logger.error(log_msg)
        elif status == 'W':
            self.logger.warning(log_msg)
        elif status == 'I':
            self.logger.info(log_msg)
        else:  # Success
            self.logger.info(log_msg)
        
        # Flush buffer if needed
        if len(self.log_buffer) >= self.buffer_size:
            self.flush_logs()
    
    def info(self, message: str, **kwargs) -> None:
        """Log info message"""
        self.log_message('INFO', 'I', message, **kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message"""
        self.log_message('WARNING', 'W', message, **kwargs)
    
    def error(self, message: str, **kwargs) -> None:
        """Log error message"""
        self.log_message('ERROR', 'E', message, **kwargs)
    
    def success(self, message: str, **kwargs) -> None:
        """Log success message"""
        self.log_message('SUCCESS', 'S', message, **kwargs)
    
    def flush_logs(self) -> None:
        """Flush log buffer to database"""
        if not self.log_buffer:
            return
        
        log_config = config.get_logging_config()
        if not log_config.get('database.enabled', True):
            self.log_buffer.clear()
            return
        
        try:
            # In production, this would write to database
            # For now, write to JSON file for testing
            log_file = Path(f'logs/etl_logs_{self.run_id}.json')
            log_file.parent.mkdir(parents=True, exist_ok=True)
            
            existing_logs = []
            if log_file.exists():
                with open(log_file, 'r') as f:
                    try:
                        existing_logs = json.load(f)
                    except json.JSONDecodeError:
                        existing_logs = []
            
            existing_logs.extend(self.log_buffer)
            
            with open(log_file, 'w') as f:
                json.dump(existing_logs, f, indent=2)
            
            self.log_buffer.clear()
            
        except Exception as e:
            self.logger.error(f"Failed to flush logs to database: {str(e)}")
    
    def get_run_id(self) -> str:
        """Get the ETL run ID"""
        return self.run_id
    
    def close(self) -> None:
        """Close logger and flush remaining logs"""
        self.flush_logs()
        
        # Close all handlers
        for handler in self.logger.handlers[:]:
            handler.close()
            self.logger.removeHandler(handler)


class LoggerFactory:
    """Factory for creating ETL loggers"""
    
    _loggers: Dict[str, ETLLogger] = {}
    
    @classmethod
    def get_logger(cls, name: str, run_id: str) -> ETLLogger:
        """
        Get or create logger
        
        Args:
            name: Logger name
            run_id: ETL run identifier
            
        Returns:
            ETL logger instance
        """
        key = f"{name}_{run_id}"
        
        if key not in cls._loggers:
            cls._loggers[key] = ETLLogger(name, run_id)
        
        return cls._loggers[key]
    
    @classmethod
    def close_all(cls) -> None:
        """Close all loggers"""
        for logger in cls._loggers.values():
            logger.close()
        cls._loggers.clear()