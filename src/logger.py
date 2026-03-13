"""
ETL Logging Framework
Structured logging with ETL stage tracking and ABAP status code mapping
"""

import logging
import sys
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum
import json


class ETLStatus(Enum):
    """ABAP status code mapping to Python logging levels"""
    NEW = ('N', logging.INFO, 'NEW')
    PROCESSED = ('P', logging.INFO, 'PROCESSED')
    ERROR = ('E', logging.ERROR, 'ERROR')
    WARNING = ('W', logging.WARNING, 'WARNING')
    SUCCESS = ('S', logging.INFO, 'SUCCESS')
    INFO = ('I', logging.INFO, 'INFO')

    def __init__(self, abap_code: str, log_level: int, description: str):
        self.abap_code = abap_code
        self.log_level = log_level
        self.description = description


class ETLStep(Enum):
    """ETL process steps"""
    INIT = 'INIT'
    EXTRACT = 'EXTRACT'
    TRANSFORM = 'TRANSFORM'
    LOAD = 'LOAD'
    VALIDATE = 'VALIDATE'
    COMPLETE = 'COMPLETE'
    ERROR = 'ERROR'


class StructuredETLFormatter(logging.Formatter):
    """Custom formatter for structured ETL logging"""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record with ETL metadata as JSON"""
        log_data = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'etl_run_id': getattr(record, 'etl_run_id', None),
            'process_step': getattr(record, 'process_step', None),
            'status': getattr(record, 'status', None),
            'records_processed': getattr(record, 'records_processed', 0),
            'records_success': getattr(record, 'records_success', 0),
            'records_error': getattr(record, 'records_error', 0),
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }

        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)

        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'created', 'filename', 'funcName',
                          'levelname', 'levelno', 'lineno', 'module', 'msecs',
                          'pathname', 'process', 'processName', 'relativeCreated',
                          'thread', 'threadName', 'exc_info', 'exc_text', 'stack_info']:
                if key not in log_data:
                    log_data[key] = value

        return json.dumps(log_data, default=str)


class ETLLogger:
    """
    ETL-specific logger with stage tracking and metadata support.
    Maps ABAP status codes to Python logging levels.
    """

    def __init__(self, etl_run_id: str, name: str = 'etl'):
        """
        Initialize ETL logger

        Args:
            etl_run_id: Unique identifier for the ETL run
            name: Logger name
        """
        self.etl_run_id = etl_run_id
        self.logger = logging.getLogger(name)
        self._log_sequence = 0

        # Prevent duplicate handlers
        if not self.logger.handlers:
            self._setup_handlers()

    def _setup_handlers(self):
        """Configure logging handlers with structured formatting"""
        self.logger.setLevel(logging.DEBUG)

        # Console handler with structured JSON output
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(StructuredETLFormatter())

        # File handler for detailed logs
        file_handler = logging.FileHandler(f'etl_{self.etl_run_id}.log')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(StructuredETLFormatter())

        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

    def _generate_log_id(self) -> str:
        """Generate unique log entry ID"""
        self._log_sequence += 1
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"LOG{timestamp}{self._log_sequence:06d}"

    def log_message(
        self,
        step: ETLStep,
        status: ETLStatus,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0,
        **extra_fields
    ):
        """
        Log ETL message with metadata

        Args:
            step: ETL process step
            status: Status code (maps to log level)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
            **extra_fields: Additional metadata fields
        """
        log_id = self._generate_log_id()

        # Create extra dict with ETL metadata
        extra = {
            'log_id': log_id,
            'etl_run_id': self.etl_run_id,
            'process_step': step.value,
            'status': status.abap_code,
            'status_description': status.description,
            'records_processed': records_processed,
            'records_success': records_success,
            'records_error': records_error,
            **extra_fields
        }

        # Log at appropriate level based on status
        self.logger.log(
            status.log_level,
            message,
            extra=extra
        )

    def log_info(self, step: ETLStep, message: str, **kwargs):
        """Log INFO level message"""
        self.log_message(step, ETLStatus.INFO, message, **kwargs)

    def log_success(self, step: ETLStep, message: str, **kwargs):
        """Log SUCCESS level message"""
        self.log_message(step, ETLStatus.SUCCESS, message, **kwargs)

    def log_warning(self, step: ETLStep, message: str, **kwargs):
        """Log WARNING level message"""
        self.log_message(step, ETLStatus.WARNING, message, **kwargs)

    def log_error(self, step: ETLStep, message: str, exception: Optional[Exception] = None, **kwargs):
        """
        Log ERROR level message

        Args:
            step: ETL process step
            message: Error message
            exception: Optional exception object
            **kwargs: Additional metadata
        """
        if exception:
            kwargs['exception_type'] = type(exception).__name__
            kwargs['exception_message'] = str(exception)

        self.log_message(step, ETLStatus.ERROR, message, **kwargs)

        # Log exception traceback if present
        if exception:
            self.logger.exception(
                f"Exception details for: {message}",
                extra={'etl_run_id': self.etl_run_id, 'process_step': step.value}
            )

    def get_etl_run_id(self) -> str:
        """Get the ETL run ID"""
        return self.etl_run_id


class LogAggregator:
    """Aggregate and summarize ETL logs"""

    def __init__(self):
        self.stats: Dict[str, Dict[str, int]] = {}

    def add_log_entry(self, step: str, status: str, records_processed: int = 0,
                     records_success: int = 0, records_error: int = 0):
        """Add log entry to aggregator"""
        if step not in self.stats:
            self.stats[step] = {
                'total_processed': 0,
                'total_success': 0,
                'total_error': 0,
                'info_count': 0,
                'warning_count': 0,
                'error_count': 0
            }

        self.stats[step]['total_processed'] += records_processed
        self.stats[step]['total_success'] += records_success
        self.stats[step]['total_error'] += records_error

        if status == 'I' or status == 'S' or status == 'P':
            self.stats[step]['info_count'] += 1
        elif status == 'W':
            self.stats[step]['warning_count'] += 1
        elif status == 'E':
            self.stats[step]['error_count'] += 1

    def get_summary(self) -> Dict[str, Any]:
        """Get aggregated summary"""
        return {
            'by_step': self.stats,
            'totals': {
                'total_processed': sum(s['total_processed'] for s in self.stats.values()),
                'total_success': sum(s['total_success'] for s in self.stats.values()),
                'total_error': sum(s['total_error'] for s in self.stats.values()),
                'total_warnings': sum(s['warning_count'] for s in self.stats.values()),
                'total_errors': sum(s['error_count'] for s in self.stats.values())
            }
        }


def generate_etl_run_id() -> str:
    """Generate unique ETL run ID"""
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
    return f"ETL{timestamp[:14]}"