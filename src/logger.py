"""
Distributed Logging System with Correlation Tracking
Replaces ABAP logger with Python structured logging framework
"""

import logging
import json
import uuid
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum
from dataclasses import dataclass, asdict
from contextvars import ContextVar


class LogLevel(Enum):
    """Map ABAP status codes to Python log levels"""
    S = logging.INFO      # Success
    I = logging.INFO      # Info
    W = logging.WARNING   # Warning
    E = logging.ERROR     # Error
    
    @classmethod
    def from_abap_status(cls, status: str) -> int:
        """Convert ABAP status code to Python log level"""
        return cls[status.upper()].value if status.upper() in cls.__members__ else logging.INFO


@dataclass
class LogContext:
    """Correlation context for distributed tracing"""
    correlation_id: str
    etl_run_id: str
    process_step: str
    execution_date: str
    execution_time: str
    user: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging"""
        return asdict(self)


# Context variable for correlation tracking across async operations
correlation_context: ContextVar[Optional[LogContext]] = ContextVar(
    'correlation_context', default=None
)


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured JSON logging"""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON"""
        log_data = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add correlation context if available
        context = correlation_context.get()
        if context:
            log_data['correlation'] = context.to_dict()
        
        # Add extra fields from record
        if hasattr(record, 'extra_fields'):
            log_data.update(record.extra_fields)
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        
        return json.dumps(log_data, default=str)


class CorrelationAdapter(logging.LoggerAdapter):
    """Adapter that enriches logs with correlation context"""
    
    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        """Add correlation context to log records"""
        extra = kwargs.get('extra', {})
        
        context = correlation_context.get()
        if context:
            extra['correlation'] = context.to_dict()
        
        kwargs['extra'] = extra
        return msg, kwargs


class ETLLogger:
    """
    Main ETL logging system with correlation tracking
    Replaces ZCL_ETL_LOGGER from ABAP
    """
    
    def __init__(
        self,
        etl_run_id: str,
        log_file: Optional[str] = None,
        console_output: bool = True,
        json_format: bool = True
    ):
        """
        Initialize ETL logger with correlation tracking
        
        Args:
            etl_run_id: Unique identifier for ETL run
            log_file: Optional file path for logging
            console_output: Enable console logging
            json_format: Use structured JSON format
        """
        self.etl_run_id = etl_run_id
        self.correlation_id = self._generate_correlation_id()
        
        # Set up logger
        self.logger = logging.getLogger(f'etl.{etl_run_id}')
        self.logger.setLevel(logging.DEBUG)
        self.logger.handlers.clear()
        
        # Create formatter
        if json_format:
            formatter = StructuredFormatter()
        else:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
        
        # Add console handler
        if console_output:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        
        # Add file handler
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
        
        # Create adapter with correlation
        self.adapter = CorrelationAdapter(self.logger, {})
        
        # Initialize context
        self._init_context()
    
    def _generate_correlation_id(self) -> str:
        """Generate unique correlation ID"""
        return str(uuid.uuid4())
    
    def _generate_log_id(self) -> str:
        """Generate unique log entry ID"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        return f'LOG{timestamp}'
    
    def _init_context(self):
        """Initialize logging context"""
        context = LogContext(
            correlation_id=self.correlation_id,
            etl_run_id=self.etl_run_id,
            process_step='INIT',
            execution_date=datetime.now().strftime('%Y-%m-%d'),
            execution_time=datetime.now().strftime('%H:%M:%S'),
            user='system'
        )
        correlation_context.set(context)
    
    def set_context(
        self,
        process_step: str,
        user: Optional[str] = None
    ):
        """Update correlation context"""
        current_context = correlation_context.get()
        if current_context:
            context = LogContext(
                correlation_id=current_context.correlation_id,
                etl_run_id=self.etl_run_id,
                process_step=process_step,
                execution_date=datetime.now().strftime('%Y-%m-%d'),
                execution_time=datetime.now().strftime('%H:%M:%S'),
                user=user or current_context.user
            )
            correlation_context.set(context)
    
    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0,
        **kwargs
    ):
        """
        Log ETL message with correlation tracking
        Maps to ABAP ZCL_ETL_LOGGER->log_message
        
        Args:
            step: Process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: ABAP status code (S, E, W, I)
            message: Log message
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Error records
            **kwargs: Additional fields
        """
        # Update context
        self.set_context(step)
        
        # Determine log level from ABAP status
        log_level = LogLevel.from_abap_status(status)
        
        # Build extra fields
        extra_fields = {
            'log_id': self._generate_log_id(),
            'etl_run_id': self.etl_run_id,
            'correlation_id': self.correlation_id,
            'process_step': step,
            'status': status,
            'records': {
                'processed': records_processed,
                'success': records_success,
                'error': records_error
            }
        }
        extra_fields.update(kwargs)
        
        # Log with appropriate level
        self.adapter.log(
            log_level,
            message,
            extra={'extra_fields': extra_fields}
        )
    
    def log_extract(
        self,
        message: str,
        records: int = 0,
        success: bool = True,
        **kwargs
    ):
        """Log extraction phase"""
        status = 'S' if success else 'E'
        self.log_message(
            step='EXTRACT',
            status=status,
            message=message,
            records_processed=records,
            records_success=records if success else 0,
            records_error=0 if success else records,
            **kwargs
        )
    
    def log_transform(
        self,
        message: str,
        records_in: int = 0,
        records_out: int = 0,
        success: bool = True,
        **kwargs
    ):
        """Log transformation phase"""
        status = 'S' if success else 'E'
        self.log_message(
            step='TRANSFORM',
            status=status,
            message=message,
            records_processed=records_in,
            records_success=records_out,
            records_error=records_in - records_out if success else records_in,
            **kwargs
        )
    
    def log_load(
        self,
        message: str,
        records: int = 0,
        success: bool = True,
        **kwargs
    ):
        """Log loading phase"""
        status = 'S' if success else 'E'
        self.log_message(
            step='LOAD',
            status=status,
            message=message,
            records_processed=records,
            records_success=records if success else 0,
            records_error=0 if success else records,
            **kwargs
        )
    
    def log_error(
        self,
        step: str,
        message: str,
        exception: Optional[Exception] = None,
        **kwargs
    ):
        """Log error with exception details"""
        extra_fields = kwargs.copy()
        if exception:
            extra_fields['exception_type'] = type(exception).__name__
            extra_fields['exception_message'] = str(exception)
        
        self.log_message(
            step=step,
            status='E',
            message=message,
            **extra_fields
        )
        
        if exception:
            self.adapter.exception(f"Exception in {step}: {message}")
    
    def log_warning(
        self,
        step: str,
        message: str,
        **kwargs
    ):
        """Log warning"""
        self.log_message(
            step=step,
            status='W',
            message=message,
            **kwargs
        )
    
    def log_info(
        self,
        step: str,
        message: str,
        **kwargs
    ):
        """Log info"""
        self.log_message(
            step=step,
            status='I',
            message=message,
            **kwargs
        )
    
    def get_etl_run_id(self) -> str:
        """Get ETL run ID"""
        return self.etl_run_id
    
    def get_correlation_id(self) -> str:
        """Get correlation ID for distributed tracing"""
        return self.correlation_id


def create_etl_logger(
    etl_run_id: Optional[str] = None,
    **kwargs
) -> ETLLogger:
    """
    Factory function to create ETL logger
    
    Args:
        etl_run_id: Optional ETL run ID (auto-generated if not provided)
        **kwargs: Additional logger configuration
    
    Returns:
        Configured ETLLogger instance
    """
    if not etl_run_id:
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        etl_run_id = f'ETL{timestamp}'
    
    return ETLLogger(etl_run_id, **kwargs)


def get_current_correlation_id() -> Optional[str]:
    """Get current correlation ID from context"""
    context = correlation_context.get()
    return context.correlation_id if context else None


def get_current_context() -> Optional[LogContext]:
    """Get current logging context"""
    return correlation_context.get()