import logging
from datetime import datetime
from typing import Optional
from src.constants import ProcessStatus, ProcessStep
from src.models import ETLLogRecord


class ETLLogger:
    """ETL logging utility"""
    
    def __init__(self, etl_run_id: str, log_level: str = "INFO"):
        self.etl_run_id = etl_run_id
        self.log_entries = []
        
        # Configure Python logger
        self.logger = logging.getLogger(f"ETL_{etl_run_id}")
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Console handler
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ) -> str:
        """Log a message and return log ID"""
        
        log_id = self._generate_log_id()
        now = datetime.now()
        
        log_entry = ETLLogRecord(
            log_id=log_id,
            etl_run_id=self.etl_run_id,
            execution_date=now,
            execution_time=now.strftime("%H:%M:%S"),
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message,
            created_at=now,
            created_by="system"
        )
        
        self.log_entries.append(log_entry)
        
        # Log to Python logger
        log_text = (
            f"[{step}] [{status}] {message} "
            f"(Processed: {records_processed}, "
            f"Success: {records_success}, "
            f"Errors: {records_error})"
        )
        
        if status == ProcessStatus.ERROR:
            self.logger.error(log_text)
        elif status == ProcessStatus.WARNING:
            self.logger.warning(log_text)
        else:
            self.logger.info(log_text)
        
        return log_id
    
    def get_etl_run_id(self) -> str:
        """Get ETL run ID"""
        return self.etl_run_id
    
    def get_log_entries(self) -> list[ETLLogRecord]:
        """Get all log entries"""
        return self.log_entries
    
    def _generate_log_id(self) -> str:
        """Generate unique log ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"LOG{timestamp}"