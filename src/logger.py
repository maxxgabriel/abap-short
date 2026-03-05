"""
Logging utility for ETL pipeline.
"""
from datetime import datetime
from typing import Optional
import logging
import uuid


class ETLLogger:
    """Logger for ETL process tracking and monitoring."""
    
    def __init__(self, etl_run_id: Optional[str] = None):
        """
        Initialize the logger.
        
        Args:
            etl_run_id: Unique ETL run identifier
        """
        self.etl_run_id = etl_run_id or self._generate_run_id()
        self.logs = []
        
        # Configure Python logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def _generate_run_id(self) -> str:
        """Generate unique ETL run ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"ETL_{timestamp}_{str(uuid.uuid4())[:8]}"
    
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
        Log an ETL message.
        
        Args:
            step: ETL step name
            status: Status code (S, E, W, I)
            message: Log message
            records_processed: Total records processed
            records_success: Successfully processed records
            records_error: Records with errors
        """
        log_entry = {
            "log_id": str(uuid.uuid4()),
            "etl_run_id": self.etl_run_id,
            "timestamp": datetime.now().isoformat(),
            "step": step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message
        }
        
        self.logs.append(log_entry)
        
        # Log to console/file
        log_level = {
            'S': logging.INFO,
            'E': logging.ERROR,
            'W': logging.WARNING,
            'I': logging.INFO
        }.get(status, logging.INFO)
        
        self.logger.log(
            log_level,
            f"[{step}] {message} (Processed: {records_processed}, "
            f"Success: {records_success}, Error: {records_error})"
        )
    
    def get_logs(self) -> list:
        """Return all logged messages."""
        return self.logs
    
    def get_etl_run_id(self) -> str:
        """Return the ETL run ID."""
        return self.etl_run_id