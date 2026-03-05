"""
ETL Logger
Handles logging for all ETL operations
"""
from datetime import datetime
from typing import Optional
import logging
import os

from src.config import ETLConfig


class ETLLogger:
    """Logger for ETL operations"""
    
    def __init__(self, config: ETLConfig):
        self.config = config
        self.etl_run_id = self._generate_etl_run_id()
        self.log_file = self._setup_logging()
        
    def _generate_etl_run_id(self) -> str:
        """Generate unique ETL run ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"ETL{timestamp}"
    
    def _setup_logging(self) -> str:
        """Setup logging configuration"""
        log_dir = self.config.get("logging.directory", "logs")
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, f"{self.etl_run_id}.log")
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        return log_file
    
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
        Log ETL message
        
        Args:
            step: Process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        log_entry = {
            "etl_run_id": self.etl_run_id,
            "timestamp": datetime.now().isoformat(),
            "step": step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message
        }
        
        # Log to file with appropriate level
        log_msg = (
            f"[{step}] {message} "
            f"(Processed: {records_processed}, Success: {records_success}, Errors: {records_error})"
        )
        
        if status == "E":
            logging.error(log_msg)
        elif status == "W":
            logging.warning(log_msg)
        elif status == "I":
            logging.info(log_msg)
        else:
            logging.info(log_msg)
        
        # In production, would also INSERT into log table
        # self._persist_log_entry(log_entry)
    
    def _persist_log_entry(self, log_entry: dict) -> None:
        """Persist log entry to database (placeholder)"""
        # Would implement database INSERT here
        pass