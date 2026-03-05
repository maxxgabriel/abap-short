"""
ETL Logger Module
Provides comprehensive logging functionality for ETL processes.
"""

import uuid
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, asdict


@dataclass
class LogEntry:
    """Data class for ETL log entries."""
    log_id: str
    etl_run_id: str
    execution_date: str
    execution_time: str
    process_step: str
    status: str
    records_processed: int
    records_success: int
    records_error: int
    message: str
    timestamp: str


class ETLLogger:
    """
    Utility class for ETL logging with structured log entries.
    """

    # Status codes
    STATUS_SUCCESS = 'S'
    STATUS_ERROR = 'E'
    STATUS_WARNING = 'W'
    STATUS_INFO = 'I'

    # Process steps
    STEP_INIT = 'INIT'
    STEP_EXTRACT = 'EXTRACT'
    STEP_TRANSFORM = 'TRANSFORM'
    STEP_LOAD = 'LOAD'
    STEP_VALIDATE = 'VALIDATE'
    STEP_COMPLETE = 'COMPLETE'
    STEP_ERROR = 'ERROR'

    def __init__(self, etl_run_id: str, log_level: str = 'INFO'):
        """
        Initialize the logger.

        Args:
            etl_run_id: Unique identifier for the ETL run
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.etl_run_id = etl_run_id
        self.log_entries: List[LogEntry] = []

        # Configure Python logging
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ):
        """
        Log a message with full context.

        Args:
            step: Processing step name
            status: Status code (S/E/W/I)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        now = datetime.now()

        log_entry = LogEntry(
            log_id=self._generate_log_id(),
            etl_run_id=self.etl_run_id,
            execution_date=now.strftime('%Y-%m-%d'),
            execution_time=now.strftime('%H:%M:%S'),
            process_step=step,
            status=status,
            records_processed=records_processed,
            records_success=records_success,
            records_error=records_error,
            message=message,
            timestamp=now.isoformat()
        )

        self.log_entries.append(log_entry)

        # Log to Python logger
        log_level = self._get_log_level(status)
        formatted_message = (
            f"[{log_entry.execution_time}] [{step}] [{status}] {message}"
        )

        if records_processed > 0:
            formatted_message += (
                f" | Processed: {records_processed}, "
                f"Success: {records_success}, "
                f"Errors: {records_error}"
            )

        self.logger.log(log_level, formatted_message)

    def _generate_log_id(self) -> str:
        """
        Generate unique log entry ID.

        Returns:
            Unique log ID with LOG prefix
        """
        unique_id = str(uuid.uuid4())
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"LOG_{timestamp}_{unique_id[:8]}"

    def _get_log_level(self, status: str) -> int:
        """
        Map status code to Python logging level.

        Args:
            status: Status code (S/E/W/I)

        Returns:
            Python logging level constant
        """
        status_map = {
            self.STATUS_SUCCESS: logging.INFO,
            self.STATUS_ERROR: logging.ERROR,
            self.STATUS_WARNING: logging.WARNING,
            self.STATUS_INFO: logging.INFO
        }
        return status_map.get(status, logging.INFO)

    def get_etl_run_id(self) -> str:
        """
        Get the ETL run ID.

        Returns:
            ETL run ID string
        """
        return self.etl_run_id

    def get_log_entries(self) -> List[Dict[str, Any]]:
        """
        Get all log entries as dictionaries.

        Returns:
            List of log entry dictionaries
        """
        return [asdict(entry) for entry in self.log_entries]

    def get_error_count(self) -> int:
        """
        Get count of error log entries.

        Returns:
            Number of error entries
        """
        return sum(1 for entry in self.log_entries if entry.status == self.STATUS_ERROR)

    def get_warning_count(self) -> int:
        """
        Get count of warning log entries.

        Returns:
            Number of warning entries
        """
        return sum(1 for entry in self.log_entries if entry.status == self.STATUS_WARNING)

    def export_logs(self, output_path: str):
        """
        Export logs to JSON file.

        Args:
            output_path: Path to output JSON file
        """
        import json

        with open(output_path, 'w') as f:
            json.dump(self.get_log_entries(), f, indent=2)

        self.logger.info(f"Logs exported to {output_path}")