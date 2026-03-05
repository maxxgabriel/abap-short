"""
Python dataclasses for ETL configuration structures.
Migrated from ABAP ZETL_TYPES configuration types.
"""

from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime


@dataclass
class ETLConfig:
    """
    ETL configuration structure.
    Maps to: ZETL_TYPES=>ty_etl_config
    """

    batch_size: int = 1000
    commit_interval: int = 500
    parallel_jobs: int = 4
    retry_attempts: int = 3
    timeout_seconds: int = 3600


@dataclass
class ETLStatistics:
    """
    ETL statistics structure for tracking execution metrics.
    Maps to: ZETL_TYPES=>ty_etl_statistics
    """

    total_records: int = 0
    success_records: int = 0
    error_records: int = 0
    warning_records: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: int = 0

    def calculate_duration(self) -> None:
        """Calculate duration from start and end times"""
        if self.start_time and self.end_time:
            delta = self.end_time - self.start_time
            self.duration_seconds = int(delta.total_seconds())


@dataclass
class StatusCodes:
    """
    Status code definitions.
    Maps to: ZETL_TYPES=>ty_status_codes and ZCL_ETL_CONSTANTS=>gc_status
    """

    NEW: str = "N"
    PROCESSED: str = "P"
    ERROR: str = "E"
    WARNING: str = "W"
    SUCCESS: str = "S"
    INFO: str = "I"


@dataclass
class ProcessSteps:
    """
    ETL process step definitions.
    Maps to: ZCL_ETL_CONSTANTS=>gc_step
    """

    INIT: str = "INIT"
    EXTRACT: str = "EXTRACT"
    TRANSFORM: str = "TRANSFORM"
    LOAD: str = "LOAD"
    VALIDATE: str = "VALIDATE"
    COMPLETE: str = "COMPLETE"
    ERROR: str = "ERROR"


@dataclass
class SaleCategories:
    """
    Sale category definitions.
    Maps to: ZCL_ETL_CONSTANTS=>gc_category
    """

    HIGH: str = "HIGH"
    MEDIUM: str = "MEDIUM"
    LOW: str = "LOW"


@dataclass
class BusinessRules:
    """
    Business rules and thresholds.
    Maps to: ZCL_ETL_CONSTANTS business rules constants
    """

    # Discount thresholds
    discount_qty_tier1: int = 10
    discount_qty_tier2: int = 15
    discount_rate_tier1: float = 0.05
    discount_rate_tier2: float = 0.10

    # Tax rate
    tax_rate: float = 0.08

    # Cost ratio for profit calculation
    cost_ratio: float = 0.60

    # Category thresholds
    category_high_threshold: float = 2000.00
    category_medium_threshold: float = 500.00


@dataclass
class ExecutionResult:
    """
    Result structure for component execution.
    Maps to: ZIF_ETL_COMPONENT=>ty_execution_result
    """

    success: bool = False
    records_total: int = 0
    records_success: int = 0
    records_error: int = 0
    message: str = ""


@dataclass
class LogEntry:
    """
    Single log entry structure.
    Maps to: ZCL_ETL_LOGGER=>ty_log_entry
    """

    log_id: str
    etl_run_id: str
    execution_date: str
    execution_time: str
    process_step: str
    status: str
    records_processed: int = 0
    records_success: int = 0
    records_error: int = 0
    message: str = ""


# Constants instance for global access
STATUS = StatusCodes()
STEPS = ProcessSteps()
CATEGORIES = SaleCategories()
RULES = BusinessRules()