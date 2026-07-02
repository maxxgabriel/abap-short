"""
Common type definitions and data classes.
Replaces ZETL_TOP common declarations and ZETL_TYPES.
"""
from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime
from decimal import Decimal


@dataclass
class ETLStatistics:
    """
    ETL execution statistics.
    Replaces: gs_statistics from ZETL_TOP
    """
    total_extracted: int = 0
    total_transformed: int = 0
    total_loaded: int = 0
    errors_count: int = 0
    warnings_count: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    @property
    def duration_seconds(self) -> float:
        """Calculate duration in seconds"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'total_extracted': self.total_extracted,
            'total_transformed': self.total_transformed,
            'total_loaded': self.total_loaded,
            'errors_count': self.errors_count,
            'warnings_count': self.warnings_count,
            'duration_seconds': self.duration_seconds
        }


@dataclass
class RawSalesRecord:
    """
    Raw sales data structure.
    Replaces: ty_raw_sales from ZETL_TYPES
    """
    trans_id: str
    trans_date: datetime
    customer_id: str
    product_id: str
    quantity: int
    unit_price: Decimal
    currency: str
    sales_rep: str
    region: str
    status: str
    created_at: Optional[datetime] = None
    created_by: Optional[str] = None


@dataclass
class AnalyticsRecord:
    """
    Analytics data structure.
    Replaces: ty_analytics from ZETL_TYPES
    """
    analytics_id: str
    trans_date: datetime
    customer_id: str
    product_id: str
    total_quantity: int
    gross_amount: Decimal
    net_amount: Decimal
    discount_amount: Decimal
    tax_amount: Decimal
    currency: str
    sales_rep: str
    region: str
    profit_margin: Decimal
    category: str
    etl_run_id: str
    loaded_at: Optional[datetime] = None
    loaded_by: Optional[str] = None


@dataclass
class ETLLogEntry:
    """
    ETL log entry structure.
    Replaces: ty_etl_log from ZETL_TYPES
    """
    log_id: str
    etl_run_id: str
    execution_date: datetime
    execution_time: datetime
    process_step: str
    status: str
    records_processed: int = 0
    records_success: int = 0
    records_error: int = 0
    message: str = ""
    created_at: Optional[datetime] = None
    created_by: Optional[str] = None


@dataclass
class ETLConfig:
    """
    ETL configuration.
    Replaces: ty_etl_config from ZETL_TYPES
    """
    batch_size: int = 1000
    commit_interval: int = 500
    parallel_jobs: int = 4
    retry_attempts: int = 3
    timeout_seconds: int = 3600


class StatusCodes:
    """
    Status code constants.
    Replaces: gc_status from ZCL_ETL_CONSTANTS
    """
    NEW = 'N'
    PROCESSED = 'P'
    ERROR = 'E'
    WARNING = 'W'
    SUCCESS = 'S'
    INFO = 'I'


class ProcessSteps:
    """
    Process step constants.
    Replaces: gc_step from ZCL_ETL_CONSTANTS
    """
    INIT = 'INIT'
    EXTRACT = 'EXTRACT'
    TRANSFORM = 'TRANSFORM'
    LOAD = 'LOAD'
    VALIDATE = 'VALIDATE'
    COMPLETE = 'COMPLETE'
    ERROR = 'ERROR'


class SaleCategories:
    """
    Sale category constants.
    Replaces: gc_category from ZCL_ETL_CONSTANTS
    """
    HIGH = 'HIGH'
    MEDIUM = 'MEDIUM'
    LOW = 'LOW'