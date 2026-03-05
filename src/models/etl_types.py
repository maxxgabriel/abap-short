"""
Module: etl_types
Description: Common data models for ETL system
Converted from: ZETL_TYPES ABAP type pool
"""

from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal
from typing import Optional


@dataclass
class RawSales:
    """
    Raw sales data structure.
    Converted from: ty_raw_sales ABAP structure
    """
    trans_id: str
    trans_date: date
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

    def __post_init__(self):
        """Validate fields."""
        if self.quantity < 0:
            raise ValueError("Quantity cannot be negative")
        if self.unit_price < 0:
            raise ValueError("Unit price cannot be negative")


@dataclass
class Analytics:
    """
    Analytics data structure.
    Converted from: ty_analytics ABAP structure
    """
    analytics_id: str
    trans_date: date
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
class ETLLog:
    """
    ETL log structure.
    Converted from: ty_etl_log ABAP structure
    """
    log_id: str
    etl_run_id: str
    execution_date: date
    execution_time: str
    process_step: str
    status: str
    records_processed: int
    records_success: int
    records_error: int
    message: str
    created_at: Optional[datetime] = None
    created_by: Optional[str] = None


@dataclass
class ETLConfig:
    """
    ETL configuration structure.
    Converted from: ty_etl_config ABAP structure
    """
    batch_size: int = 1000
    commit_interval: int = 500
    parallel_jobs: int = 1
    retry_attempts: int = 3
    timeout_seconds: int = 3600


@dataclass
class ETLStatistics:
    """
    ETL statistics structure.
    Converted from: ty_etl_statistics ABAP structure
    """
    total_records: int = 0
    success_records: int = 0
    error_records: int = 0
    warning_records: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: Optional[int] = None


class StatusCodes:
    """
    Status code constants.
    Converted from: ty_status_codes ABAP structure
    """
    NEW = 'N'
    PROCESSED = 'P'
    ERROR = 'E'
    WARNING = 'W'
    SUCCESS = 'S'
    INFO = 'I'


class SaleCategory:
    """Sale category constants."""
    HIGH = 'HIGH'
    MEDIUM = 'MEDIUM'
    LOW = 'LOW'