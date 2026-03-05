"""
Type definitions for ETL system.
Converted from ABAP ZETL_TYPES type pool.
"""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Optional


@dataclass
class RawSalesData:
    """Raw sales data structure mapping from ABAP ty_raw_sales."""
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


@dataclass
class AnalyticsData:
    """Analytics data structure mapping from ABAP ty_analytics."""
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
class ETLLogEntry:
    """ETL log entry structure mapping from ABAP ty_etl_log."""
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
class ETLStatistics:
    """ETL execution statistics mapping from ABAP ty_etl_statistics."""
    total_records: int = 0
    success_records: int = 0
    error_records: int = 0
    warning_records: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: int = 0


@dataclass
class ETLConfiguration:
    """ETL runtime configuration mapping from ABAP ty_etl_config."""
    batch_size: int = 1000
    commit_interval: int = 500
    parallel_jobs: int = 4
    retry_attempts: int = 3
    timeout_seconds: int = 3600