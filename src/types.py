"""
Type Definitions Module
Defines data structures used throughout the ETL pipeline.
"""
from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal
from typing import Optional


@dataclass
class RawSales:
    """Raw sales data structure."""
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
class Analytics:
    """Transformed analytics data structure."""
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
    """ETL execution log entry."""
    log_id: str
    etl_run_id: str
    execution_date: date
    execution_time: str
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
    """ETL configuration structure."""
    batch_size: int = 1000
    commit_interval: int = 500
    parallel_jobs: int = 4
    retry_attempts: int = 3
    timeout_seconds: int = 3600


@dataclass
class ETLStatistics:
    """ETL execution statistics."""
    total_records: int = 0
    success_records: int = 0
    error_records: int = 0
    warning_records: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0


@dataclass
class ExecutionResult:
    """Result of component execution."""
    success: bool
    records_total: int = 0
    records_success: int = 0
    records_error: int = 0
    message: str = ""