"""
Type definitions and dataclasses for ETL system.
Converted from ABAP includes (ZETL_TOP, ZETL_TYPES, PACKAGE).
"""

from dataclasses import dataclass, field
from datetime import datetime, date, time
from decimal import Decimal
from typing import Optional, List
from enum import Enum


class StatusCode(str, Enum):
    """Status codes for ETL processing."""
    NEW = "N"
    PROCESSED = "P"
    ERROR = "E"
    WARNING = "W"
    SUCCESS = "S"
    INFO = "I"


class ProcessStep(str, Enum):
    """ETL process steps."""
    INIT = "INIT"
    EXTRACT = "EXTRACT"
    TRANSFORM = "TRANSFORM"
    LOAD = "LOAD"
    VALIDATE = "VALIDATE"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"


class SaleCategory(str, Enum):
    """Sale categorization."""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


@dataclass
class RawSales:
    """Raw sales data structure (from ZSALES_RAW table)."""
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
    """Analytics data structure (for ZSALES_ANALYTICS table)."""
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
    """ETL log entry structure (for ZETL_LOG table)."""
    log_id: str
    etl_run_id: str
    execution_date: date
    execution_time: time
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
    """ETL configuration settings."""
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
    """Result of ETL component execution."""
    success: bool
    records_total: int
    records_success: int
    records_error: int
    message: str


@dataclass
class ETLContext:
    """Global ETL execution context."""
    etl_run_id: str
    test_mode: bool = False
    batch_size: int = 1000
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    statistics: ETLStatistics = field(default_factory=ETLStatistics)
    raw_sales: List[RawSales] = field(default_factory=list)
    analytics: List[Analytics] = field(default_factory=list)
    logs: List[ETLLog] = field(default_factory=list)


# Schema definitions for Spark
RAW_SALES_SCHEMA = {
    "trans_id": "string",
    "trans_date": "date",
    "customer_id": "string",
    "product_id": "string",
    "quantity": "integer",
    "unit_price": "decimal(16,2)",
    "currency": "string",
    "sales_rep": "string",
    "region": "string",
    "status": "string",
    "created_at": "timestamp",
    "created_by": "string"
}

ANALYTICS_SCHEMA = {
    "analytics_id": "string",
    "trans_date": "date",
    "customer_id": "string",
    "product_id": "string",
    "total_quantity": "integer",
    "gross_amount": "decimal(16,2)",
    "net_amount": "decimal(16,2)",
    "discount_amount": "decimal(16,2)",
    "tax_amount": "decimal(16,2)",
    "currency": "string",
    "sales_rep": "string",
    "region": "string",
    "profit_margin": "decimal(5,2)",
    "category": "string",
    "etl_run_id": "string",
    "loaded_at": "timestamp",
    "loaded_by": "string"
}

ETL_LOG_SCHEMA = {
    "log_id": "string",
    "etl_run_id": "string",
    "execution_date": "date",
    "execution_time": "string",
    "process_step": "string",
    "status": "string",
    "records_processed": "integer",
    "records_success": "integer",
    "records_error": "integer",
    "message": "string",
    "created_at": "timestamp",
    "created_by": "string"
}