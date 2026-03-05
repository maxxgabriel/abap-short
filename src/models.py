"""
Data models for ETL system.

This module defines dataclass structures for all ETL data types,
replacing ABAP type definitions with Python dataclasses.
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Optional


@dataclass
class RawSalesData:
    """Raw sales data structure from source system."""
    
    trans_id: str
    trans_date: date
    customer_id: str
    product_id: str
    quantity: int
    unit_price: Decimal
    currency: str
    sales_rep: str
    region: str
    status: str = "N"
    created_at: Optional[datetime] = None
    created_by: Optional[str] = None
    
    def __post_init__(self):
        """Validate and normalize data after initialization."""
        if self.created_at is None:
            self.created_at = datetime.now()
        
        # Ensure quantity is positive
        if self.quantity < 0:
            raise ValueError(f"Quantity must be positive: {self.quantity}")
        
        # Ensure unit_price is positive
        if self.unit_price < 0:
            raise ValueError(f"Unit price must be positive: {self.unit_price}")


@dataclass
class AnalyticsData:
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
    
    def __post_init__(self):
        """Validate data after initialization."""
        if self.loaded_at is None:
            self.loaded_at = datetime.now()
        
        # Validate category
        valid_categories = ["HIGH", "MEDIUM", "LOW"]
        if self.category not in valid_categories:
            raise ValueError(
                f"Invalid category: {self.category}. "
                f"Must be one of {valid_categories}"
            )


@dataclass
class ETLLogEntry:
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
    
    def __post_init__(self):
        """Set timestamp if not provided."""
        if self.created_at is None:
            self.created_at = datetime.now()


@dataclass
class ETLConfig:
    """ETL configuration parameters."""
    
    batch_size: int = 1000
    commit_interval: int = 500
    parallel_jobs: int = 1
    retry_attempts: int = 3
    timeout_seconds: int = 3600
    
    def __post_init__(self):
        """Validate configuration values."""
        if self.batch_size <= 0:
            raise ValueError("Batch size must be positive")
        if self.commit_interval <= 0:
            raise ValueError("Commit interval must be positive")
        if self.parallel_jobs < 1:
            raise ValueError("Parallel jobs must be at least 1")
        if self.retry_attempts < 0:
            raise ValueError("Retry attempts must be non-negative")
        if self.timeout_seconds <= 0:
            raise ValueError("Timeout seconds must be positive")


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
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage."""
        if self.total_records == 0:
            return 0.0
        return (self.success_records / self.total_records) * 100
    
    @property
    def error_rate(self) -> float:
        """Calculate error rate percentage."""
        if self.total_records == 0:
            return 0.0
        return (self.error_records / self.total_records) * 100
    
    def calculate_duration(self):
        """Calculate duration if start and end times are set."""
        if self.start_time and self.end_time:
            delta = self.end_time - self.start_time
            self.duration_seconds = delta.total_seconds()