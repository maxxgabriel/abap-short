"""
ETL Data Structures
Dataclasses representing the core data structures used in the ETL pipeline
"""
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Optional


@dataclass
class RawSalesData:
    """Raw sales data structure from source system"""
    trans_id: str
    trans_date: date
    customer_id: str
    product_id: str
    quantity: int
    unit_price: Decimal
    currency: str
    sales_rep: str
    region: str
    status: str = 'N'  # N=New, P=Processed, E=Error
    created_at: Optional[datetime] = None
    created_by: Optional[str] = None
    
    def __post_init__(self):
        """Validate and convert types after initialization"""
        if isinstance(self.unit_price, (int, float)):
            self.unit_price = Decimal(str(self.unit_price))
        if isinstance(self.trans_date, str):
            self.trans_date = datetime.strptime(self.trans_date, '%Y-%m-%d').date()


@dataclass
class AnalyticsData:
    """Transformed analytics data structure for target system"""
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
    category: str  # HIGH, MEDIUM, LOW
    etl_run_id: str
    loaded_at: Optional[datetime] = None
    loaded_by: Optional[str] = None
    
    def __post_init__(self):
        """Validate and convert types after initialization"""
        decimal_fields = ['gross_amount', 'net_amount', 'discount_amount', 
                         'tax_amount', 'profit_margin']
        for field_name in decimal_fields:
            value = getattr(self, field_name)
            if isinstance(value, (int, float)):
                setattr(self, field_name, Decimal(str(value)))
        
        if isinstance(self.trans_date, str):
            self.trans_date = datetime.strptime(self.trans_date, '%Y-%m-%d').date()


@dataclass
class ETLLogEntry:
    """ETL execution log entry"""
    log_id: str
    etl_run_id: str
    execution_date: date
    execution_time: str
    process_step: str
    status: str  # S=Success, E=Error, W=Warning, I=Info
    records_processed: int = 0
    records_success: int = 0
    records_error: int = 0
    message: str = ""
    created_at: Optional[datetime] = None
    created_by: Optional[str] = None
    
    def __post_init__(self):
        """Set defaults after initialization"""
        if not self.created_at:
            self.created_at = datetime.now()


@dataclass
class ETLConfiguration:
    """ETL runtime configuration"""
    batch_size: int = 1000
    commit_interval: int = 500
    parallel_jobs: int = 1
    retry_attempts: int = 3
    timeout_seconds: int = 3600
    test_mode: bool = False
    
    def validate(self) -> bool:
        """Validate configuration values"""
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.commit_interval <= 0:
            raise ValueError("commit_interval must be positive")
        if self.parallel_jobs <= 0:
            raise ValueError("parallel_jobs must be positive")
        if self.retry_attempts < 0:
            raise ValueError("retry_attempts must be non-negative")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        return True


@dataclass
class ETLStatistics:
    """ETL execution statistics"""
    total_records: int = 0
    success_records: int = 0
    error_records: int = 0
    warning_records: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    @property
    def duration_seconds(self) -> float:
        """Calculate execution duration in seconds"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage"""
        if self.total_records == 0:
            return 0.0
        return (self.success_records / self.total_records) * 100
    
    def start(self):
        """Mark statistics start time"""
        self.start_time = datetime.now()
    
    def end(self):
        """Mark statistics end time"""
        self.end_time = datetime.now()


@dataclass
class StatusCodes:
    """Status code constants"""
    NEW: str = 'N'
    PROCESSED: str = 'P'
    ERROR: str = 'E'
    WARNING: str = 'W'
    SUCCESS: str = 'S'
    INFO: str = 'I'


@dataclass
class ProcessSteps:
    """Process step constants"""
    INIT: str = 'INIT'
    EXTRACT: str = 'EXTRACT'
    TRANSFORM: str = 'TRANSFORM'
    LOAD: str = 'LOAD'
    VALIDATE: str = 'VALIDATE'
    COMPLETE: str = 'COMPLETE'
    ERROR: str = 'ERROR'


@dataclass
class SaleCategories:
    """Sale category constants"""
    HIGH: str = 'HIGH'
    MEDIUM: str = 'MEDIUM'
    LOW: str = 'LOW'