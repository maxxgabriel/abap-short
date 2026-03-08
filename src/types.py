"""
Type system with Pydantic schemas and Protocol classes for ETL system.

This module provides:
1. Runtime validation using Pydantic models
2. Protocol classes for structural typing (duck typing)
3. Type aliases and enums for domain concepts
4. Conversion utilities between schemas
"""

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Protocol, Optional, List, Dict, Any, runtime_checkable
from pydantic import BaseModel, Field, validator, ConfigDict
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, DateType, TimestampType
)


# =============================================================================
# Enums - Domain Constants
# =============================================================================

class StatusCode(str, Enum):
    """ETL record status codes."""
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
    """Sale categorization levels."""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


# =============================================================================
# Pydantic Models - Runtime Validation
# =============================================================================

class RawSalesRecord(BaseModel):
    """
    Raw sales data from source system.
    
    Provides runtime validation and serialization for extracted data.
    """
    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
        use_enum_values=True
    )
    
    trans_id: str = Field(..., min_length=1, max_length=10)
    trans_date: date
    customer_id: str = Field(..., min_length=1, max_length=10)
    product_id: str = Field(..., min_length=1, max_length=10)
    quantity: int = Field(..., gt=0)
    unit_price: Decimal = Field(..., gt=0, decimal_places=2)
    currency: str = Field(..., min_length=3, max_length=3)
    sales_rep: str = Field(..., max_length=20)
    region: str = Field(..., max_length=10)
    status: StatusCode = StatusCode.NEW
    created_at: Optional[datetime] = None
    created_by: Optional[str] = Field(None, max_length=12)
    
    @validator("trans_date")
    def validate_trans_date(cls, v: date) -> date:
        """Ensure transaction date is not in the future."""
        if v > date.today():
            raise ValueError("Transaction date cannot be in the future")
        return v
    
    @validator("currency")
    def validate_currency(cls, v: str) -> str:
        """Ensure currency code is uppercase."""
        return v.upper()


class AnalyticsRecord(BaseModel):
    """
    Transformed analytics data for target system.
    
    Contains calculated fields and business categorizations.
    """
    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
        use_enum_values=True
    )
    
    analytics_id: str = Field(..., min_length=1, max_length=20)
    trans_date: date
    customer_id: str = Field(..., min_length=1, max_length=10)
    product_id: str = Field(..., min_length=1, max_length=10)
    total_quantity: int = Field(..., ge=0)
    gross_amount: Decimal = Field(..., ge=0, decimal_places=2)
    net_amount: Decimal = Field(..., ge=0, decimal_places=2)
    discount_amount: Decimal = Field(..., ge=0, decimal_places=2)
    tax_amount: Decimal = Field(..., ge=0, decimal_places=2)
    currency: str = Field(..., min_length=3, max_length=3)
    sales_rep: str = Field(..., max_length=20)
    region: str = Field(..., max_length=10)
    profit_margin: Decimal = Field(..., decimal_places=2)
    category: SaleCategory
    etl_run_id: str = Field(..., max_length=20)
    loaded_at: Optional[datetime] = None
    loaded_by: Optional[str] = Field(None, max_length=12)
    
    @validator("net_amount")
    def validate_net_amount(cls, v: Decimal, values: Dict[str, Any]) -> Decimal:
        """Ensure net amount is reasonable compared to gross."""
        if "gross_amount" in values:
            gross = values["gross_amount"]
            if v > gross * Decimal("1.5"):  # Net shouldn't exceed 150% of gross
                raise ValueError(f"Net amount {v} exceeds reasonable threshold for gross {gross}")
        return v
    
    @validator("currency")
    def validate_currency(cls, v: str) -> str:
        """Ensure currency code is uppercase."""
        return v.upper()


class ETLLogRecord(BaseModel):
    """
    ETL execution log entry for audit trail.
    """
    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
        use_enum_values=True
    )
    
    log_id: str = Field(..., max_length=20)
    etl_run_id: str = Field(..., max_length=20)
    execution_date: date
    execution_time: str = Field(..., regex=r"^\d{2}:\d{2}:\d{2}$")
    process_step: ProcessStep
    status: StatusCode
    records_processed: int = Field(default=0, ge=0)
    records_success: int = Field(default=0, ge=0)
    records_error: int = Field(default=0, ge=0)
    message: str = Field(..., max_length=255)
    created_at: Optional[datetime] = None
    created_by: Optional[str] = Field(None, max_length=12)
    
    @validator("records_success", "records_error")
    def validate_record_counts(cls, v: int, values: Dict[str, Any]) -> int:
        """Ensure record counts don't exceed total processed."""
        if "records_processed" in values:
            if v > values["records_processed"]:
                raise ValueError("Success/error count cannot exceed total processed")
        return v


class ETLConfig(BaseModel):
    """
    ETL configuration parameters.
    """
    batch_size: int = Field(default=1000, ge=1, le=10000)
    commit_interval: int = Field(default=500, ge=1, le=5000)
    parallel_jobs: int = Field(default=4, ge=1, le=32)
    retry_attempts: int = Field(default=3, ge=0, le=10)
    timeout_seconds: int = Field(default=3600, ge=60, le=86400)
    
    # Business rule thresholds
    discount_qty_tier1: int = Field(default=10, ge=0)
    discount_qty_tier2: int = Field(default=15, ge=0)
    discount_rate_tier1: Decimal = Field(default=Decimal("0.05"), ge=0, le=1)
    discount_rate_tier2: Decimal = Field(default=Decimal("0.10"), ge=0, le=1)
    tax_rate: Decimal = Field(default=Decimal("0.08"), ge=0, le=1)
    cost_ratio: Decimal = Field(default=Decimal("0.60"), ge=0, le=1)
    category_high_threshold: Decimal = Field(default=Decimal("2000.00"), ge=0)
    category_medium_threshold: Decimal = Field(default=Decimal("500.00"), ge=0)
    
    @validator("discount_qty_tier2")
    def validate_tier2_greater(cls, v: int, values: Dict[str, Any]) -> int:
        """Ensure tier 2 threshold is greater than tier 1."""
        if "discount_qty_tier1" in values and v <= values["discount_qty_tier1"]:
            raise ValueError("Tier 2 quantity must be greater than tier 1")
        return v
    
    @validator("category_medium_threshold")
    def validate_medium_threshold(cls, v: Decimal, values: Dict[str, Any]) -> Decimal:
        """Ensure medium threshold is less than high threshold."""
        if "category_high_threshold" in values and v >= values["category_high_threshold"]:
            raise ValueError("Medium threshold must be less than high threshold")
        return v


class ETLStatistics(BaseModel):
    """
    ETL execution statistics for monitoring and reporting.
    """
    total_records: int = Field(default=0, ge=0)
    success_records: int = Field(default=0, ge=0)
    error_records: int = Field(default=0, ge=0)
    warning_records: int = Field(default=0, ge=0)
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[int] = Field(None, ge=0)
    
    def calculate_duration(self) -> None:
        """Calculate duration if end_time is set."""
        if self.end_time:
            delta = self.end_time - self.start_time
            self.duration_seconds = int(delta.total_seconds())


class ExecutionResult(BaseModel):
    """
    Result of an ETL component execution.
    """
    success: bool
    records_total: int = Field(default=0, ge=0)
    records_success: int = Field(default=0, ge=0)
    records_error: int = Field(default=0, ge=0)
    message: str = ""
    error_details: Optional[List[str]] = None


# =============================================================================
# Protocol Classes - Structural Typing
# =============================================================================

@runtime_checkable
class ETLComponentProtocol(Protocol):
    """
    Protocol for ETL component implementations.
    
    Any class implementing these methods can be used as an ETL component,
    regardless of inheritance hierarchy.
    """
    
    def execute(self) -> ExecutionResult:
        """
        Execute the ETL component's main logic.
        
        Returns:
            ExecutionResult with success status and statistics
        """
        ...
    
    def get_component_name(self) -> str:
        """
        Get the name of this component.
        
        Returns:
            Component name string
        """
        ...
    
    def validate_prerequisites(self) -> bool:
        """
        Validate that all prerequisites are met before execution.
        
        Returns:
            True if prerequisites are met, False otherwise
        """
        ...


@runtime_checkable
class ETLLoggerProtocol(Protocol):
    """
    Protocol for ETL logging implementations.
    
    Defines the interface that any logger must implement.
    """
    
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
        Log a message with context and statistics.
        
        Args:
            step: Process step name
            status: Status code (S/E/W/I)
            message: Log message text
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        ...
    
    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run identifier.
        
        Returns:
            ETL run ID string
        """
        ...


@runtime_checkable
class DataExtractorProtocol(Protocol):
    """Protocol for data extraction components."""
    
    def extract_data(
        self, 
        from_date: date, 
        to_date: date
    ) -> List[RawSalesRecord]:
        """Extract raw data for the given date range."""
        ...


@runtime_checkable
class DataTransformerProtocol(Protocol):
    """Protocol for data transformation components."""
    
    def transform_data(
        self, 
        raw_data: List[RawSalesRecord]
    ) -> List[AnalyticsRecord]:
        """Transform raw data into analytics records."""
        ...


@runtime_checkable
class DataLoaderProtocol(Protocol):
    """Protocol for data loading components."""
    
    def load_data(
        self, 
        analytics_data: List[AnalyticsRecord]
    ) -> bool:
        """Load analytics data into target system."""
        ...


# =============================================================================
# Spark Schema Definitions
# =============================================================================

class SparkSchemas:
    """
    Spark SQL schema definitions for ETL tables.
    
    These schemas ensure type safety when working with DataFrames.
    """
    
    @staticmethod
    def raw_sales_schema() -> StructType:
        """Schema for raw sales data."""
        return StructType([
            StructField("trans_id", StringType(), False),
            StructField("trans_date", DateType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("unit_price", DecimalType(16, 2), False),
            StructField("currency", StringType(), False),
            StructField("sales_rep", StringType(), False),
            StructField("region", StringType(), False),
            StructField("status", StringType(), False),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True),
        ])
    
    @staticmethod
    def analytics_schema() -> StructType:
        """Schema for analytics data."""
        return StructType([
            StructField("analytics_id", StringType(), False),
            StructField("trans_date", DateType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("total_quantity", IntegerType(), False),
            StructField("gross_amount", DecimalType(16, 2), False),
            StructField("net_amount", DecimalType(16, 2), False),
            StructField("discount_amount", DecimalType(16, 2), False),
            StructField("tax_amount", DecimalType(16, 2), False),
            StructField("currency", StringType(), False),
            StructField("sales_rep", StringType(), False),
            StructField("region", StringType(), False),
            StructField("profit_margin", DecimalType(5, 2), False),
            StructField("category", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("loaded_at", TimestampType(), True),
            StructField("loaded_by", StringType(), True),
        ])
    
    @staticmethod
    def etl_log_schema() -> StructType:
        """Schema for ETL log data."""
        return StructType([
            StructField("log_id", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("execution_date", DateType(), False),
            StructField("execution_time", StringType(), False),
            StructField("process_step", StringType(), False),
            StructField("status", StringType(), False),
            StructField("records_processed", IntegerType(), False),
            StructField("records_success", IntegerType(), False),
            StructField("records_error", IntegerType(), False),
            StructField("message", StringType(), False),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True),
        ])


# =============================================================================
# Utility Functions
# =============================================================================

def raw_sales_to_dict(record: RawSalesRecord) -> Dict[str, Any]:
    """Convert RawSalesRecord to dictionary for Spark."""
    return record.model_dump(mode='json')


def analytics_to_dict(record: AnalyticsRecord) -> Dict[str, Any]:
    """Convert AnalyticsRecord to dictionary for Spark."""
    return record.model_dump(mode='json')


def dict_to_raw_sales(data: Dict[str, Any]) -> RawSalesRecord:
    """Convert dictionary to validated RawSalesRecord."""
    return RawSalesRecord(**data)


def dict_to_analytics(data: Dict[str, Any]) -> AnalyticsRecord:
    """Convert dictionary to validated AnalyticsRecord."""
    return AnalyticsRecord(**data)


def validate_component(obj: Any) -> bool:
    """
    Check if an object implements the ETL component protocol.
    
    Args:
        obj: Object to validate
        
    Returns:
        True if object implements ETLComponentProtocol
    """
    return isinstance(obj, ETLComponentProtocol)


def validate_logger(obj: Any) -> bool:
    """
    Check if an object implements the logger protocol.
    
    Args:
        obj: Object to validate
        
    Returns:
        True if object implements ETLLoggerProtocol
    """
    return isinstance(obj, ETLLoggerProtocol)