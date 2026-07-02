"""
Type definitions and interface protocols for ETL components.

This module defines abstract base classes and protocols that establish
contracts for ETL components with abstract methods and type hints.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Protocol, List, Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession


# ============================================================================
# Enumerations
# ============================================================================

class StatusCode(str, Enum):
    """ETL process status codes."""
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


# ============================================================================
# Data Structures
# ============================================================================

@dataclass
class RawSalesRecord:
    """Raw sales data record structure."""
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
    """Transformed analytics data record structure."""
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
class LogEntry:
    """ETL log entry structure."""
    log_id: str
    etl_run_id: str
    execution_date: datetime
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
class ExecutionResult:
    """Result of ETL component execution."""
    success: bool
    records_total: int
    records_success: int
    records_error: int
    message: str
    duration_seconds: Optional[float] = None


@dataclass
class ETLConfig:
    """ETL configuration parameters."""
    batch_size: int = 1000
    commit_interval: int = 500
    parallel_jobs: int = 4
    retry_attempts: int = 3
    timeout_seconds: int = 3600
    discount_qty_tier1: int = 10
    discount_qty_tier2: int = 15
    discount_rate_tier1: Decimal = Decimal("0.05")
    discount_rate_tier2: Decimal = Decimal("0.10")
    tax_rate: Decimal = Decimal("0.08")
    cost_ratio: Decimal = Decimal("0.60")
    category_high_threshold: Decimal = Decimal("2000.00")
    category_medium_threshold: Decimal = Decimal("500.00")


@dataclass
class ETLStatistics:
    """ETL process statistics."""
    total_records: int = 0
    success_records: int = 0
    error_records: int = 0
    warning_records: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None


# ============================================================================
# Logger Protocol
# ============================================================================

class ETLLoggerProtocol(Protocol):
    """Protocol for ETL logging interface."""

    @property
    def etl_run_id(self) -> str:
        """Get the ETL run identifier."""
        ...

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
        Log a message for the ETL process.

        Args:
            step: Process step identifier
            status: Status code (S/E/W/I)
            message: Log message text
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        ...

    def get_logs(self) -> List[LogEntry]:
        """Retrieve all log entries for this ETL run."""
        ...


# ============================================================================
# Component Interface (Abstract Base Class)
# ============================================================================

class ETLComponent(ABC):
    """
    Abstract base class for all ETL components.

    All ETL components (Extract, Transform, Load) must inherit from this
    class and implement the required methods.
    """

    def __init__(self, logger: ETLLoggerProtocol, config: ETLConfig):
        """
        Initialize the ETL component.

        Args:
            logger: Logger instance for logging operations
            config: Configuration parameters
        """
        self._logger = logger
        self._config = config

    @abstractmethod
    def execute(self, **kwargs) -> ExecutionResult:
        """
        Execute the ETL component logic.

        Returns:
            ExecutionResult containing success status and statistics
        
        Raises:
            ETLError: If component execution fails
        """
        pass

    @abstractmethod
    def get_component_name(self) -> str:
        """
        Get the component name.

        Returns:
            Component name as string
        """
        pass

    @abstractmethod
    def validate_prerequisites(self) -> bool:
        """
        Validate that all prerequisites are met.

        Returns:
            True if prerequisites are met, False otherwise
        """
        pass

    @property
    def logger(self) -> ETLLoggerProtocol:
        """Get the logger instance."""
        return self._logger

    @property
    def config(self) -> ETLConfig:
        """Get the configuration."""
        return self._config


# ============================================================================
# Extractor Interface
# ============================================================================

class ETLExtractor(ETLComponent):
    """
    Abstract base class for data extraction components.
    """

    @abstractmethod
    def extract_data(
        self,
        spark: SparkSession,
        from_date: datetime,
        to_date: datetime
    ) -> DataFrame:
        """
        Extract raw sales data from source.

        Args:
            spark: SparkSession instance
            from_date: Start date for extraction
            to_date: End date for extraction

        Returns:
            DataFrame containing raw sales records

        Raises:
            ETLError: If extraction fails
        """
        pass


# ============================================================================
# Transformer Interface
# ============================================================================

class ETLTransformer(ETLComponent):
    """
    Abstract base class for data transformation components.
    """

    @abstractmethod
    def transform_data(
        self,
        spark: SparkSession,
        raw_data: DataFrame
    ) -> DataFrame:
        """
        Transform raw data into analytics format.

        Args:
            spark: SparkSession instance
            raw_data: DataFrame with raw sales records

        Returns:
            DataFrame containing transformed analytics records

        Raises:
            ETLError: If transformation fails
        """
        pass

    @abstractmethod
    def calculate_analytics(
        self,
        raw_record: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Calculate analytics metrics for a single record.

        Args:
            raw_record: Dictionary containing raw sales data

        Returns:
            Dictionary containing calculated analytics
        """
        pass

    @abstractmethod
    def categorize_sale(self, gross_amount: Decimal) -> str:
        """
        Categorize sale based on gross amount.

        Args:
            gross_amount: Gross sale amount

        Returns:
            Category string (HIGH/MEDIUM/LOW)
        """
        pass


# ============================================================================
# Loader Interface
# ============================================================================

class ETLLoader(ETLComponent):
    """
    Abstract base class for data loading components.
    """

    @abstractmethod
    def load_data(
        self,
        spark: SparkSession,
        analytics_data: DataFrame
    ) -> ExecutionResult:
        """
        Load transformed data into target.

        Args:
            spark: SparkSession instance
            analytics_data: DataFrame with analytics records

        Returns:
            ExecutionResult with load statistics

        Raises:
            ETLError: If loading fails
        """
        pass

    @abstractmethod
    def validate_record(self, record: Dict[str, Any]) -> bool:
        """
        Validate a single record before loading.

        Args:
            record: Dictionary containing record data

        Returns:
            True if record is valid, False otherwise
        """
        pass


# ============================================================================
# Orchestrator Interface
# ============================================================================

class ETLOrchestrator(ABC):
    """
    Abstract base class for ETL orchestration.
    """

    @abstractmethod
    def run_etl(
        self,
        spark: SparkSession,
        from_date: datetime,
        to_date: datetime
    ) -> ExecutionResult:
        """
        Execute the complete ETL process.

        Args:
            spark: SparkSession instance
            from_date: Start date for processing
            to_date: End date for processing

        Returns:
            ExecutionResult with overall statistics

        Raises:
            ETLError: If ETL process fails
        """
        pass

    @abstractmethod
    def get_etl_run_id(self) -> str:
        """Get the current ETL run identifier."""
        pass

    @abstractmethod
    def get_statistics(self) -> ETLStatistics:
        """Get ETL execution statistics."""
        pass


# ============================================================================
# Exception Classes
# ============================================================================

class ETLError(Exception):
    """Base exception class for ETL errors."""

    def __init__(
        self,
        message: str,
        error_step: Optional[str] = None,
        record_id: Optional[str] = None
    ):
        """
        Initialize ETL error.

        Args:
            message: Error message
            error_step: ETL step where error occurred
            record_id: Record identifier if applicable
        """
        super().__init__(message)
        self.message = message
        self.error_step = error_step
        self.record_id = record_id


class ExtractError(ETLError):
    """Exception raised during data extraction."""
    pass


class TransformError(ETLError):
    """Exception raised during data transformation."""
    pass


class LoadError(ETLError):
    """Exception raised during data loading."""
    pass


class ValidationError(ETLError):
    """Exception raised during data validation."""
    pass