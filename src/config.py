"""
Configuration dataclasses for ETL processing.
Migrated from ABAP ZETL_TYPES configuration structures and ZCL_ETL_CONSTANTS.
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict


@dataclass(frozen=True)
class StatusCodes:
    """
    Status codes for ETL processing.
    Migrated from ZCL_ETL_CONSTANTS.gc_status.
    """
    NEW: str = "N"
    PROCESSED: str = "P"
    ERROR: str = "E"
    WARNING: str = "W"
    SUCCESS: str = "S"
    INFO: str = "I"


@dataclass(frozen=True)
class ProcessSteps:
    """
    ETL process step identifiers.
    Migrated from ZCL_ETL_CONSTANTS.gc_step.
    """
    INIT: str = "INIT"
    EXTRACT: str = "EXTRACT"
    TRANSFORM: str = "TRANSFORM"
    LOAD: str = "LOAD"
    VALIDATE: str = "VALIDATE"
    COMPLETE: str = "COMPLETE"
    ERROR: str = "ERROR"


@dataclass(frozen=True)
class SaleCategories:
    """
    Sale categorization values.
    Migrated from ZCL_ETL_CONSTANTS.gc_category.
    """
    HIGH: str = "HIGH"
    MEDIUM: str = "MEDIUM"
    LOW: str = "LOW"


@dataclass(frozen=True)
class BusinessRules:
    """
    Business rules for sales calculations.
    Migrated from ZCL_ETL_CONSTANTS business rule constants.
    """
    # Discount thresholds
    DISCOUNT_QTY_TIER1: int = 10
    DISCOUNT_QTY_TIER2: int = 15
    DISCOUNT_RATE_TIER1: Decimal = Decimal("0.05")
    DISCOUNT_RATE_TIER2: Decimal = Decimal("0.10")
    
    # Tax rate
    TAX_RATE: Decimal = Decimal("0.08")
    
    # Cost calculation
    COST_RATIO: Decimal = Decimal("0.60")
    
    # Category thresholds
    CATEGORY_HIGH_THRESHOLD: Decimal = Decimal("2000.00")
    CATEGORY_MEDIUM_THRESHOLD: Decimal = Decimal("500.00")


@dataclass
class ETLConfig:
    """
    ETL runtime configuration parameters.
    Migrated from ZETL_TYPES.ty_etl_config and ZCL_ETL_CONSTANTS defaults.
    """
    batch_size: int = 1000
    commit_interval: int = 500
    parallel_jobs: int = 4
    retry_attempts: int = 3
    timeout_seconds: int = 3600
    
    # Spark-specific configurations
    spark_configs: Dict[str, str] = field(default_factory=lambda: {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.shuffle.partitions": "200",
    })


@dataclass
class IDPrefixes:
    """
    ID prefix constants for generated identifiers.
    Migrated from ZCL_ETL_CONSTANTS ID prefix constants.
    """
    ETL_RUN: str = "ETL"
    LOG_ID: str = "LOG"
    ANALYTICS_ID: str = "ANL"


@dataclass
class MessageTexts:
    """
    Standard message texts for logging.
    Migrated from ZCL_ETL_CONSTANTS message constants.
    """
    INIT_SUCCESS: str = "ETL process initialized successfully"
    EXTRACT_START: str = "Starting data extraction"
    EXTRACT_COMPLETE: str = "Data extraction completed"
    TRANSFORM_START: str = "Starting data transformation"
    TRANSFORM_COMPLETE: str = "Data transformation completed"
    LOAD_START: str = "Starting data load"
    LOAD_COMPLETE: str = "Data load completed"
    ETL_COMPLETE: str = "ETL process completed successfully"
    ETL_ERROR: str = "ETL process failed"


@dataclass
class ETLStatistics:
    """
    ETL execution statistics tracking.
    Migrated from ZETL_TYPES.ty_etl_statistics.
    """
    total_records: int = 0
    success_records: int = 0
    error_records: int = 0
    warning_records: int = 0
    start_time: str = ""
    end_time: str = ""
    duration_seconds: int = 0
    
    def to_dict(self) -> dict:
        """Convert statistics to dictionary for logging."""
        return {
            "total_records": self.total_records,
            "success_records": self.success_records,
            "error_records": self.error_records,
            "warning_records": self.warning_records,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": self.duration_seconds,
        }


# Global singleton instances for easy access
STATUS = StatusCodes()
STEPS = ProcessSteps()
CATEGORIES = SaleCategories()
RULES = BusinessRules()
PREFIXES = IDPrefixes()
MESSAGES = MessageTexts()