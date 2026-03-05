"""
Configuration module for Sales ETL System.
Migrated from ABAP ZETL_TOP and ZCL_ETL_CONSTANTS.
"""
from dataclasses import dataclass, field
from typing import Dict, Any
from decimal import Decimal
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, DateType, TimestampType
)


@dataclass
class StatusCodes:
    """ETL status codes."""
    NEW: str = 'N'
    PROCESSED: str = 'P'
    ERROR: str = 'E'
    WARNING: str = 'W'
    SUCCESS: str = 'S'
    INFO: str = 'I'


@dataclass
class ProcessSteps:
    """ETL process step identifiers."""
    INIT: str = 'INIT'
    EXTRACT: str = 'EXTRACT'
    TRANSFORM: str = 'TRANSFORM'
    LOAD: str = 'LOAD'
    VALIDATE: str = 'VALIDATE'
    COMPLETE: str = 'COMPLETE'
    ERROR: str = 'ERROR'


@dataclass
class SaleCategories:
    """Sale categorization values."""
    HIGH: str = 'HIGH'
    MEDIUM: str = 'MEDIUM'
    LOW: str = 'LOW'


@dataclass
class BusinessRules:
    """Business rule constants for ETL transformations."""
    # Discount thresholds
    DISCOUNT_QTY_TIER1: int = 10
    DISCOUNT_QTY_TIER2: int = 15
    DISCOUNT_RATE_TIER1: Decimal = Decimal('0.05')
    DISCOUNT_RATE_TIER2: Decimal = Decimal('0.10')
    
    # Tax and cost ratios
    TAX_RATE: Decimal = Decimal('0.08')
    COST_RATIO: Decimal = Decimal('0.60')
    
    # Category thresholds
    CATEGORY_HIGH_THRESHOLD: Decimal = Decimal('2000.00')
    CATEGORY_MEDIUM_THRESHOLD: Decimal = Decimal('500.00')


@dataclass
class ETLDefaults:
    """Default configuration values for ETL processing."""
    BATCH_SIZE: int = 1000
    COMMIT_INTERVAL: int = 500
    RETRY_ATTEMPTS: int = 3
    TIMEOUT_SECONDS: int = 3600
    PARALLEL_JOBS: int = 4


@dataclass
class IDPrefixes:
    """Prefixes for generated identifiers."""
    ETL_RUN: str = 'ETL'
    LOG_ID: str = 'LOG'
    ANALYTICS_ID: str = 'ANL'


@dataclass
class Messages:
    """Standard message templates."""
    INIT_SUCCESS: str = 'ETL process initialized successfully'
    EXTRACT_START: str = 'Starting data extraction'
    EXTRACT_COMPLETE: str = 'Data extraction completed'
    TRANSFORM_START: str = 'Starting data transformation'
    TRANSFORM_COMPLETE: str = 'Data transformation completed'
    LOAD_START: str = 'Starting data load'
    LOAD_COMPLETE: str = 'Data load completed'
    ETL_COMPLETE: str = 'ETL process completed successfully'
    ETL_ERROR: str = 'ETL process failed'


class Schemas:
    """PySpark DataFrame schemas for ETL data structures."""
    
    @staticmethod
    def raw_sales_schema() -> StructType:
        """Schema for raw sales data (source table)."""
        return StructType([
            StructField("trans_id", StringType(), nullable=False),
            StructField("trans_date", DateType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("product_id", StringType(), nullable=False),
            StructField("quantity", IntegerType(), nullable=False),
            StructField("unit_price", DecimalType(16, 2), nullable=False),
            StructField("currency", StringType(), nullable=False),
            StructField("sales_rep", StringType(), nullable=True),
            StructField("region", StringType(), nullable=True),
            StructField("status", StringType(), nullable=False),
            StructField("created_at", TimestampType(), nullable=True),
            StructField("created_by", StringType(), nullable=True),
        ])
    
    @staticmethod
    def analytics_schema() -> StructType:
        """Schema for transformed analytics data (target table)."""
        return StructType([
            StructField("analytics_id", StringType(), nullable=False),
            StructField("trans_date", DateType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("product_id", StringType(), nullable=False),
            StructField("total_quantity", IntegerType(), nullable=False),
            StructField("gross_amount", DecimalType(16, 2), nullable=False),
            StructField("net_amount", DecimalType(16, 2), nullable=False),
            StructField("discount_amount", DecimalType(16, 2), nullable=False),
            StructField("tax_amount", DecimalType(16, 2), nullable=False),
            StructField("currency", StringType(), nullable=False),
            StructField("sales_rep", StringType(), nullable=True),
            StructField("region", StringType(), nullable=True),
            StructField("profit_margin", DecimalType(5, 2), nullable=False),
            StructField("category", StringType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("loaded_at", TimestampType(), nullable=True),
            StructField("loaded_by", StringType(), nullable=True),
        ])
    
    @staticmethod
    def etl_log_schema() -> StructType:
        """Schema for ETL execution logs."""
        return StructType([
            StructField("log_id", StringType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("execution_date", DateType(), nullable=False),
            StructField("execution_time", StringType(), nullable=False),
            StructField("process_step", StringType(), nullable=False),
            StructField("status", StringType(), nullable=False),
            StructField("records_processed", IntegerType(), nullable=False),
            StructField("records_success", IntegerType(), nullable=False),
            StructField("records_error", IntegerType(), nullable=False),
            StructField("message", StringType(), nullable=True),
            StructField("created_at", TimestampType(), nullable=True),
            StructField("created_by", StringType(), nullable=True),
        ])


@dataclass
class ETLConfig:
    """Main ETL configuration class combining all constants."""
    status: StatusCodes = field(default_factory=StatusCodes)
    steps: ProcessSteps = field(default_factory=ProcessSteps)
    categories: SaleCategories = field(default_factory=SaleCategories)
    business_rules: BusinessRules = field(default_factory=BusinessRules)
    defaults: ETLDefaults = field(default_factory=ETLDefaults)
    prefixes: IDPrefixes = field(default_factory=IDPrefixes)
    messages: Messages = field(default_factory=Messages)
    schemas: Schemas = field(default_factory=Schemas)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary format."""
        return {
            'status_codes': {
                'new': self.status.NEW,
                'processed': self.status.PROCESSED,
                'error': self.status.ERROR,
                'warning': self.status.WARNING,
                'success': self.status.SUCCESS,
                'info': self.status.INFO,
            },
            'process_steps': {
                'init': self.steps.INIT,
                'extract': self.steps.EXTRACT,
                'transform': self.steps.TRANSFORM,
                'load': self.steps.LOAD,
                'validate': self.steps.VALIDATE,
                'complete': self.steps.COMPLETE,
                'error': self.steps.ERROR,
            },
            'categories': {
                'high': self.categories.HIGH,
                'medium': self.categories.MEDIUM,
                'low': self.categories.LOW,
            },
            'business_rules': {
                'discount_qty_tier1': self.business_rules.DISCOUNT_QTY_TIER1,
                'discount_qty_tier2': self.business_rules.DISCOUNT_QTY_TIER2,
                'discount_rate_tier1': str(self.business_rules.DISCOUNT_RATE_TIER1),
                'discount_rate_tier2': str(self.business_rules.DISCOUNT_RATE_TIER2),
                'tax_rate': str(self.business_rules.TAX_RATE),
                'cost_ratio': str(self.business_rules.COST_RATIO),
                'category_high_threshold': str(self.business_rules.CATEGORY_HIGH_THRESHOLD),
                'category_medium_threshold': str(self.business_rules.CATEGORY_MEDIUM_THRESHOLD),
            },
            'etl_defaults': {
                'batch_size': self.defaults.BATCH_SIZE,
                'commit_interval': self.defaults.COMMIT_INTERVAL,
                'retry_attempts': self.defaults.RETRY_ATTEMPTS,
                'timeout_seconds': self.defaults.TIMEOUT_SECONDS,
                'parallel_jobs': self.defaults.PARALLEL_JOBS,
            },
            'id_prefixes': {
                'etl_run': self.prefixes.ETL_RUN,
                'log_id': self.prefixes.LOG_ID,
                'analytics_id': self.prefixes.ANALYTICS_ID,
            },
        }


# Global configuration instance
config = ETLConfig()