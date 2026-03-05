"""
PySpark schema definitions for Sales ETL System.
Converted from ABAP ZETL_TYPES type definitions.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    DateType,
    TimestampType,
)


class ETLSchemas:
    """Central schema definitions for ETL system."""

    @staticmethod
    def raw_sales_schema() -> StructType:
        """
        Raw sales data structure from source system.
        Corresponds to ABAP ty_raw_sales.
        """
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
        """
        Analytics data structure for target system.
        Corresponds to ABAP ty_analytics.
        """
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
        """
        ETL execution log structure.
        Corresponds to ABAP ty_etl_log.
        """
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

    @staticmethod
    def etl_config_schema() -> StructType:
        """
        ETL configuration structure.
        Corresponds to ABAP ty_etl_config.
        """
        return StructType([
            StructField("batch_size", IntegerType(), nullable=False),
            StructField("commit_interval", IntegerType(), nullable=False),
            StructField("parallel_jobs", IntegerType(), nullable=False),
            StructField("retry_attempts", IntegerType(), nullable=False),
            StructField("timeout_seconds", IntegerType(), nullable=False),
        ])

    @staticmethod
    def etl_statistics_schema() -> StructType:
        """
        ETL statistics structure.
        Corresponds to ABAP ty_etl_statistics.
        """
        return StructType([
            StructField("total_records", IntegerType(), nullable=False),
            StructField("success_records", IntegerType(), nullable=False),
            StructField("error_records", IntegerType(), nullable=False),
            StructField("warning_records", IntegerType(), nullable=False),
            StructField("start_time", TimestampType(), nullable=False),
            StructField("end_time", TimestampType(), nullable=True),
            StructField("duration_seconds", IntegerType(), nullable=True),
        ])


class StatusCodes:
    """Status code constants matching ABAP gc_status."""
    
    NEW = "N"
    PROCESSED = "P"
    ERROR = "E"
    WARNING = "W"
    SUCCESS = "S"
    INFO = "I"


class ProcessSteps:
    """Process step constants matching ABAP gc_step."""
    
    INIT = "INIT"
    EXTRACT = "EXTRACT"
    TRANSFORM = "TRANSFORM"
    LOAD = "LOAD"
    VALIDATE = "VALIDATE"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"


class SaleCategories:
    """Sale category constants matching ABAP gc_category."""
    
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"