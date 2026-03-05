"""
PySpark Schema Definitions for Sales ETL System

This module contains all StructType schemas converted from ABAP type definitions.
Schemas are used for DataFrame operations throughout the ETL pipeline.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    DateType,
    TimestampType,
    BooleanType,
)


class ETLSchemas:
    """Centralized schema definitions for the ETL system"""

    @staticmethod
    def raw_sales_schema() -> StructType:
        """
        Schema for raw sales data (source table: ZSALES_RAW)
        
        Equivalent to ABAP ty_raw_sales structure
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
        Schema for analytics data (target table: ZSALES_ANALYTICS)
        
        Equivalent to ABAP ty_analytics structure
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
            StructField("profit_margin", DecimalType(5, 2), nullable=True),
            StructField("category", StringType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("loaded_at", TimestampType(), nullable=True),
            StructField("loaded_by", StringType(), nullable=True),
        ])

    @staticmethod
    def etl_log_schema() -> StructType:
        """
        Schema for ETL log entries (log table: ZETL_LOG)
        
        Equivalent to ABAP ty_etl_log structure
        """
        return StructType([
            StructField("log_id", StringType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("execution_date", DateType(), nullable=False),
            StructField("execution_time", StringType(), nullable=False),
            StructField("process_step", StringType(), nullable=False),
            StructField("status", StringType(), nullable=False),
            StructField("records_processed", IntegerType(), nullable=True),
            StructField("records_success", IntegerType(), nullable=True),
            StructField("records_error", IntegerType(), nullable=True),
            StructField("message", StringType(), nullable=True),
            StructField("created_at", TimestampType(), nullable=True),
            StructField("created_by", StringType(), nullable=True),
        ])

    @staticmethod
    def etl_config_schema() -> StructType:
        """
        Schema for ETL configuration
        
        Equivalent to ABAP ty_etl_config structure
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
        Schema for ETL execution statistics
        
        Equivalent to ABAP ty_etl_statistics structure
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

    @staticmethod
    def execution_result_schema() -> StructType:
        """
        Schema for component execution results
        
        Equivalent to ABAP zif_etl_component=>ty_execution_result
        """
        return StructType([
            StructField("success", BooleanType(), nullable=False),
            StructField("records_total", IntegerType(), nullable=False),
            StructField("records_success", IntegerType(), nullable=False),
            StructField("records_error", IntegerType(), nullable=False),
            StructField("message", StringType(), nullable=True),
        ])


class StatusCodes:
    """Status code constants (equivalent to ABAP gc_status)"""
    NEW = "N"
    PROCESSED = "P"
    ERROR = "E"
    WARNING = "W"
    SUCCESS = "S"
    INFO = "I"


class ProcessSteps:
    """Process step constants (equivalent to ABAP gc_step)"""
    INIT = "INIT"
    EXTRACT = "EXTRACT"
    TRANSFORM = "TRANSFORM"
    LOAD = "LOAD"
    VALIDATE = "VALIDATE"
    COMPLETE = "COMPLETE"
    ERROR = "ERROR"


class SaleCategories:
    """Sale category constants (equivalent to ABAP gc_category)"""
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


# Convenience function to get all schemas
def get_all_schemas() -> dict:
    """
    Returns all schemas as a dictionary for easy access
    
    Returns:
        dict: Dictionary mapping schema names to StructType objects
    """
    return {
        "raw_sales": ETLSchemas.raw_sales_schema(),
        "analytics": ETLSchemas.analytics_schema(),
        "etl_log": ETLSchemas.etl_log_schema(),
        "etl_config": ETLSchemas.etl_config_schema(),
        "etl_statistics": ETLSchemas.etl_statistics_schema(),
        "execution_result": ETLSchemas.execution_result_schema(),
    }