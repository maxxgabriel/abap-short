"""
PySpark schema definitions for ETL type structures.
Converted from ABAP type definitions (ty_raw_sales, ty_analytics, ty_etl_log).
"""
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    DateType,
    TimestampType,
    BooleanType
)


class ETLSchemas:
    """Container for all ETL-related PySpark schemas."""
    
    @staticmethod
    def raw_sales_schema() -> StructType:
        """
        Raw sales data schema.
        
        Converted from ABAP ty_raw_sales:
        - trans_id (char10) -> StringType
        - trans_date (dats) -> DateType
        - customer_id (char10) -> StringType
        - product_id (char10) -> StringType
        - quantity (i) -> IntegerType
        - unit_price (p LENGTH 16 DECIMALS 2) -> DecimalType(16, 2)
        - currency (waers) -> StringType
        - sales_rep (char20) -> StringType
        - region (char10) -> StringType
        - status (char1) -> StringType
        - created_at (timestampl) -> TimestampType
        - created_by (syuname) -> StringType
        
        Returns:
            StructType: Schema for raw sales data
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
            StructField("created_by", StringType(), nullable=True)
        ])
    
    @staticmethod
    def analytics_schema() -> StructType:
        """
        Analytics data schema.
        
        Converted from ABAP ty_analytics:
        - analytics_id (char20) -> StringType
        - trans_date (dats) -> DateType
        - customer_id (char10) -> StringType
        - product_id (char10) -> StringType
        - total_quantity (i) -> IntegerType
        - gross_amount (p LENGTH 16 DECIMALS 2) -> DecimalType(16, 2)
        - net_amount (p LENGTH 16 DECIMALS 2) -> DecimalType(16, 2)
        - discount_amount (p LENGTH 16 DECIMALS 2) -> DecimalType(16, 2)
        - tax_amount (p LENGTH 16 DECIMALS 2) -> DecimalType(16, 2)
        - currency (waers) -> StringType
        - sales_rep (char20) -> StringType
        - region (char10) -> StringType
        - profit_margin (p LENGTH 5 DECIMALS 2) -> DecimalType(5, 2)
        - category (char10) -> StringType
        - etl_run_id (char20) -> StringType
        - loaded_at (timestampl) -> TimestampType
        - loaded_by (syuname) -> StringType
        
        Returns:
            StructType: Schema for analytics data
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
            StructField("loaded_by", StringType(), nullable=True)
        ])
    
    @staticmethod
    def etl_log_schema() -> StructType:
        """
        ETL log schema.
        
        Converted from ABAP ty_etl_log:
        - log_id (char20) -> StringType
        - etl_run_id (char20) -> StringType
        - execution_date (dats) -> DateType
        - execution_time (tims) -> StringType (HH:MM:SS format)
        - process_step (char20) -> StringType
        - status (char1) -> StringType
        - records_processed (i) -> IntegerType
        - records_success (i) -> IntegerType
        - records_error (i) -> IntegerType
        - message (char255) -> StringType
        - created_at (timestampl) -> TimestampType
        - created_by (syuname) -> StringType
        
        Returns:
            StructType: Schema for ETL log entries
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
            StructField("created_by", StringType(), nullable=True)
        ])
    
    @staticmethod
    def etl_config_schema() -> StructType:
        """
        ETL configuration schema.
        
        Converted from ABAP ty_etl_config.
        
        Returns:
            StructType: Schema for ETL configuration
        """
        return StructType([
            StructField("batch_size", IntegerType(), nullable=False),
            StructField("commit_interval", IntegerType(), nullable=False),
            StructField("parallel_jobs", IntegerType(), nullable=False),
            StructField("retry_attempts", IntegerType(), nullable=False),
            StructField("timeout_seconds", IntegerType(), nullable=False)
        ])
    
    @staticmethod
    def etl_statistics_schema() -> StructType:
        """
        ETL statistics schema.
        
        Converted from ABAP ty_etl_statistics.
        
        Returns:
            StructType: Schema for ETL statistics
        """
        return StructType([
            StructField("total_records", IntegerType(), nullable=False),
            StructField("success_records", IntegerType(), nullable=False),
            StructField("error_records", IntegerType(), nullable=False),
            StructField("warning_records", IntegerType(), nullable=False),
            StructField("start_time", TimestampType(), nullable=False),
            StructField("end_time", TimestampType(), nullable=True),
            StructField("duration_seconds", IntegerType(), nullable=True)
        ])


# Convenience function to get all schemas
def get_all_schemas() -> dict:
    """
    Get all ETL schemas in a dictionary.
    
    Returns:
        dict: Dictionary of schema name to StructType
    """
    return {
        "raw_sales": ETLSchemas.raw_sales_schema(),
        "analytics": ETLSchemas.analytics_schema(),
        "etl_log": ETLSchemas.etl_log_schema(),
        "etl_config": ETLSchemas.etl_config_schema(),
        "etl_statistics": ETLSchemas.etl_statistics_schema()
    }