"""
PySpark Schema Definitions

Defines StructType schemas for ETL data structures.
Migrated from ABAP type definitions.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    IntegerType, DecimalType, TimestampType
)


class RawSalesSchema:
    """
    Schema for raw sales data.
    Migrated from ABAP ty_raw_sales type.
    """
    
    @staticmethod
    def get_schema() -> StructType:
        """
        Get the StructType schema for raw sales data.
        
        Returns:
            StructType schema
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
        ])


class AnalyticsSchema:
    """
    Schema for analytics data.
    Migrated from ABAP ty_analytics type.
    """
    
    @staticmethod
    def get_schema() -> StructType:
        """
        Get the StructType schema for analytics data.
        
        Returns:
            StructType schema
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


class ETLLogSchema:
    """
    Schema for ETL log entries.
    Migrated from ABAP ty_etl_log type.
    """
    
    @staticmethod
    def get_schema() -> StructType:
        """
        Get the StructType schema for ETL log data.
        
        Returns:
            StructType schema
        """
        return StructType([
            StructField("log_id", StringType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("execution_date", StringType(), nullable=False),
            StructField("execution_time", StringType(), nullable=False),
            StructField("process_step", StringType(), nullable=False),
            StructField("status", StringType(), nullable=False),
            StructField("records_processed", IntegerType(), nullable=True),
            StructField("records_success", IntegerType(), nullable=True),
            StructField("records_error", IntegerType(), nullable=True),
            StructField("message", StringType(), nullable=True),
        ])