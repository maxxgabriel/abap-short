"""
Schema definitions for ETL data structures.
Replaces ABAP TYPE definitions with PySpark StructType schemas.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, DateType, TimestampType
)


class ETLSchemas:
    """Container for all ETL schema definitions"""
    
    @staticmethod
    def raw_sales_schema() -> StructType:
        """
        Schema for raw sales data (replaces ABAP ty_raw_sales)
        Equivalent to ZSALES_RAW table structure
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
        Schema for analytics data (replaces ABAP ty_analytics)
        Equivalent to ZSALES_ANALYTICS table structure
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
            StructField("loaded_by", StringType(), nullable=True)
        ])
    
    @staticmethod
    def etl_log_schema() -> StructType:
        """
        Schema for ETL log data (replaces ABAP ty_etl_log)
        Equivalent to ZETL_LOG table structure
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