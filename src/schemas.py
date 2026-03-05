"""
PySpark schema definitions for ETL data structures.
Migrated from ABAP ZETL_TYPES type pool.
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
    """Schema definitions for all ETL DataFrames."""
    
    @staticmethod
    def raw_sales_schema() -> StructType:
        """
        Schema for raw sales data from ZSALES_RAW table.
        Migrated from ZETL_TYPES=>ty_raw_sales.
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
        Schema for analytics data for ZSALES_ANALYTICS table.
        Migrated from ZETL_TYPES=>ty_analytics.
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
        Schema for ETL execution log table ZETL_LOG.
        Migrated from ZETL_TYPES=>ty_etl_log.
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
        Schema for ETL configuration.
        Migrated from ZETL_TYPES=>ty_etl_config.
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
        Schema for ETL statistics tracking.
        Migrated from ZETL_TYPES=>ty_etl_statistics.
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