"""
Schema definitions for ETL data structures.
Defines StructType schemas for raw and analytics data.
"""
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    IntegerType, DecimalType, TimestampType
)


# Raw sales data schema (maps to ty_raw_sales ABAP structure)
SALES_RAW_SCHEMA = StructType([
    StructField("trans_id", StringType(), False),
    StructField("trans_date", DateType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DecimalType(16, 2), False),
    StructField("currency", StringType(), False),
    StructField("sales_rep", StringType(), True),
    StructField("region", StringType(), True),
    StructField("status", StringType(), False),
    StructField("created_at", TimestampType(), True),
    StructField("created_by", StringType(), True)
])


# Analytics data schema (maps to ty_analytics ABAP structure)
SALES_ANALYTICS_SCHEMA = StructType([
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
    StructField("sales_rep", StringType(), True),
    StructField("region", StringType(), True),
    StructField("profit_margin", DecimalType(5, 2), False),
    StructField("category", StringType(), False),
    StructField("etl_run_id", StringType(), False),
    StructField("loaded_at", TimestampType(), False),
    StructField("loaded_by", StringType(), False)
])


# ETL log schema
ETL_LOG_SCHEMA = StructType([
    StructField("log_id", StringType(), False),
    StructField("etl_run_id", StringType(), False),
    StructField("execution_date", StringType(), False),
    StructField("execution_time", StringType(), False),
    StructField("process_step", StringType(), False),
    StructField("status", StringType(), False),
    StructField("records_processed", IntegerType(), False),
    StructField("records_success", IntegerType(), False),
    StructField("records_error", IntegerType(), False),
    StructField("message", StringType(), True)
])