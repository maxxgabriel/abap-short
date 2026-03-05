"""
ETL Type Definitions
Python equivalent of ABAP ZETL_TYPES type pool.
Defines data structures using PySpark StructType schemas.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, DateType, TimestampType
)
from typing import NamedTuple
from decimal import Decimal
from datetime import date, datetime


# PySpark Schema Definitions

RAW_SALES_SCHEMA = StructType([
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

ANALYTICS_SCHEMA = StructType([
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

ETL_LOG_SCHEMA = StructType([
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


# Python NamedTuple Definitions (for in-memory operations)

class RawSalesRecord(NamedTuple):
    """Raw sales data structure - equivalent to ABAP ty_raw_sales"""
    trans_id: str
    trans_date: date
    customer_id: str
    product_id: str
    quantity: int
    unit_price: Decimal
    currency: str
    sales_rep: str
    region: str
    status: str
    created_at: datetime = None
    created_by: str = None


class AnalyticsRecord(NamedTuple):
    """Analytics data structure - equivalent to ABAP ty_analytics"""
    analytics_id: str
    trans_date: date
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
    loaded_at: datetime = None
    loaded_by: str = None


class ETLLogRecord(NamedTuple):
    """ETL log structure - equivalent to ABAP ty_etl_log"""
    log_id: str
    etl_run_id: str
    execution_date: date
    execution_time: str
    process_step: str
    status: str
    records_processed: int
    records_success: int
    records_error: int
    message: str
    created_at: datetime = None
    created_by: str = None


class ETLConfig(NamedTuple):
    """ETL configuration structure - equivalent to ABAP ty_etl_config"""
    batch_size: int
    commit_interval: int
    parallel_jobs: int
    retry_attempts: int
    timeout_seconds: int


class ETLStatistics(NamedTuple):
    """ETL statistics structure - equivalent to ABAP ty_etl_statistics"""
    total_records: int
    success_records: int
    error_records: int
    warning_records: int
    start_time: datetime
    end_time: datetime
    duration_seconds: float


class ExecutionResult(NamedTuple):
    """Execution result structure - equivalent to ABAP ty_execution_result"""
    success: bool
    records_total: int
    records_success: int
    records_error: int
    message: str