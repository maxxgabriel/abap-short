from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    IntegerType, DecimalType, TimestampType
)
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class RawSalesRecord:
    """Raw sales data model"""
    trans_id: str
    trans_date: datetime
    customer_id: str
    product_id: str
    quantity: int
    unit_price: float
    currency: str
    sales_rep: str
    region: str
    status: str
    created_at: Optional[datetime] = None
    created_by: Optional[str] = None


@dataclass
class AnalyticsRecord:
    """Analytics data model"""
    analytics_id: str
    trans_date: datetime
    customer_id: str
    product_id: str
    total_quantity: int
    gross_amount: float
    net_amount: float
    discount_amount: float
    tax_amount: float
    currency: str
    sales_rep: str
    region: str
    profit_margin: float
    category: str
    etl_run_id: str
    loaded_at: Optional[datetime] = None
    loaded_by: Optional[str] = None


@dataclass
class ETLLogRecord:
    """ETL log entry model"""
    log_id: str
    etl_run_id: str
    execution_date: datetime
    execution_time: str
    process_step: str
    status: str
    records_processed: int
    records_success: int
    records_error: int
    message: str
    created_at: Optional[datetime] = None
    created_by: Optional[str] = None


class SchemaDefinitions:
    """Centralized schema definitions"""
    
    @staticmethod
    def raw_sales_schema() -> StructType:
        return StructType([
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
    
    @staticmethod
    def analytics_schema() -> StructType:
        return StructType([
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
            StructField("loaded_at", TimestampType(), True),
            StructField("loaded_by", StringType(), True)
        ])
    
    @staticmethod
    def etl_log_schema() -> StructType:
        return StructType([
            StructField("log_id", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("execution_date", DateType(), False),
            StructField("execution_time", StringType(), False),
            StructField("process_step", StringType(), False),
            StructField("status", StringType(), False),
            StructField("records_processed", IntegerType(), False),
            StructField("records_success", IntegerType(), False),
            StructField("records_error", IntegerType(), False),
            StructField("message", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True)
        ])