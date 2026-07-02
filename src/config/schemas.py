```python
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, IntegerType,
    DecimalType, TimestampType
)

def get_raw_sales_schema():
    """Schema for raw sales data from ZSALES_RAW table"""
    return StructType([
        StructField("transaction_id", StringType(), False),
        StructField("transaction_date", DateType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DecimalType(18, 2), False),
        StructField("gross_amount", DecimalType(18, 2), False),
        StructField("currency", StringType(), False),
        StructField("sales_rep", StringType(), True),
        StructField("region", StringType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("created_by", StringType(), True)
    ])

def get_analytics_schema():
    """Schema for analytics data for ZSALES_ANALYTICS table"""
    return StructType([
        StructField("analytics_id", StringType(), False),
        StructField("transaction_id", StringType(), False),
        StructField("transaction_date", DateType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DecimalType(18, 2), False),
        StructField("gross_amount", DecimalType(18, 2), False),
        StructField("discount_rate", DecimalType(5, 2), False),
        StructField("discount_amount", DecimalType(18, 2), False),
        StructField("tax_rate", DecimalType(5, 2), False),
        StructField("tax_amount", DecimalType(18, 2), False),
        StructField("net_amount", DecimalType(18, 2), False),
        StructField("cost_amount", DecimalType(18, 2), False),
        StructField("profit_margin", DecimalType(5, 2), False),
        StructField("sale_category", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("sales_rep", StringType(), True),
        StructField("region", StringType(), True),
        StructField("processed_at", TimestampType(), False),
        StructField("etl_run_id", StringType(), False)
    ])

def get_etl_log_schema():
    """Schema for ETL log data for ZETL_LOG table"""
    return StructType([
        StructField("log_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("process_step", StringType(), False),
        StructField("status", StringType(), False),
        StructField("message", StringType(), True),
        StructField("records_processed", IntegerType(), True),
        StructField("records_success", IntegerType(), True),
        StructField("records_failed", IntegerType(), True),
        StructField("execution_time", IntegerType(), True),
        StructField("timestamp", TimestampType(), False),
        StructField("user_name", StringType(), True)
    ])
```