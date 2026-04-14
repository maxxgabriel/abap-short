"""
PySpark extraction module for reading raw sales data.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from typing import Dict
import logging

logger = logging.getLogger(__name__)


def get_raw_sales_schema() -> StructType:
    """
    Define schema for raw sales data.
    
    Returns:
        StructType schema for raw sales data
    """
    return StructType([
        StructField("trans_id", StringType(), False),
        StructField("trans_date", DateType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DecimalType(16, 2), False),
        StructField("currency", StringType(), False),
        StructField("sales_rep", StringType(), False),
        StructField("region", StringType(), False),
        StructField("status", StringType(), False)
    ])


def extract_sales_data(spark: SparkSession, config: Dict, 
                      from_date: str, to_date: str) -> DataFrame:
    """
    Extract raw sales data from source.
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        from_date: Start date for extraction (YYYY-MM-DD)
        to_date: End date for extraction (YYYY-MM-DD)
        
    Returns:
        DataFrame containing raw sales data
    """
    source_config = config.get('source', {})
    source_type = source_config.get('type', 'parquet')
    source_path = source_config.get('path', 'data/raw/sales')
    
    logger.info(f"Extracting sales data from {from_date} to {to_date}")
    logger.info(f"Source: {source_type} at {source_path}")
    
    schema = get_raw_sales_schema()
    
    if source_type == 'parquet':
        df = spark.read.schema(schema).parquet(source_path)
    elif source_type == 'csv':
        df = spark.read.schema(schema).option("header", "true").csv(source_path)
    elif source_type == 'jdbc':
        jdbc_config = source_config.get('jdbc', {})
        df = spark.read.jdbc(
            url=jdbc_config.get('url'),
            table=jdbc_config.get('table', 'zsales_raw'),
            properties={
                'user': jdbc_config.get('user'),
                'password': jdbc_config.get('password'),
                'driver': jdbc_config.get('driver', 'com.sap.db.jdbc.Driver')
            }
        )
    else:
        raise ValueError(f"Unsupported source type: {source_type}")
    
    # Filter by date range and status
    df = df.filter(
        (df.trans_date >= from_date) & 
        (df.trans_date <= to_date) &
        (df.status == 'N')
    )
    
    record_count = df.count()
    logger.info(f"Extracted {record_count} records")
    
    return df