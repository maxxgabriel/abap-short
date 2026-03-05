"""
Data extraction module for Sales ETL Pipeline.
Reads raw sales data from source and prepares for transformation.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType
from datetime import datetime
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class SalesDataExtractor:
    """Extracts raw sales data from source systems."""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize the extractor.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary with source settings
        """
        self.spark = spark
        self.config = config
        self.source_path = config.get('source_path')
        self.source_format = config.get('source_format', 'parquet')
        
    def get_schema(self) -> StructType:
        """
        Define schema for raw sales data.
        
        Returns:
            StructType schema matching ABAP ZSALES_RAW table structure
        """
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
            StructField("status", StringType(), False)
        ])
    
    def extract(self, from_date: str, to_date: str) -> Optional[DataFrame]:
        """
        Extract raw sales data for the specified date range.
        
        Args:
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame containing raw sales data or None on error
        """
        try:
            logger.info(f"Starting extraction from {from_date} to {to_date}")
            logger.info(f"Reading from source: {self.source_path}")
            
            # Read data with schema enforcement
            df = self.spark.read \
                .format(self.source_format) \
                .schema(self.get_schema()) \
                .load(self.source_path)
            
            # Filter by date range and status
            df_filtered = df.filter(
                (df.trans_date >= from_date) & 
                (df.trans_date <= to_date) &
                (df.status == 'N')
            )
            
            record_count = df_filtered.count()
            logger.info(f"Extracted {record_count} records successfully")
            
            return df_filtered
            
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}", exc_info=True)
            return None
    
    def validate_data(self, df: DataFrame) -> bool:
        """
        Validate extracted data quality.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            True if validation passes, False otherwise
        """
        try:
            # Check for null values in required fields
            null_counts = df.select([
                df[col].isNull().cast("int").alias(col)
                for col in ["trans_id", "trans_date", "customer_id", "product_id", "quantity", "unit_price"]
            ]).agg(*[sum(col).alias(col) for col in ["trans_id", "trans_date", "customer_id", "product_id", "quantity", "unit_price"]]).collect()[0]
            
            has_nulls = any(count > 0 for count in null_counts.asDict().values())
            
            if has_nulls:
                logger.warning(f"Null values found in required fields: {null_counts.asDict()}")
                return False
            
            # Check for negative quantities or prices
            invalid_values = df.filter(
                (df.quantity <= 0) | (df.unit_price <= 0)
            ).count()
            
            if invalid_values > 0:
                logger.warning(f"Found {invalid_values} records with invalid quantities or prices")
                return False
            
            logger.info("Data validation passed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Validation error: {str(e)}", exc_info=True)
            return False


def create_sample_data(spark: SparkSession, output_path: str):
    """
    Generate sample raw sales data for testing.
    
    Args:
        spark: Active SparkSession
        output_path: Path to write sample data
    """
    from pyspark.sql import Row
    from datetime import date
    
    sample_data = [
        Row(trans_id="T000001", trans_date=date.today(), customer_id="CUST001",
            product_id="PROD001", quantity=10, unit_price=99.99,
            currency="USD", sales_rep="John Doe", region="NORTH", status="N"),
        Row(trans_id="T000002", trans_date=date.today(), customer_id="CUST002",
            product_id="PROD002", quantity=5, unit_price=149.99,
            currency="USD", sales_rep="Jane Smith", region="SOUTH", status="N"),
        Row(trans_id="T000003", trans_date=date.today(), customer_id="CUST003",
            product_id="PROD001", quantity=20, unit_price=99.99,
            currency="USD", sales_rep="John Doe", region="EAST", status="N"),
        Row(trans_id="T000004", trans_date=date.today(), customer_id="CUST001",
            product_id="PROD003", quantity=3, unit_price=299.99,
            currency="USD", sales_rep="Bob Wilson", region="WEST", status="N"),
        Row(trans_id="T000005", trans_date=date.today(), customer_id="CUST004",
            product_id="PROD002", quantity=15, unit_price=149.99,
            currency="USD", sales_rep="Jane Smith", region="SOUTH", status="N")
    ]
    
    df = spark.createDataFrame(sample_data)
    df.write.mode("overwrite").parquet(output_path)
    logger.info(f"Sample data written to {output_path}")