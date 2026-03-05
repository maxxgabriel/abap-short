"""
Data extraction module for Sales ETL pipeline.
Extracts raw sales data from source systems.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType, TimestampType
)
from typing import Optional
import logging
from datetime import datetime


class SalesDataExtractor:
    """Extracts raw sales data from source systems."""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize the extractor.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def get_raw_sales_schema(self) -> StructType:
        """
        Define schema for raw sales data.
        
        Returns:
            StructType schema definition
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
            StructField("status", StringType(), False),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True)
        ])
    
    def extract_data(
        self, 
        from_date: str, 
        to_date: str,
        source_path: Optional[str] = None
    ) -> DataFrame:
        """
        Extract raw sales data for the specified date range.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            source_path: Optional override for source data path
            
        Returns:
            DataFrame containing raw sales data
        """
        self.logger.info(
            f"Starting extraction from {from_date} to {to_date}"
        )
        
        try:
            # Get source path from config or parameter
            path = source_path or self.config.get("source_data_path")
            
            if not path:
                # Create sample data if no path specified
                self.logger.warning(
                    "No source path specified, generating sample data"
                )
                return self._generate_sample_data()
            
            # Read data with schema
            schema = self.get_raw_sales_schema()
            df = self.spark.read.schema(schema).parquet(path)
            
            # Filter by date range and status
            filtered_df = df.filter(
                (df.trans_date >= from_date) &
                (df.trans_date <= to_date) &
                (df.status == 'N')
            )
            
            record_count = filtered_df.count()
            self.logger.info(
                f"Extracted {record_count} records successfully"
            )
            
            return filtered_df
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _generate_sample_data(self) -> DataFrame:
        """
        Generate sample data for demonstration purposes.
        
        Returns:
            DataFrame with sample sales data
        """
        from datetime import date
        
        sample_data = [
            (
                "T000001", date.today(), "CUST001", "PROD001", 
                10, 99.99, "USD", "John Doe", "NORTH", "N",
                datetime.now(), "SYSTEM"
            ),
            (
                "T000002", date.today(), "CUST002", "PROD002",
                5, 149.99, "USD", "Jane Smith", "SOUTH", "N",
                datetime.now(), "SYSTEM"
            ),
            (
                "T000003", date.today(), "CUST003", "PROD001",
                20, 99.99, "USD", "John Doe", "EAST", "N",
                datetime.now(), "SYSTEM"
            ),
            (
                "T000004", date.today(), "CUST001", "PROD003",
                3, 299.99, "USD", "Bob Wilson", "WEST", "N",
                datetime.now(), "SYSTEM"
            ),
            (
                "T000005", date.today(), "CUST004", "PROD002",
                15, 149.99, "USD", "Jane Smith", "SOUTH", "N",
                datetime.now(), "SYSTEM"
            )
        ]
        
        schema = self.get_raw_sales_schema()
        df = self.spark.createDataFrame(sample_data, schema)
        
        self.logger.info(f"Generated {len(sample_data)} sample records")
        
        return df
    
    def validate_extracted_data(self, df: DataFrame) -> bool:
        """
        Validate extracted data quality.
        
        Args:
            df: Extracted DataFrame
            
        Returns:
            True if validation passes
        """
        # Check for null values in required fields
        null_counts = df.select([
            df[col].isNull().cast("int").alias(col)
            for col in ["trans_id", "customer_id", "product_id", "quantity"]
        ]).groupBy().sum()
        
        has_nulls = any(
            null_counts.first()[col] > 0 
            for col in null_counts.columns
        )
        
        if has_nulls:
            self.logger.warning("Extracted data contains null values")
            return False
        
        # Check for negative quantities or prices
        invalid_count = df.filter(
            (df.quantity <= 0) | (df.unit_price <= 0)
        ).count()
        
        if invalid_count > 0:
            self.logger.warning(
                f"Found {invalid_count} records with invalid quantities/prices"
            )
            return False
        
        self.logger.info("Data validation passed")
        return True