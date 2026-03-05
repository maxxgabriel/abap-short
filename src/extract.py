"""
Extract module for Sales ETL System.
Extracts raw sales data from source data store.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from typing import Optional
import logging
from datetime import date


class SalesExtractor:
    """Extracts raw sales data from source."""
    
    def __init__(self, spark: SparkSession, logger: logging.Logger):
        """
        Initialize the extractor.
        
        Args:
            spark: SparkSession instance
            logger: Logger instance
        """
        self.spark = spark
        self.logger = logger
        self._schema = self._get_raw_sales_schema()
    
    def _get_raw_sales_schema(self) -> StructType:
        """Define the schema for raw sales data."""
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
            StructField("status", StringType(), nullable=False)
        ])
    
    def extract_data(self, from_date: date, to_date: date, source_path: Optional[str] = None) -> DataFrame:
        """
        Extract raw sales data within date range.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            source_path: Optional path to source data (for file-based sources)
        
        Returns:
            DataFrame containing raw sales data
        
        Raises:
            Exception: If extraction fails
        """
        try:
            self.logger.info(f"Starting extraction from {from_date} to {to_date}")
            
            if source_path:
                # Extract from file source (CSV, Parquet, etc.)
                df = self.spark.read.schema(self._schema).parquet(source_path)
            else:
                # For demonstration: create sample data
                df = self._create_sample_data()
            
            # Filter by date range and status
            df_filtered = df.filter(
                (df.trans_date >= from_date) &
                (df.trans_date <= to_date) &
                (df.status == 'N')
            )
            
            record_count = df_filtered.count()
            self.logger.info(f"Extracted {record_count} records successfully")
            
            return df_filtered
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _create_sample_data(self) -> DataFrame:
        """Create sample data for demonstration purposes."""
        from datetime import datetime
        
        data = [
            ("T000001", datetime.now().date(), "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
            ("T000002", datetime.now().date(), "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", datetime.now().date(), "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
            ("T000004", datetime.now().date(), "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", datetime.now().date(), "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
        ]
        
        return self.spark.createDataFrame(data, schema=self._schema)


class ExtractorInterface:
    """Interface contract for all extractor components."""
    
    def extract_data(self, from_date: date, to_date: date, **kwargs) -> DataFrame:
        """Extract data within date range."""
        raise NotImplementedError("Subclasses must implement extract_data()")
    
    def get_component_name(self) -> str:
        """Return component name."""
        raise NotImplementedError("Subclasses must implement get_component_name()")
    
    def validate_prerequisites(self) -> bool:
        """Validate that prerequisites are met before extraction."""
        raise NotImplementedError("Subclasses must implement validate_prerequisites()")