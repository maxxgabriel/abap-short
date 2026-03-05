"""
Data Extractor Module
Extracts raw sales data from source with date filtering using PySpark DataFrame operations.
Implements dependency injection pattern for logger.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from datetime import date
from typing import Optional
import logging


class DataExtractor:
    """
    Extracts raw sales data from source table with date-based filtering.
    Uses PySpark DataFrame API for scalable data extraction.
    """
    
    def __init__(self, spark: SparkSession, logger: logging.Logger, config: dict):
        """
        Initialize the DataExtractor with dependencies.
        
        Args:
            spark: SparkSession instance for data operations
            logger: Logger instance for logging extraction activities
            config: Configuration dictionary with extraction parameters
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.schema = self._define_schema()
        
    def _define_schema(self) -> StructType:
        """
        Define the schema for raw sales data.
        
        Returns:
            StructType schema for raw sales records
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
            StructField("status", StringType(), nullable=False)
        ])
    
    def extract_data(
        self, 
        from_date: date, 
        to_date: date,
        source_table: Optional[str] = None
    ) -> Optional[DataFrame]:
        """
        Extract raw sales data within the specified date range.
        
        Args:
            from_date: Start date for extraction (inclusive)
            to_date: End date for extraction (inclusive)
            source_table: Optional override for source table name
            
        Returns:
            DataFrame containing extracted sales data, or None if extraction fails
        """
        try:
            table_name = source_table or self.config.get('source_table', 'zsales_raw')
            status_filter = self.config.get('extract_status_filter', 'N')
            
            self.logger.info(
                f"Starting extraction from {table_name} for date range: {from_date} to {to_date}"
            )
            
            # Read data from source table with date filtering
            df = self._read_source_data(table_name, from_date, to_date, status_filter)
            
            if df is None:
                self.logger.error("Failed to read source data")
                return None
            
            # Apply additional filters and validations
            df_filtered = self._apply_filters(df)
            
            record_count = df_filtered.count()
            
            self.logger.info(
                f"Extraction completed successfully. Records extracted: {record_count}",
                extra={
                    'step': 'EXTRACT',
                    'status': 'SUCCESS',
                    'records_processed': record_count,
                    'records_success': record_count
                }
            )
            
            return df_filtered
            
        except Exception as e:
            self.logger.error(
                f"Extraction failed: {str(e)}",
                extra={'step': 'EXTRACT', 'status': 'ERROR'},
                exc_info=True
            )
            return None
    
    def _read_source_data(
        self, 
        table_name: str, 
        from_date: date, 
        to_date: date,
        status_filter: str
    ) -> Optional[DataFrame]:
        """
        Read data from source table with date and status filters.
        
        Args:
            table_name: Name of the source table
            from_date: Start date for filtering
            to_date: End date for filtering
            status_filter: Status value to filter ('N' for new records)
            
        Returns:
            DataFrame with filtered data or None if read fails
        """
        try:
            # For production: read from actual database/table
            # df = self.spark.read.table(table_name)
            
            # For demonstration: create sample data
            df = self._create_sample_data()
            
            # Apply date range filter
            df_filtered = df.filter(
                (df.trans_date >= from_date) & 
                (df.trans_date <= to_date) &
                (df.status == status_filter)
            )
            
            return df_filtered
            
        except Exception as e:
            self.logger.error(f"Error reading source data: {str(e)}")
            return None
    
    def _create_sample_data(self) -> DataFrame:
        """
        Create sample data for demonstration purposes.
        
        Returns:
            DataFrame with sample sales records
        """
        sample_data = [
            ('T000001', date.today(), 'CUST001', 'PROD001', 10, 99.99, 'USD', 'John Doe', 'NORTH', 'N'),
            ('T000002', date.today(), 'CUST002', 'PROD002', 5, 149.99, 'USD', 'Jane Smith', 'SOUTH', 'N'),
            ('T000003', date.today(), 'CUST003', 'PROD001', 20, 99.99, 'USD', 'John Doe', 'EAST', 'N'),
            ('T000004', date.today(), 'CUST001', 'PROD003', 3, 299.99, 'USD', 'Bob Wilson', 'WEST', 'N'),
            ('T000005', date.today(), 'CUST004', 'PROD002', 15, 149.99, 'USD', 'Jane Smith', 'SOUTH', 'N'),
        ]
        
        return self.spark.createDataFrame(sample_data, schema=self.schema)
    
    def _apply_filters(self, df: DataFrame) -> DataFrame:
        """
        Apply additional business filters to extracted data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Filtered DataFrame
        """
        # Filter out invalid records
        df_clean = df.filter(
            (df.quantity > 0) & 
            (df.unit_price > 0) &
            (df.trans_id.isNotNull()) &
            (df.customer_id.isNotNull()) &
            (df.product_id.isNotNull())
        )
        
        return df_clean
    
    def get_extraction_stats(self, df: DataFrame) -> dict:
        """
        Calculate extraction statistics.
        
        Args:
            df: Extracted DataFrame
            
        Returns:
            Dictionary containing extraction statistics
        """
        try:
            total_records = df.count()
            distinct_customers = df.select("customer_id").distinct().count()
            distinct_products = df.select("product_id").distinct().count()
            
            return {
                'total_records': total_records,
                'distinct_customers': distinct_customers,
                'distinct_products': distinct_products,
                'date_range': {
                    'min_date': df.agg({"trans_date": "min"}).collect()[0][0],
                    'max_date': df.agg({"trans_date": "max"}).collect()[0][0]
                }
            }
        except Exception as e:
            self.logger.warning(f"Failed to calculate extraction stats: {str(e)}")
            return {}