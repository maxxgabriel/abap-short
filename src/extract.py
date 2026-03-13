"""
Data extraction module for Sales ETL process.
Extracts raw sales data from source system.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from datetime import datetime
from typing import Tuple
import logging


class SalesDataExtractor:
    """Extracts raw sales data from source tables."""
    
    def __init__(self, spark: SparkSession, config: dict, logger: logging.Logger):
        """
        Initialize extractor.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            logger: Logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self.raw_sales_schema = self._get_raw_sales_schema()
    
    def _get_raw_sales_schema(self) -> StructType:
        """Define schema for raw sales data."""
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
        ])
    
    def extract_sales_data(
        self, 
        from_date: str, 
        to_date: str,
        source_path: str = None
    ) -> Tuple[DataFrame, dict]:
        """
        Extract raw sales data for the specified date range.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            source_path: Optional override for source data path
            
        Returns:
            Tuple of (DataFrame with raw sales data, extraction statistics)
        """
        try:
            self.logger.info(f"Starting extraction from {from_date} to {to_date}")
            
            # Use config path or override
            data_path = source_path or self.config.get('source_data_path')
            
            # Read raw sales data
            if data_path.endswith('.parquet'):
                df = self.spark.read.parquet(data_path)
            elif data_path.endswith('.csv'):
                df = self.spark.read.csv(
                    data_path,
                    schema=self.raw_sales_schema,
                    header=True
                )
            elif data_path.startswith('jdbc:'):
                # JDBC source
                df = self._extract_from_jdbc(from_date, to_date)
            else:
                # For demo/testing - create sample data
                df = self._create_sample_data()
            
            # Filter by date range and status
            df_filtered = df.filter(
                (df.trans_date >= from_date) & 
                (df.trans_date <= to_date) &
                (df.status == 'N')
            )
            
            # Cache for performance
            df_filtered.cache()
            
            # Collect statistics
            record_count = df_filtered.count()
            stats = {
                'records_extracted': record_count,
                'extraction_time': datetime.now().isoformat(),
                'from_date': from_date,
                'to_date': to_date
            }
            
            self.logger.info(f"Extracted {record_count} records successfully")
            
            return df_filtered, stats
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _extract_from_jdbc(self, from_date: str, to_date: str) -> DataFrame:
        """Extract data from JDBC source."""
        jdbc_config = self.config.get('jdbc', {})
        
        query = f"""
            (SELECT trans_id, trans_date, customer_id, product_id, 
                    quantity, unit_price, currency, sales_rep, 
                    region, status
             FROM {jdbc_config.get('source_table', 'ZSALES_RAW')}
             WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
             AND status = 'N') as sales_data
        """
        
        df = self.spark.read.jdbc(
            url=jdbc_config['url'],
            table=query,
            properties={
                'user': jdbc_config['user'],
                'password': jdbc_config['password'],
                'driver': jdbc_config.get('driver', 'com.sap.db.jdbc.Driver')
            }
        )
        
        return df
    
    def _create_sample_data(self) -> DataFrame:
        """Create sample data for testing/demo purposes."""
        from pyspark.sql.functions import current_date
        
        sample_data = [
            ('T000001', 'CUST001', 'PROD001', 10, 99.99, 'USD', 'John Doe', 'NORTH', 'N'),
            ('T000002', 'CUST002', 'PROD002', 5, 149.99, 'USD', 'Jane Smith', 'SOUTH', 'N'),
            ('T000003', 'CUST003', 'PROD001', 20, 99.99, 'USD', 'John Doe', 'EAST', 'N'),
            ('T000004', 'CUST001', 'PROD003', 3, 299.99, 'USD', 'Bob Wilson', 'WEST', 'N'),
            ('T000005', 'CUST004', 'PROD002', 15, 149.99, 'USD', 'Jane Smith', 'SOUTH', 'N'),
        ]
        
        df = self.spark.createDataFrame(
            sample_data,
            ['trans_id', 'customer_id', 'product_id', 'quantity', 
             'unit_price', 'currency', 'sales_rep', 'region', 'status']
        )
        
        # Add current date
        df = df.withColumn('trans_date', current_date())
        
        return df


def create_extractor(spark: SparkSession, config: dict, logger: logging.Logger) -> SalesDataExtractor:
    """Factory function to create extractor instance."""
    return SalesDataExtractor(spark, config, logger)