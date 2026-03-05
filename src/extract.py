"""
Data Extraction Module
Extracts raw sales data from source
"""
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType

from src.utils.logger import ETLLogger


class DataExtractor:
    """Handles data extraction from source systems"""
    
    def __init__(
        self,
        spark: SparkSession,
        config: dict,
        logger: ETLLogger,
        etl_run_id: str
    ):
        """
        Initialize data extractor
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: Logger instance
            etl_run_id: ETL run identifier
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self.etl_run_id = etl_run_id
        
        # Define schema for raw sales data
        self.raw_schema = StructType([
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
    
    def extract_data(
        self,
        from_date: datetime,
        to_date: datetime
    ) -> Optional[DataFrame]:
        """
        Extract raw sales data for date range
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            
        Returns:
            DataFrame containing raw sales data
        """
        try:
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                message=f'Starting extraction from {from_date.date()} to {to_date.date()}'
            )
            
            # Get source configuration
            source_config = self.config.get('source', {})
            source_type = source_config.get('type', 'sample')
            
            # Extract based on source type
            if source_type == 'sample':
                df = self._extract_sample_data(from_date, to_date)
            elif source_type == 'jdbc':
                df = self._extract_from_database(from_date, to_date, source_config)
            elif source_type == 'parquet':
                df = self._extract_from_parquet(from_date, to_date, source_config)
            else:
                raise ValueError(f"Unsupported source type: {source_type}")
            
            # Filter by date range and status
            df = df.filter(
                (df.trans_date >= from_date.date()) &
                (df.trans_date <= to_date.date()) &
                (df.status == 'N')
            )
            
            record_count = df.count()
            
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                records_processed=record_count,
                records_success=record_count,
                message=f'Extracted {record_count} records successfully'
            )
            
            return df
            
        except Exception as e:
            self.logger.log_message(
                step='EXTRACT',
                status='E',
                message=f'Extraction failed: {str(e)}'
            )
            raise
    
    def _extract_sample_data(
        self,
        from_date: datetime,
        to_date: datetime
    ) -> DataFrame:
        """Generate sample data for demonstration"""
        from datetime import date
        
        sample_data = [
            ('T000001', date.today(), 'CUST001', 'PROD001', 10, 99.99, 'USD', 'John Doe', 'NORTH', 'N'),
            ('T000002', date.today(), 'CUST002', 'PROD002', 5, 149.99, 'USD', 'Jane Smith', 'SOUTH', 'N'),
            ('T000003', date.today(), 'CUST003', 'PROD001', 20, 99.99, 'USD', 'John Doe', 'EAST', 'N'),
            ('T000004', date.today(), 'CUST001', 'PROD003', 3, 299.99, 'USD', 'Bob Wilson', 'WEST', 'N'),
            ('T000005', date.today(), 'CUST004', 'PROD002', 15, 149.99, 'USD', 'Jane Smith', 'SOUTH', 'N'),
        ]
        
        return self.spark.createDataFrame(sample_data, schema=self.raw_schema)
    
    def _extract_from_database(
        self,
        from_date: datetime,
        to_date: datetime,
        source_config: dict
    ) -> DataFrame:
        """Extract from JDBC database source"""
        jdbc_url = source_config.get('jdbc_url')
        table = source_config.get('table', 'zsales_raw')
        
        return (self.spark.read
                .format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", table)
                .option("user", source_config.get('user'))
                .option("password", source_config.get('password'))
                .option("driver", source_config.get('driver', 'org.postgresql.Driver'))
                .load())
    
    def _extract_from_parquet(
        self,
        from_date: datetime,
        to_date: datetime,
        source_config: dict
    ) -> DataFrame:
        """Extract from Parquet files"""
        path = source_config.get('path')
        
        return self.spark.read.parquet(path)