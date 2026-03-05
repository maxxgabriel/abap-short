"""
Data Extractor
Migrated from ABAP ZCL_ETL_EXTRACTOR
Extracts raw sales data from source
"""
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType

from src.utils.logger import ETLLogger


class Extractor:
    """Extracts raw sales data from source"""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize extractor
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
    
    def get_raw_sales_schema(self) -> StructType:
        """
        Define schema for raw sales data
        
        Returns:
            StructType schema
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
    
    def extract_data(self, from_date: datetime, to_date: datetime) -> Optional[DataFrame]:
        """
        Extract raw sales data for date range
        
        Args:
            from_date: Start date
            to_date: End date
            
        Returns:
            DataFrame with raw sales data, or None if failed
        """
        try:
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                message=f"Starting extraction from {from_date.date()} to {to_date.date()}"
            )
            
            # Get source configuration
            source_config = self.config.get('source', {})
            source_type = source_config.get('type', 'sample')
            
            if source_type == 'sample':
                # Generate sample data for demonstration
                raw_df = self._generate_sample_data(from_date, to_date)
            elif source_type == 'jdbc':
                # Read from database
                raw_df = self._extract_from_jdbc(from_date, to_date, source_config)
            elif source_type == 'parquet':
                # Read from parquet files
                raw_df = self._extract_from_parquet(from_date, to_date, source_config)
            elif source_type == 'csv':
                # Read from CSV files
                raw_df = self._extract_from_csv(from_date, to_date, source_config)
            else:
                raise ValueError(f"Unsupported source type: {source_type}")
            
            # Filter by status = 'N' (new records)
            raw_df = raw_df.filter(raw_df.status == 'N')
            
            record_count = raw_df.count()
            
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return raw_df
            
        except Exception as e:
            self.logger.log_message(
                step='EXTRACT',
                status='E',
                message=f"Extraction failed: {str(e)}"
            )
            return None
    
    def _generate_sample_data(self, from_date: datetime, to_date: datetime) -> DataFrame:
        """Generate sample data for demonstration"""
        from pyspark.sql import Row
        from decimal import Decimal
        
        sample_data = [
            Row(
                trans_id='T000001',
                trans_date=from_date.date(),
                customer_id='CUST001',
                product_id='PROD001',
                quantity=10,
                unit_price=Decimal('99.99'),
                currency='USD',
                sales_rep='John Doe',
                region='NORTH',
                status='N'
            ),
            Row(
                trans_id='T000002',
                trans_date=from_date.date(),
                customer_id='CUST002',
                product_id='PROD002',
                quantity=5,
                unit_price=Decimal('149.99'),
                currency='USD',
                sales_rep='Jane Smith',
                region='SOUTH',
                status='N'
            ),
            Row(
                trans_id='T000003',
                trans_date=from_date.date(),
                customer_id='CUST003',
                product_id='PROD001',
                quantity=20,
                unit_price=Decimal('99.99'),
                currency='USD',
                sales_rep='John Doe',
                region='EAST',
                status='N'
            ),
            Row(
                trans_id='T000004',
                trans_date=from_date.date(),
                customer_id='CUST001',
                product_id='PROD003',
                quantity=3,
                unit_price=Decimal('299.99'),
                currency='USD',
                sales_rep='Bob Wilson',
                region='WEST',
                status='N'
            ),
            Row(
                trans_id='T000005',
                trans_date=from_date.date(),
                customer_id='CUST004',
                product_id='PROD002',
                quantity=15,
                unit_price=Decimal('149.99'),
                currency='USD',
                sales_rep='Jane Smith',
                region='SOUTH',
                status='N'
            )
        ]
        
        return self.spark.createDataFrame(sample_data, schema=self.get_raw_sales_schema())
    
    def _extract_from_jdbc(self, from_date: datetime, to_date: datetime, config: dict) -> DataFrame:
        """Extract data from JDBC source"""
        jdbc_url = config.get('jdbc_url')
        table_name = config.get('table', 'zsales_raw')
        
        return (self.spark.read
                .format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", table_name)
                .option("user", config.get('user'))
                .option("password", config.get('password'))
                .load()
                .filter(f"trans_date BETWEEN '{from_date.date()}' AND '{to_date.date()}'"))
    
    def _extract_from_parquet(self, from_date: datetime, to_date: datetime, config: dict) -> DataFrame:
        """Extract data from Parquet files"""
        path = config.get('path')
        
        return (self.spark.read
                .parquet(path)
                .filter(f"trans_date BETWEEN '{from_date.date()}' AND '{to_date.date()}'"))
    
    def _extract_from_csv(self, from_date: datetime, to_date: datetime, config: dict) -> DataFrame:
        """Extract data from CSV files"""
        path = config.get('path')
        
        return (self.spark.read
                .schema(self.get_raw_sales_schema())
                .option("header", "true")
                .csv(path)
                .filter(f"trans_date BETWEEN '{from_date.date()}' AND '{to_date.date()}'"))