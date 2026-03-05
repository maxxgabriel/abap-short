"""
ETL Extractor
Migrated from ZCL_ETL_EXTRACTOR
"""

from datetime import date
from typing import List, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType

from src.logger import ETLLogger


class ETLExtractor:
    """Extracts raw sales data from source"""
    
    def __init__(self, logger: ETLLogger, config: Dict):
        """
        Initialize extractor
        
        Args:
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.logger = logger
        self.config = config
        self.spark = self._get_spark_session()
    
    def _get_spark_session(self) -> SparkSession:
        """Get or create Spark session"""
        return SparkSession.builder \
            .appName("ETL_Extractor") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
    
    def extract_data(self, from_date: date, to_date: date) -> List[Dict]:
        """
        Extract raw sales data
        
        Args:
            from_date: Start date
            to_date: End date
            
        Returns:
            List of raw sales records
        """
        try:
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                message=f'Starting extraction from {from_date} to {to_date}'
            )
            
            # Read from source
            df = self._read_source_data(from_date, to_date)
            
            # Convert to list of dicts
            raw_data = [row.asDict() for row in df.collect()]
            
            count = len(raw_data)
            
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                records_processed=count,
                records_success=count,
                message=f'Extracted {count} records successfully'
            )
            
            return raw_data
            
        except Exception as e:
            self.logger.log_message(
                step='EXTRACT',
                status='E',
                message=f'Extraction failed: {str(e)}'
            )
            raise
    
    def _read_source_data(self, from_date: date, to_date: date) -> DataFrame:
        """
        Read source data from database or file
        
        Args:
            from_date: Start date
            to_date: End date
            
        Returns:
            DataFrame with raw sales data
        """
        # Define schema
        schema = StructType([
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
        
        # Read from configured source
        source_config = self.config.get('source', {})
        source_type = source_config.get('type', 'jdbc')
        
        if source_type == 'jdbc':
            df = self._read_from_jdbc(schema, from_date, to_date)
        elif source_type == 'file':
            df = self._read_from_file(schema, from_date, to_date)
        else:
            # For demo: create sample data
            df = self._create_sample_data(schema)
        
        # Filter by date range and status
        df = df.filter(
            (df.trans_date >= from_date) &
            (df.trans_date <= to_date) &
            (df.status == 'N')
        )
        
        return df
    
    def _read_from_jdbc(self, schema: StructType, from_date: date, to_date: date) -> DataFrame:
        """Read from JDBC source"""
        jdbc_config = self.config['source']['jdbc']
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", jdbc_config['table']) \
            .option("user", jdbc_config.get('user', '')) \
            .option("password", jdbc_config.get('password', '')) \
            .option("driver", jdbc_config.get('driver', 'org.postgresql.Driver')) \
            .load()
        
        return df
    
    def _read_from_file(self, schema: StructType, from_date: date, to_date: date) -> DataFrame:
        """Read from file source"""
        file_config = self.config['source']['file']
        
        df = self.spark.read \
            .format(file_config.get('format', 'parquet')) \
            .schema(schema) \
            .load(file_config['path'])
        
        return df
    
    def _create_sample_data(self, schema: StructType) -> DataFrame:
        """Create sample data for testing"""
        from datetime import datetime
        from decimal import Decimal
        
        sample_data = [
            ('T000001', datetime.now().date(), 'CUST001', 'PROD001', 10, Decimal('99.99'), 'USD', 'John Doe', 'NORTH', 'N'),
            ('T000002', datetime.now().date(), 'CUST002', 'PROD002', 5, Decimal('149.99'), 'USD', 'Jane Smith', 'SOUTH', 'N'),
            ('T000003', datetime.now().date(), 'CUST003', 'PROD001', 20, Decimal('99.99'), 'USD', 'John Doe', 'EAST', 'N'),
            ('T000004', datetime.now().date(), 'CUST001', 'PROD003', 3, Decimal('299.99'), 'USD', 'Bob Wilson', 'WEST', 'N'),
            ('T000005', datetime.now().date(), 'CUST004', 'PROD002', 15, Decimal('149.99'), 'USD', 'Jane Smith', 'SOUTH', 'N')
        ]
        
        df = self.spark.createDataFrame(sample_data, schema)
        return df