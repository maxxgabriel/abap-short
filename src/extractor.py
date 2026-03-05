"""
Data Extraction Module

Extracts raw sales data from source systems.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from datetime import datetime
import logging


class DataExtractor:
    """
    Extracts raw sales data from source table.
    """
    
    def __init__(self, logger, config):
        """
        Initialize data extractor.
        
        Args:
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.logger = logger
        self.config = config
        self.spark = self._get_spark_session()
    
    def _get_spark_session(self) -> SparkSession:
        """
        Get or create Spark session.
        
        Returns:
            SparkSession: Active Spark session
        """
        return SparkSession.builder \
            .appName(self.config.get('app_name', 'SalesETL')) \
            .config("spark.sql.shuffle.partitions", 
                   self.config.get('shuffle_partitions', 200)) \
            .getOrCreate()
    
    def extract_data(self, from_date: str, to_date: str) -> DataFrame:
        """
        Extract raw sales data for given date range.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            DataFrame: Raw sales data
        """
        try:
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                message=f'Starting extraction from {from_date} to {to_date}'
            )
            
            # Define schema
            schema = self._get_raw_sales_schema()
            
            # Extract data based on source type
            source_type = self.config.get('source_type', 'jdbc')
            
            if source_type == 'jdbc':
                df = self._extract_from_jdbc(from_date, to_date, schema)
            elif source_type == 'csv':
                df = self._extract_from_csv(schema)
            elif source_type == 'parquet':
                df = self._extract_from_parquet()
            else:
                # Default to sample data for demonstration
                df = self._create_sample_data(schema)
            
            # Filter by date range
            df = df.filter(
                (df.trans_date >= from_date) & 
                (df.trans_date <= to_date) &
                (df.status == 'N')
            )
            
            count = df.count()
            
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                records_processed=count,
                records_success=count,
                message=f'Extracted {count} records successfully'
            )
            
            return df
            
        except Exception as e:
            self.logger.log_message(
                step='EXTRACT',
                status='E',
                message=f'Extraction failed: {str(e)}'
            )
            raise
    
    def _get_raw_sales_schema(self) -> StructType:
        """
        Define schema for raw sales data.
        
        Returns:
            StructType: Schema definition
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
    
    def _extract_from_jdbc(self, from_date: str, to_date: str, schema: StructType) -> DataFrame:
        """
        Extract data from JDBC source.
        
        Args:
            from_date: Start date
            to_date: End date
            schema: Data schema
            
        Returns:
            DataFrame: Extracted data
        """
        jdbc_config = self.config.get('jdbc', {})
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config.get('url')) \
            .option("dbtable", jdbc_config.get('table', 'zsales_raw')) \
            .option("user", jdbc_config.get('user')) \
            .option("password", jdbc_config.get('password')) \
            .option("driver", jdbc_config.get('driver', 'com.sap.db.jdbc.Driver')) \
            .load()
        
        return df
    
    def _extract_from_csv(self, schema: StructType) -> DataFrame:
        """
        Extract data from CSV file.
        
        Args:
            schema: Data schema
            
        Returns:
            DataFrame: Extracted data
        """
        csv_path = self.config.get('source_path', 'data/raw_sales.csv')
        
        df = self.spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("dateFormat", "yyyy-MM-dd") \
            .schema(schema) \
            .load(csv_path)
        
        return df
    
    def _extract_from_parquet(self) -> DataFrame:
        """
        Extract data from Parquet file.
        
        Returns:
            DataFrame: Extracted data
        """
        parquet_path = self.config.get('source_path', 'data/raw_sales.parquet')
        
        df = self.spark.read \
            .format("parquet") \
            .load(parquet_path)
        
        return df
    
    def _create_sample_data(self, schema: StructType) -> DataFrame:
        """
        Create sample data for demonstration.
        
        Args:
            schema: Data schema
            
        Returns:
            DataFrame: Sample data
        """
        from datetime import date
        
        sample_data = [
            ("T000001", date.today(), "CUST001", "PROD001", 10, 99.99, "USD", "John Doe", "NORTH", "N"),
            ("T000002", date.today(), "CUST002", "PROD002", 5, 149.99, "USD", "Jane Smith", "SOUTH", "N"),
            ("T000003", date.today(), "CUST003", "PROD001", 20, 99.99, "USD", "John Doe", "EAST", "N"),
            ("T000004", date.today(), "CUST001", "PROD003", 3, 299.99, "USD", "Bob Wilson", "WEST", "N"),
            ("T000005", date.today(), "CUST004", "PROD002", 15, 149.99, "USD", "Jane Smith", "SOUTH", "N")
        ]
        
        return self.spark.createDataFrame(sample_data, schema)