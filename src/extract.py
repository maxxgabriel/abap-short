"""
Sales Data Extractor Module
Extracts raw sales data from source table based on date range.
"""
from datetime import datetime
from typing import Optional
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType

from src.logger import ETLLogger
from src.exceptions import ExtractionError


class SalesExtractor:
    """
    Extractor component for raw sales data.
    Reads data from source table/file based on date range filters.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize extractor with Spark session and logger.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
    
    @staticmethod
    def get_raw_sales_schema() -> StructType:
        """
        Define schema for raw sales data.
        
        Returns:
            StructType schema for raw sales
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
    
    def extract_data(self, from_date: str, to_date: str) -> DataFrame:
        """
        Extract raw sales data for specified date range.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            DataFrame containing raw sales data
            
        Raises:
            ExtractionError: If extraction fails
        """
        try:
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                message=f'Starting extraction from {from_date} to {to_date}'
            )
            logging.info(f"Extracting data from {from_date} to {to_date}")
            
            # Get source configuration
            source_config = self.config.get('source', {})
            source_type = source_config.get('type', 'csv')
            source_path = source_config.get('path', 'data/raw/sales_raw.csv')
            
            # Read data based on source type
            if source_type == 'csv':
                raw_df = self._extract_from_csv(source_path, from_date, to_date)
            elif source_type == 'parquet':
                raw_df = self._extract_from_parquet(source_path, from_date, to_date)
            elif source_type == 'jdbc':
                raw_df = self._extract_from_jdbc(source_config, from_date, to_date)
            else:
                raise ExtractionError(f"Unsupported source type: {source_type}")
            
            # Apply status filter (only extract new records)
            raw_df = raw_df.filter(raw_df.status == 'N')
            
            record_count = raw_df.count()
            
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                records_processed=record_count,
                records_success=record_count,
                message=f'Extracted {record_count} records successfully'
            )
            logging.info(f"Successfully extracted {record_count} records")
            
            return raw_df
            
        except Exception as e:
            error_msg = f"Extraction failed: {str(e)}"
            self.logger.log_message(
                step='EXTRACT',
                status='E',
                message=error_msg
            )
            logging.error(error_msg)
            raise ExtractionError(error_msg) from e
    
    def _extract_from_csv(
        self,
        source_path: str,
        from_date: str,
        to_date: str
    ) -> DataFrame:
        """
        Extract data from CSV file.
        
        Args:
            source_path: Path to CSV file
            from_date: Start date
            to_date: End date
            
        Returns:
            DataFrame with filtered data
        """
        schema = self.get_raw_sales_schema()
        
        df = self.spark.read \
            .schema(schema) \
            .option("header", "true") \
            .option("dateFormat", "yyyy-MM-dd") \
            .csv(source_path)
        
        # Apply date filter
        df = df.filter(
            (df.trans_date >= from_date) &
            (df.trans_date <= to_date)
        )
        
        return df
    
    def _extract_from_parquet(
        self,
        source_path: str,
        from_date: str,
        to_date: str
    ) -> DataFrame:
        """
        Extract data from Parquet file.
        
        Args:
            source_path: Path to Parquet file
            from_date: Start date
            to_date: End date
            
        Returns:
            DataFrame with filtered data
        """
        df = self.spark.read.parquet(source_path)
        
        # Apply date filter
        df = df.filter(
            (df.trans_date >= from_date) &
            (df.trans_date <= to_date)
        )
        
        return df
    
    def _extract_from_jdbc(
        self,
        source_config: dict,
        from_date: str,
        to_date: str
    ) -> DataFrame:
        """
        Extract data from database via JDBC.
        
        Args:
            source_config: JDBC configuration
            from_date: Start date
            to_date: End date
            
        Returns:
            DataFrame with filtered data
        """
        jdbc_url = source_config.get('jdbc_url')
        table_name = source_config.get('table', 'zsales_raw')
        
        # Build query with date filter
        query = f"""
            (SELECT * FROM {table_name}
             WHERE trans_date BETWEEN '{from_date}' AND '{to_date}')
        """
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("user", source_config.get('user')) \
            .option("password", source_config.get('password')) \
            .option("driver", source_config.get('driver', 'org.postgresql.Driver')) \
            .load()
        
        return df