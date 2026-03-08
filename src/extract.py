"""
ETL Extractor Module
Extracts raw sales data from source tables.
"""

from typing import Optional
from datetime import date
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
)
from pyspark.sql.functions import col, lit
import logging


class ETLExtractor:
    """
    Extracts raw sales data from source system.
    Reads from database tables or files based on configuration.
    """
    
    def __init__(self, spark: SparkSession, config: dict, logger):
        """
        Initialize extractor with Spark session and configuration.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: Logger instance for tracking extraction
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        
    def get_raw_sales_schema(self) -> StructType:
        """
        Define schema for raw sales data.
        
        Returns:
            StructType: Schema definition for raw sales DataFrame
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
    
    def extract_data(self, from_date: date, to_date: date) -> DataFrame:
        """
        Extract raw sales data for the specified date range.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            
        Returns:
            DataFrame: Extracted raw sales data
        """
        try:
            self.logger.log_message(
                step='EXTRACT',
                status='I',
                message=f'Starting extraction from {from_date} to {to_date}'
            )
            
            source_type = self.config['source']['type']
            
            if source_type == 'jdbc':
                df = self._extract_from_jdbc(from_date, to_date)
            elif source_type == 'csv':
                df = self._extract_from_csv()
            elif source_type == 'parquet':
                df = self._extract_from_parquet()
            else:
                raise ValueError(f"Unsupported source type: {source_type}")
            
            # Filter by date range and status
            df_filtered = df.filter(
                (col("trans_date") >= lit(from_date)) &
                (col("trans_date") <= lit(to_date)) &
                (col("status") == lit("N"))
            )
            
            record_count = df_filtered.count()
            
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                records_processed=record_count,
                records_success=record_count,
                message=f'Extracted {record_count} records successfully'
            )
            
            return df_filtered
            
        except Exception as e:
            self.logger.log_message(
                step='EXTRACT',
                status='E',
                message=f'Extraction failed: {str(e)}'
            )
            raise
    
    def _extract_from_jdbc(self, from_date: date, to_date: date) -> DataFrame:
        """
        Extract data from JDBC source (database).
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            
        Returns:
            DataFrame: Extracted data from JDBC source
        """
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
    
    def _extract_from_csv(self) -> DataFrame:
        """
        Extract data from CSV file source.
        
        Returns:
            DataFrame: Extracted data from CSV
        """
        csv_config = self.config['source']['csv']
        
        df = self.spark.read \
            .format("csv") \
            .option("header", csv_config.get('header', True)) \
            .option("inferSchema", csv_config.get('infer_schema', False)) \
            .schema(self.get_raw_sales_schema()) \
            .load(csv_config['path'])
        
        return df
    
    def _extract_from_parquet(self) -> DataFrame:
        """
        Extract data from Parquet file source.
        
        Returns:
            DataFrame: Extracted data from Parquet
        """
        parquet_config = self.config['source']['parquet']
        
        df = self.spark.read \
            .format("parquet") \
            .load(parquet_config['path'])
        
        return df