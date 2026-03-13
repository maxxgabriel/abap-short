"""
Extract module for Sales ETL Pipeline
Extracts raw sales data from source systems
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from datetime import datetime
from typing import Optional
import logging


class SalesExtractor:
    """Handles extraction of raw sales data from source systems"""
    
    def __init__(self, spark: SparkSession, config: dict, logger: logging.Logger):
        """
        Initialize the extractor
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: Logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self.schema = self._get_sales_schema()
    
    def _get_sales_schema(self) -> StructType:
        """
        Define the schema for raw sales data
        
        Returns:
            StructType: Schema definition for sales data
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
        from_date: str, 
        to_date: str,
        source_path: Optional[str] = None
    ) -> DataFrame:
        """
        Extract sales data for the specified date range
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            source_path: Optional override for source path
            
        Returns:
            DataFrame: Extracted sales data
        """
        try:
            self.logger.info(f"Starting extraction from {from_date} to {to_date}")
            
            # Get source configuration
            source = source_path or self.config['extract']['source_path']
            source_format = self.config['extract']['source_format']
            
            # Read data based on format
            if source_format == 'parquet':
                df = self.spark.read.parquet(source)
            elif source_format == 'csv':
                df = self.spark.read.csv(
                    source,
                    schema=self.schema,
                    header=True
                )
            elif source_format == 'delta':
                df = self.spark.read.format('delta').load(source)
            elif source_format == 'jdbc':
                df = self._extract_from_jdbc(from_date, to_date)
            else:
                raise ValueError(f"Unsupported source format: {source_format}")
            
            # Apply date filter
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
    
    def _extract_from_jdbc(self, from_date: str, to_date: str) -> DataFrame:
        """
        Extract data from JDBC source
        
        Args:
            from_date: Start date
            to_date: End date
            
        Returns:
            DataFrame: Extracted data
        """
        jdbc_config = self.config['extract']['jdbc']
        
        query = f"""
        (SELECT * FROM {jdbc_config['table']}
         WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
         AND status = 'N') as sales_data
        """
        
        df = self.spark.read.jdbc(
            url=jdbc_config['url'],
            table=query,
            properties={
                "user": jdbc_config['user'],
                "password": jdbc_config['password'],
                "driver": jdbc_config['driver']
            }
        )
        
        return df
    
    def validate_extracted_data(self, df: DataFrame) -> bool:
        """
        Validate extracted data quality
        
        Args:
            df: DataFrame to validate
            
        Returns:
            bool: True if validation passes
        """
        try:
            # Check if dataframe is empty
            if df.count() == 0:
                self.logger.warning("Extracted data is empty")
                return False
            
            # Check for null values in critical columns
            critical_columns = ['trans_id', 'customer_id', 'product_id', 'quantity', 'unit_price']
            for col in critical_columns:
                null_count = df.filter(df[col].isNull()).count()
                if null_count > 0:
                    self.logger.error(f"Found {null_count} null values in column {col}")
                    return False
            
            # Check for negative quantities or prices
            invalid_count = df.filter(
                (df.quantity <= 0) | (df.unit_price <= 0)
            ).count()
            
            if invalid_count > 0:
                self.logger.error(f"Found {invalid_count} records with invalid quantity or price")
                return False
            
            self.logger.info("Data validation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Validation failed: {str(e)}")
            return False