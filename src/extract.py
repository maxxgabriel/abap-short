"""
Data Extraction Module
Extracts raw sales data from source with date range filtering and schema mapping.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, IntegerType, 
    DecimalType, TimestampType
)
from datetime import datetime
from typing import Optional, Tuple
import logging

from src.logger import ETLLogger
from src.exceptions import ETLExtractionError


class DataExtractor:
    """
    Extracts raw sales data from source systems.
    Converts ABAP SELECT statements to PySpark read operations.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the data extractor.
        
        Args:
            spark: SparkSession instance
            logger: ETLLogger instance for logging
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        
    def get_raw_sales_schema(self) -> StructType:
        """
        Define schema for raw sales data.
        Maps ABAP internal table structure to PySpark StructType.
        
        Returns:
            StructType: Schema definition for raw sales data
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
            StructField("status", StringType(), nullable=False),
            StructField("created_at", TimestampType(), nullable=True),
            StructField("created_by", StringType(), nullable=True)
        ])
    
    def extract_data(
        self, 
        from_date: str, 
        to_date: str,
        source_path: Optional[str] = None
    ) -> Tuple[DataFrame, bool]:
        """
        Extract raw sales data with date range filtering.
        Equivalent to ABAP: SELECT * FROM zsales_raw WHERE trans_date BETWEEN...
        
        Args:
            from_date: Start date in format 'YYYY-MM-DD'
            to_date: End date in format 'YYYY-MM-DD'
            source_path: Optional override for source data path
            
        Returns:
            Tuple of (DataFrame with extracted data, success boolean)
            
        Raises:
            ETLExtractionError: If extraction fails
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="I",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Get source path from config or parameter
            data_source = source_path or self.config.get("source_path")
            source_format = self.config.get("source_format", "parquet")
            
            # Read data with schema
            schema = self.get_raw_sales_schema()
            
            if source_format == "jdbc":
                df = self._read_from_jdbc(data_source, from_date, to_date)
            elif source_format == "csv":
                df = self._read_from_csv(data_source, schema)
            elif source_format == "parquet":
                df = self._read_from_parquet(data_source, schema)
            else:
                df = self._read_generic(data_source, source_format, schema)
            
            # Apply date range filter (equivalent to BETWEEN clause)
            df_filtered = df.filter(
                (df.trans_date >= from_date) & 
                (df.trans_date <= to_date) &
                (df.status == 'N')  # Only new records (not processed)
            )
            
            # Cache for performance
            df_filtered.cache()
            record_count = df_filtered.count()
            
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return df_filtered, True
            
        except Exception as e:
            error_msg = f"Extraction failed: {str(e)}"
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=error_msg
            )
            raise ETLExtractionError(error_msg, step="EXTRACT") from e
    
    def _read_from_jdbc(
        self, 
        connection_config: dict, 
        from_date: str, 
        to_date: str
    ) -> DataFrame:
        """
        Read data from JDBC source (e.g., SAP HANA, Oracle).
        
        Args:
            connection_config: JDBC connection configuration
            from_date: Start date
            to_date: End date
            
        Returns:
            DataFrame with raw data
        """
        jdbc_url = connection_config.get("url")
        table_name = connection_config.get("table", "zsales_raw")
        
        # Build query with date filter (pushdown predicate)
        query = f"""
            (SELECT * FROM {table_name} 
             WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
             AND status = 'N') AS raw_sales
        """
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("user", connection_config.get("user")) \
            .option("password", connection_config.get("password")) \
            .option("driver", connection_config.get("driver", "com.sap.db.jdbc.Driver")) \
            .load()
        
        return df
    
    def _read_from_csv(self, path: str, schema: StructType) -> DataFrame:
        """Read data from CSV files."""
        df = self.spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("dateFormat", "yyyy-MM-dd") \
            .schema(schema) \
            .load(path)
        return df
    
    def _read_from_parquet(self, path: str, schema: StructType) -> DataFrame:
        """Read data from Parquet files."""
        df = self.spark.read \
            .format("parquet") \
            .schema(schema) \
            .load(path)
        return df
    
    def _read_generic(
        self, 
        path: str, 
        format: str, 
        schema: StructType
    ) -> DataFrame:
        """Read data from generic source format."""
        df = self.spark.read \
            .format(format) \
            .schema(schema) \
            .load(path)
        return df
    
    def validate_extracted_data(self, df: DataFrame) -> Tuple[bool, str]:
        """
        Validate extracted data for completeness and quality.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (validation success boolean, validation message)
        """
        try:
            # Check for null values in mandatory fields
            mandatory_fields = ["trans_id", "trans_date", "customer_id", 
                              "product_id", "quantity", "unit_price"]
            
            for field in mandatory_fields:
                null_count = df.filter(df[field].isNull()).count()
                if null_count > 0:
                    return False, f"Found {null_count} null values in {field}"
            
            # Check for negative quantities or prices
            invalid_count = df.filter(
                (df.quantity <= 0) | (df.unit_price <= 0)
            ).count()
            
            if invalid_count > 0:
                return False, f"Found {invalid_count} records with invalid quantity/price"
            
            return True, "Data validation successful"
            
        except Exception as e:
            return False, f"Validation error: {str(e)}"