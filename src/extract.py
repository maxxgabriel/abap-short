"""
Sales ETL - Extract Module
Extracts raw sales data from source system/files.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType, TimestampType
)
from datetime import datetime
import logging
from typing import Optional

from src.utils.logger import ETLLogger
from src.utils.exceptions import ExtractionError


class SalesExtractor:
    """Handles extraction of raw sales data."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the extractor.
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        
    def get_schema(self) -> StructType:
        """
        Define schema for raw sales data.
        
        Returns:
            StructType schema definition
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
            StructField("status", StringType(), False),
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True)
        ])
    
    def extract_from_source(
        self, 
        from_date: str, 
        to_date: str,
        source_type: str = "parquet"
    ) -> DataFrame:
        """
        Extract raw sales data from source.
        
        Args:
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)
            source_type: Source type (parquet, csv, jdbc, etc.)
            
        Returns:
            DataFrame with extracted data
            
        Raises:
            ExtractionError: If extraction fails
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="I",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Get source configuration
            source_config = self.config.get("source", {})
            source_path = source_config.get("path")
            
            if source_type == "parquet":
                df = self._extract_from_parquet(source_path, from_date, to_date)
            elif source_type == "csv":
                df = self._extract_from_csv(source_path, from_date, to_date)
            elif source_type == "jdbc":
                df = self._extract_from_jdbc(from_date, to_date)
            else:
                raise ExtractionError(f"Unsupported source type: {source_type}")
            
            # Filter by date range and status
            df = df.filter(
                (df.trans_date >= from_date) & 
                (df.trans_date <= to_date) &
                (df.status == "N")  # Only new records
            )
            
            record_count = df.count()
            
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return df
            
        except Exception as e:
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=f"Extraction failed: {str(e)}"
            )
            raise ExtractionError(f"Failed to extract data: {str(e)}") from e
    
    def _extract_from_parquet(
        self, 
        path: str, 
        from_date: str, 
        to_date: str
    ) -> DataFrame:
        """Extract from Parquet files."""
        return self.spark.read.schema(self.get_schema()).parquet(path)
    
    def _extract_from_csv(
        self, 
        path: str, 
        from_date: str, 
        to_date: str
    ) -> DataFrame:
        """Extract from CSV files."""
        return self.spark.read.schema(self.get_schema()).csv(
            path,
            header=True,
            dateFormat="yyyy-MM-dd"
        )
    
    def _extract_from_jdbc(self, from_date: str, to_date: str) -> DataFrame:
        """Extract from JDBC source (SAP/Database)."""
        jdbc_config = self.config.get("source", {}).get("jdbc", {})
        
        query = f"""
            (SELECT 
                trans_id, trans_date, customer_id, product_id,
                quantity, unit_price, currency, sales_rep, region,
                status, created_at, created_by
            FROM {jdbc_config.get('table', 'ZSALES_RAW')}
            WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
                AND status = 'N') as sales_data
        """
        
        return self.spark.read.format("jdbc") \
            .option("url", jdbc_config.get("url")) \
            .option("dbtable", query) \
            .option("user", jdbc_config.get("user")) \
            .option("password", jdbc_config.get("password")) \
            .option("driver", jdbc_config.get("driver", "com.sap.db.jdbc.Driver")) \
            .load()
    
    def validate_extracted_data(self, df: DataFrame) -> bool:
        """
        Validate extracted data for completeness and quality.
        
        Args:
            df: Extracted DataFrame
            
        Returns:
            True if validation passes
        """
        # Check for null values in critical columns
        critical_cols = ["trans_id", "customer_id", "product_id", "quantity"]
        
        for col in critical_cols:
            null_count = df.filter(df[col].isNull()).count()
            if null_count > 0:
                self.logger.log_message(
                    step="EXTRACT",
                    status="W",
                    message=f"Found {null_count} null values in column {col}"
                )
        
        # Check for negative quantities or prices
        invalid_count = df.filter(
            (df.quantity <= 0) | (df.unit_price <= 0)
        ).count()
        
        if invalid_count > 0:
            self.logger.log_message(
                step="EXTRACT",
                status="W",
                message=f"Found {invalid_count} records with invalid quantity/price"
            )
        
        return True