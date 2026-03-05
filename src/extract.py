"""
Data Extraction Module for Sales ETL System

Extracts raw sales data from source system and validates records.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType, TimestampType
)
from datetime import datetime
from typing import Tuple, Optional
import logging

from src.logger import ETLLogger
from src.exceptions import ETLExtractionError


class SalesExtractor:
    """Extracts raw sales data from source tables."""
    
    # Schema definition for raw sales data
    RAW_SALES_SCHEMA = StructType([
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
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize extractor.
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.log = logging.getLogger(__name__)
    
    def extract_data(
        self, 
        from_date: str, 
        to_date: str,
        source_table: Optional[str] = None
    ) -> Tuple[DataFrame, bool]:
        """
        Extract raw sales data for given date range.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            source_table: Optional table name override
            
        Returns:
            Tuple of (DataFrame, success_flag)
            
        Raises:
            ETLExtractionError: If extraction fails
        """
        try:
            step = "EXTRACT"
            self.logger.log_message(
                step=step,
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Get source configuration
            table_name = source_table or self.config.get("source_table", "zsales_raw")
            batch_size = self.config.get("batch_size", 1000)
            
            # Build source DataFrame
            df_raw = self._read_source_data(table_name, from_date, to_date)
            
            # Validate schema
            self._validate_schema(df_raw)
            
            # Filter for new records
            df_filtered = df_raw.filter(df_raw.status == "N")
            
            # Apply batch limit if configured
            if batch_size > 0:
                df_filtered = df_filtered.limit(batch_size)
            
            # Cache for reuse
            df_filtered.cache()
            record_count = df_filtered.count()
            
            # Validate extracted data
            if record_count == 0:
                self.logger.log_message(
                    step=step,
                    status="W",
                    records_processed=0,
                    message="No new records found for extraction"
                )
                return df_filtered, True
            
            # Log success
            self.logger.log_message(
                step=step,
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
    
    def _read_source_data(
        self, 
        table_name: str, 
        from_date: str, 
        to_date: str
    ) -> DataFrame:
        """
        Read data from source table or file.
        
        Args:
            table_name: Source table/file name
            from_date: Start date
            to_date: End date
            
        Returns:
            DataFrame with raw data
        """
        source_type = self.config.get("source_type", "jdbc")
        
        if source_type == "jdbc":
            return self._read_jdbc(table_name, from_date, to_date)
        elif source_type == "parquet":
            return self._read_parquet(table_name, from_date, to_date)
        elif source_type == "csv":
            return self._read_csv(table_name, from_date, to_date)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
    
    def _read_jdbc(
        self, 
        table_name: str, 
        from_date: str, 
        to_date: str
    ) -> DataFrame:
        """Read from JDBC source (SAP HANA, PostgreSQL, etc.)."""
        jdbc_config = self.config.get("jdbc", {})
        
        # Build query with date filter
        query = f"""
        (SELECT * FROM {table_name}
         WHERE trans_date BETWEEN '{from_date}' AND '{to_date}'
         AND status = 'N') AS filtered_data
        """
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_config.get("url")) \
            .option("dbtable", query) \
            .option("user", jdbc_config.get("user")) \
            .option("password", jdbc_config.get("password")) \
            .option("driver", jdbc_config.get("driver", "org.postgresql.Driver")) \
            .load()
        
        return df
    
    def _read_parquet(
        self, 
        path: str, 
        from_date: str, 
        to_date: str
    ) -> DataFrame:
        """Read from Parquet files."""
        df = self.spark.read \
            .schema(self.RAW_SALES_SCHEMA) \
            .parquet(path)
        
        # Apply date filter
        df = df.filter(
            (df.trans_date >= from_date) & 
            (df.trans_date <= to_date) &
            (df.status == "N")
        )
        
        return df
    
    def _read_csv(
        self, 
        path: str, 
        from_date: str, 
        to_date: str
    ) -> DataFrame:
        """Read from CSV files."""
        df = self.spark.read \
            .schema(self.RAW_SALES_SCHEMA) \
            .option("header", "true") \
            .option("dateFormat", "yyyy-MM-dd") \
            .csv(path)
        
        # Apply date filter
        df = df.filter(
            (df.trans_date >= from_date) & 
            (df.trans_date <= to_date) &
            (df.status == "N")
        )
        
        return df
    
    def _validate_schema(self, df: DataFrame) -> None:
        """
        Validate DataFrame schema matches expected structure.
        
        Args:
            df: DataFrame to validate
            
        Raises:
            ETLExtractionError: If schema is invalid
        """
        expected_fields = {field.name for field in self.RAW_SALES_SCHEMA.fields}
        actual_fields = {field.name for field in df.schema.fields}
        
        missing_fields = expected_fields - actual_fields
        if missing_fields:
            raise ETLExtractionError(
                f"Missing required fields: {missing_fields}",
                step="EXTRACT"
            )
        
        extra_fields = actual_fields - expected_fields
        if extra_fields:
            self.log.warning(f"Extra fields found (will be ignored): {extra_fields}")