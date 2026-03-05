"""
Data Extraction Module
Extracts raw sales data from source systems.
"""

from typing import Dict, Any, Optional
from datetime import date
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    IntegerType, DecimalType, TimestampType
)

from src.exceptions import ExtractError, ValidationError
from src.logger import ETLLogger


class DataExtractor:
    """Handles data extraction from source systems."""
    
    # Define schema for raw sales data
    RAW_SALES_SCHEMA = StructType([
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
    
    def __init__(
        self,
        spark: SparkSession,
        config: Dict[str, Any],
        logger: ETLLogger
    ):
        """
        Initialize data extractor.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: ETL logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        self.source_config = config.get("source", {})
    
    def extract_data(
        self,
        from_date: date,
        to_date: date
    ) -> DataFrame:
        """
        Extract raw sales data from source.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            
        Returns:
            DataFrame containing raw sales data
            
        Raises:
            ExtractError: If extraction fails
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Validate date range
            self._validate_date_range(from_date, to_date)
            
            # Extract based on source format
            source_format = self.source_config.get("format", "parquet")
            
            if source_format == "jdbc":
                df = self._extract_from_database(from_date, to_date)
            elif source_format in ["parquet", "csv", "json"]:
                df = self._extract_from_file(from_date, to_date, source_format)
            else:
                raise ExtractError(
                    message=f"Unsupported source format: {source_format}",
                    details={"format": source_format}
                )
            
            # Validate extracted data
            self._validate_extracted_data(df)
            
            # Cache the DataFrame for reuse
            df.cache()
            
            record_count = df.count()
            
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return df
            
        except ExtractError:
            raise
        except Exception as e:
            error_msg = f"Extraction failed: {str(e)}"
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=error_msg
            )
            raise ExtractError(
                message=error_msg,
                details={"error": str(e)}
            )
    
    def _extract_from_database(
        self,
        from_date: date,
        to_date: date
    ) -> DataFrame:
        """
        Extract data from database using JDBC.
        
        Args:
            from_date: Start date
            to_date: End date
            
        Returns:
            DataFrame with extracted data
        """
        jdbc_config = self.source_config.get("jdbc", {})
        
        if not jdbc_config:
            raise ExtractError(
                message="JDBC configuration not found",
                source_table="unknown"
            )
        
        url = jdbc_config.get("url")
        table = jdbc_config.get("table")
        user = jdbc_config.get("user")
        password = jdbc_config.get("password")
        driver = jdbc_config.get("driver")
        
        # Build query with date filter
        date_column = self.source_config.get("date_column", "trans_date")
        status_column = self.source_config.get("status_column", "status")
        filter_status = self.source_config.get("filter_status", "N")
        
        query = f"""
            (SELECT * FROM {table}
             WHERE {date_column} BETWEEN '{from_date}' AND '{to_date}'
             AND {status_column} = '{filter_status}') AS filtered_data
        """
        
        try:
            df = self.spark.read \
                .format("jdbc") \
                .option("url", url) \
                .option("dbtable", query) \
                .option("user", user) \
                .option("password", password) \
                .option("driver", driver) \
                .option("fetchsize", jdbc_config.get("fetch_size", 10000)) \
                .load()
            
            return df
            
        except Exception as e:
            raise ExtractError(
                message=f"Database extraction failed: {str(e)}",
                source_table=table,
                query=query
            )
    
    def _extract_from_file(
        self,
        from_date: date,
        to_date: date,
        file_format: str
    ) -> DataFrame:
        """
        Extract data from file source.
        
        Args:
            from_date: Start date
            to_date: End date
            file_format: File format (parquet, csv, json)
            
        Returns:
            DataFrame with extracted data
        """
        source_path = self.source_config.get("path")
        
        if not source_path:
            raise ExtractError(
                message="Source path not configured",
                details={"format": file_format}
            )
        
        try:
            # Read data with schema
            df = self.spark.read \
                .format(file_format) \
                .schema(self.RAW_SALES_SCHEMA) \
                .load(source_path)
            
            # Apply date filter
            date_column = self.source_config.get("date_column", "trans_date")
            status_column = self.source_config.get("status_column", "status")
            filter_status = self.source_config.get("filter_status", "N")
            
            df = df.filter(
                (F.col(date_column) >= F.lit(from_date)) &
                (F.col(date_column) <= F.lit(to_date)) &
                (F.col(status_column) == F.lit(filter_status))
            )
            
            return df
            
        except Exception as e:
            raise ExtractError(
                message=f"File extraction failed: {str(e)}",
                source_table=source_path,
                details={"format": file_format, "path": source_path}
            )
    
    def _validate_date_range(self, from_date: date, to_date: date) -> None:
        """
        Validate date range parameters.
        
        Args:
            from_date: Start date
            to_date: End date
            
        Raises:
            ValidationError: If date range is invalid
        """
        if from_date > to_date:
            raise ValidationError(
                message="From date cannot be later than to date",
                validation_type="date_range",
                expected_value=f"from_date <= to_date",
                actual_value=f"{from_date} > {to_date}"
            )
        
        if to_date > date.today():
            raise ValidationError(
                message="To date cannot be in the future",
                validation_type="date_range",
                expected_value=f"to_date <= {date.today()}",
                actual_value=str(to_date)
            )
    
    def _validate_extracted_data(self, df: DataFrame) -> None:
        """
        Validate extracted data structure and content.
        
        Args:
            df: Extracted DataFrame
            
        Raises:
            ValidationError: If validation fails
        """
        # Check if DataFrame is empty
        if df.rdd.isEmpty():
            raise ValidationError(
                message="No data extracted for the specified date range",
                validation_type="data_availability"
            )
        
        # Validate required fields
        required_fields = self.config.get("validation", {}).get("required_fields", [])
        missing_fields = [field for field in required_fields if field not in df.columns]
        
        if missing_fields:
            raise ValidationError(
                message=f"Missing required fields: {', '.join(missing_fields)}",
                validation_type="schema",
                expected_value=required_fields,
                actual_value=df.columns
            )
        
        # Check for null values in required fields
        null_counts = df.select([
            F.sum(F.when(F.col(field).isNull(), 1).otherwise(0)).alias(field)
            for field in required_fields
        ]).first().asDict()
        
        fields_with_nulls = {k: v for k, v in null_counts.items() if v > 0}
        
        if fields_with_nulls:
            raise ValidationError(
                message=f"Required fields contain null values: {fields_with_nulls}",
                validation_type="null_check",
                actual_value=fields_with_nulls
            )