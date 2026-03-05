===FILE: src/extract.py===
"""
Data Extraction Module
Converts ABAP SELECT statements to PySpark read operations with date range filtering
"""

from datetime import datetime
from typing import Tuple, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType, TimestampType
)
from pyspark.sql import functions as F
import logging

from src.logger import ETLLogger
from src.exceptions import ExtractionError


class DataExtractor:
    """
    Extracts raw sales data from source with date range filtering.
    Maps ABAP internal table schemas to PySpark DataFrames.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the data extractor.
        
        Args:
            spark: SparkSession instance
            logger: ETLLogger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.raw_sales_schema = self._define_raw_sales_schema()
    
    def _define_raw_sales_schema(self) -> StructType:
        """
        Define schema for raw sales data (maps to ABAP ZSALES_RAW structure).
        
        Returns:
            StructType: Schema definition
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
        Maps ABAP SELECT with WHERE clause to PySpark filter.
        
        ABAP equivalent:
        SELECT * FROM zsales_raw INTO TABLE @et_sales_data
          WHERE trans_date BETWEEN @iv_from_date AND @iv_to_date
          AND status = 'N'.
        
        Args:
            from_date: Start date (YYYY-MM-DD format)
            to_date: End date (YYYY-MM-DD format)
            source_path: Optional source data path (defaults to config)
            
        Returns:
            Tuple[DataFrame, bool]: Extracted data and success flag
            
        Raises:
            ExtractionError: If extraction fails
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Get source path from config if not provided
            if source_path is None:
                source_path = self.config.get("source_path")
            
            # Read data with schema
            df_raw = self._read_source_data(source_path)
            
            # Apply date range filter (equivalent to ABAP WHERE clause)
            df_filtered = self._apply_date_filter(df_raw, from_date, to_date)
            
            # Filter by status = 'N' (new/unprocessed records)
            df_filtered = df_filtered.filter(F.col("status") == "N")
            
            # Cache for performance if enabled
            if self.config.get("cache_enabled", True):
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
            raise ExtractionError(
                error_text=error_msg,
                error_step="EXTRACT"
            ) from e
    
    def _read_source_data(self, source_path: str) -> DataFrame:
        """
        Read source data based on format configuration.
        
        Args:
            source_path: Path to source data
            
        Returns:
            DataFrame: Raw data
        """
        source_format = self.config.get("source_format", "parquet")
        
        if source_format == "parquet":
            df = self.spark.read.schema(self.raw_sales_schema).parquet(source_path)
        elif source_format == "csv":
            df = self.spark.read.schema(self.raw_sales_schema).csv(
                source_path, 
                header=True
            )
        elif source_format == "jdbc":
            df = self._read_from_database(source_path)
        else:
            raise ValueError(f"Unsupported source format: {source_format}")
        
        return df
    
    def _read_from_database(self, table_name: str) -> DataFrame:
        """
        Read from database using JDBC (for migrating from actual SAP tables).
        
        Args:
            table_name: Database table name
            
        Returns:
            DataFrame: Data from database
        """
        jdbc_config = self.config.get("jdbc", {})
        
        df = self.spark.read.format("jdbc").options(
            url=jdbc_config.get("url"),
            dbtable=table_name,
            user=jdbc_config.get("user"),
            password=jdbc_config.get("password"),
            driver=jdbc_config.get("driver", "com.sap.db.jdbc.Driver")
        ).load()
        
        return df
    
    def _apply_date_filter(
        self, 
        df: DataFrame, 
        from_date: str, 
        to_date: str
    ) -> DataFrame:
        """
        Apply date range filter (maps ABAP BETWEEN operator).
        
        Args:
            df: Input DataFrame
            from_date: Start date
            to_date: End date
            
        Returns:
            DataFrame: Filtered data
        """
        # Convert string dates to date type if needed
        df_filtered = df.filter(
            (F.col("trans_date") >= F.lit(from_date)) &
            (F.col("trans_date") <= F.lit(to_date))
        )
        
        return df_filtered
    
    def get_extraction_stats(self, df: DataFrame) -> dict:
        """
        Get extraction statistics.
        
        Args:
            df: Extracted DataFrame
            
        Returns:
            dict: Statistics
        """
        return {
            "total_records": df.count(),
            "date_range": {
                "min_date": df.agg(F.min("trans_date")).collect()[0][0],
                "max_date": df.agg(F.max("trans_date")).collect()[0][0]
            },
            "unique_customers": df.select("customer_id").distinct().count(),
            "unique_products": df.select("product_id").distinct().count(),
            "regions": df.select("region").distinct().collect()
        }


===FILE: src/transform.py===
"""
Data Transformation Module
Implements business logic from ABAP transformations with PySpark
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType, TimestampType
)
from typing import Tuple

from src.logger import ETLLogger
from src.exceptions import TransformationError


class DataTransformer:
    """
    Transforms raw sales data into analytics format.
    Implements ABAP business rules using PySpark DataFrame operations.
    """
    
    def __init__(self, logger: ETLLogger, config: dict):
        """
        Initialize the data transformer.
        
        Args:
            logger: ETLLogger instance
            config: Configuration dictionary
        """
        self.logger = logger
        self.config = config
        self.analytics_schema = self._define_analytics_schema()
        
        # Business rule thresholds from config
        self.discount_qty_tier1 = config.get("discount_qty_tier1", 10)
        self.discount_qty_tier2 = config.get("discount_qty_tier2", 15)
        self.discount_rate_tier1 = config.get("discount_rate_tier1", 0.05)
        self.discount_rate_tier2 = config.get("discount_rate_tier2", 0.10)
        self.tax_rate = config.get("tax_rate", 0.08)
        self.cost_ratio = config.get("cost_ratio", 0.60)
        self.category_high_threshold = config.get("category_high_threshold", 2000.00)
        self.category_medium_threshold = config.get("category_medium_threshold", 500.00)
    
    def _define_analytics_schema(self) -> StructType:
        """
        Define schema for analytics data (maps to ABAP ZSALES_ANALYTICS).
        
        Returns:
            StructType: Schema definition
        """
        return StructType([
            StructField("analytics_id", StringType(), nullable=False),
            StructField("trans_date", DateType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("product_id", StringType(), nullable=False),
            StructField("total_quantity", IntegerType(), nullable=False),
            StructField("gross_amount", DecimalType(16, 2), nullable=False),
            StructField("net_amount", DecimalType(16, 2), nullable=False),
            StructField("discount_amount", DecimalType(16, 2), nullable=False),
            StructField("tax_amount", DecimalType(16, 2), nullable=False),
            StructField("currency", StringType(), nullable=False),
            StructField("sales_rep", StringType(), nullable=True),
            StructField("region", StringType(), nullable=True),
            StructField("profit_margin", DecimalType(5, 2), nullable=True),
            StructField("category", StringType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("loaded_at", TimestampType(), nullable=True),
            StructField("loaded_by", StringType(), nullable=True)
        ])
    
    def transform_data(
        self, 
        df_raw: DataFrame,
        etl_run_id: str
    ) -> Tuple[DataFrame, bool]:
        """
        Transform raw sales data into analytics format.
        
        ABAP equivalent: ZCL_ETL_TRANSFORMER->TRANSFORM_DATA method
        
        Args:
            df_raw: Raw sales DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Tuple[DataFrame, bool]: Transformed data and success flag
            
        Raises:
            TransformationError: If transformation fails
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                message="Starting data transformation"
            )
            
            input_count = df_raw.count()
            
            # Calculate analytics fields using business rules
            df_analytics = self._calculate_analytics(df_raw, etl_run_id)
            
            # Categorize sales
            df_analytics = self._categorize_sales(df_analytics)
            
            # Add metadata
            df_analytics = self._add_metadata(df_analytics, etl_run_id)
            
            # Validate transformed data
            df_analytics = self._validate_records(df_analytics)
            
            output_count = df_analytics.count()
            error_count = input_count - output_count
            
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=input_count,
                records_success=output_count,
                records_error=error_count,
                message=f"Transformed {output_count} of {input_count} records"
            )
            
            return df_analytics, True
            
        except Exception as e:
            error_msg = f"Transformation failed: {str(e)}"
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=error_msg
            )
            raise TransformationError(
                error_text=error_msg,
                error_step="TRANSFORM"
            ) from e
    
    def _calculate_analytics(
        self, 
        df: DataFrame,
        etl_run_id: str
    ) -> DataFrame:
        """
        Calculate analytics fields implementing ABAP business rules.
        
        ABAP equivalent: ZCL_ETL_TRANSFORMER->CALCULATE_ANALYTICS method
        
        Args:
            df: Raw sales DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            DataFrame: Data with calculated fields
        """
        # Calculate gross amount
        df = df.withColumn(
            "gross_amount",
            F.col("quantity") * F.col("unit_price")
        )
        
        # Calculate discount based on quantity tiers
        df = df.withColumn(
            "discount_amount",
            F.when(
                F.col("quantity") > self.discount_qty_tier2,
                F.col("gross_amount") * self.discount_rate_tier2
            ).when(
                F.col("quantity") > self.discount_qty_tier1,
                F.col("gross_amount") * self.discount_rate_tier1
            ).otherwise(0.0)
        )
        
        # Calculate tax on (gross - discount)
        df = df.withColumn(
            "tax_amount",
            (F.col("gross_amount") - F.col("discount_amount")) * self.tax_rate
        )
        
        # Calculate net amount
        df = df.withColumn(
            "net_amount",
            F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
        )
        
        # Calculate cost and profit margin
        df = df.withColumn(
            "cost_amount",
            F.col("quantity") * F.col("unit_price") * self.cost_ratio
        )
        
        df = df.withColumn(
            "profit_margin",
            F.when(
                F.col("net_amount") > 0,
                ((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100
            ).otherwise(0.0)
        )
        
        # Generate analytics ID (maps to ABAP CONCATENATE with timestamp)
        df = df.withColumn(
            "analytics_id",
            F.concat(
                F.lit("ANL"),
                F.col("trans_id"),
                F.date_format(F.current_timestamp(), "HHmmss")
            )
        )
        
        # Rename quantity to total_quantity for analytics schema
        df = df.withColumnRenamed("quantity", "total_quantity")
        
        return df
    
    def _categorize_sales(self, df: DataFrame) -> DataFrame:
        """
        Categorize sales based on gross amount.
        
        ABAP equivalent: ZCL_ETL_TRANSFORMER->CATEGORIZE_SALE method
        
        Args:
            df: DataFrame with calculated amounts
            
        Returns:
            DataFrame: Data with category field
        """
        df = df.withColumn(
            "category",
            F.when(
                F.col("gross_amount") >= self.category_high_threshold,
                F.lit("HIGH")
            ).when(
                F.col("gross_amount") >= self.category_medium_threshold,
                F.lit("MEDIUM")
            ).otherwise(F.lit("LOW"))
        )
        
        return df
    
    def _add_metadata(self, df: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Add ETL metadata fields.
        
        Args:
            df: DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            DataFrame: Data with metadata
        """
        df = df.withColumn("etl_run_id", F.lit(etl_run_id))
        df = df.withColumn("loaded_at", F.current_timestamp())
        df = df.withColumn("loaded_by", F.lit("etl_system"))
        
        return df
    
    def _validate_records(self, df: DataFrame) -> DataFrame:
        """
        Validate transformed records and filter out invalid ones.
        
        ABAP equivalent: ZCL_ETL_LOADER->VALIDATE_RECORD method
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            DataFrame: Valid records only
        """
        # Filter out records with invalid data
        df_valid = df.filter(
            (F.col("analytics_id").isNotNull()) &
            (F.col("customer_id").isNotNull()) &
            (F.col("product_id").isNotNull()) &
            (F.col("gross_amount") > 0) &
            (F.col("currency").isNotNull()) &
            (F.col("category").isin(["HIGH", "MEDIUM", "LOW"]))
        )
        
        return df_valid
    
    def get_transformation_stats(self, df: DataFrame) -> dict:
        """
        Get transformation statistics.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            dict: Statistics
        """
        stats = df.agg(
            F.sum("gross_amount").alias("total_gross"),
            F.sum("net_amount").alias("total_net"),
            F.sum("discount_amount").alias("total_discount"),
            F.sum("tax_amount").alias("total_tax"),
            F.avg("profit_margin").alias("avg_profit_margin")
        ).collect()[0]
        
        category_dist = df.groupBy("category").count().collect()
        
        return {
            "total_gross_amount": float(stats["total_gross"] or 0),
            "total_net_amount": float(stats["total_net"] or 0),
            "total_discount": float(stats["total_discount"] or 0),
            "total_tax": float(stats["total_tax"] or 0),
            "avg_profit_margin": float(stats["avg_profit_margin"] or 0),
            "category_distribution": {row["category"]: row["count"] for row in category_dist}
        }


===FILE: src/load.py===
"""
Data Load Module
Loads transformed data into target analytics table
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Tuple

from src.logger import ETLLogger
from src.exceptions import LoadError


class DataLoader:
    """
    Loads transformed analytics data into target storage.
    Maps ABAP INSERT/UPDATE operations to PySpark write operations.
    """
    
    def __init__(self, logger: ETLLogger, config: dict):
        """
        Initialize the data loader.
        
        Args:
            logger: ETLLogger instance
            config: Configuration dictionary
        """
        self.logger = logger
        self.config = config
    
    def load_data(
        self, 
        df_analytics: DataFrame,
        target_path: str = None
    ) -> Tuple[int, bool]:
        """
        Load transformed data into target analytics table.
        
        ABAP equivalent: ZCL_ETL_LOADER->LOAD_DATA method
        (INSERT zsales_analytics FROM @ls_analytics)
        
        Args:
            df_analytics: Transformed analytics DataFrame
            target_path: Optional target path (defaults to config)
            
        Returns:
            Tuple[int, bool]: Number of loaded records and success flag
            
        Raises:
            LoadError: If load fails
        """
        try:
            self.logger.log_message(
                step="LOAD",
                status="S",
                message="Starting data load"
            )
            
            # Get target configuration
            if target_path is None:
                target_path = self.config.get("target_path")
            
            target_format = self.config.get("target_format", "parquet")
            
            # Count records before load
            record_count = df_analytics.count()
            
            # Write data based on target format
            self._write_target_data(df_analytics, target_path, target_format)
            
            # Update source status (equivalent to ABAP UPDATE zsales_raw SET status = 'P')
            self._update_source_status(df_analytics)
            
            self.logger.log_message(
                step="LOAD",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Loaded {record_count} records successfully"
            )
            
            return record_count, True
            
        except Exception as e:
            error_msg = f"Load failed: {str(e)}"
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=error_msg
            )
            raise LoadError(
                error_text=error_msg,
                error_step="LOAD"
            ) from e
    
    def _write_target_data(
        self, 
        df: DataFrame, 
        target_path: str,
        target_format: str
    ) -> None:
        """
        Write data to target based on format.
        
        Args:
            df: DataFrame to write
            target_path: Target path
            target_format: Target format (parquet, delta, jdbc)
        """
        write_mode = self.config.get("write_mode", "append")
        
        if target_format == "parquet":
            df.write.mode(write_mode).parquet(target_path)
        elif target_format == "delta":
            df.write.format("delta").mode(write_mode).save(target_path)
        elif target_format == "jdbc":
            self._write_to_database(df, target_path)
        else:
            raise ValueError(f"Unsupported target format: {target_format}")
    
    def _write_to_database(self, df: DataFrame, table_name: str) -> None:
        """
        Write to database using JDBC.
        
        Args:
            df: DataFrame to write
            table_name: Target table name
        """
        jdbc_config = self.config.get("jdbc", {})
        
        df.write.format("jdbc").mode("append").options(
            url=jdbc_config.get("url"),
            dbtable=table_name,
            user=jdbc_config.get("user"),
            password=jdbc_config.get("password"),
            driver=jdbc_config.get("driver", "com.sap.db.jdbc.Driver")
        ).save()
    
    def _update_source_status(self, df_analytics: DataFrame) -> None:
        """
        Update source table status to mark records as processed.
        
        ABAP equivalent: UPDATE zsales_raw SET status = 'P' WHERE trans_id IN @lt_ids
        
        Args:
            df_analytics: Analytics data with transaction IDs
        """
        # In a real implementation, this would update the source table
        # For now, we log the operation
        trans_ids = df_analytics.select("trans_id").distinct().count()
        
        self.logger.log_message(
            step="LOAD",
            status="I",
            message=f"Would update {trans_ids} source records to status='P'"
        )
    
    def validate_load(
        self, 
        df_source: DataFrame,
        target_path: str
    ) -> dict:
        """
        Validate loaded data against source.
        
        Args:
            df_source: Source DataFrame
            target_path: Target path to validate
            
        Returns:
            dict: Validation results
        """
        try:
            target_format = self.config.get("target_format", "parquet")
            
            if target_format == "parquet":
                df_target = self.spark.read.parquet(target_path)
            elif target_format == "delta":
                df_target = self.spark.read.format("delta").load(target_path)
            else:
                return {"valid": False, "message": "Cannot validate JDBC target"}
            
            source_count = df_source.count()
            target_count = df_target.count()
            
            return {
                "valid": source_count == target_count,
                "source_count": source_count,
                "target_count": target_count,
                "difference": abs(source_count - target_count)
            }
            
        except Exception as e:
            return {
                "valid": False,
                "message": f"Validation failed: {str(e)}"
            }


===FILE: src/logger.py===
"""
ETL Logger Module
Implements logging functionality equivalent to ABAP ZCL_ETL_LOGGER
"""

from datetime import datetime
from typing import Optional
import logging


class ETLLogger:
    """
    Logger for ETL operations.
    Maps ABAP ZCL_ETL_LOGGER functionality to Python logging.
    """
    
    def __init__(self, etl_run_id: str, log_level: str = "INFO"):
        """
        Initialize the ETL logger.
        
        Args:
            etl_run_id: Unique ETL run identifier
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.etl_run_id = etl_run_id
        self.log_entries = []
        
        # Configure Python logger
        self.logger = logging.getLogger(f"ETL.{etl_run_id}")
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, log_level.upper()))
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)
        
        # Add handler
        if not self.logger.handlers:
            self.logger.addHandler(console_handler)
    
    def log_message(
        self,
        step: str,
        status: str,
        message: str,
        records_processed: int = 0,
        records_success: int = 0,
        records_error: int = 0
    ) -> None:
        """
        Log an ETL message.
        
        ABAP equivalent: ZCL_ETL_LOGGER->LOG_MESSAGE method
        
        Args:
            step: Process step (EXTRACT, TRANSFORM, LOAD, etc.)
            status: Status code (S=Success, E=Error, W=Warning, I=Info)
            message: Log message
            records_processed: Number of records processed
            records_success: Number of successful records
            records_error: Number of error records
        """
        log_entry = {
            "log_id": self._generate_log_id(),
            "etl_run_id": self.etl_run_id,
            "execution_date": datetime.now().strftime("%Y-%m-%d"),
            "execution_time": datetime.now().strftime("%H:%M:%S"),
            "process_step": step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message
        }
        
        self.log_entries.append(log_entry)
        
        # Log to Python logger
        log_msg = (
            f"[{step}] {message} "
            f"(Processed: {records_processed}, Success: {records_success}, Error: {records_error})"
        )
        
        if status == "E":
            self.logger.error(log_msg)
        elif status == "W":
            self.logger.warning(log_msg)
        elif status == "I":
            self.logger.info(log_msg)
        else:
            self.logger.info(log_msg)
    
    def _generate_log_id(self) -> str:
        """
        Generate unique log ID.
        
        ABAP equivalent: ZCL_ETL_LOGGER->GENERATE_LOG_ID method
        
        Returns:
            str: Unique log ID
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"LOG{timestamp}"
    
    def get_etl_run_id(self) -> str:
        """
        Get ETL run ID.
        
        Returns:
            str: ETL run ID
        """
        return self.etl_run_id
    
    def get_log_entries(self) -> list:
        """
        Get all log entries.
        
        Returns:
            list: List of log entry dictionaries
        """
        return self.log_entries
    
    def get_summary(self) -> dict:
        """
        Get log summary statistics.
        
        Returns:
            dict: Summary statistics
        """
        total_processed = sum(e["records_processed"] for e in self.log_entries)
        total_success = sum(e["records_success"] for e in self.log_entries)
        total_error = sum(e["records_error"] for e in self.log_entries)
        
        status_counts = {}
        for entry in self.log_entries:
            status = entry["status"]
            status_counts[status] = status_counts.get(status, 0) + 1
        
        return {
            "total_entries": len(self.log_entries),
            "total_processed": total_processed,
            "total_success": total_success,
            "total_error": total_error,
            "status_counts": status_counts
        }


===FILE: src/exceptions.py===
"""
ETL Exception Classes
Maps ABAP exception class ZCX_ETL_ERROR to Python exceptions
"""


class ETLError(Exception):
    """
    Base exception class for ETL errors.
    Maps ABAP ZCX_ETL_ERROR class.
    """
    
    def __init__(
        self,
        error_text: str,
        error_step: str = None,
        record_id: str = None
    ):
        """
        Initialize ETL error.
        
        Args:
            error_text: Error message text
            error_step: ETL step where error occurred
            record_id: Record ID that caused the error
        """
        self.error_text = error_text
        self.error_step = error_step
        self.record_id = record_id
        super().__init__(self.format_message())
    
    def format_message(self) -> str:
        """Format error message with context."""
        msg_parts = [self.error_text]
        
        if self.error_step:
            msg_parts.append(f"Step: {self.error_step}")
        
        if self.record_id:
            msg_parts.append(f"Record: {self.record_id}")
        
        return " | ".join(msg_parts)


class ExtractionError(ETLError):
    """
    Exception for extraction errors.
    Maps ABAP ZCX_ETL_ERROR=>EXTRACT_ERROR.
    """
    pass


class TransformationError(ETLError):