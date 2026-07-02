===FILE: src/extract.py===
"""
Extract module for Sales ETL process.
Extracts raw sales data from source systems.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType, TimestampType
)
from datetime import datetime
from typing import Optional
import logging

from src.logger import ETLLogger


class SalesExtractor:
    """Extracts raw sales data from source table."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the extractor.
        
        Args:
            spark: SparkSession instance
            logger: ETLLogger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        
    def get_raw_sales_schema(self) -> StructType:
        """Define schema for raw sales data."""
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
    
    def extract_data(
        self, 
        from_date: str, 
        to_date: str,
        source_path: Optional[str] = None
    ) -> DataFrame:
        """
        Extract raw sales data for date range.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            source_path: Optional override for source data path
            
        Returns:
            DataFrame containing raw sales data
            
        Raises:
            Exception: If extraction fails
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Use source_path from config if not provided
            data_source = source_path or self.config.get("source_table_path")
            
            # Read data with schema
            schema = self.get_raw_sales_schema()
            
            # Read from source (could be table, parquet, CSV, etc.)
            if data_source.endswith('.parquet'):
                df = self.spark.read.parquet(data_source)
            elif data_source.endswith('.csv'):
                df = self.spark.read.csv(
                    data_source, 
                    header=True, 
                    schema=schema
                )
            else:
                # Assume JDBC or catalog table
                df = self.spark.read.table(data_source)
            
            # Filter by date range and status
            df_filtered = df.filter(
                (df.trans_date >= from_date) &
                (df.trans_date <= to_date) &
                (df.status == 'N')
            )
            
            # Cache for multiple operations
            df_filtered.cache()
            
            record_count = df_filtered.count()
            
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Extracted {record_count} records successfully"
            )
            
            return df_filtered
            
        except Exception as e:
            self.logger.log_message(
                step="EXTRACT",
                status="E",
                message=f"Extraction failed: {str(e)}"
            )
            raise
    
    def create_sample_data(self) -> DataFrame:
        """
        Create sample data for testing.
        
        Returns:
            DataFrame with sample sales data
        """
        schema = self.get_raw_sales_schema()
        
        sample_data = [
            ("T000001", datetime(2024, 1, 15).date(), "CUST001", "PROD001", 
             10, 99.99, "USD", "John Doe", "NORTH", "N", 
             datetime.now(), "SYSTEM"),
            ("T000002", datetime(2024, 1, 15).date(), "CUST002", "PROD002", 
             5, 149.99, "USD", "Jane Smith", "SOUTH", "N", 
             datetime.now(), "SYSTEM"),
            ("T000003", datetime(2024, 1, 15).date(), "CUST003", "PROD001", 
             20, 99.99, "USD", "John Doe", "EAST", "N", 
             datetime.now(), "SYSTEM"),
            ("T000004", datetime(2024, 1, 15).date(), "CUST001", "PROD003", 
             3, 299.99, "USD", "Bob Wilson", "WEST", "N", 
             datetime.now(), "SYSTEM"),
            ("T000005", datetime(2024, 1, 15).date(), "CUST004", "PROD002", 
             15, 149.99, "USD", "Jane Smith", "SOUTH", "N", 
             datetime.now(), "SYSTEM")
        ]
        
        return self.spark.createDataFrame(sample_data, schema)


===FILE: src/transform.py===
"""
Transform module for Sales ETL process.
Transforms raw sales data into analytics format with business rules.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    IntegerType, DecimalType, TimestampType
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Dict

from src.logger import ETLLogger


class SalesTransformer:
    """Transforms raw sales data applying business rules."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the transformer.
        
        Args:
            spark: SparkSession instance
            logger: ETLLogger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        
        # Load business rules from config
        self.discount_qty_tier1 = config.get("discount_qty_tier1", 10)
        self.discount_qty_tier2 = config.get("discount_qty_tier2", 15)
        self.discount_rate_tier1 = config.get("discount_rate_tier1", 0.05)
        self.discount_rate_tier2 = config.get("discount_rate_tier2", 0.10)
        self.tax_rate = config.get("tax_rate", 0.08)
        self.cost_ratio = config.get("cost_ratio", 0.60)
        self.category_high_threshold = config.get("category_high_threshold", 2000.00)
        self.category_medium_threshold = config.get("category_medium_threshold", 500.00)
    
    def get_analytics_schema(self) -> StructType:
        """Define schema for analytics data."""
        return StructType([
            StructField("analytics_id", StringType(), False),
            StructField("trans_date", DateType(), False),
            StructField("customer_id", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("total_quantity", IntegerType(), False),
            StructField("gross_amount", DecimalType(16, 2), False),
            StructField("net_amount", DecimalType(16, 2), False),
            StructField("discount_amount", DecimalType(16, 2), False),
            StructField("tax_amount", DecimalType(16, 2), False),
            StructField("currency", StringType(), False),
            StructField("sales_rep", StringType(), True),
            StructField("region", StringType(), True),
            StructField("profit_margin", DecimalType(5, 2), True),
            StructField("category", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("loaded_at", TimestampType(), False),
            StructField("loaded_by", StringType(), False)
        ])
    
    def transform_data(self, raw_df: DataFrame, etl_run_id: str) -> DataFrame:
        """
        Transform raw sales data into analytics format.
        
        Args:
            raw_df: DataFrame with raw sales data
            etl_run_id: Unique ETL run identifier
            
        Returns:
            DataFrame with transformed analytics data
            
        Raises:
            Exception: If transformation fails
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                message="Starting data transformation"
            )
            
            input_count = raw_df.count()
            
            # Calculate gross amount
            df_with_gross = raw_df.withColumn(
                "gross_amount",
                F.col("quantity") * F.col("unit_price")
            )
            
            # Calculate discount based on quantity tiers
            df_with_discount = df_with_gross.withColumn(
                "discount_amount",
                F.when(
                    F.col("quantity") > self.discount_qty_tier2,
                    F.col("gross_amount") * F.lit(self.discount_rate_tier2)
                ).when(
                    F.col("quantity") > self.discount_qty_tier1,
                    F.col("gross_amount") * F.lit(self.discount_rate_tier1)
                ).otherwise(F.lit(0.0))
            )
            
            # Calculate tax on (gross - discount)
            df_with_tax = df_with_discount.withColumn(
                "taxable_amount",
                F.col("gross_amount") - F.col("discount_amount")
            ).withColumn(
                "tax_amount",
                F.col("taxable_amount") * F.lit(self.tax_rate)
            )
            
            # Calculate net amount
            df_with_net = df_with_tax.withColumn(
                "net_amount",
                F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
            )
            
            # Calculate profit margin
            df_with_profit = df_with_net.withColumn(
                "cost_amount",
                F.col("quantity") * F.col("unit_price") * F.lit(self.cost_ratio)
            ).withColumn(
                "profit_margin",
                F.round(
                    ((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100,
                    2
                )
            )
            
            # Categorize sales
            df_categorized = df_with_profit.withColumn(
                "category",
                F.when(
                    F.col("gross_amount") >= self.category_high_threshold,
                    F.lit("HIGH")
                ).when(
                    F.col("gross_amount") >= self.category_medium_threshold,
                    F.lit("MEDIUM")
                ).otherwise(F.lit("LOW"))
            )
            
            # Generate analytics ID
            df_with_id = df_categorized.withColumn(
                "analytics_id",
                F.concat(
                    F.lit("ANL"),
                    F.col("trans_id"),
                    F.date_format(F.current_timestamp(), "HHmmss")
                )
            )
            
            # Add ETL metadata
            df_final = df_with_id.withColumn(
                "etl_run_id", F.lit(etl_run_id)
            ).withColumn(
                "loaded_at", F.current_timestamp()
            ).withColumn(
                "loaded_by", F.lit("ETL_SYSTEM")
            ).withColumn(
                "total_quantity", F.col("quantity")
            )
            
            # Select final columns
            analytics_df = df_final.select(
                "analytics_id",
                "trans_date",
                "customer_id",
                "product_id",
                "total_quantity",
                "gross_amount",
                "net_amount",
                "discount_amount",
                "tax_amount",
                "currency",
                "sales_rep",
                "region",
                "profit_margin",
                "category",
                "etl_run_id",
                "loaded_at",
                "loaded_by"
            )
            
            # Cache transformed data
            analytics_df.cache()
            
            output_count = analytics_df.count()
            
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                records_processed=input_count,
                records_success=output_count,
                message=f"Transformed {output_count} of {input_count} records"
            )
            
            return analytics_df
            
        except Exception as e:
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=f"Transformation failed: {str(e)}"
            )
            raise
    
    def validate_transformed_data(self, analytics_df: DataFrame) -> Dict[str, int]:
        """
        Validate transformed data quality.
        
        Args:
            analytics_df: Transformed analytics DataFrame
            
        Returns:
            Dictionary with validation statistics
        """
        validations = {
            "total_records": analytics_df.count(),
            "null_analytics_id": analytics_df.filter(F.col("analytics_id").isNull()).count(),
            "null_customer_id": analytics_df.filter(F.col("customer_id").isNull()).count(),
            "invalid_gross_amount": analytics_df.filter(F.col("gross_amount") <= 0).count(),
            "invalid_category": analytics_df.filter(
                ~F.col("category").isin(["HIGH", "MEDIUM", "LOW"])
            ).count()
        }
        
        return validations


===FILE: src/load.py===
"""
Load module for Sales ETL process.
Loads transformed analytics data into target table.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, Optional

from src.logger import ETLLogger


class SalesLoader:
    """Loads transformed analytics data into target."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize the loader.
        
        Args:
            spark: SparkSession instance
            logger: ETLLogger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
    
    def validate_record(self, row: dict) -> bool:
        """
        Validate a single record before loading.
        
        Args:
            row: Dictionary representing a record
            
        Returns:
            True if valid, False otherwise
        """
        # Check required fields
        if not row.get("analytics_id") or not row.get("customer_id"):
            return False
        
        if not row.get("product_id"):
            return False
        
        # Check gross amount
        if row.get("gross_amount", 0) <= 0:
            return False
        
        # Check currency
        if not row.get("currency"):
            return False
        
        # Check category
        if row.get("category") not in ["HIGH", "MEDIUM", "LOW"]:
            return False
        
        return True
    
    def load_data(
        self, 
        analytics_df: DataFrame,
        target_path: Optional[str] = None,
        mode: str = "append"
    ) -> bool:
        """
        Load analytics data to target table.
        
        Args:
            analytics_df: DataFrame with analytics data
            target_path: Optional override for target path
            mode: Write mode (append, overwrite, etc.)
            
        Returns:
            True if successful, False otherwise
            
        Raises:
            Exception: If load fails
        """
        try:
            self.logger.log_message(
                step="LOAD",
                status="S",
                message="Starting data load"
            )
            
            total_records = analytics_df.count()
            
            # Use target_path from config if not provided
            target = target_path or self.config.get("target_table_path")
            
            # Add validation flag
            analytics_with_validation = analytics_df.withColumn(
                "_valid",
                F.when(
                    (F.col("analytics_id").isNotNull()) &
                    (F.col("customer_id").isNotNull()) &
                    (F.col("product_id").isNotNull()) &
                    (F.col("gross_amount") > 0) &
                    (F.col("currency").isNotNull()) &
                    (F.col("category").isin(["HIGH", "MEDIUM", "LOW"])),
                    F.lit(True)
                ).otherwise(F.lit(False))
            )
            
            # Separate valid and invalid records
            valid_df = analytics_with_validation.filter(F.col("_valid") == True).drop("_valid")
            invalid_df = analytics_with_validation.filter(F.col("_valid") == False).drop("_valid")
            
            valid_count = valid_df.count()
            invalid_count = invalid_df.count()
            
            # Write valid records to target
            if valid_count > 0:
                if target.endswith('.parquet'):
                    valid_df.write.mode(mode).parquet(target)
                elif target.endswith('.csv'):
                    valid_df.write.mode(mode).csv(target, header=True)
                else:
                    # Write to catalog table
                    valid_df.write.mode(mode).saveAsTable(target)
            
            # Log invalid records if any
            if invalid_count > 0:
                self.logger.log_message(
                    step="LOAD",
                    status="W",
                    records_error=invalid_count,
                    message=f"Skipped {invalid_count} invalid records"
                )
                
                # Optionally write invalid records to error table
                error_target = self.config.get("error_table_path")
                if error_target:
                    invalid_df.withColumn(
                        "error_reason", F.lit("Validation failed")
                    ).withColumn(
                        "error_timestamp", F.current_timestamp()
                    ).write.mode("append").parquet(error_target)
            
            self.logger.log_message(
                step="LOAD",
                status="S",
                records_processed=total_records,
                records_success=valid_count,
                records_error=invalid_count,
                message=f"Loaded {valid_count} of {total_records} records"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=f"Load failed: {str(e)}"
            )
            raise
    
    def update_source_status(
        self,
        analytics_df: DataFrame,
        source_path: Optional[str] = None
    ) -> None:
        """
        Update status of processed records in source table.
        
        Args:
            analytics_df: DataFrame with loaded analytics
            source_path: Optional path to source table
        """
        try:
            # Extract transaction IDs that were processed
            trans_ids = [
                row.analytics_id.replace("ANL", "").split("T")[1][:6] 
                for row in analytics_df.select("analytics_id").collect()
            ]
            
            # In production, execute UPDATE statement on source table
            # UPDATE source_table SET status = 'P' WHERE trans_id IN (...)
            
            self.logger.log_message(
                step="LOAD",
                status="I",
                message=f"Updated status for {len(trans_ids)} source records"
            )
            
        except Exception as e:
            self.logger.log_message(
                step="LOAD",
                status="W",
                message=f"Failed to update source status: {str(e)}"
            )


===FILE: src/orchestrator.py===
"""
Orchestrator module for Sales ETL process.
Coordinates the complete ETL pipeline execution.
"""
from pyspark.sql import SparkSession
from datetime import datetime
from typing import Optional, Dict
import time

from src.extract import SalesExtractor
from src.transform import SalesTransformer
from src.load import SalesLoader
from src.logger import ETLLogger


class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline."""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize the orchestrator.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        
        # Generate unique ETL run ID
        self.etl_run_id = self._generate_etl_run_id()
        
        # Initialize logger
        self.logger = ETLLogger(
            spark=spark,
            etl_run_id=self.etl_run_id,
            config=config
        )
        
        # Initialize ETL components
        self.extractor = SalesExtractor(spark, self.logger, config)
        self.transformer = SalesTransformer(spark, self.logger, config)
        self.loader = SalesLoader(spark, self.logger, config)
        
        # Track execution metrics
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.metrics: Dict[str, any] = {}
        
        # Log initialization
        self.logger.log_message(
            step="INIT",
            status="S",
            message=f"ETL process initialized with run ID: {self.etl_run_id}"
        )
    
    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run identifier.
        
        Returns:
            Unique run ID string
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"ETL{timestamp}"
    
    def run_etl(
        self,
        from_date: str,
        to_date: str,
        use_sample_data: bool = False
    ) -> bool:
        """
        Execute the complete ETL pipeline.
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            use_sample_data: If True, use sample data instead of reading source
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Record start time
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step="START",
                status="S",
                message=f"ETL process started at {self.start_time}"
            )
            
            # Step 1: Extract
            print("=" * 60)
            print("=== EXTRACT Phase ===")
            print("=" * 60)
            
            if use_sample_data:
                raw_df = self.extractor.create_sample_data()
            else:
                raw_df = self.extractor.extract_data(from_date, to_date)
            
            if raw_df.count() == 0:
                self.logger.log_message(
                    step="EXTRACT",
                    status="W",
                    message="No data found for specified date range"
                )
                return False
            
            self.metrics["extracted_records"] = raw_df.count()
            
            # Step 2: Transform
            print("\n" + "=" * 60)
            print("=== TRANSFORM Phase ===")
            print("=" * 60)
            
            analytics_df = self.transformer.transform_data(raw_df, self.etl_run_id)
            
            # Validate transformed data
            validation_stats = self.transformer.validate_transformed_data(analytics_df)
            self.metrics["validation_stats"] = validation_stats
            
            if validation_stats["invalid_gross_amount"] > 0:
                self.logger.log_message(
                    step="TRANSFORM",
                    status="W",
                    message=f"Found {validation_stats['invalid_gross_amount']} records with invalid amounts"
                )
            
            self.metrics["transformed_records"] = analytics_df.count()
            
            # Step 3: Load
            print("\n" + "=" * 60)
            print("=== LOAD Phase ===")
            print("=" * 60)
            
            load_success = self.loader.load_data(analytics_df)
            
            if not load_success:
                raise Exception("Data load failed")
            
            self.metrics["loaded_records"] = analytics_df.count()
            
            # Update source status (optional)
            if self.config.get("update_source_status", True):
                self.loader.update_source_status(analytics_df)
            
            # Record end time
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="COMPLETE",
                status="S",
                message=f"ETL process completed successfully at {self.end_time}"
            )
            
            # Display summary
            self.display_summary()
            
            return True
            
        except Exception as e:
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step="ERROR",
                status="E",
                message=f"ETL process failed: {str(e)}"
            )
            
            print(f"\n*** ETL Process Failed ***")
            print(f"Error: {str(e)}")
            
            return False
        
        finally:
            # Cleanup
            self.spark.catalog.clearCache()
    
    def get_etl_run_id(self) -> str:
        """Get the current ETL run ID."""
        return self.etl_run_id
    
    def display_summary(self) -> None:
        """Display execution summary."""
        print("\n" + "=" * 60)
        print("ETL Process Summary")
        print("=" * 60)
        print(f"ETL Run ID:    {self.etl_run_id}")
        print(f"Start Time:    {self.start_time}")
        print(f"End Time:      {self.end_time}")
        
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            print(f"Duration:      {duration:.2f} seconds")
        
        print("\nRecord Counts:")
        print(f"  Extracted:   {self.metrics.get('extracted_records', 0)}")
        print(f"  Transformed: {self.metrics.get('transformed_records', 0)}")
        print(f"  Loaded:      {self.metrics.get('loaded_records', 0)}")
        
        if "validation_stats" in self.metrics:
            vstats = self.metrics["validation_stats"]
            print("\nValidation Statistics:")
            print(f"  Total Records:       {vstats.get('total_records', 0)}")
            print(f"  Null Analytics ID:   {vstats.get('null_analytics_id', 0)}")
            print(f"  Null Customer ID:    {vstats.get('null_customer_id', 0)}")
            print(f"  Invalid Gross Amt:   {vstats.get('invalid_gross_amount', 0)}")
            print(f"  Invalid Category:    {vstats.get('invalid_category', 0)}")
        
        print("=" * 60)
    
    def get_metrics(self) -> Dict[str, any]:
        """
        Get execution metrics.
        
        Returns:
            Dictionary with execution metrics
        """
        return self.metrics.copy()


===FILE: src/logger.py===
"""
Logger module for ETL process.
Handles logging of ETL execution steps and metrics.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)
from datetime import datetime
from typing import Optional


class ETLLogger:
    """Logger for ETL process execution."""
    
    def __init__(self, spark: SparkSession, etl_run_id: str, config: dict):
        """
        Initialize the logger.
        
        Args:
            spark: SparkSession instance
            etl_run_id: Unique ETL run identifier
            config: Configuration dictionary
        """
        self.spark = spark
        self.etl_run_id = etl_run_id
        self.config = config
        self.log_entries = []
    
    def get_log_schema(self) -> StructType:
        """Define schema for log entries."""
        return StructType([
            StructField("log_id", StringType(), False),
            StructField("etl_run_id", StringType(), False),
            StructField("execution_timestamp", TimestampType(), False),
            StructField("process_step", StringType(), False),
            StructField("status", StringType(), False),
            StructField("records_processed", IntegerType(), True),
            StructField("records_success", IntegerType(), True),
            StructField("records_error", IntegerType(), True),
            StructField("message", StringType(), True)
        ])
    
    def _generate_log_id(self) -> str:
        """Generate unique log entry ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"LOG{timestamp}"
    
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
        Log a message for the ETL process.
        
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
            "execution_timestamp": datetime.now(),
            "process_step": step,
            "status": status,
            "records_processed": records_processed,
            "records_success": records_success,
            "records_error": records_error,
            "message": message
        }
        
        # Store in memory
        self.log_entries.append(log_entry)
        
        # Console output
        timestamp_str = log_entry["execution_timestamp"].strftime("%H:%M:%S")
        print(f"[{timestamp_str}] [{step}] [{status}] {message}")
        
        # If configured, write to log table
        if self.config.get("write_logs_to_table", False):
            self._write_to_log_table(log_entry)
    
    def _write_to_log_table(self, log_entry: dict) -> None:
        """
        Write log entry to persistent log table.
        
        Args:
            log_entry: Log entry dictionary
        """
        try:
            log_df = self