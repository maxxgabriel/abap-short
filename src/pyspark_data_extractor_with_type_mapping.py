===FILE: src/extract.py===
"""
PySpark Data Extractor Module
Extracts raw sales data from source with ABAP-to-Spark type mappings
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, DateType, TimestampType
)
from pyspark.sql import functions as F
from datetime import datetime
from typing import Optional
import logging

from src.logger import ETLLogger
from src.config import Config


class DataExtractor:
    """
    Extracts raw sales data with proper ABAP-to-Spark type mappings.
    
    ABAP Type Mappings:
    - CHAR(n) -> StringType
    - DATS (YYYYMMDD) -> DateType
    - I (integer) -> IntegerType
    - P (packed decimal) -> DecimalType
    - WAERS (currency) -> StringType
    - TIMESTAMPL -> TimestampType
    """
    
    # Schema with ABAP type mappings
    RAW_SALES_SCHEMA = StructType([
        StructField("trans_id", StringType(), nullable=False),      # ABAP: CHAR(10)
        StructField("trans_date", DateType(), nullable=False),      # ABAP: DATS
        StructField("customer_id", StringType(), nullable=False),   # ABAP: CHAR(10)
        StructField("product_id", StringType(), nullable=False),    # ABAP: CHAR(10)
        StructField("quantity", IntegerType(), nullable=False),     # ABAP: I
        StructField("unit_price", DecimalType(16, 2), nullable=False),  # ABAP: P LENGTH 16 DECIMALS 2
        StructField("currency", StringType(), nullable=False),      # ABAP: WAERS
        StructField("sales_rep", StringType(), nullable=True),      # ABAP: CHAR(20)
        StructField("region", StringType(), nullable=True),         # ABAP: CHAR(10)
        StructField("status", StringType(), nullable=False),        # ABAP: CHAR(1)
        StructField("created_at", TimestampType(), nullable=True),  # ABAP: TIMESTAMPL
        StructField("created_by", StringType(), nullable=True)      # ABAP: SYUNAME
    ])
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: Config):
        """
        Initialize Data Extractor.
        
        Args:
            spark: Active SparkSession
            logger: ETL Logger instance
            config: Configuration object
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.log = logging.getLogger(__name__)
    
    def extract_data(
        self, 
        from_date: str, 
        to_date: str,
        source_path: Optional[str] = None
    ) -> DataFrame:
        """
        Extract raw sales data with date-based filtering.
        
        Converts ABAP SELECT logic:
        SELECT * FROM zsales_raw
        WHERE trans_date BETWEEN @iv_from_date AND @iv_to_date
        AND status = 'N'
        
        Args:
            from_date: Start date in YYYY-MM-DD format (ABAP DATS)
            to_date: End date in YYYY-MM-DD format (ABAP DATS)
            source_path: Optional override for source data path
            
        Returns:
            DataFrame with extracted and filtered data
            
        Raises:
            Exception: If extraction fails
        """
        try:
            self.logger.log_message(
                step="EXTRACT",
                status="S",
                message=f"Starting extraction from {from_date} to {to_date}"
            )
            
            # Determine source path
            data_source = source_path or self.config.get("extraction.source_path")
            file_format = self.config.get("extraction.source_format", "parquet")
            
            # Read data with schema enforcement
            self.log.info(f"Reading from {data_source} in {file_format} format")
            
            df_raw = self._read_source_data(data_source, file_format)
            
            # Apply ABAP date filtering logic
            # BETWEEN in ABAP is inclusive on both ends
            df_filtered = self._apply_date_filter(df_raw, from_date, to_date)
            
            # Apply status filter (equivalent to WHERE status = 'N')
            df_filtered = df_filtered.filter(F.col("status") == "N")
            
            # Cache for performance if enabled
            if self.config.get("extraction.cache_data", False):
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
            self.log.error(f"Extraction error: {str(e)}", exc_info=True)
            raise
    
    def _read_source_data(self, source_path: str, file_format: str) -> DataFrame:
        """
        Read source data with appropriate format handler.
        
        Args:
            source_path: Path to source data
            file_format: Format type (parquet, csv, jdbc, etc.)
            
        Returns:
            DataFrame with raw data
        """
        if file_format.lower() == "parquet":
            return self.spark.read.schema(self.RAW_SALES_SCHEMA).parquet(source_path)
        
        elif file_format.lower() == "csv":
            return self.spark.read.schema(self.RAW_SALES_SCHEMA) \
                .option("header", "true") \
                .option("dateFormat", "yyyy-MM-dd") \
                .csv(source_path)
        
        elif file_format.lower() == "jdbc":
            # JDBC connection for reading from actual database
            jdbc_config = self.config.get("extraction.jdbc", {})
            return self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_config.get("url")) \
                .option("dbtable", jdbc_config.get("table", "zsales_raw")) \
                .option("user", jdbc_config.get("user")) \
                .option("password", jdbc_config.get("password")) \
                .option("driver", jdbc_config.get("driver", "org.postgresql.Driver")) \
                .load()
        
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
    
    def _apply_date_filter(
        self, 
        df: DataFrame, 
        from_date: str, 
        to_date: str
    ) -> DataFrame:
        """
        Apply date-based filtering with ABAP DATS format handling.
        
        ABAP DATS format: YYYYMMDD (stored as string in ABAP)
        Spark DateType: native date type
        
        Args:
            df: Input DataFrame
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            Filtered DataFrame
        """
        # Convert string dates to proper date types if needed
        from_date_typed = F.to_date(F.lit(from_date), "yyyy-MM-dd")
        to_date_typed = F.to_date(F.lit(to_date), "yyyy-MM-dd")
        
        # Apply BETWEEN filter (inclusive)
        df_filtered = df.filter(
            (F.col("trans_date") >= from_date_typed) & 
            (F.col("trans_date") <= to_date_typed)
        )
        
        return df_filtered
    
    def extract_with_partitions(
        self,
        from_date: str,
        to_date: str,
        partition_column: str = "trans_date"
    ) -> DataFrame:
        """
        Extract data with partition pruning for large datasets.
        
        Args:
            from_date: Start date
            to_date: End date
            partition_column: Column to use for partitioning
            
        Returns:
            Partitioned DataFrame
        """
        df = self.extract_data(from_date, to_date)
        
        # Repartition based on config
        num_partitions = self.config.get("extraction.num_partitions", 10)
        
        if partition_column:
            df = df.repartition(num_partitions, partition_column)
        else:
            df = df.repartition(num_partitions)
        
        return df
    
    def validate_extracted_data(self, df: DataFrame) -> tuple[bool, str]:
        """
        Validate extracted data meets quality criteria.
        
        Args:
            df: Extracted DataFrame
            
        Returns:
            Tuple of (is_valid, validation_message)
        """
        issues = []
        
        # Check for null values in required fields
        required_fields = ["trans_id", "trans_date", "customer_id", "product_id", "quantity"]
        for field in required_fields:
            null_count = df.filter(F.col(field).isNull()).count()
            if null_count > 0:
                issues.append(f"{field} has {null_count} null values")
        
        # Check for negative quantities
        negative_qty = df.filter(F.col("quantity") < 0).count()
        if negative_qty > 0:
            issues.append(f"Found {negative_qty} records with negative quantity")
        
        # Check for zero or negative prices
        invalid_price = df.filter(F.col("unit_price") <= 0).count()
        if invalid_price > 0:
            issues.append(f"Found {invalid_price} records with invalid unit_price")
        
        if issues:
            return False, "; ".join(issues)
        
        return True, "All validation checks passed"


===FILE: src/transform.py===
"""
PySpark Data Transformer Module
Transforms raw sales data into analytics format with business rules
"""

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, DateType, TimestampType
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import logging

from src.logger import ETLLogger
from src.config import Config


class DataTransformer:
    """
    Transforms raw sales data applying business logic and calculations.
    
    Implements ABAP transformation logic:
    - Discount calculation based on quantity tiers
    - Tax calculation (8% on net after discount)
    - Profit margin calculation
    - Category assignment
    """
    
    # Analytics schema with ABAP type mappings
    ANALYTICS_SCHEMA = StructType([
        StructField("analytics_id", StringType(), nullable=False),      # ABAP: CHAR(20)
        StructField("trans_date", DateType(), nullable=False),          # ABAP: DATS
        StructField("customer_id", StringType(), nullable=False),       # ABAP: CHAR(10)
        StructField("product_id", StringType(), nullable=False),        # ABAP: CHAR(10)
        StructField("total_quantity", IntegerType(), nullable=False),   # ABAP: I
        StructField("gross_amount", DecimalType(16, 2), nullable=False),    # ABAP: P LENGTH 16 DECIMALS 2
        StructField("net_amount", DecimalType(16, 2), nullable=False),      # ABAP: P LENGTH 16 DECIMALS 2
        StructField("discount_amount", DecimalType(16, 2), nullable=False), # ABAP: P LENGTH 16 DECIMALS 2
        StructField("tax_amount", DecimalType(16, 2), nullable=False),      # ABAP: P LENGTH 16 DECIMALS 2
        StructField("currency", StringType(), nullable=False),          # ABAP: WAERS
        StructField("sales_rep", StringType(), nullable=True),          # ABAP: CHAR(20)
        StructField("region", StringType(), nullable=True),             # ABAP: CHAR(10)
        StructField("profit_margin", DecimalType(5, 2), nullable=True), # ABAP: P LENGTH 5 DECIMALS 2
        StructField("category", StringType(), nullable=False),          # ABAP: CHAR(10)
        StructField("etl_run_id", StringType(), nullable=False),        # ABAP: CHAR(20)
        StructField("loaded_at", TimestampType(), nullable=True),       # ABAP: TIMESTAMPL
        StructField("loaded_by", StringType(), nullable=True)           # ABAP: SYUNAME
    ])
    
    def __init__(self, spark, logger: ETLLogger, config: Config, etl_run_id: str):
        """
        Initialize Data Transformer.
        
        Args:
            spark: SparkSession
            logger: ETL Logger instance
            config: Configuration object
            etl_run_id: Unique ETL run identifier
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.etl_run_id = etl_run_id
        self.log = logging.getLogger(__name__)
    
    def transform_data(self, df_raw: DataFrame) -> DataFrame:
        """
        Transform raw sales data into analytics format.
        
        Implements ABAP transformation logic from ZCL_ETL_TRANSFORMER.
        
        Args:
            df_raw: Raw sales DataFrame
            
        Returns:
            Transformed analytics DataFrame
        """
        try:
            self.logger.log_message(
                step="TRANSFORM",
                status="S",
                message="Starting data transformation"
            )
            
            input_count = df_raw.count()
            
            # Apply transformations
            df_analytics = self._calculate_analytics(df_raw)
            
            # Add ETL metadata
            df_analytics = self._add_etl_metadata(df_analytics)
            
            # Validate transformed data
            df_analytics = self._validate_transformed_data(df_analytics)
            
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
            
            return df_analytics
            
        except Exception as e:
            self.logger.log_message(
                step="TRANSFORM",
                status="E",
                message=f"Transformation failed: {str(e)}"
            )
            self.log.error(f"Transformation error: {str(e)}", exc_info=True)
            raise
    
    def _calculate_analytics(self, df: DataFrame) -> DataFrame:
        """
        Apply core business logic calculations.
        
        Matches ABAP method: calculate_analytics
        
        Args:
            df: Raw sales DataFrame
            
        Returns:
            DataFrame with calculated fields
        """
        # Get business rule constants from config
        discount_tier1_qty = self.config.get("business_rules.discount_qty_tier1", 10)
        discount_tier2_qty = self.config.get("business_rules.discount_qty_tier2", 15)
        discount_rate_tier1 = self.config.get("business_rules.discount_rate_tier1", 0.05)
        discount_rate_tier2 = self.config.get("business_rules.discount_rate_tier2", 0.10)
        tax_rate = self.config.get("business_rules.tax_rate", 0.08)
        cost_ratio = self.config.get("business_rules.cost_ratio", 0.60)
        
        # Step 1: Calculate gross amount
        df = df.withColumn(
            "gross_amount",
            F.col("quantity") * F.col("unit_price")
        )
        
        # Step 2: Calculate discount based on quantity tiers
        # ABAP logic: IF quantity > 15 THEN 10%, ELSEIF > 10 THEN 5%, ELSE 0%
        df = df.withColumn(
            "discount_amount",
            F.when(F.col("quantity") > discount_tier2_qty, 
                   F.col("gross_amount") * discount_rate_tier2)
            .when(F.col("quantity") > discount_tier1_qty,
                  F.col("gross_amount") * discount_rate_tier1)
            .otherwise(F.lit(0.0))
        )
        
        # Step 3: Calculate tax (8% on gross - discount)
        df = df.withColumn(
            "tax_amount",
            (F.col("gross_amount") - F.col("discount_amount")) * tax_rate
        )
        
        # Step 4: Calculate net amount
        df = df.withColumn(
            "net_amount",
            F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
        )
        
        # Step 5: Calculate cost and profit margin
        # ABAP: lv_cost = quantity * unit_price * 0.60
        # profit_margin = ((net - cost) / net) * 100
        df = df.withColumn("cost_amount", 
                          F.col("quantity") * F.col("unit_price") * cost_ratio)
        
        df = df.withColumn(
            "profit_margin",
            F.when(F.col("net_amount") > 0,
                   ((F.col("net_amount") - F.col("cost_amount")) / F.col("net_amount")) * 100)
            .otherwise(F.lit(0.0))
        )
        
        # Step 6: Categorize sales
        df = df.withColumn("category", self._categorize_sale_udf(F.col("gross_amount")))
        
        # Step 7: Generate analytics ID
        df = df.withColumn(
            "analytics_id",
            F.concat(
                F.lit("ANL"),
                F.col("trans_id"),
                F.date_format(F.current_timestamp(), "HHmmss")
            )
        )
        
        # Step 8: Select and rename fields for final schema
        df_analytics = df.select(
            F.col("analytics_id"),
            F.col("trans_date"),
            F.col("customer_id"),
            F.col("product_id"),
            F.col("quantity").alias("total_quantity"),
            F.col("gross_amount"),
            F.col("net_amount"),
            F.col("discount_amount"),
            F.col("tax_amount"),
            F.col("currency"),
            F.col("sales_rep"),
            F.col("region"),
            F.col("profit_margin"),
            F.col("category")
        )
        
        return df_analytics
    
    def _categorize_sale_udf(self, gross_amount_col):
        """
        Create UDF for sale categorization.
        
        ABAP logic:
        IF gross_amount >= 2000 THEN 'HIGH'
        ELSEIF gross_amount >= 500 THEN 'MEDIUM'
        ELSE 'LOW'
        
        Args:
            gross_amount_col: Column reference for gross_amount
            
        Returns:
            Column expression with category
        """
        high_threshold = self.config.get("business_rules.category_high_threshold", 2000.00)
        medium_threshold = self.config.get("business_rules.category_medium_threshold", 500.00)
        
        return F.when(gross_amount_col >= high_threshold, F.lit("HIGH")) \
                .when(gross_amount_col >= medium_threshold, F.lit("MEDIUM")) \
                .otherwise(F.lit("LOW"))
    
    def _add_etl_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add ETL execution metadata.
        
        Args:
            df: DataFrame to augment
            
        Returns:
            DataFrame with metadata columns
        """
        current_user = self.config.get("etl.current_user", "SPARK_ETL")
        
        df = df.withColumn("etl_run_id", F.lit(self.etl_run_id)) \
               .withColumn("loaded_at", F.current_timestamp()) \
               .withColumn("loaded_by", F.lit(current_user))
        
        return df
    
    def _validate_transformed_data(self, df: DataFrame) -> DataFrame:
        """
        Validate transformed records and filter invalid ones.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            DataFrame with only valid records
        """
        # Filter out invalid records
        df_valid = df.filter(
            (F.col("analytics_id").isNotNull()) &
            (F.col("customer_id").isNotNull()) &
            (F.col("product_id").isNotNull()) &
            (F.col("gross_amount") > 0) &
            (F.col("currency").isNotNull()) &
            (F.col("category").isin("HIGH", "MEDIUM", "LOW"))
        )
        
        return df_valid
    
    def aggregate_by_customer(self, df_analytics: DataFrame) -> DataFrame:
        """
        Aggregate analytics data by customer for additional insights.
        
        Args:
            df_analytics: Analytics DataFrame
            
        Returns:
            Customer-level aggregated DataFrame
        """
        df_customer_agg = df_analytics.groupBy("customer_id", "region") \
            .agg(
                F.count("analytics_id").alias("total_transactions"),
                F.sum("total_quantity").alias("total_quantity"),
                F.sum("gross_amount").alias("total_gross_amount"),
                F.sum("net_amount").alias("total_net_amount"),
                F.avg("profit_margin").alias("avg_profit_margin")
            )
        
        return df_customer_agg
    
    def aggregate_by_product(self, df_analytics: DataFrame) -> DataFrame:
        """
        Aggregate analytics data by product.
        
        Args:
            df_analytics: Analytics DataFrame
            
        Returns:
            Product-level aggregated DataFrame
        """
        df_product_agg = df_analytics.groupBy("product_id", "category") \
            .agg(
                F.count("analytics_id").alias("total_transactions"),
                F.sum("total_quantity").alias("total_quantity_sold"),
                F.sum("gross_amount").alias("total_gross_amount"),
                F.avg("profit_margin").alias("avg_profit_margin")
            )
        
        return df_product_agg


===FILE: src/load.py===
"""
PySpark Data Loader Module
Loads transformed analytics data to target destination
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from datetime import datetime
import logging

from src.logger import ETLLogger
from src.config import Config


class DataLoader:
    """
    Loads transformed analytics data into target storage.
    
    Implements ABAP load logic from ZCL_ETL_LOADER with:
    - Data validation before load
    - Batch processing
    - Error handling
    - Status updates
    """
    
    def __init__(self, spark, logger: ETLLogger, config: Config):
        """
        Initialize Data Loader.
        
        Args:
            spark: SparkSession
            logger: ETL Logger instance
            config: Configuration object
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.log = logging.getLogger(__name__)
    
    def load_data(self, df_analytics: DataFrame, target_path: str = None) -> bool:
        """
        Load analytics data to target destination.
        
        Args:
            df_analytics: Transformed analytics DataFrame
            target_path: Optional override for target path
            
        Returns:
            True if load successful, False otherwise
        """
        try:
            self.logger.log_message(
                step="LOAD",
                status="S",
                message="Starting data load"
            )
            
            input_count = df_analytics.count()
            
            # Validate records before loading
            df_valid = self._validate_records(df_analytics)
            valid_count = df_valid.count()
            error_count = input_count - valid_count
            
            if error_count > 0:
                self.logger.log_message(
                    step="LOAD",
                    status="W",
                    message=f"Filtered out {error_count} invalid records"
                )
            
            # Load to target
            target = target_path or self.config.get("load.target_path")
            self._write_to_target(df_valid, target)
            
            # Update source table status (simulated)
            self._update_source_status(df_valid)
            
            self.logger.log_message(
                step="LOAD",
                status="S",
                records_processed=input_count,
                records_success=valid_count,
                records_error=error_count,
                message=f"Loaded {valid_count} of {input_count} records"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=f"Load failed: {str(e)}"
            )
            self.log.error(f"Load error: {str(e)}", exc_info=True)
            return False
    
    def _validate_records(self, df: DataFrame) -> DataFrame:
        """
        Validate records before loading.
        
        Implements ABAP method: validate_record
        
        Args:
            df: DataFrame to validate
            
        Returns:
            DataFrame with only valid records
        """
        # Required field validation
        df_valid = df.filter(
            (F.col("analytics_id").isNotNull()) &
            (F.col("customer_id").isNotNull()) &
            (F.col("product_id").isNotNull()) &
            (F.col("gross_amount") > 0)
        )
        
        # Currency validation
        df_valid = df_valid.filter(F.col("currency").isNotNull())
        
        # Category validation
        df_valid = df_valid.filter(
            F.col("category").isin("HIGH", "MEDIUM", "LOW")
        )
        
        return df_valid
    
    def _write_to_target(self, df: DataFrame, target_path: str):
        """
        Write DataFrame to target storage.
        
        Args:
            df: DataFrame to write
            target_path: Target path or table name
        """
        target_format = self.config.get("load.target_format", "parquet")
        write_mode = self.config.get("load.write_mode", "append")
        partition_by = self.config.get("load.partition_by", None)
        
        self.log.info(f"Writing to {target_path} in {target_format} format")
        
        writer = df.write.mode(write_mode)
        
        if partition_by:
            writer = writer.partitionBy(partition_by)
        
        if target_format.lower() == "parquet":
            writer.parquet(target_path)
        
        elif target_format.lower() == "delta":
            writer.format("delta").save(target_path)
        
        elif target_format.lower() == "jdbc":
            # JDBC write for database targets
            jdbc_config = self.config.get("load.jdbc", {})
            writer.format("jdbc") \
                .option("url", jdbc_config.get("url")) \
                .option("dbtable", jdbc_config.get("table", "zsales_analytics")) \
                .option("user", jdbc_config.get("user")) \
                .option("password", jdbc_config.get("password")) \
                .option("driver", jdbc_config.get("driver", "org.postgresql.Driver")) \
                .save()
        
        else:
            raise ValueError(f"Unsupported target format: {target_format}")
    
    def _update_source_status(self, df_loaded: DataFrame):
        """
        Update source table status to 'P' (Processed).
        
        In ABAP: UPDATE zsales_raw SET status = 'P' WHERE trans_id IN @lt_ids
        
        Args:
            df_loaded: DataFrame with successfully loaded records
        """
        # In a real implementation, this would update the source table
        # For now, we log the transaction IDs that would be updated
        
        trans_ids = df_loaded.select("analytics_id").distinct().collect()
        id_count = len(trans_ids)
        
        self.log.info(f"Would update {id_count} source records to status 'P'")
        
        # If JDBC source is configured, perform actual update
        if self.config.get("load.update_source_status", False):
            # This would require additional JDBC logic
            pass
    
    def load_with_batches(
        self, 
        df_analytics: DataFrame, 
        target_path: str = None,
        batch_size: int = None
    ) -> bool:
        """
        Load data in batches for large datasets.
        
        Args:
            df_analytics: Analytics DataFrame
            target_path: Target path
            batch_size: Records per batch
            
        Returns:
            True if successful
        """
        batch_size = batch_size or self.config.get("load.batch_size", 1000)
        
        self.log.info(f"Loading data in batches of {batch_size}")
        
        # Add batch ID
        total_records = df_analytics.count()
        num_batches = (total_records // batch_size) + 1
        
        df_with_batch = df_analytics.withColumn(
            "batch_id",
            F.monotonically_increasing_id() / batch_size
        )
        
        success = True
        for batch_id in range(num_batches):
            try:
                df_batch = df_with_batch.filter(F.col("batch_id") == batch_id)
                df_batch = df_batch.drop("batch_id")
                
                self._write_to_target(df_batch, target_path)
                
                self.log.info(f"Loaded batch {batch_id + 1}/{num_batches}")
                
            except Exception as e:
                self.log.error(f"Failed to load batch {batch_id}: {str(e)}")
                success = False
        
        return success
    
    def create_summary_table(self, df_analytics: DataFrame, summary_path: str):
        """
        Create summary aggregation table.
        
        Args:
            df_analytics: Analytics DataFrame
            summary_path: Path for summary table
        """
        df_summary = df_analytics.groupBy("trans_date", "region", "category") \
            .agg(
                F.count("analytics_id").alias("transaction_count"),
                F.sum("total_quantity").alias("total_quantity"),
                F.sum("gross_amount").alias("total_gross_amount"),
                F.sum("net_amount").alias("total_net_amount"),
                F.avg("profit_margin").alias("avg_profit_margin")
            )
        
        self._write_to_target(df_summary, summary_path)
        
        self.log.