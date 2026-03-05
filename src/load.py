"""
Data Loading Module with Delta Lake Writer
Converts ABAP INSERT/MODIFY statements to PySpark write operations with batch configuration.
Maps ZSALES_ANALYTICS structure to Delta Lake schema.
"""

from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, DateType, TimestampType
)
from delta import DeltaTable
import logging
from datetime import datetime


class DeltaLakeLoader:
    """
    Loads transformed analytics data into Delta Lake format.
    Implements batch write operations with validation and error handling.
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any], logger: Optional[logging.Logger] = None):
        """
        Initialize Delta Lake loader.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary with batch and write settings
            logger: Optional logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        
        # Load configuration
        self.target_table = config['load']['target_table']
        self.delta_path = config['load']['delta_path']
        self.batch_size = config['load']['batch_size']
        self.write_mode = config['load']['write_mode']
        self.partition_columns = config['load'].get('partition_columns', [])
        self.enable_optimize = config['load'].get('enable_optimize', True)
        self.enable_vacuum = config['load'].get('enable_vacuum', False)
        self.vacuum_retention_hours = config['load'].get('vacuum_retention_hours', 168)
        
        # Statistics
        self.stats = {
            'total_records': 0,
            'success_records': 0,
            'error_records': 0,
            'batches_processed': 0
        }
    
    def get_analytics_schema(self) -> StructType:
        """
        Define schema for ZSALES_ANALYTICS table mapping.
        
        Returns:
            StructType schema matching the analytics data structure
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
            StructField("loaded_at", TimestampType(), nullable=False),
            StructField("loaded_by", StringType(), nullable=True)
        ])
    
    def validate_record(self, row: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Validate a single record before loading.
        Implements ABAP validation logic from ZCL_ETL_LOADER.
        
        Args:
            row: Dictionary representing a single record
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Validate required fields
        required_fields = ['analytics_id', 'customer_id', 'product_id', 'gross_amount']
        for field in required_fields:
            if not row.get(field):
                return False, f"Missing or empty required field: {field}"
        
        # Validate gross_amount is positive
        if row.get('gross_amount', 0) <= 0:
            return False, f"Invalid gross_amount: {row.get('gross_amount')}"
        
        # Validate currency
        if not row.get('currency'):
            return False, "Missing currency"
        
        # Validate category
        valid_categories = ['HIGH', 'MEDIUM', 'LOW']
        if row.get('category') not in valid_categories:
            return False, f"Invalid category: {row.get('category')}"
        
        return True, None
    
    def load_data(self, analytics_df: DataFrame, etl_run_id: str) -> bool:
        """
        Main load method - writes analytics data to Delta Lake.
        Implements batch processing and error handling.
        
        Args:
            analytics_df: DataFrame containing transformed analytics data
            etl_run_id: ETL run identifier for tracking
            
        Returns:
            Boolean indicating success
        """
        try:
            self.logger.info(f"Starting data load to Delta Lake: {self.delta_path}")
            self.logger.info(f"ETL Run ID: {etl_run_id}")
            
            # Add metadata columns
            load_timestamp = datetime.now()
            analytics_df = analytics_df.withColumn("loaded_at", 
                                                   self.spark.sql.functions.lit(load_timestamp))
            analytics_df = analytics_df.withColumn("loaded_by", 
                                                   self.spark.sql.functions.lit("PYSPARK_ETL"))
            analytics_df = analytics_df.withColumn("etl_run_id", 
                                                   self.spark.sql.functions.lit(etl_run_id))
            
            # Validate schema
            expected_schema = self.get_analytics_schema()
            if not self._validate_schema(analytics_df, expected_schema):
                raise ValueError("DataFrame schema does not match expected analytics schema")
            
            # Get record count
            total_records = analytics_df.count()
            self.stats['total_records'] = total_records
            self.logger.info(f"Total records to load: {total_records}")
            
            # Perform validation on DataFrame level
            validated_df = self._validate_dataframe(analytics_df)
            valid_count = validated_df.count()
            invalid_count = total_records - valid_count
            
            if invalid_count > 0:
                self.logger.warning(f"Filtered out {invalid_count} invalid records")
                self.stats['error_records'] = invalid_count
            
            # Write to Delta Lake
            success = self._write_to_delta(validated_df)
            
            if success:
                self.stats['success_records'] = valid_count
                
                # Post-load operations
                if self.enable_optimize:
                    self._optimize_table()
                
                if self.enable_vacuum:
                    self._vacuum_table()
                
                self.logger.info(f"Successfully loaded {valid_count} records to Delta Lake")
                return True
            else:
                self.logger.error("Failed to write data to Delta Lake")
                return False
                
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}", exc_info=True)
            return False
    
    def _validate_schema(self, df: DataFrame, expected_schema: StructType) -> bool:
        """
        Validate DataFrame schema against expected schema.
        
        Args:
            df: Input DataFrame
            expected_schema: Expected schema
            
        Returns:
            Boolean indicating if schemas match
        """
        df_fields = {field.name: field.dataType for field in df.schema.fields}
        expected_fields = {field.name: field.dataType for field in expected_schema.fields}
        
        for field_name, field_type in expected_fields.items():
            if field_name not in df_fields:
                self.logger.error(f"Missing field in DataFrame: {field_name}")
                return False
            if not isinstance(df_fields[field_name], type(field_type)):
                self.logger.warning(f"Type mismatch for field {field_name}: "
                                  f"expected {field_type}, got {df_fields[field_name]}")
        
        return True
    
    def _validate_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Apply validation rules to DataFrame.
        Filter out invalid records.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Validated DataFrame
        """
        from pyspark.sql.functions import col
        
        # Apply validation rules
        validated_df = df.filter(
            (col("analytics_id").isNotNull()) &
            (col("customer_id").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("gross_amount") > 0) &
            (col("currency").isNotNull()) &
            (col("category").isin(['HIGH', 'MEDIUM', 'LOW']))
        )
        
        return validated_df
    
    def _write_to_delta(self, df: DataFrame) -> bool:
        """
        Write DataFrame to Delta Lake with configured options.
        Implements INSERT/MODIFY logic from ABAP.
        
        Args:
            df: DataFrame to write
            
        Returns:
            Boolean indicating success
        """
        try:
            # Configure write options
            writer = df.write.format("delta")
            
            # Set write mode (append, overwrite, merge)
            writer = writer.mode(self.write_mode)
            
            # Add partitioning if configured
            if self.partition_columns:
                writer = writer.partitionBy(*self.partition_columns)
            
            # Delta Lake options
            writer = writer.option("mergeSchema", "true")
            writer = writer.option("overwriteSchema", "false")
            
            # Batch size configuration
            if self.batch_size:
                writer = writer.option("maxRecordsPerFile", self.batch_size)
            
            # Write to Delta path
            self.logger.info(f"Writing to Delta Lake: {self.delta_path}")
            writer.save(self.delta_path)
            
            # Create or update table in metastore if configured
            if self.target_table:
                self._register_table()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to write to Delta Lake: {str(e)}", exc_info=True)
            return False
    
    def _register_table(self):
        """
        Register Delta table in Spark catalog.
        """
        try:
            # Check if table exists
            if self.spark.catalog.tableExists(self.target_table):
                self.logger.info(f"Table {self.target_table} already exists")
            else:
                # Create table
                self.spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {self.target_table}
                    USING DELTA
                    LOCATION '{self.delta_path}'
                """)
                self.logger.info(f"Created table {self.target_table}")
                
        except Exception as e:
            self.logger.warning(f"Failed to register table: {str(e)}")
    
    def _optimize_table(self):
        """
        Run OPTIMIZE command on Delta table.
        Improves query performance by compacting small files.
        """
        try:
            self.logger.info("Running OPTIMIZE on Delta table")
            delta_table = DeltaTable.forPath(self.spark, self.delta_path)
            delta_table.optimize().executeCompaction()
            self.logger.info("OPTIMIZE completed successfully")
        except Exception as e:
            self.logger.warning(f"OPTIMIZE failed: {str(e)}")
    
    def _vacuum_table(self):
        """
        Run VACUUM command on Delta table.
        Removes old files according to retention policy.
        """
        try:
            self.logger.info(f"Running VACUUM with retention of {self.vacuum_retention_hours} hours")
            delta_table = DeltaTable.forPath(self.spark, self.delta_path)
            delta_table.vacuum(self.vacuum_retention_hours)
            self.logger.info("VACUUM completed successfully")
        except Exception as e:
            self.logger.warning(f"VACUUM failed: {str(e)}")
    
    def merge_data(self, updates_df: DataFrame, merge_keys: list[str]) -> bool:
        """
        Perform MERGE operation (upsert) to Delta table.
        Implements MODIFY logic from ABAP.
        
        Args:
            updates_df: DataFrame with updates/inserts
            merge_keys: List of columns to use as merge keys
            
        Returns:
            Boolean indicating success
        """
        try:
            self.logger.info(f"Starting MERGE operation with keys: {merge_keys}")
            
            # Check if Delta table exists
            if not DeltaTable.isDeltaTable(self.spark, self.delta_path):
                self.logger.info("Delta table does not exist, performing initial load")
                return self.load_data(updates_df, updates_df.first()['etl_run_id'])
            
            # Load existing Delta table
            delta_table = DeltaTable.forPath(self.spark, self.delta_path)
            
            # Build merge condition
            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
            
            # Prepare update columns (all except keys and metadata)
            update_cols = {col: f"source.{col}" for col in updates_df.columns 
                          if col not in merge_keys}
            
            # Execute merge
            (delta_table.alias("target")
             .merge(updates_df.alias("source"), merge_condition)
             .whenMatchedUpdate(set=update_cols)
             .whenNotMatchedInsertAll()
             .execute())
            
            self.logger.info("MERGE operation completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"MERGE operation failed: {str(e)}", exc_info=True)
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get load statistics.
        
        Returns:
            Dictionary with load statistics
        """
        return self.stats.copy()


def create_loader(spark: SparkSession, config: Dict[str, Any], 
                 logger: Optional[logging.Logger] = None) -> DeltaLakeLoader:
    """
    Factory function to create DeltaLakeLoader instance.
    
    Args:
        spark: SparkSession instance
        config: Configuration dictionary
        logger: Optional logger instance
        
    Returns:
        DeltaLakeLoader instance
    """
    return DeltaLakeLoader(spark, config, logger)