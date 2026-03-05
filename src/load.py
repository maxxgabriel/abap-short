"""
Data Loading Module with Delta Lake Writer
Converts ABAP INSERT/MODIFY statements to PySpark write operations
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from pyspark.sql import functions as F
from delta import DeltaTable
from typing import Dict, Tuple, Optional
import logging
from datetime import datetime
import yaml


class DeltaLakeLoader:
    """
    Loads transformed analytics data into Delta Lake tables.
    Implements batch write operations with validation and error handling.
    """
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize Delta Lake Loader.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary with Delta Lake settings
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.batch_size = config.get('load', {}).get('batch_size', 1000)
        self.write_mode = config.get('load', {}).get('write_mode', 'append')
        self.optimize_after_write = config.get('load', {}).get('optimize_after_write', True)
        self.vacuum_retention_hours = config.get('load', {}).get('vacuum_retention_hours', 168)
        
    def get_analytics_schema(self) -> StructType:
        """
        Define schema for ZSALES_ANALYTICS target table.
        Maps ABAP structure to PySpark schema.
        
        Returns:
            StructType: Analytics table schema
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
    
    def validate_record(self, row: Dict) -> Tuple[bool, Optional[str]]:
        """
        Validate individual record before loading.
        Mimics ABAP validate_record method.
        
        Args:
            row: Dictionary representing a record
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Validate required fields
        if not row.get('analytics_id'):
            return False, "Missing analytics_id"
        
        if not row.get('customer_id'):
            return False, "Missing customer_id"
        
        if not row.get('product_id'):
            return False, "Missing product_id"
        
        # Validate gross_amount
        gross_amount = row.get('gross_amount')
        if gross_amount is None or gross_amount <= 0:
            return False, "Invalid gross_amount"
        
        # Validate currency
        if not row.get('currency'):
            return False, "Missing currency"
        
        # Validate category
        category = row.get('category')
        if category not in ['HIGH', 'MEDIUM', 'LOW']:
            return False, f"Invalid category: {category}"
        
        return True, None
    
    def validate_dataframe(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Validate entire DataFrame and split into valid/invalid records.
        
        Args:
            df: Input DataFrame to validate
            
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        self.logger.info("Validating records before load")
        
        # Add validation column
        validation_conditions = (
            (F.col("analytics_id").isNotNull()) &
            (F.col("customer_id").isNotNull()) &
            (F.col("product_id").isNotNull()) &
            (F.col("gross_amount") > 0) &
            (F.col("currency").isNotNull()) &
            (F.col("category").isin(['HIGH', 'MEDIUM', 'LOW']))
        )
        
        df_with_validation = df.withColumn("is_valid", validation_conditions)
        
        valid_df = df_with_validation.filter(F.col("is_valid") == True).drop("is_valid")
        invalid_df = df_with_validation.filter(F.col("is_valid") == False).drop("is_valid")
        
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        
        self.logger.info(f"Validation complete: {valid_count} valid, {invalid_count} invalid")
        
        return valid_df, invalid_df
    
    def write_to_delta(
        self,
        df: DataFrame,
        table_path: str,
        table_name: str,
        partition_cols: Optional[list] = None
    ) -> Dict[str, int]:
        """
        Write DataFrame to Delta Lake table.
        Converts ABAP INSERT/MODIFY to Delta write operations.
        
        Args:
            df: DataFrame to write
            table_path: Path to Delta table
            table_name: Name of the table
            partition_cols: Optional list of partition columns
            
        Returns:
            Dictionary with write statistics
        """
        self.logger.info(f"Writing {df.count()} records to Delta table: {table_name}")
        
        start_time = datetime.now()
        
        try:
            # Check if table exists
            table_exists = DeltaTable.isDeltaTable(self.spark, table_path)
            
            if table_exists:
                self.logger.info(f"Delta table exists at {table_path}, using merge operation")
                delta_table = DeltaTable.forPath(self.spark, table_path)
                
                # Perform MERGE operation (equivalent to ABAP MODIFY)
                merge_stats = self._merge_data(delta_table, df)
                operation = "merge"
            else:
                self.logger.info(f"Creating new Delta table at {table_path}")
                # Initial write (equivalent to ABAP INSERT)
                writer = df.write.format("delta").mode("overwrite")
                
                if partition_cols:
                    writer = writer.partitionBy(*partition_cols)
                
                writer.save(table_path)
                
                merge_stats = {
                    "inserted": df.count(),
                    "updated": 0,
                    "deleted": 0
                }
                operation = "insert"
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            self.logger.info(
                f"Write complete: {merge_stats.get('inserted', 0)} inserted, "
                f"{merge_stats.get('updated', 0)} updated in {duration:.2f}s"
            )
            
            # Optimize table if configured
            if self.optimize_after_write:
                self._optimize_table(table_path)
            
            return {
                "operation": operation,
                "records_processed": df.count(),
                "records_inserted": merge_stats.get("inserted", 0),
                "records_updated": merge_stats.get("updated", 0),
                "duration_seconds": duration
            }
            
        except Exception as e:
            self.logger.error(f"Failed to write to Delta table: {str(e)}")
            raise
    
    def _merge_data(self, delta_table: DeltaTable, df: DataFrame) -> Dict[str, int]:
        """
        Merge new data into existing Delta table.
        Implements UPSERT logic (ABAP MODIFY equivalent).
        
        Args:
            delta_table: Existing Delta table
            df: New data to merge
            
        Returns:
            Dictionary with merge statistics
        """
        merge_condition = "target.analytics_id = source.analytics_id"
        
        merge_result = (
            delta_table.alias("target")
            .merge(
                df.alias("source"),
                merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        
        # Parse merge metrics
        stats = {
            "inserted": merge_result.get("num_target_rows_inserted", 0),
            "updated": merge_result.get("num_target_rows_updated", 0),
            "deleted": merge_result.get("num_target_rows_deleted", 0)
        }
        
        return stats
    
    def _optimize_table(self, table_path: str):
        """
        Optimize Delta table for better query performance.
        
        Args:
            table_path: Path to Delta table
        """
        try:
            self.logger.info(f"Optimizing Delta table: {table_path}")
            delta_table = DeltaTable.forPath(self.spark, table_path)
            delta_table.optimize().executeCompaction()
            self.logger.info("Table optimization complete")
        except Exception as e:
            self.logger.warning(f"Table optimization failed: {str(e)}")
    
    def vacuum_table(self, table_path: str):
        """
        Clean up old versions of Delta table.
        
        Args:
            table_path: Path to Delta table
        """
        try:
            self.logger.info(f"Vacuuming Delta table: {table_path}")
            delta_table = DeltaTable.forPath(self.spark, table_path)
            delta_table.vacuum(self.vacuum_retention_hours)
            self.logger.info("Table vacuum complete")
        except Exception as e:
            self.logger.warning(f"Table vacuum failed: {str(e)}")
    
    def load_data(
        self,
        analytics_df: DataFrame,
        etl_run_id: str
    ) -> Dict[str, any]:
        """
        Main load method - orchestrates the complete load process.
        Equivalent to ZCL_ETL_LOADER->load_data in ABAP.
        
        Args:
            analytics_df: Transformed analytics DataFrame
            etl_run_id: Current ETL run identifier
            
        Returns:
            Dictionary with load statistics and status
        """
        self.logger.info(f"Starting data load for ETL run: {etl_run_id}")
        
        load_stats = {
            "total_records": 0,
            "valid_records": 0,
            "invalid_records": 0,
            "loaded_records": 0,
            "failed_records": 0,
            "success": False
        }
        
        try:
            # Add load metadata
            df_with_metadata = analytics_df.withColumn(
                "loaded_at", F.current_timestamp()
            ).withColumn(
                "loaded_by", F.lit(self.config.get('etl', {}).get('user', 'ETL_USER'))
            )
            
            load_stats["total_records"] = df_with_metadata.count()
            
            # Validate records
            valid_df, invalid_df = self.validate_dataframe(df_with_metadata)
            
            load_stats["valid_records"] = valid_df.count()
            load_stats["invalid_records"] = invalid_df.count()
            
            # Log invalid records
            if load_stats["invalid_records"] > 0:
                self._log_invalid_records(invalid_df, etl_run_id)
            
            # Write valid records to Delta Lake
            if load_stats["valid_records"] > 0:
                table_path = self.config.get('delta', {}).get('analytics_table_path')
                partition_cols = self.config.get('delta', {}).get('partition_columns', ['trans_date'])
                
                write_stats = self.write_to_delta(
                    valid_df,
                    table_path,
                    "zsales_analytics",
                    partition_cols
                )
                
                load_stats["loaded_records"] = write_stats["records_inserted"] + write_stats["records_updated"]
                load_stats["write_stats"] = write_stats
            
            # Update source table status (simulated)
            self._update_source_status(analytics_df, etl_run_id)
            
            load_stats["success"] = True
            load_stats["failed_records"] = load_stats["valid_records"] - load_stats["loaded_records"]
            
            self.logger.info(
                f"Load complete: {load_stats['loaded_records']} loaded, "
                f"{load_stats['invalid_records']} invalid, "
                f"{load_stats['failed_records']} failed"
            )
            
            return load_stats
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            load_stats["success"] = False
            load_stats["error"] = str(e)
            raise
    
    def _log_invalid_records(self, invalid_df: DataFrame, etl_run_id: str):
        """
        Log invalid records to error table.
        
        Args:
            invalid_df: DataFrame with invalid records
            etl_run_id: Current ETL run ID
        """
        error_table_path = self.config.get('delta', {}).get('error_table_path')
        
        if error_table_path:
            try:
                error_df = invalid_df.withColumn("etl_run_id", F.lit(etl_run_id)) \
                                    .withColumn("error_time", F.current_timestamp())
                
                error_df.write.format("delta") \
                       .mode("append") \
                       .save(error_table_path)
                
                self.logger.info(f"Logged {invalid_df.count()} invalid records to error table")
            except Exception as e:
                self.logger.warning(f"Failed to log invalid records: {str(e)}")
    
    def _update_source_status(self, analytics_df: DataFrame, etl_run_id: str):
        """
        Update source table status after successful load.
        Simulates ABAP UPDATE statement.
        
        Args:
            analytics_df: Loaded analytics data
            etl_run_id: Current ETL run ID
        """
        # In production, this would update the source ZSALES_RAW table
        # to mark records as processed
        self.logger.info(f"Source table status updated for ETL run: {etl_run_id}")


def create_loader(config_path: str = "config.yaml") -> DeltaLakeLoader:
    """
    Factory function to create DeltaLakeLoader instance.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Configured DeltaLakeLoader instance
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    spark = SparkSession.builder \
        .appName("Sales ETL - Load") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    return DeltaLakeLoader(spark, config)