"""
Data Loading Module with Delta Lake Writer
Converts ABAP INSERT/MODIFY statements to PySpark write operations.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import col, current_timestamp, lit
from delta import DeltaTable
from typing import Dict, Tuple, Optional
import logging
from datetime import datetime
import yaml


class DeltaLakeLoader:
    """
    Loads transformed analytics data into Delta Lake tables.
    Maps ZSALES_ANALYTICS ABAP structure to Delta schema.
    """
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize loader with Spark session and configuration.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Extract configuration
        self.target_path = config['loader']['target_path']
        self.batch_size = config['loader']['batch_size']
        self.write_mode = config['loader']['write_mode']
        self.enable_validation = config['loader']['enable_validation']
        self.partition_columns = config['loader']['partition_columns']
        self.checkpoint_location = config['loader']['checkpoint_location']
        
    def get_analytics_schema(self) -> StructType:
        """
        Define schema for ZSALES_ANALYTICS table mapping.
        Maps ABAP types to Spark types.
        
        Returns:
            StructType schema definition
        """
        return StructType([
            StructField("analytics_id", StringType(), False),      # CHAR20
            StructField("trans_date", DateType(), False),          # DATS
            StructField("customer_id", StringType(), False),       # CHAR10
            StructField("product_id", StringType(), False),        # CHAR10
            StructField("total_quantity", IntegerType(), False),   # INT
            StructField("gross_amount", DecimalType(16, 2), False),    # P LENGTH 16 DECIMALS 2
            StructField("net_amount", DecimalType(16, 2), False),      # P LENGTH 16 DECIMALS 2
            StructField("discount_amount", DecimalType(16, 2), False), # P LENGTH 16 DECIMALS 2
            StructField("tax_amount", DecimalType(16, 2), False),      # P LENGTH 16 DECIMALS 2
            StructField("currency", StringType(), False),              # WAERS
            StructField("sales_rep", StringType(), True),              # CHAR20
            StructField("region", StringType(), True),                 # CHAR10
            StructField("profit_margin", DecimalType(5, 2), True),     # P LENGTH 5 DECIMALS 2
            StructField("category", StringType(), False),              # CHAR10
            StructField("etl_run_id", StringType(), False),           # CHAR20
            StructField("loaded_at", TimestampType(), False),         # TIMESTAMPL
            StructField("loaded_by", StringType(), False),            # SYUNAME
        ])
    
    def validate_records(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Validate records before loading (maps to ABAP validate_record method).
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        self.logger.info("Starting record validation")
        
        # Validation rules from ABAP code
        valid_df = df.filter(
            (col("analytics_id").isNotNull()) &
            (col("customer_id").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("gross_amount") > 0) &
            (col("currency").isNotNull()) &
            (col("category").isin("HIGH", "MEDIUM", "LOW"))
        )
        
        invalid_df = df.subtract(valid_df)
        
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        
        self.logger.info(f"Validation complete: {valid_count} valid, {invalid_count} invalid")
        
        return valid_df, invalid_df
    
    def load_data(
        self, 
        analytics_df: DataFrame,
        etl_run_id: str
    ) -> Dict[str, any]:
        """
        Load analytics data to Delta Lake (maps to ABAP load_data method).
        Implements INSERT/MODIFY operations with batch configuration.
        
        Args:
            analytics_df: Transformed analytics DataFrame
            etl_run_id: ETL run identifier
            
        Returns:
            Dictionary with load statistics
        """
        try:
            self.logger.info(f"Starting data load for ETL run: {etl_run_id}")
            
            total_records = analytics_df.count()
            self.logger.info(f"Total records to load: {total_records}")
            
            # Add metadata columns (maps to ABAP loaded_at, loaded_by)
            df_with_metadata = analytics_df \
                .withColumn("loaded_at", current_timestamp()) \
                .withColumn("loaded_by", lit("PYSPARK_ETL"))
            
            # Validate if enabled
            if self.enable_validation:
                valid_df, invalid_df = self.validate_records(df_with_metadata)
                
                # Log invalid records
                if invalid_df.count() > 0:
                    self.logger.warning(f"Found {invalid_df.count()} invalid records")
                    invalid_path = f"{self.config['loader']['error_path']}/invalid_records_{etl_run_id}"
                    invalid_df.write.mode("overwrite").parquet(invalid_path)
                    self.logger.info(f"Invalid records saved to: {invalid_path}")
            else:
                valid_df = df_with_metadata
            
            success_count = valid_df.count()
            
            # Write to Delta Lake with batch configuration
            if success_count > 0:
                self._write_to_delta(valid_df, etl_run_id)
            
            error_count = total_records - success_count
            
            # Build statistics
            stats = {
                "success": True,
                "total_records": total_records,
                "success_records": success_count,
                "error_records": error_count,
                "target_path": self.target_path,
                "etl_run_id": etl_run_id,
                "timestamp": datetime.now().isoformat()
            }
            
            self.logger.info(
                f"Load complete: {success_count}/{total_records} records loaded successfully"
            )
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}", exc_info=True)
            raise LoadException(f"Failed to load data: {str(e)}")
    
    def _write_to_delta(self, df: DataFrame, etl_run_id: str):
        """
        Write DataFrame to Delta Lake with optimized settings.
        Maps ABAP INSERT/MODIFY to Delta merge/append operations.
        
        Args:
            df: DataFrame to write
            etl_run_id: ETL run identifier
        """
        try:
            # Check if Delta table exists
            if DeltaTable.isDeltaTable(self.spark, self.target_path):
                self._merge_data(df, etl_run_id)
            else:
                self._create_initial_table(df)
                
        except Exception as e:
            self.logger.error(f"Delta write failed: {str(e)}", exc_info=True)
            raise
    
    def _create_initial_table(self, df: DataFrame):
        """
        Create initial Delta table with partitioning.
        
        Args:
            df: Initial DataFrame
        """
        self.logger.info(f"Creating new Delta table at: {self.target_path}")
        
        writer = df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true")
        
        # Add partitioning if configured
        if self.partition_columns:
            writer = writer.partitionBy(*self.partition_columns)
        
        writer.save(self.target_path)
        
        self.logger.info("Delta table created successfully")
    
    def _merge_data(self, df: DataFrame, etl_run_id: str):
        """
        Merge data into existing Delta table (maps to ABAP MODIFY).
        Uses MERGE operation for upsert functionality.
        
        Args:
            df: DataFrame to merge
            etl_run_id: ETL run identifier
        """
        self.logger.info("Merging data into existing Delta table")
        
        delta_table = DeltaTable.forPath(self.spark, self.target_path)
        
        # MERGE operation (equivalent to ABAP MODIFY)
        # Match on analytics_id (primary key)
        merge_condition = "target.analytics_id = source.analytics_id"
        
        if self.write_mode == "merge":
            delta_table.alias("target").merge(
                df.alias("source"),
                merge_condition
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            
            self.logger.info("Merge operation completed")
        else:
            # Append mode (equivalent to ABAP INSERT)
            df.write \
                .format("delta") \
                .mode("append") \
                .save(self.target_path)
            
            self.logger.info("Append operation completed")
    
    def optimize_table(self):
        """
        Optimize Delta table (compaction and Z-ordering).
        Should be run periodically for better query performance.
        """
        try:
            self.logger.info("Starting table optimization")
            
            delta_table = DeltaTable.forPath(self.spark, self.target_path)
            
            # Compact small files
            delta_table.optimize().executeCompaction()
            
            # Z-order by frequently queried columns
            if self.config['loader'].get('z_order_columns'):
                z_order_cols = self.config['loader']['z_order_columns']
                delta_table.optimize().executeZOrderBy(*z_order_cols)
            
            self.logger.info("Table optimization completed")
            
        except Exception as e:
            self.logger.warning(f"Optimization failed: {str(e)}")
    
    def vacuum_table(self, retention_hours: int = 168):
        """
        Clean up old versions of Delta table.
        
        Args:
            retention_hours: Hours to retain (default 7 days)
        """
        try:
            self.logger.info(f"Starting vacuum with {retention_hours}h retention")
            
            delta_table = DeltaTable.forPath(self.spark, self.target_path)
            delta_table.vacuum(retention_hours)
            
            self.logger.info("Vacuum completed")
            
        except Exception as e:
            self.logger.warning(f"Vacuum failed: {str(e)}")
    
    def get_table_stats(self) -> Dict:
        """
        Get statistics about the Delta table.
        
        Returns:
            Dictionary with table statistics
        """
        try:
            delta_table = DeltaTable.forPath(self.spark, self.target_path)
            
            # Get latest version
            history_df = delta_table.history(1)
            
            # Count records
            df = self.spark.read.format("delta").load(self.target_path)
            record_count = df.count()
            
            stats = {
                "record_count": record_count,
                "table_path": self.target_path,
                "latest_version": history_df.select("version").first()[0] if history_df.count() > 0 else 0
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to get table stats: {str(e)}")
            return {}


class LoadException(Exception):
    """Custom exception for load errors."""
    pass


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
        .appName(config['spark']['app_name']) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    return DeltaLakeLoader(spark, config)


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    loader = create_loader()
    print(f"Loader initialized with target: {loader.target_path}")