"""
Delta Lake Loader Module
Handles loading transformed data into Delta Lake with merge-based upserts
and partition pruning optimization.
"""

from typing import Dict, List, Optional, Tuple
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from delta import DeltaTable
import logging
import yaml


class DeltaLakeLoader:
    """Loads analytics data into Delta Lake with upsert capabilities."""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize Delta Lake loader.
        
        Args:
            config_path: Path to configuration YAML file
        """
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
        self.spark = self._create_spark_session()
        
        # Extract configuration
        self.target_path = self.config['target']['path']
        self.table_name = self.config['target']['table_name']
        self.database = self.config['target']['database']
        self.partition_columns = self.config['target']['partition_columns']
        self.merge_enabled = self.config['target']['merge']['enabled']
        self.merge_keys = self.config['target']['merge']['merge_keys']
        
        # Statistics
        self.stats = {
            'records_processed': 0,
            'records_inserted': 0,
            'records_updated': 0,
            'records_deleted': 0,
            'load_duration_seconds': 0
        }
    
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration."""
        log_config = self.config['logging']
        
        logging.basicConfig(
            level=getattr(logging, log_config['level']),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        logger = logging.getLogger(__name__)
        
        if log_config['console_output']:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(getattr(logging, log_config['level']))
            logger.addHandler(console_handler)
        
        return logger
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Delta Lake configuration."""
        spark_config = self.config['spark']
        
        builder = (SparkSession.builder
                   .appName(spark_config['app_name'])
                   .config("spark.sql.extensions", spark_config['sql_extensions'])
                   .config("spark.sql.catalog.spark_catalog", 
                          "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                   .config("spark.driver.memory", spark_config['driver_memory'])
                   .config("spark.executor.memory", spark_config['executor_memory'])
                   .config("spark.executor.cores", spark_config['executor_cores'])
                   .config("spark.sql.shuffle.partitions", spark_config['shuffle_partitions'])
                   .config("spark.sql.adaptive.enabled", spark_config['adaptive_enabled']))
        
        # Add Delta Lake configs
        for key, value in self.config['target']['properties'].items():
            builder = builder.config(f"spark.databricks.{key}", value)
        
        spark = builder.getOrCreate()
        
        self.logger.info(f"Spark session created: {spark.version}")
        return spark
    
    def get_analytics_schema(self) -> StructType:
        """
        Define schema for analytics data based on ABAP structure.
        
        Returns:
            StructType schema for analytics table
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
            StructField("region", StringType(), nullable=False),
            StructField("profit_margin", DecimalType(5, 2), nullable=True),
            StructField("category", StringType(), nullable=False),
            StructField("etl_run_id", StringType(), nullable=False),
            StructField("loaded_at", TimestampType(), nullable=False),
            StructField("loaded_by", StringType(), nullable=True),
            # Partition columns
            StructField("trans_year", IntegerType(), nullable=False),
            StructField("trans_month", IntegerType(), nullable=False)
        ])
    
    def prepare_data(self, df: DataFrame) -> DataFrame:
        """
        Prepare data for loading with partition columns and validation.
        
        Args:
            df: Input DataFrame with analytics data
            
        Returns:
            Prepared DataFrame with partition columns
        """
        self.logger.info("Preparing data for Delta Lake load")
        
        # Add partition columns if not present
        if 'trans_year' not in df.columns:
            df = df.withColumn('trans_year', F.year(F.col('trans_date')))
        
        if 'trans_month' not in df.columns:
            df = df.withColumn('trans_month', F.month(F.col('trans_date')))
        
        # Add metadata columns
        if 'loaded_at' not in df.columns:
            df = df.withColumn('loaded_at', F.current_timestamp())
        
        if 'loaded_by' not in df.columns:
            df = df.withColumn('loaded_by', F.lit('pyspark_etl'))
        
        # Validate data if enabled
        if self.config['etl']['validate_before_load']:
            df = self._validate_data(df)
        
        # Repartition before write if configured
        if self.config['etl']['repartition_before_write']:
            num_partitions = self.config['etl']['coalesce_partitions']
            df = df.repartition(num_partitions, *self.partition_columns)
        
        return df
    
    def _validate_data(self, df: DataFrame) -> DataFrame:
        """
        Validate data quality based on configuration.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Validated DataFrame
        """
        quality_checks = self.config['etl']['quality_checks']
        
        for check in quality_checks:
            if 'check_nulls' in check:
                # Check for null values in required columns
                for col in check['check_nulls']:
                    null_count = df.filter(F.col(col).isNull()).count()
                    if null_count > 0:
                        self.logger.warning(f"Found {null_count} null values in {col}")
                        df = df.filter(F.col(col).isNotNull())
            
            if 'check_positive' in check:
                # Check for positive values in amount columns
                for col in check['check_positive']:
                    negative_count = df.filter(F.col(col) < 0).count()
                    if negative_count > 0:
                        self.logger.warning(f"Found {negative_count} negative values in {col}")
                        df = df.filter(F.col(col) >= 0)
            
            if 'check_category_values' in check:
                # Validate category values
                valid_categories = check['check_category_values']
                invalid_count = df.filter(~F.col('category').isin(valid_categories)).count()
                if invalid_count > 0:
                    self.logger.warning(f"Found {invalid_count} invalid category values")
                    df = df.filter(F.col('category').isin(valid_categories))
        
        return df
    
    def table_exists(self) -> bool:
        """Check if Delta table exists."""
        try:
            DeltaTable.forPath(self.spark, self.target_path)
            return True
        except Exception:
            return False
    
    def create_table(self, df: DataFrame) -> None:
        """
        Create new Delta table with partitioning.
        
        Args:
            df: Initial DataFrame to create table from
        """
        self.logger.info(f"Creating new Delta table at {self.target_path}")
        
        # Write initial data with partitioning
        (df.write
         .format("delta")
         .mode("overwrite")
         .partitionBy(*self.partition_columns)
         .option("overwriteSchema", "true")
         .save(self.target_path))
        
        # Create managed table if database specified
        if self.database:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.database}.{self.table_name}
                USING DELTA
                LOCATION '{self.target_path}'
            """)
        
        self.logger.info("Delta table created successfully")
    
    def perform_merge(self, source_df: DataFrame, delta_table: DeltaTable) -> Dict:
        """
        Perform merge (upsert) operation on Delta table.
        
        Args:
            source_df: Source DataFrame with new/updated records
            delta_table: Target Delta table
            
        Returns:
            Dictionary with merge statistics
        """
        self.logger.info("Performing merge operation")
        
        # Build merge condition based on merge keys
        merge_condition = " AND ".join([
            f"target.{key} = source.{key}" for key in self.merge_keys
        ])
        
        # Get update condition from config
        update_condition = self.config['target']['merge'].get('update_condition', None)
        
        # Prepare update dict (all columns except keys)
        all_columns = source_df.columns
        update_dict = {col: f"source.{col}" for col in all_columns}
        
        # Execute merge
        merge_builder = (delta_table.alias("target")
                        .merge(source_df.alias("source"), merge_condition))
        
        # When matched - update
        if update_condition:
            merge_builder = merge_builder.whenMatchedUpdate(
                condition=update_condition,
                set=update_dict
            )
        else:
            merge_builder = merge_builder.whenMatchedUpdateAll()
        
        # When not matched - insert
        merge_builder = merge_builder.whenNotMatchedInsertAll()
        
        # Execute merge
        merge_builder.execute()
        
        # Get merge statistics
        history = delta_table.history(1).select("operationMetrics").collect()[0][0]
        
        stats = {
            'records_inserted': int(history.get('numTargetRowsInserted', 0)),
            'records_updated': int(history.get('numTargetRowsUpdated', 0)),
            'records_deleted': int(history.get('numTargetRowsDeleted', 0))
        }
        
        self.logger.info(f"Merge completed: {stats}")
        return stats
    
    def perform_append(self, df: DataFrame) -> None:
        """
        Perform append operation (no merge).
        
        Args:
            df: DataFrame to append
        """
        self.logger.info("Performing append operation")
        
        (df.write
         .format("delta")
         .mode("append")
         .partitionBy(*self.partition_columns)
         .save(self.target_path))
        
        self.logger.info("Append completed")
    
    def optimize_table(self) -> None:
        """Optimize Delta table with compaction and Z-ordering."""
        self.logger.info("Optimizing Delta table")
        
        delta_table = DeltaTable.forPath(self.spark, self.target_path)
        
        # Get Z-order columns from config
        zorder_columns = self.config['target'].get('zorder_columns', [])
        
        if zorder_columns:
            # Optimize with Z-ordering
            optimize_cmd = f"""
                OPTIMIZE delta.`{self.target_path}`
                ZORDER BY ({', '.join(zorder_columns)})
            """
            self.spark.sql(optimize_cmd)
            self.logger.info(f"Z-order optimization completed on columns: {zorder_columns}")
        else:
            # Basic optimization (compaction)
            delta_table.optimize().executeCompaction()
            self.logger.info("Compaction completed")
    
    def vacuum_table(self, retention_hours: Optional[int] = None) -> None:
        """
        Vacuum Delta table to remove old files.
        
        Args:
            retention_hours: Retention period in hours (uses config if not specified)
        """
        if retention_hours is None:
            retention_hours = self.config['spark']['delta']['vacuum_retention_hours']
        
        self.logger.info(f"Vacuuming Delta table (retention: {retention_hours} hours)")
        
        delta_table = DeltaTable.forPath(self.spark, self.target_path)
        delta_table.vacuum(retention_hours)
        
        self.logger.info("Vacuum completed")
    
    def load(self, source_df: DataFrame, mode: Optional[str] = None) -> Dict:
        """
        Main load method - handles all loading logic.
        
        Args:
            source_df: Source DataFrame to load
            mode: Load mode (merge, append, overwrite) - uses config if not specified
            
        Returns:
            Dictionary with load statistics
        """
        start_time = datetime.now()
        self.logger.info(f"Starting Delta Lake load at {start_time}")
        
        # Determine mode
        if mode is None:
            mode = self.config['etl']['write_mode']
        
        # Prepare data
        prepared_df = self.prepare_data(source_df)
        record_count = prepared_df.count()
        self.stats['records_processed'] = record_count
        
        self.logger.info(f"Loading {record_count} records in '{mode}' mode")
        
        try:
            # Check if table exists
            table_exists = self.table_exists()
            
            if not table_exists:
                # Create table
                self.create_table(prepared_df)
                self.stats['records_inserted'] = record_count
            
            elif mode == 'merge' and self.merge_enabled:
                # Perform merge
                delta_table = DeltaTable.forPath(self.spark, self.target_path)
                merge_stats = self.perform_merge(prepared_df, delta_table)
                self.stats.update(merge_stats)
            
            elif mode == 'append':
                # Perform append
                self.perform_append(prepared_df)
                self.stats['records_inserted'] = record_count
            
            elif mode == 'overwrite':
                # Overwrite table
                (prepared_df.write
                 .format("delta")
                 .mode("overwrite")
                 .partitionBy(*self.partition_columns)
                 .option("overwriteSchema", "true")
                 .save(self.target_path))
                self.stats['records_inserted'] = record_count
            
            else:
                raise ValueError(f"Unsupported load mode: {mode}")
            
            # Post-load optimization
            self.optimize_table()
            
            # Calculate duration
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            self.stats['load_duration_seconds'] = duration
            
            self.logger.info(f"Load completed in {duration:.2f} seconds")
            self.logger.info(f"Load statistics: {self.stats}")
            
            return self.stats
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}", exc_info=True)
            raise
    
    def get_table_info(self) -> Dict:
        """Get information about the Delta table."""
        if not self.table_exists():
            return {"error": "Table does not exist"}
        
        delta_table = DeltaTable.forPath(self.spark, self.target_path)
        
        # Get table details
        detail = delta_table.detail().collect()[0].asDict()
        
        # Get history
        history = delta_table.history(10).collect()
        
        # Get record count
        record_count = self.spark.read.format("delta").load(self.target_path).count()
        
        return {
            "location": detail['location'],
            "format": detail['format'],
            "partition_columns": detail['partitionColumns'],
            "num_files": detail['numFiles'],
            "size_in_bytes": detail['sizeInBytes'],
            "record_count": record_count,
            "recent_operations": [h.operation for h in history]
        }
    
    def read_table(self, 
                   partition_filter: Optional[Dict] = None,
                   columns: Optional[List[str]] = None) -> DataFrame:
        """
        Read data from Delta table with optional partition pruning.
        
        Args:
            partition_filter: Dictionary of partition column filters
                             e.g., {'region': 'NORTH', 'trans_year': 2024}
            columns: List of columns to select (None = all columns)
            
        Returns:
            DataFrame with filtered data
        """
        self.logger.info("Reading from Delta table")
        
        df = self.spark.read.format("delta").load(self.target_path)
        
        # Apply partition filters for pruning
        if partition_filter:
            for col, value in partition_filter.items():
                df = df.filter(F.col(col) == value)
                self.logger.info(f"Applied partition filter: {col} = {value}")
        
        # Select specific columns
        if columns:
            df = df.select(*columns)
        
        return df
    
    def close(self):
        """Clean up resources."""
        if self.spark:
            self.spark.stop()
            self.logger.info("Spark session stopped")


def main():
    """Main execution function for testing."""
    from src.extract import DataExtractor
    from src.transform import DataTransformer
    
    # Initialize components
    extractor = DataExtractor()
    transformer = DataTransformer()
    loader = DeltaLakeLoader()
    
    try:
        # Extract
        raw_df = extractor.extract_sales_data(
            from_date='2024-01-01',
            to_date='2024-01-31'
        )
        
        # Transform
        analytics_df = transformer.transform(raw_df)
        
        # Load
        stats = loader.load(analytics_df, mode='merge')
        
        print("\n=== Load Statistics ===")
        for key, value in stats.items():
            print(f"{key}: {value}")
        
        # Display table info
        print("\n=== Table Information ===")
        info = loader.get_table_info()
        for key, value in info.items():
            print(f"{key}: {value}")
        
    finally:
        loader.close()
        extractor.close()
        transformer.close()


if __name__ == "__main__":
    main()