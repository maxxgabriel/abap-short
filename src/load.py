"""
Data loading module for Sales ETL pipeline.
Loads transformed data into target analytics table.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import logging

logger = logging.getLogger(__name__)


class SalesLoader:
    """Loads transformed analytics data into target table."""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize loader with Spark session and configuration.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logger
    
    def load_data(
        self, 
        analytics_df: DataFrame,
        target_table: str = None,
        write_mode: str = None
    ) -> dict:
        """
        Load analytics data into target table.
        
        Args:
            analytics_df: DataFrame containing transformed analytics data
            target_table: Optional target table name override
            write_mode: Write mode (append, overwrite, etc.)
            
        Returns:
            Dictionary containing load statistics
        """
        table_name = target_table or self.config['target']['table_name']
        mode = write_mode or self.config['target']['write_mode']
        
        self.logger.info(f"Starting data load to table {table_name} with mode {mode}")
        
        try:
            # Get initial count
            initial_count = analytics_df.count()
            
            # Write to target table
            analytics_df.write.mode(mode).saveAsTable(table_name)
            
            self.logger.info(f"Successfully loaded {initial_count} records to {table_name}")
            
            # Return statistics
            return {
                "success": True,
                "records_loaded": initial_count,
                "target_table": table_name,
                "write_mode": mode
            }
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            return {
                "success": False,
                "records_loaded": 0,
                "error": str(e)
            }
    
    def load_with_partitioning(
        self,
        analytics_df: DataFrame,
        partition_columns: list,
        target_table: str = None
    ) -> dict:
        """
        Load data with partitioning for better query performance.
        
        Args:
            analytics_df: DataFrame containing analytics data
            partition_columns: List of columns to partition by
            target_table: Optional target table name
            
        Returns:
            Dictionary containing load statistics
        """
        table_name = target_table or self.config['target']['table_name']
        
        self.logger.info(f"Loading data with partitioning on {partition_columns}")
        
        try:
            initial_count = analytics_df.count()
            
            # Write with partitioning
            analytics_df.write \
                .mode("append") \
                .partitionBy(*partition_columns) \
                .saveAsTable(table_name)
            
            self.logger.info(f"Successfully loaded {initial_count} records with partitioning")
            
            return {
                "success": True,
                "records_loaded": initial_count,
                "target_table": table_name,
                "partitions": partition_columns
            }
            
        except Exception as e:
            self.logger.error(f"Partitioned load failed: {str(e)}")
            return {
                "success": False,
                "records_loaded": 0,
                "error": str(e)
            }
    
    def update_source_status(
        self,
        processed_ids: list,
        source_table: str = None
    ) -> bool:
        """
        Update status of processed records in source table.
        
        Args:
            processed_ids: List of transaction IDs that were processed
            source_table: Optional source table name
            
        Returns:
            True if update successful, False otherwise
        """
        table_name = source_table or self.config['source']['table_name']
        
        self.logger.info(f"Updating status for {len(processed_ids)} records in {table_name}")
        
        try:
            # Read source table
            source_df = self.spark.read.table(table_name)
            
            # Update status to 'P' (Processed) for matching IDs
            updated_df = source_df.withColumn(
                "status",
                when(col("trans_id").isin(processed_ids), lit("P"))
                .otherwise(col("status"))
            )
            
            # Write back (overwrite mode for status update)
            updated_df.write.mode("overwrite").saveAsTable(table_name)
            
            self.logger.info("Source table status updated successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Status update failed: {str(e)}")
            return False
    
    def validate_load(self, target_table: str, etl_run_id: str) -> bool:
        """
        Validate that data was loaded correctly.
        
        Args:
            target_table: Target table name
            etl_run_id: ETL run ID to validate
            
        Returns:
            True if validation passes, False otherwise
        """
        self.logger.info(f"Validating load for ETL run {etl_run_id}")
        
        try:
            # Read loaded data
            loaded_df = self.spark.read.table(target_table)
            
            # Filter by ETL run ID
            run_df = loaded_df.filter(col("etl_run_id") == etl_run_id)
            
            # Check record count
            record_count = run_df.count()
            
            if record_count == 0:
                self.logger.error(f"No records found for ETL run {etl_run_id}")
                return False
            
            # Check for duplicates
            distinct_count = run_df.select("analytics_id").distinct().count()
            
            if distinct_count != record_count:
                self.logger.error(f"Duplicate analytics_id detected: {record_count - distinct_count} duplicates")
                return False
            
            self.logger.info(f"Load validation passed: {record_count} records")
            return True
            
        except Exception as e:
            self.logger.error(f"Load validation failed: {str(e)}")
            return False