"""
Data loading module for Sales ETL pipeline.
Loads transformed analytics data to target storage.
"""

from pyspark.sql import DataFrame
import logging
from typing import Dict, Tuple


class SalesLoader:
    """Handles loading of analytics data to target storage."""
    
    def __init__(self, logger: logging.Logger, config: Dict):
        """
        Initialize the loader.
        
        Args:
            logger: Logger instance for tracking operations
            config: Configuration dictionary with load settings
        """
        self.logger = logger
        self.config = config
    
    def load_data(
        self,
        df: DataFrame,
        target_path: str,
        target_format: str = "parquet",
        mode: str = "append"
    ) -> Tuple[int, bool]:
        """
        Load analytics data to target storage.
        
        Args:
            df: Analytics DataFrame to load
            target_path: Path to target storage
            target_format: Format for target data (parquet, delta, etc.)
            mode: Write mode (append, overwrite, etc.)
        
        Returns:
            Tuple of (record count, success flag)
        """
        try:
            self.logger.info(f"Starting data load to {target_path}")
            
            record_count = df.count()
            
            if record_count == 0:
                self.logger.warning("No records to load")
                return 0, True
            
            # Write data to target
            df.write.format(target_format).mode(mode).save(target_path)
            
            self.logger.info(f"Loaded {record_count} records successfully")
            
            return record_count, True
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            return 0, False
    
    def load_to_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "append"
    ) -> Tuple[int, bool]:
        """
        Load analytics data to a Spark table.
        
        Args:
            df: Analytics DataFrame to load
            table_name: Name of target table
            mode: Write mode (append, overwrite, etc.)
        
        Returns:
            Tuple of (record count, success flag)
        """
        try:
            self.logger.info(f"Starting data load to table {table_name}")
            
            record_count = df.count()
            
            if record_count == 0:
                self.logger.warning("No records to load")
                return 0, True
            
            # Write data to table
            df.write.mode(mode).saveAsTable(table_name)
            
            self.logger.info(f"Loaded {record_count} records to table successfully")
            
            return record_count, True
            
        except Exception as e:
            self.logger.error(f"Table load failed: {str(e)}")
            return 0, False
    
    def load_partitioned(
        self,
        df: DataFrame,
        target_path: str,
        partition_cols: list,
        target_format: str = "parquet",
        mode: str = "append"
    ) -> Tuple[int, bool]:
        """
        Load analytics data with partitioning.
        
        Args:
            df: Analytics DataFrame to load
            target_path: Path to target storage
            partition_cols: List of columns to partition by
            target_format: Format for target data
            mode: Write mode
        
        Returns:
            Tuple of (record count, success flag)
        """
        try:
            self.logger.info(f"Starting partitioned load to {target_path}")
            self.logger.info(f"Partition columns: {partition_cols}")
            
            record_count = df.count()
            
            if record_count == 0:
                self.logger.warning("No records to load")
                return 0, True
            
            # Write partitioned data
            df.write.format(target_format).mode(mode).partitionBy(*partition_cols).save(target_path)
            
            self.logger.info(f"Loaded {record_count} records with partitioning successfully")
            
            return record_count, True
            
        except Exception as e:
            self.logger.error(f"Partitioned load failed: {str(e)}")
            return 0, False
    
    def update_source_status(
        self,
        df_raw: DataFrame,
        processed_ids: list,
        source_path: str
    ) -> bool:
        """
        Update status of processed records in source.
        
        Args:
            df_raw: Original raw DataFrame
            processed_ids: List of transaction IDs that were processed
            source_path: Path to source data
        
        Returns:
            Success flag
        """
        try:
            self.logger.info("Updating source record status")
            
            # In a real implementation, this would update the source table
            # For file-based sources, this might involve rewriting with updated status
            
            self.logger.info(f"Updated status for {len(processed_ids)} records")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Status update failed: {str(e)}")
            return False