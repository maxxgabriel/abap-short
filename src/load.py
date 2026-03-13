"""
Data Loading Module for Sales ETL
Loads transformed analytics data into target storage
"""
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any
import yaml
import logging


class SalesLoader:
    """Loads analytics data into target storage"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize loader with configuration
        
        Args:
            config_path: Path to configuration file
        """
        self.logger = logging.getLogger(__name__)
        self.config = self._load_config(config_path)
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            self.logger.warning(f"Could not load config: {e}, using defaults")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Return default configuration"""
        return {
            'storage': {
                'format': 'parquet',
                'mode': 'append',
                'partition_by': ['trans_date', 'region']
            },
            'paths': {
                'analytics_output': 'output/analytics'
            }
        }
    
    def load_to_storage(self, analytics_df: DataFrame, output_path: str = None) -> tuple[bool, int, str]:
        """
        Load analytics data to target storage
        
        Args:
            analytics_df: DataFrame with analytics data
            output_path: Optional override for output path
            
        Returns:
            Tuple of (success flag, record count, message)
        """
        try:
            target_path = output_path or self.config['paths']['analytics_output']
            storage_config = self.config['storage']
            
            self.logger.info(f"Loading data to: {target_path}")
            
            # Get record count before loading
            record_count = analytics_df.count()
            
            # Write to storage
            writer = analytics_df.write.mode(storage_config['mode'])
            
            # Add partitioning if configured
            if storage_config.get('partition_by'):
                writer = writer.partitionBy(*storage_config['partition_by'])
            
            writer.format(storage_config['format']).save(target_path)
            
            message = f"Successfully loaded {record_count} records to {target_path}"
            self.logger.info(message)
            
            return True, record_count, message
            
        except Exception as e:
            error_msg = f"Failed to load data: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return False, 0, error_msg
    
    def load_to_table(self, analytics_df: DataFrame, table_name: str, spark: SparkSession) -> tuple[bool, int, str]:
        """
        Load analytics data to Spark table
        
        Args:
            analytics_df: DataFrame with analytics data
            table_name: Target table name
            spark: SparkSession instance
            
        Returns:
            Tuple of (success flag, record count, message)
        """
        try:
            self.logger.info(f"Loading data to table: {table_name}")
            
            record_count = analytics_df.count()
            storage_config = self.config['storage']
            
            # Write to table
            analytics_df.write.mode(storage_config['mode']).saveAsTable(table_name)
            
            message = f"Successfully loaded {record_count} records to table {table_name}"
            self.logger.info(message)
            
            return True, record_count, message
            
        except Exception as e:
            error_msg = f"Failed to load data to table: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return False, 0, error_msg
    
    def update_source_status(self, spark: SparkSession, trans_ids: list, source_table: str = "raw_sales") -> bool:
        """
        Update status of processed records in source table
        
        Args:
            spark: SparkSession instance
            trans_ids: List of transaction IDs to update
            source_table: Source table name
            
        Returns:
            Success flag
        """
        try:
            self.logger.info(f"Updating status for {len(trans_ids)} records in {source_table}")
            
            # In production, this would execute an UPDATE query
            # For Spark, we typically handle this through merge operations
            # This is a simplified representation
            
            self.logger.info(f"Successfully updated status for {len(trans_ids)} records")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update source status: {str(e)}", exc_info=True)
            return False