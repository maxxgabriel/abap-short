"""
Load module for Sales ETL Pipeline
Loads transformed analytics data into target systems
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import logging
from typing import Optional


class SalesLoader:
    """Handles loading of transformed analytics data into target systems"""
    
    def __init__(self, spark: SparkSession, config: dict, logger: logging.Logger):
        """
        Initialize the loader
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: Logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
    
    def load_data(
        self, 
        analytics_df: DataFrame,
        target_path: Optional[str] = None,
        mode: str = "append"
    ) -> bool:
        """
        Load analytics data to target system
        
        Args:
            analytics_df: Transformed analytics DataFrame
            target_path: Optional override for target path
            mode: Write mode (append, overwrite, etc.)
            
        Returns:
            bool: True if load successful
        """
        try:
            self.logger.info("Starting data load")
            
            record_count = analytics_df.count()
            
            # Validate before loading
            if not self._validate_before_load(analytics_df):
                self.logger.error("Pre-load validation failed")
                return False
            
            # Get target configuration
            target = target_path or self.config['load']['target_path']
            target_format = self.config['load']['target_format']
            
            # Write data based on format
            if target_format == 'parquet':
                self._write_parquet(analytics_df, target, mode)
            elif target_format == 'delta':
                self._write_delta(analytics_df, target, mode)
            elif target_format == 'jdbc':
                self._write_jdbc(analytics_df)
            else:
                raise ValueError(f"Unsupported target format: {target_format}")
            
            self.logger.info(f"Loaded {record_count} records successfully")
            
            # Update source status if configured
            if self.config['load'].get('update_source_status', False):
                self._update_source_status(analytics_df)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Load failed: {str(e)}")
            return False
    
    def _write_parquet(self, df: DataFrame, target: str, mode: str):
        """Write data in Parquet format"""
        partition_columns = self.config['load'].get('partition_by', [])
        
        if partition_columns:
            df.write.partitionBy(*partition_columns).mode(mode).parquet(target)
        else:
            df.write.mode(mode).parquet(target)
        
        self.logger.info(f"Data written to Parquet: {target}")
    
    def _write_delta(self, df: DataFrame, target: str, mode: str):
        """Write data in Delta format"""
        partition_columns = self.config['load'].get('partition_by', [])
        
        writer = df.write.format("delta").mode(mode)
        
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        
        writer.save(target)
        
        self.logger.info(f"Data written to Delta: {target}")
    
    def _write_jdbc(self, df: DataFrame):
        """Write data to JDBC target"""
        jdbc_config = self.config['load']['jdbc']
        
        df.write.jdbc(
            url=jdbc_config['url'],
            table=jdbc_config['table'],
            mode=jdbc_config.get('mode', 'append'),
            properties={
                "user": jdbc_config['user'],
                "password": jdbc_config['password'],
                "driver": jdbc_config['driver'],
                "batchsize": str(jdbc_config.get('batch_size', 1000))
            }
        )
        
        self.logger.info(f"Data written to JDBC: {jdbc_config['table']}")
    
    def _validate_before_load(self, df: DataFrame) -> bool:
        """
        Validate data before loading
        
        Args:
            df: DataFrame to validate
            
        Returns:
            bool: True if validation passes
        """
        try:
            # Check for duplicates
            total_count = df.count()
            distinct_count = df.select("analytics_id").distinct().count()
            
            if total_count != distinct_count:
                self.logger.error(f"Found duplicate analytics_id values")
                return False
            
            # Check for required columns
            required_columns = [
                'analytics_id', 'customer_id', 'product_id', 
                'gross_amount', 'net_amount', 'category'
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                self.logger.error(f"Missing required columns: {missing_columns}")
                return False
            
            # Check for null values in critical columns
            for column in required_columns:
                null_count = df.filter(col(column).isNull()).count()
                if null_count > 0:
                    self.logger.error(f"Found {null_count} null values in {column}")
                    return False
            
            self.logger.info("Pre-load validation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Pre-load validation error: {str(e)}")
            return False
    
    def _update_source_status(self, analytics_df: DataFrame):
        """
        Update source table status to mark records as processed
        
        Args:
            analytics_df: Loaded analytics data
        """
        try:
            if self.config['extract']['source_format'] == 'jdbc':
                jdbc_config = self.config['extract']['jdbc']
                
                # Get list of processed transaction IDs
                # In production, this would execute an UPDATE statement
                self.logger.info("Source status update would be executed here")
                
        except Exception as e:
            self.logger.warning(f"Failed to update source status: {str(e)}")
    
    def create_summary_report(self, analytics_df: DataFrame) -> dict:
        """
        Create summary statistics report
        
        Args:
            analytics_df: Loaded analytics data
            
        Returns:
            dict: Summary statistics
        """
        try:
            summary = {
                'total_records': analytics_df.count(),
                'total_gross_amount': analytics_df.agg({'gross_amount': 'sum'}).collect()[0][0],
                'total_net_amount': analytics_df.agg({'net_amount': 'sum'}).collect()[0][0],
                'total_discount': analytics_df.agg({'discount_amount': 'sum'}).collect()[0][0],
                'total_tax': analytics_df.agg({'tax_amount': 'sum'}).collect()[0][0],
                'by_category': analytics_df.groupBy('category').count().collect(),
                'by_region': analytics_df.groupBy('region').count().collect()
            }
            
            self.logger.info("Summary report created")
            return summary
            
        except Exception as e:
            self.logger.error(f"Failed to create summary report: {str(e)}")
            return {}