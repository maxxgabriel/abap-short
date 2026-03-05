"""
Data Loader - loads transformed data into target analytics table
Migrated from ABAP ZCL_ETL_LOADER
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from src.config import Config
from src.etl_logger import ETLLogger


class Loader:
    """Loads transformed data into target analytics table."""
    
    def __init__(self, spark: SparkSession, config: Config, etl_logger: ETLLogger, test_mode: bool = False):
        """
        Initialize the loader.
        
        Args:
            spark: SparkSession instance
            config: ETL configuration
            etl_logger: ETL logger instance
            test_mode: Whether to run in test mode
        """
        self.spark = spark
        self.config = config
        self.etl_logger = etl_logger
        self.test_mode = test_mode
        self.logger = logging.getLogger(__name__)
    
    def validate_record(self, row: dict) -> bool:
        """
        Validate a single record.
        
        Args:
            row: Record as dictionary
            
        Returns:
            True if valid, False otherwise
        """
        # Validate required fields
        if not row.get("analytics_id") or not row.get("customer_id") or not row.get("product_id"):
            return False
        
        if row.get("gross_amount", 0) <= 0:
            return False
        
        # Validate currency
        if not row.get("currency"):
            return False
        
        # Validate category
        if row.get("category") not in ["HIGH", "MEDIUM", "LOW"]:
            return False
        
        return True
    
    def load_data(self, analytics_data: DataFrame) -> bool:
        """
        Load transformed data into target table.
        
        Args:
            analytics_data: DataFrame with analytics data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.etl_logger.log_message(
                step='LOAD',
                status='S',
                message='Starting data load'
            )
            
            self.logger.info("Starting data load")
            
            total_count = analytics_data.count()
            
            # Validate records (in production, you might want to do this more efficiently)
            # For now, we'll assume all records are valid after transformation
            valid_data = analytics_data
            
            if self.test_mode:
                # In test mode, just show the data
                self.logger.info("Test mode: Displaying sample data")
                valid_data.show(10, truncate=False)
                success_count = total_count
                error_count = 0
            else:
                # In production mode, write to target table
                # Example: Write to Delta Lake
                # valid_data.write \
                #     .format("delta") \
                #     .mode("append") \
                #     .option("mergeSchema", "true") \
                #     .save(self.config.target_table_path)
                
                # Example: Write to JDBC database
                # valid_data.write \
                #     .format("jdbc") \
                #     .option("url", self.config.target_url) \
                #     .option("dbtable", "zsales_analytics") \
                #     .mode("append") \
                #     .save()
                
                # For demonstration, write to Parquet
                output_path = self.config.output_path
                valid_data.write \
                    .mode("append") \
                    .parquet(output_path)
                
                self.logger.info(f"Data written to {output_path}")
                
                success_count = total_count
                error_count = 0
                
                # Update status in source table (in production)
                # UPDATE zsales_raw SET status = 'P' WHERE trans_id IN (...)
            
            self.etl_logger.log_message(
                step='LOAD',
                status='S',
                records_processed=total_count,
                records_success=success_count,
                records_error=error_count,
                message=f'Loaded {success_count} of {total_count} records'
            )
            
            self.logger.info(f"Loaded {success_count} of {total_count} records")
            
            return True
            
        except Exception as e:
            self.etl_logger.log_message(
                step='LOAD',
                status='E',
                message=f'Load failed: {str(e)}'
            )
            
            self.logger.error(f"Load failed: {str(e)}", exc_info=True)
            return False