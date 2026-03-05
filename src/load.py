"""
Data loading module for Sales ETL Pipeline.
Writes transformed analytics data to target destination.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import logging

logger = logging.getLogger(__name__)


class SalesDataLoader:
    """Loads transformed analytics data to target system."""
    
    def __init__(self, config: dict):
        """
        Initialize the loader.
        
        Args:
            config: Configuration dictionary with target settings
        """
        self.config = config
        self.target_path = config.get('target_path')
        self.target_format = config.get('target_format', 'parquet')
        self.write_mode = config.get('write_mode', 'append')
        self.partition_by = config.get('partition_by', ['trans_date'])
        
    def validate_analytics_data(self, df: DataFrame) -> tuple[bool, int]:
        """
        Validate analytics data before loading.
        
        Args:
            df: Analytics DataFrame to validate
            
        Returns:
            Tuple of (is_valid, invalid_count)
        """
        try:
            logger.info("Validating analytics data")
            
            # Check for required fields
            required_fields = [
                "analytics_id", "customer_id", "product_id", 
                "gross_amount", "net_amount", "currency", "category"
            ]
            
            missing_fields = [field for field in required_fields if field not in df.columns]
            if missing_fields:
                logger.error(f"Missing required fields: {missing_fields}")
                return False, df.count()
            
            # Check for invalid records
            invalid_records = df.filter(
                col("analytics_id").isNull() |
                col("customer_id").isNull() |
                col("product_id").isNull() |
                (col("gross_amount") <= 0) |
                col("currency").isNull() |
                (~col("category").isin(["HIGH", "MEDIUM", "LOW"]))
            )
            
            invalid_count = invalid_records.count()
            
            if invalid_count > 0:
                logger.warning(f"Found {invalid_count} invalid records")
                # Log sample of invalid records
                invalid_records.show(5, truncate=False)
                return False, invalid_count
            
            logger.info("Validation passed successfully")
            return True, 0
            
        except Exception as e:
            logger.error(f"Validation error: {str(e)}", exc_info=True)
            return False, -1
    
    def load(self, df: DataFrame) -> bool:
        """
        Load analytics data to target destination.
        
        Args:
            df: Analytics DataFrame to load
            
        Returns:
            True if load successful, False otherwise
        """
        try:
            logger.info(f"Starting data load to {self.target_path}")
            
            # Validate before loading
            is_valid, invalid_count = self.validate_analytics_data(df)
            if not is_valid:
                logger.error(f"Validation failed with {invalid_count} invalid records")
                return False
            
            record_count = df.count()
            logger.info(f"Loading {record_count} records")
            
            # Write data with partitioning
            df.write \
                .mode(self.write_mode) \
                .format(self.target_format) \
                .partitionBy(*self.partition_by) \
                .save(self.target_path)
            
            logger.info(f"Successfully loaded {record_count} records to {self.target_path}")
            return True
            
        except Exception as e:
            logger.error(f"Load failed: {str(e)}", exc_info=True)
            return False
    
    def load_with_validation_report(self, df: DataFrame) -> dict:
        """
        Load data and return detailed validation report.
        
        Args:
            df: Analytics DataFrame to load
            
        Returns:
            Dictionary with load results and statistics
        """
        try:
            total_records = df.count()
            is_valid, invalid_count = self.validate_analytics_data(df)
            
            result = {
                "success": False,
                "total_records": total_records,
                "valid_records": total_records - invalid_count if is_valid else 0,
                "invalid_records": invalid_count,
                "target_path": self.target_path
            }
            
            if is_valid:
                load_success = self.load(df)
                result["success"] = load_success
                
                if load_success:
                    logger.info(f"Load completed: {result}")
            else:
                logger.error("Load skipped due to validation failures")
            
            return result
            
        except Exception as e:
            logger.error(f"Load with validation failed: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }


def update_source_status(spark, source_path: str, trans_ids: list, new_status: str = 'P'):
    """
    Update status of processed records in source table.
    
    Args:
        spark: Active SparkSession
        source_path: Path to source data
        trans_ids: List of transaction IDs to update
        new_status: New status value (default 'P' for processed)
    """
    try:
        logger.info(f"Updating status for {len(trans_ids)} records")
        
        # This is a simplified implementation
        # In production, this would update the actual source table/database
        df_source = spark.read.parquet(source_path)
        
        df_updated = df_source.withColumn(
            "status",
            when(col("trans_id").isin(trans_ids), lit(new_status))
            .otherwise(col("status"))
        )
        
        df_updated.write.mode("overwrite").parquet(source_path)
        logger.info("Source status updated successfully")
        
    except Exception as e:
        logger.error(f"Failed to update source status: {str(e)}", exc_info=True)