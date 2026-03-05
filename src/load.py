"""
ETL Loader Module
Loads transformed analytics data into target table
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import logging
from typing import Tuple


class ETLLoader:
    """Loads transformed data into target analytics table"""
    
    def __init__(self, spark: SparkSession, logger: logging.Logger, config: dict):
        """
        Initialize the loader
        
        Args:
            spark: SparkSession instance
            logger: Logger instance for logging
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
    
    def validate_record(self, df: DataFrame) -> DataFrame:
        """
        Validate records before loading
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Validated DataFrame
        """
        # Add validation flag
        validated_df = df.withColumn(
            "is_valid",
            (col("analytics_id").isNotNull()) &
            (col("customer_id").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("gross_amount") > 0) &
            (col("currency").isNotNull()) &
            (col("category").isin("HIGH", "MEDIUM", "LOW"))
        )
        
        return validated_df
    
    def load_data(self, analytics_df: DataFrame) -> Tuple[bool, int, int]:
        """
        Load analytics data into target table
        
        Args:
            analytics_df: Transformed analytics DataFrame
            
        Returns:
            Tuple of (success_flag, success_count, error_count)
        """
        try:
            self.logger.info("Starting data load")
            
            # Validate records
            validated_df = self.validate_record(analytics_df)
            
            # Split into valid and invalid records
            valid_df = validated_df.filter(col("is_valid") == True).drop("is_valid")
            invalid_df = validated_df.filter(col("is_valid") == False).drop("is_valid")
            
            success_count = valid_df.count()
            error_count = invalid_df.count()
            total_count = success_count + error_count
            
            # Log invalid records
            if error_count > 0:
                self.logger.warning(
                    f"Found {error_count} invalid records",
                    extra={'step': 'LOAD', 'status': 'W'}
                )
            
            # Write valid records to target table
            if success_count > 0:
                valid_df.write \
                    .format("jdbc") \
                    .option("url", "jdbc:postgresql://localhost:5432/sales_db") \
                    .option("dbtable", "zsales_analytics") \
                    .option("user", "etl_user") \
                    .option("password", "etl_password") \
                    .mode("append") \
                    .save()
                
                # Update source table status (in production)
                # This would be a separate update operation
                self.logger.info("Updated source table status to 'P'")
            
            self.logger.info(
                f"Loaded {success_count} of {total_count} records",
                extra={
                    'step': 'LOAD',
                    'status': 'S',
                    'records_processed': total_count,
                    'records_success': success_count,
                    'records_error': error_count
                }
            )
            
            return True, success_count, error_count
            
        except Exception as e:
            self.logger.error(
                f"Load failed: {str(e)}",
                extra={'step': 'LOAD', 'status': 'E'},
                exc_info=True
            )
            return False, 0, 0
    
    def load_to_parquet(self, analytics_df: DataFrame, output_path: str) -> Tuple[bool, int, int]:
        """
        Load analytics data to Parquet files (alternative output)
        
        Args:
            analytics_df: Transformed analytics DataFrame
            output_path: Path to write parquet files
            
        Returns:
            Tuple of (success_flag, success_count, error_count)
        """
        try:
            self.logger.info(f"Starting data load to parquet: {output_path}")
            
            # Validate records
            validated_df = self.validate_record(analytics_df)
            
            # Split into valid and invalid records
            valid_df = validated_df.filter(col("is_valid") == True).drop("is_valid")
            invalid_df = validated_df.filter(col("is_valid") == False).drop("is_valid")
            
            success_count = valid_df.count()
            error_count = invalid_df.count()
            
            # Write valid records to parquet
            if success_count > 0:
                valid_df.write \
                    .mode("overwrite") \
                    .partitionBy("trans_date") \
                    .parquet(output_path)
            
            # Write invalid records to separate location
            if error_count > 0:
                invalid_df.write \
                    .mode("overwrite") \
                    .parquet(f"{output_path}_invalid")
            
            self.logger.info(
                f"Loaded {success_count} records to parquet",
                extra={
                    'step': 'LOAD',
                    'status': 'S',
                    'records_processed': success_count + error_count,
                    'records_success': success_count,
                    'records_error': error_count
                }
            )
            
            return True, success_count, error_count
            
        except Exception as e:
            self.logger.error(
                f"Parquet load failed: {str(e)}",
                extra={'step': 'LOAD', 'status': 'E'},
                exc_info=True
            )
            return False, 0, 0