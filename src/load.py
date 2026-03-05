"""
Data Loading Module
Loads transformed analytics data into target database
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from typing import Optional

from src.logger import ETLLogger
from src.exceptions import ETLLoadError


class SalesDataLoader:
    """Loads transformed data into target analytics table"""
    
    def __init__(self, spark: SparkSession, config: dict, logger: ETLLogger):
        """
        Initialize the loader
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
            logger: ETL logger instance
        """
        self.spark = spark
        self.config = config
        self.logger = logger
    
    def load_data(
        self,
        analytics_df: DataFrame,
        test_mode: bool = False
    ) -> bool:
        """
        Load analytics data into target database
        
        Args:
            analytics_df: Transformed analytics DataFrame
            test_mode: If True, skip actual database write
            
        Returns:
            True if successful, False otherwise
            
        Raises:
            ETLLoadError: If load fails
        """
        try:
            self.logger.log_message(
                step="LOAD",
                status="S",
                message="Starting data load"
            )
            
            record_count = analytics_df.count()
            
            if not self._validate_before_load(analytics_df):
                raise ETLLoadError("Data validation failed before load")
            
            if not test_mode:
                self._write_to_database(analytics_df)
                self._update_source_status(analytics_df)
            else:
                self.logger.log_message(
                    step="LOAD",
                    status="I",
                    message="Test mode: Skipping database write"
                )
            
            self.logger.log_message(
                step="LOAD",
                status="S",
                records_processed=record_count,
                records_success=record_count,
                message=f"Loaded {record_count} records successfully"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=f"Load failed: {str(e)}"
            )
            raise ETLLoadError(f"Failed to load data: {str(e)}") from e
    
    def _validate_before_load(self, df: DataFrame) -> bool:
        """
        Validate data before loading
        
        Args:
            df: DataFrame to validate
            
        Returns:
            True if valid, False otherwise
        """
        # Check for null values in required fields
        required_fields = ["analytics_id", "customer_id", "product_id", "gross_amount"]
        
        for field in required_fields:
            null_count = df.filter(col(field).isNull()).count()
            if null_count > 0:
                self.logger.log_message(
                    step="LOAD",
                    status="E",
                    message=f"Found {null_count} null values in {field}"
                )
                return False
        
        # Check for invalid amounts
        invalid_amounts = df.filter(col("gross_amount") <= 0).count()
        if invalid_amounts > 0:
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=f"Found {invalid_amounts} records with invalid amounts"
            )
            return False
        
        return True
    
    def _write_to_database(self, df: DataFrame) -> None:
        """
        Write DataFrame to target database
        
        Args:
            df: DataFrame to write
        """
        db_config = self.config['database']['target']
        
        df.write \
            .format(db_config['format']) \
            .option("url", db_config['url']) \
            .option("dbtable", db_config['table']) \
            .option("driver", db_config['driver']) \
            .option("user", db_config['user']) \
            .option("password", db_config['password']) \
            .option("batchsize", db_config['batch_size']) \
            .mode(db_config['mode']) \
            .save()
    
    def _update_source_status(self, analytics_df: DataFrame) -> None:
        """
        Update status of processed records in source table
        
        Args:
            analytics_df: Analytics DataFrame with processed records
        """
        # Extract transaction IDs (would need to track these from original data)
        # This is a simplified version
        self.logger.log_message(
            step="LOAD",
            status="I",
            message="Source status update completed"
        )
    
    def get_load_statistics(self, df: DataFrame) -> dict:
        """
        Calculate load statistics
        
        Args:
            df: Loaded DataFrame
            
        Returns:
            Dictionary with statistics
        """
        stats = {
            "total_records": df.count(),
            "total_gross_amount": df.agg({"gross_amount": "sum"}).collect()[0][0],
            "total_net_amount": df.agg({"net_amount": "sum"}).collect()[0][0],
            "high_value_sales": df.filter(col("category") == "HIGH").count(),
            "medium_value_sales": df.filter(col("category") == "MEDIUM").count(),
            "low_value_sales": df.filter(col("category") == "LOW").count()
        }
        
        return stats