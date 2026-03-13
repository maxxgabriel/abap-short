"""
Load module for Sales ETL process.
Loads transformed analytics data into target table.
"""
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from src.logger import ETLLogger


class SalesLoader:
    """Loads analytics data into target table."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize loader.
        
        Args:
            spark: SparkSession instance
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
    
    def load_data(
        self, 
        analytics_df: DataFrame,
        target_table: str = "zsales_analytics",
        update_source: bool = True
    ) -> bool:
        """
        Load analytics data into target table.
        
        Args:
            analytics_df: Analytics DataFrame to load
            target_table: Target table name
            update_source: Whether to update source status
            
        Returns:
            Success flag
        """
        try:
            self.logger.log_message(
                step="LOAD",
                status="S",
                message="Starting data load"
            )
            
            total_count = analytics_df.count()
            
            # Validate records before loading
            valid_df = self._validate_before_load(analytics_df)
            valid_count = valid_df.count()
            error_count = total_count - valid_count
            
            # Write to target
            # In production, use JDBC or appropriate connector
            self._write_to_target(valid_df, target_table)
            
            # Update source table status (simulated)
            if update_source:
                self._update_source_status(valid_df)
            
            self.logger.log_message(
                step="LOAD",
                status="S",
                records_processed=total_count,
                records_success=valid_count,
                records_error=error_count,
                message=f"Loaded {valid_count} of {total_count} records"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=f"Load failed: {str(e)}"
            )
            return False
    
    def _validate_before_load(self, df: DataFrame) -> DataFrame:
        """
        Final validation before loading.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Validated DataFrame
        """
        from pyspark.sql import functions as F
        
        # Filter out any records with null required fields
        valid_df = df.filter(
            (F.col("analytics_id").isNotNull()) &
            (F.col("customer_id").isNotNull()) &
            (F.col("product_id").isNotNull()) &
            (F.col("gross_amount") > 0) &
            (F.col("currency").isNotNull()) &
            (F.col("category").isin(["HIGH", "MEDIUM", "LOW"]))
        )
        
        invalid_count = df.count() - valid_df.count()
        if invalid_count > 0:
            self.logger.log_message(
                step="LOAD",
                status="W",
                message=f"Skipped {invalid_count} invalid records"
            )
        
        return valid_df
    
    def _write_to_target(self, df: DataFrame, table_name: str) -> None:
        """
        Write DataFrame to target table.
        
        Args:
            df: DataFrame to write
            table_name: Target table name
        """
        # In production, use appropriate write method:
        # df.write.jdbc() for JDBC
        # df.write.saveAsTable() for Hive
        # df.write.parquet() for file-based storage
        
        write_mode = self.config['load']['write_mode']
        
        # For demonstration, write to parquet
        output_path = f"{self.config['load']['output_path']}/{table_name}"
        
        df.write.mode(write_mode).parquet(output_path)
        
        self.logger.log_message(
            step="LOAD",
            status="I",
            message=f"Data written to {output_path}"
        )
    
    def _update_source_status(self, df: DataFrame) -> None:
        """
        Update source table status to 'P' (Processed).
        
        Args:
            df: DataFrame with loaded records
        """
        # In production, execute UPDATE statement on source table
        # UPDATE zsales_raw SET status = 'P' 
        # WHERE trans_id IN (SELECT DISTINCT trans_id FROM analytics)
        
        self.logger.log_message(
            step="LOAD",
            status="I",
            message="Source table status updated (simulated)"
        )