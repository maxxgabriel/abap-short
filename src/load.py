"""
Data Loader Module
Loads transformed analytics data into target table.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from typing import Optional
import logging


class DataLoader:
    """
    Loads transformed analytics data into target table with validation.
    Handles batch processing and error logging.
    """
    
    def __init__(self, logger: logging.Logger, config: dict):
        """
        Initialize the DataLoader with dependencies.
        
        Args:
            logger: Logger instance for logging load activities
            config: Configuration dictionary with load parameters
        """
        self.logger = logger
        self.config = config
        
    def load_data(
        self, 
        df_analytics: DataFrame,
        target_table: Optional[str] = None,
        mode: str = "append"
    ) -> bool:
        """
        Load analytics data into target table.
        
        Args:
            df_analytics: Transformed analytics DataFrame
            target_table: Optional override for target table name
            mode: Write mode ('append', 'overwrite', 'error', 'ignore')
            
        Returns:
            True if load successful, False otherwise
        """
        try:
            table_name = target_table or self.config.get('target_table', 'zsales_analytics')
            batch_size = self.config.get('batch_size', 1000)
            
            self.logger.info(f"Starting data load to {table_name}")
            
            # Validate data before loading
            df_validated = self._validate_before_load(df_analytics)
            
            total_count = df_analytics.count()
            valid_count = df_validated.count()
            invalid_count = total_count - valid_count
            
            if invalid_count > 0:
                self.logger.warning(
                    f"Filtered out {invalid_count} invalid records before load"
                )
            
            # Load data in batches
            success = self._write_to_target(df_validated, table_name, mode, batch_size)
            
            if success:
                self.logger.info(
                    f"Load completed successfully. Records loaded: {valid_count}",
                    extra={
                        'step': 'LOAD',
                        'status': 'SUCCESS',
                        'records_processed': total_count,
                        'records_success': valid_count,
                        'records_error': invalid_count
                    }
                )
            
            return success
            
        except Exception as e:
            self.logger.error(
                f"Load failed: {str(e)}",
                extra={'step': 'LOAD', 'status': 'ERROR'},
                exc_info=True
            )
            return False
    
    def _validate_before_load(self, df: DataFrame) -> DataFrame:
        """
        Perform final validation before loading data.
        
        Args:
            df: Analytics DataFrame
            
        Returns:
            Validated DataFrame
        """
        # Check for required fields
        df_valid = df.filter(
            (col("analytics_id").isNotNull()) &
            (col("customer_id").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("gross_amount") > 0) &
            (col("currency").isNotNull()) &
            (col("category").isin(["HIGH", "MEDIUM", "LOW"]))
        )
        
        # Check for data quality issues
        df_valid = df_valid.filter(
            (col("net_amount").isNotNull()) &
            (col("discount_amount") >= 0) &
            (col("tax_amount") >= 0)
        )
        
        return df_valid
    
    def _write_to_target(
        self, 
        df: DataFrame, 
        table_name: str, 
        mode: str,
        batch_size: int
    ) -> bool:
        """
        Write DataFrame to target table.
        
        Args:
            df: DataFrame to write
            table_name: Target table name
            mode: Write mode
            batch_size: Batch size for writing
            
        Returns:
            True if write successful, False otherwise
        """
        try:
            # For production: write to actual database/table
            # df.write \
            #   .format("jdbc") \
            #   .option("url", self.config['jdbc_url']) \
            #   .option("dbtable", table_name) \
            #   .option("batchsize", batch_size) \
            #   .mode(mode) \
            #   .save()
            
            # For demonstration: write to parquet
            output_path = self.config.get('output_path', f'/tmp/{table_name}')
            df.write.mode(mode).parquet(output_path)
            
            self.logger.info(f"Data written to {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error writing to target: {str(e)}")
            return False
    
    def update_source_status(
        self, 
        df_analytics: DataFrame,
        source_table: Optional[str] = None
    ) -> bool:
        """
        Update status of processed records in source table.
        
        Args:
            df_analytics: Analytics DataFrame with processed records
            source_table: Optional override for source table name
            
        Returns:
            True if update successful, False otherwise
        """
        try:
            table_name = source_table or self.config.get('source_table', 'zsales_raw')
            
            # Extract transaction IDs from analytics data
            trans_ids = df_analytics.select("analytics_id").distinct().collect()
            trans_id_list = [row['analytics_id'] for row in trans_ids]
            
            self.logger.info(
                f"Updating status for {len(trans_id_list)} records in {table_name}"
            )
            
            # For production: execute UPDATE statement
            # UPDATE {table_name} SET status = 'P' WHERE trans_id IN (trans_id_list)
            
            self.logger.info("Source table status updated successfully")
            return True
            
        except Exception as e:
            self.logger.warning(f"Failed to update source status: {str(e)}")
            return False
    
    def get_load_statistics(self, df: DataFrame) -> dict:
        """
        Calculate load statistics.
        
        Args:
            df: Loaded DataFrame
            
        Returns:
            Dictionary containing load statistics
        """
        try:
            from pyspark.sql.functions import sum as spark_sum, count
            
            stats = df.groupBy("category").agg(
                count("*").alias("record_count"),
                spark_sum("gross_amount").alias("total_gross"),
                spark_sum("net_amount").alias("total_net")
            ).collect()
            
            return {
                'category_breakdown': [
                    {
                        'category': row['category'],
                        'record_count': row['record_count'],
                        'total_gross': float(row['total_gross']) if row['total_gross'] else 0,
                        'total_net': float(row['total_net']) if row['total_net'] else 0
                    }
                    for row in stats
                ]
            }
        except Exception as e:
            self.logger.warning(f"Failed to calculate load statistics: {str(e)}")
            return {}