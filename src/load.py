"""
Load Module - Sales ETL System
Loads transformed analytics data into target table
"""
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import logging

from src.logger import ETLLogger


class ETLLoader:
    """Loads transformed data into target analytics table"""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize loader
        
        Args:
            spark: SparkSession instance
            logger: ETLLogger instance
        """
        self.spark = spark
        self.logger = logger
        self.log = logging.getLogger(__name__)
    
    def load_data(
        self, 
        analytics_df: DataFrame,
        target_table: str = "zsales_analytics",
        mode: str = "append"
    ) -> Tuple[int, int, bool]:
        """
        Load analytics data into target table
        
        Args:
            analytics_df: Analytics DataFrame to load
            target_table: Target table name
            mode: Write mode (append, overwrite, etc.)
            
        Returns:
            Tuple of (success_count, error_count, success_flag)
        """
        try:
            self.logger.log_message(
                step="LOAD",
                status="S",
                message="Starting data load"
            )
            
            total_count = analytics_df.count()
            
            # Validate records before loading
            valid_df = self._validate_records(analytics_df)
            valid_count = valid_df.count()
            error_count = total_count - valid_count
            
            if error_count > 0:
                self.logger.log_message(
                    step="LOAD",
                    status="W",
                    message=f"Skipped {error_count} invalid records"
                )
            
            if valid_count > 0:
                # Write to target table
                # In production: valid_df.write.mode(mode).saveAsTable(target_table)
                # For demonstration, just show the data
                self.log.info(f"Would write {valid_count} records to {target_table}")
                
                # Update source table status
                # In production: UPDATE zsales_raw SET status = 'P' WHERE trans_id IN (...)
                self._update_source_status(valid_df)
            
            self.logger.log_message(
                step="LOAD",
                status="S",
                records_processed=total_count,
                records_success=valid_count,
                records_error=error_count,
                message=f"Loaded {valid_count} of {total_count} records"
            )
            
            return valid_count, error_count, True
            
        except Exception as e:
            self.log.error(f"Load failed: {str(e)}", exc_info=True)
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=f"Load failed: {str(e)}"
            )
            return 0, total_count if 'total_count' in locals() else 0, False
    
    def _validate_records(self, df: DataFrame) -> DataFrame:
        """
        Validate records before loading
        
        Args:
            df: DataFrame to validate
            
        Returns:
            DataFrame with only valid records
        """
        # Validate required fields
        valid_df = df.filter(
            col("analytics_id").isNotNull() &
            col("customer_id").isNotNull() &
            col("product_id").isNotNull() &
            (col("gross_amount") > 0) &
            col("currency").isNotNull() &
            col("category").isin(["HIGH", "MEDIUM", "LOW"])
        )
        
        return valid_df
    
    def _update_source_status(self, df: DataFrame) -> None:
        """
        Update source table status for processed records
        
        Args:
            df: DataFrame with processed records
        """
        # In production, extract trans_ids and update source table
        # UPDATE zsales_raw SET status = 'P' WHERE trans_id IN (trans_ids)
        self.log.info("Source table status would be updated to 'P' for processed records")