"""
Sales Data Loading Module
Loads transformed analytics data into target tables
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from typing import Optional
import logging

from src.logger import ETLLogger
from src.config import ETLConfig


class SalesDataLoader:
    """Loads analytics data into target tables"""
    
    def __init__(self, logger: ETLLogger, config: ETLConfig):
        self.logger = logger
        self.config = config
        
    def load_data(self, analytics_df: DataFrame) -> bool:
        """
        Load analytics data to target table
        
        Args:
            analytics_df: Transformed analytics DataFrame
            
        Returns:
            True if load succeeds, False otherwise
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
            
            # Load to target table
            target_table = self.config.get("target.table", "zsales_analytics")
            write_mode = self.config.get("target.write_mode", "append")
            
            valid_df.write.mode(write_mode).saveAsTable(target_table)
            
            # Update source records status
            self._update_source_status(analytics_df)
            
            self.logger.log_message(
                step="LOAD",
                status="S",
                records_processed=total_count,
                records_success=valid_count,
                records_error=error_count,
                message=f"Loaded {valid_count} of {total_count} records to {target_table}"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=f"Load failed: {str(e)}"
            )
            logging.error(f"Load error: {str(e)}", exc_info=True)
            return False
    
    def _validate_records(self, df: DataFrame) -> DataFrame:
        """Validate records before loading"""
        return df.filter(
            (col("analytics_id").isNotNull()) &
            (col("customer_id").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("gross_amount") > 0) &
            (col("currency").isNotNull()) &
            (col("category").isin("HIGH", "MEDIUM", "LOW"))
        )
    
    def _update_source_status(self, df: DataFrame) -> None:
        """Update status in source table (simulation)"""
        try:
            # In production, would update source table status to 'P' (Processed)
            # For now, just log the action
            self.logger.log_message(
                step="LOAD",
                status="I",
                message="Source records marked as processed"
            )
        except Exception as e:
            logging.warning(f"Could not update source status: {str(e)}")