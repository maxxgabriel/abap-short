"""
ETL Load Module - Load transformed data to target
Loads analytics data into target database/storage
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import logging
from typing import Tuple

from src.logger import ETLLogger
from src.config import Config


class SalesLoader:
    """Load analytics data to target"""
    
    def __init__(self, logger: ETLLogger, config: Config):
        """
        Initialize loader
        
        Args:
            logger: ETL logger instance
            config: Configuration object
        """
        self.logger = logger
        self.config = config
        self.log = logging.getLogger(self.__class__.__name__)
    
    def validate_record(self, row) -> bool:
        """
        Validate a single record
        
        Args:
            row: Row to validate
            
        Returns:
            True if valid, False otherwise
        """
        # Check required fields
        if not row.analytics_id or not row.customer_id or not row.product_id:
            return False
        
        # Check positive amounts
        if row.gross_amount <= 0:
            return False
        
        # Check currency
        if not row.currency:
            return False
        
        # Check category
        if row.category not in ['HIGH', 'MEDIUM', 'LOW']:
            return False
        
        return True
    
    def load_data(self, analytics_df: DataFrame) -> bool:
        """
        Load analytics data to target
        
        Args:
            analytics_df: Analytics DataFrame to load
            
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
            
            # Filter out invalid records if validation is enabled
            if self.config.validate_before_load:
                from pyspark.sql.functions import udf
                from pyspark.sql.types import BooleanType
                
                validate_udf = udf(self.validate_record, BooleanType())
                valid_df = analytics_df.filter(validate_udf(col("*")))
                invalid_count = total_count - valid_df.count()
                
                if invalid_count > 0:
                    self.logger.log_message(
                        step="LOAD",
                        status="W",
                        message=f"Filtered out {invalid_count} invalid records"
                    )
            else:
                valid_df = analytics_df
                invalid_count = 0
            
            valid_count = valid_df.count()
            
            # Write to target
            if self.config.target_type == "jdbc":
                self._load_to_jdbc(valid_df)
            elif self.config.target_type == "parquet":
                self._load_to_parquet(valid_df)
            elif self.config.target_type == "delta":
                self._load_to_delta(valid_df)
            else:
                # Default to parquet
                self._load_to_parquet(valid_df)
            
            self.logger.log_message(
                step="LOAD",
                status="S",
                records_processed=total_count,
                records_success=valid_count,
                records_error=invalid_count,
                message=f"Loaded {valid_count} of {total_count} records"
            )
            
            return True
            
        except Exception as e:
            self.log.error(f"Load failed: {str(e)}", exc_info=True)
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=f"Load failed: {str(e)}"
            )
            return False
    
    def _load_to_jdbc(self, df: DataFrame) -> None:
        """Load data to JDBC target"""
        df.write \
            .format("jdbc") \
            .option("url", self.config.target_jdbc_url) \
            .option("dbtable", self.config.target_table) \
            .option("user", self.config.target_jdbc_user) \
            .option("password", self.config.target_jdbc_password) \
            .option("driver", self.config.target_jdbc_driver) \
            .mode(self.config.write_mode) \
            .save()
    
    def _load_to_parquet(self, df: DataFrame) -> None:
        """Load data to Parquet files"""
        df.write \
            .mode(self.config.write_mode) \
            .partitionBy("trans_date") \
            .parquet(self.config.target_path)
    
    def _load_to_delta(self, df: DataFrame) -> None:
        """Load data to Delta Lake"""
        df.write \
            .format("delta") \
            .mode(self.config.write_mode) \
            .partitionBy("trans_date") \
            .save(self.config.target_path)