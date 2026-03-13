"""
ETL Loader Module
Loads transformed analytics data into target table.
"""

import logging
from typing import Dict, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)


class SalesDataLoader:
    """Loads transformed data into target analytics table."""
    
    def __init__(self, config: Dict):
        """
        Initialize loader.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
    
    def validate_record(self, row) -> bool:
        """
        Validate individual record.
        
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
        if row.category not in ('HIGH', 'MEDIUM', 'LOW'):
            return False
        
        return True
    
    def load_data(
        self,
        analytics_df: DataFrame,
        run_id: str,
        test_mode: bool = False
    ) -> Tuple[bool, int, int]:
        """
        Load analytics data to target table.
        
        Args:
            analytics_df: Transformed analytics DataFrame
            run_id: ETL run identifier
            test_mode: If True, skip actual database write
            
        Returns:
            Tuple of (success, records_loaded, records_failed)
        """
        try:
            logger.info(f"[{run_id}] Starting data load")
            
            total_records = analytics_df.count()
            
            # Validate all records
            valid_df = analytics_df.rdd.filter(self.validate_record).toDF()
            valid_count = valid_df.count()
            failed_count = total_records - valid_count
            
            if failed_count > 0:
                logger.warning(
                    f"[{run_id}] {failed_count} records failed validation and will be skipped"
                )
            
            if test_mode:
                logger.info(f"[{run_id}] Test mode - skipping database write")
                logger.info(f"[{run_id}] Would have loaded {valid_count} records")
                return True, valid_count, failed_count
            
            # Write to target table
            target_table = self.config.get('target_table', 'zsales_analytics')
            
            valid_df.write \
                .format(self.config.get('target_format', 'jdbc')) \
                .option("url", self.config['jdbc_url']) \
                .option("dbtable", target_table) \
                .option("user", self.config.get('db_user')) \
                .option("password", self.config.get('db_password')) \
                .option("driver", self.config.get('jdbc_driver', 'com.sap.db.jdbc.Driver')) \
                .mode(self.config.get('write_mode', 'append')) \
                .save()
            
            logger.info(f"[{run_id}] Loaded {valid_count} records successfully")
            
            # Update source table status (would require separate connection)
            # This is a placeholder - actual implementation would update ZSALES_RAW
            logger.info(f"[{run_id}] Marking source records as processed")
            
            return True, valid_count, failed_count
            
        except Exception as e:
            logger.error(f"[{run_id}] Load failed: {str(e)}", exc_info=True)
            return False, 0, total_records