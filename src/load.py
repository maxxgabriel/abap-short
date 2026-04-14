"""
PySpark load module for writing validated analytics data.
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Dict
import logging

logger = logging.getLogger(__name__)


def load_analytics_data(spark: SparkSession, analytics_df: DataFrame, 
                       config: Dict) -> Dict:
    """
    Load validated analytics data to target.
    
    Args:
        spark: SparkSession
        analytics_df: Transformed and validated DataFrame
        config: Configuration dictionary
        
    Returns:
        Dictionary with load statistics
    """
    target_config = config.get('target', {})
    target_type = target_config.get('type', 'parquet')
    target_path = target_config.get('path', 'data/analytics/sales')
    
    logger.info(f"Loading analytics data to {target_type} at {target_path}")
    
    # Separate valid and invalid records
    valid_df = analytics_df.filter(analytics_df.validation_status == 'VALID')
    invalid_df = analytics_df.filter(analytics_df.validation_status == 'INVALID')
    
    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    
    logger.info(f"Valid records: {valid_count}, Invalid records: {invalid_count}")
    
    # Write valid records to target
    if valid_count > 0:
        if target_type == 'parquet':
            valid_df.write.mode('append').parquet(target_path)
        elif target_type == 'csv':
            valid_df.write.mode('append').option("header", "true").csv(target_path)
        elif target_type == 'jdbc':
            jdbc_config = target_config.get('jdbc', {})
            valid_df.write.jdbc(
                url=jdbc_config.get('url'),
                table=jdbc_config.get('table', 'zsales_analytics'),
                mode='append',
                properties={
                    'user': jdbc_config.get('user'),
                    'password': jdbc_config.get('password'),
                    'driver': jdbc_config.get('driver', 'com.sap.db.jdbc.Driver')
                }
            )
        else:
            raise ValueError(f"Unsupported target type: {target_type}")
        
        logger.info(f"Successfully loaded {valid_count} valid records")
    
    # Write invalid records to error path
    if invalid_count > 0:
        error_path = target_config.get('error_path', 'data/errors/sales')
        invalid_df.write.mode('append').parquet(error_path)
        logger.warning(f"Wrote {invalid_count} invalid records to error path: {error_path}")
    
    return {
        "valid_records_loaded": valid_count,
        "invalid_records": invalid_count,
        "total_processed": valid_count + invalid_count
    }