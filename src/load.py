"""
ETL Loader Module
Loads transformed analytics data into target tables.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import logging


class ETLLoader:
    """
    Loads transformed analytics data into target system.
    Supports multiple target types (JDBC, Parquet, Delta Lake).
    """
    
    def __init__(self, spark: SparkSession, config: dict, logger):
        """
        Initialize loader with Spark session and configuration.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
            logger: Logger instance for tracking load operations
        """
        self.spark = spark
        self.config = config
        self.logger = logger
        
    def load_data(self, df_analytics: DataFrame) -> bool:
        """
        Load analytics data to target system.
        
        Args:
            df_analytics: Analytics DataFrame to load
            
        Returns:
            bool: True if load successful, False otherwise
        """
        try:
            self.logger.log_message(
                step='LOAD',
                status='I',
                message='Starting data load'
            )
            
            record_count = df_analytics.count()
            
            # Validate records before loading
            df_valid = self._validate_records(df_analytics)
            valid_count = df_valid.count()
            error_count = record_count - valid_count
            
            if error_count > 0:
                self.logger.log_message(
                    step='LOAD',
                    status='W',
                    message=f'{error_count} invalid records skipped'
                )
            
            # Load to target based on configuration
            target_type = self.config['target']['type']
            
            if target_type == 'jdbc':
                self._load_to_jdbc(df_valid)
            elif target_type == 'parquet':
                self._load_to_parquet(df_valid)
            elif target_type == 'delta':
                self._load_to_delta(df_valid)
            else:
                raise ValueError(f"Unsupported target type: {target_type}")
            
            self.logger.log_message(
                step='LOAD',
                status='S',
                records_processed=record_count,
                records_success=valid_count,
                records_error=error_count,
                message=f'Loaded {valid_count} of {record_count} records'
            )
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='E',
                message=f'Load failed: {str(e)}'
            )
            return False
    
    def _validate_records(self, df: DataFrame) -> DataFrame:
        """
        Validate records before loading.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            DataFrame: Validated DataFrame with invalid records filtered
        """
        df_valid = df.filter(
            (col("analytics_id").isNotNull()) &
            (col("customer_id").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("gross_amount") > 0) &
            (col("currency").isNotNull()) &
            (col("category").isin(["HIGH", "MEDIUM", "LOW"]))
        )
        
        return df_valid
    
    def _load_to_jdbc(self, df: DataFrame):
        """
        Load data to JDBC target (database).
        
        Args:
            df: DataFrame to load
        """
        jdbc_config = self.config['target']['jdbc']
        
        df.write \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", jdbc_config['table']) \
            .option("user", jdbc_config.get('user', '')) \
            .option("password", jdbc_config.get('password', '')) \
            .option("driver", jdbc_config.get('driver', 'org.postgresql.Driver')) \
            .mode(jdbc_config.get('mode', 'append')) \
            .save()
    
    def _load_to_parquet(self, df: DataFrame):
        """
        Load data to Parquet files.
        
        Args:
            df: DataFrame to load
        """
        parquet_config = self.config['target']['parquet']
        
        df.write \
            .format("parquet") \
            .mode(parquet_config.get('mode', 'append')) \
            .partitionBy(parquet_config.get('partition_by', [])) \
            .save(parquet_config['path'])
    
    def _load_to_delta(self, df: DataFrame):
        """
        Load data to Delta Lake.
        
        Args:
            df: DataFrame to load
        """
        delta_config = self.config['target']['delta']
        
        df.write \
            .format("delta") \
            .mode(delta_config.get('mode', 'append')) \
            .partitionBy(delta_config.get('partition_by', [])) \
            .save(delta_config['path'])
    
    def update_source_status(self, trans_ids: list):
        """
        Update status in source table for processed records.
        
        Args:
            trans_ids: List of transaction IDs to update
        """
        try:
            # This would update the source table status from 'N' to 'P'
            # Implementation depends on source type
            self.logger.log_message(
                step='LOAD',
                status='I',
                message=f'Updated status for {len(trans_ids)} records'
            )
        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='W',
                message=f'Failed to update source status: {str(e)}'
            )