"""
Load module for Sales ETL Pipeline
Loads transformed analytics data to target destination
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import logging
from typing import Dict, Tuple

logger = logging.getLogger(__name__)


class SalesDataLoader:
    """Loads transformed analytics data to target"""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize loader
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
    
    def load_data(
        self, 
        df_analytics: DataFrame,
        etl_run_id: str
    ) -> Tuple[bool, Dict[str, int]]:
        """
        Load analytics data to target destination
        
        Args:
            df_analytics: Transformed analytics DataFrame
            etl_run_id: Unique ETL run identifier
            
        Returns:
            Tuple of (success_flag, statistics_dict)
        """
        try:
            logger.info("Starting data load")
            
            # Validate records before loading
            df_valid, stats = self._validate_records(df_analytics)
            
            if stats['valid_count'] == 0:
                logger.error("No valid records to load")
                return False, stats
            
            # Load to target
            self._write_to_target(df_valid)
            
            # Update source status (mark as processed)
            self._update_source_status(df_valid, etl_run_id)
            
            logger.info(
                f"Loaded {stats['valid_count']} records, "
                f"skipped {stats['invalid_count']} invalid records"
            )
            
            return True, stats
            
        except Exception as e:
            logger.error(f"Load failed: {str(e)}")
            raise
    
    def _validate_records(
        self, 
        df: DataFrame
    ) -> Tuple[DataFrame, Dict[str, int]]:
        """
        Validate records before loading
        
        Returns:
            Tuple of (valid_dataframe, statistics)
        """
        total_count = df.count()
        
        # Define validation rules
        df_valid = df.filter(
            (col("analytics_id").isNotNull()) &
            (col("customer_id").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("gross_amount") > 0) &
            (col("currency").isNotNull()) &
            (col("category").isin("HIGH", "MEDIUM", "LOW"))
        )
        
        valid_count = df_valid.count()
        invalid_count = total_count - valid_count
        
        if invalid_count > 0:
            logger.warning(f"Found {invalid_count} invalid records")
        
        stats = {
            "total_count": total_count,
            "valid_count": valid_count,
            "invalid_count": invalid_count
        }
        
        return df_valid, stats
    
    def _write_to_target(self, df: DataFrame) -> None:
        """Write data to target destination"""
        target_config = self.config['target']
        target_format = target_config['format']
        target_path = target_config['path']
        
        if target_format.lower() == 'parquet':
            self._write_parquet(df, target_path, target_config)
        elif target_format.lower() == 'delta':
            self._write_delta(df, target_path, target_config)
        elif target_format.lower() == 'jdbc':
            self._write_jdbc(df, target_path, target_config)
        else:
            raise ValueError(f"Unsupported target format: {target_format}")
    
    def _write_parquet(
        self, 
        df: DataFrame, 
        path: str, 
        config: dict
    ) -> None:
        """Write to Parquet format"""
        mode = config.get('mode', 'append')
        partition_by = config.get('partition_by', None)
        
        writer = df.write.mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        writer.parquet(path)
        logger.info(f"Written to Parquet: {path}")
    
    def _write_delta(
        self, 
        df: DataFrame, 
        path: str, 
        config: dict
    ) -> None:
        """Write to Delta Lake format"""
        mode = config.get('mode', 'append')
        partition_by = config.get('partition_by', None)
        
        writer = df.write.format("delta").mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        writer.save(path)
        logger.info(f"Written to Delta: {path}")
    
    def _write_jdbc(
        self, 
        df: DataFrame, 
        table_name: str, 
        config: dict
    ) -> None:
        """Write to JDBC destination (SAP HANA, etc.)"""
        jdbc_config = config.get('jdbc', {})
        mode = config.get('mode', 'append')
        batch_size = config.get('batch_size', 1000)
        
        df.write.format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("dbtable", table_name) \
            .option("user", jdbc_config['user']) \
            .option("password", jdbc_config['password']) \
            .option("driver", jdbc_config['driver']) \
            .option("batchsize", batch_size) \
            .mode(mode) \
            .save()
        
        logger.info(f"Written to JDBC table: {table_name}")
    
    def _update_source_status(
        self, 
        df_loaded: DataFrame, 
        etl_run_id: str
    ) -> None:
        """
        Update source table to mark records as processed
        In production, this would update the ZSALES_RAW table status to 'P'
        """
        # Extract transaction IDs that were successfully loaded
        trans_ids = df_loaded.select("trans_id").distinct().collect()
        id_list = [row.trans_id for row in trans_ids]
        
        logger.info(f"Marking {len(id_list)} source records as processed")
        
        # In production with JDBC:
        # UPDATE zsales_raw SET status = 'P', processed_at = current_timestamp
        # WHERE trans_id IN (id_list)
        
        # For demonstration, log the action
        logger.info(
            f"Would update source status for transaction IDs: "
            f"{id_list[:5]}... (showing first 5)"
        )