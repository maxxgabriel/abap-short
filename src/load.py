"""
ETL Loader Module
Loads transformed analytics data into target table using PySpark.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from src.utils.etl_logger import ETLLogger


class ETLLoader:
    """
    Loads transformed data into target analytics table.
    Converted from ZCL_ETL_LOADER ABAP class.
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        """
        Initialize loader.
        
        Args:
            spark: SparkSession instance
            logger: ETLLogger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
    
    def load_data(self, analytics_df: DataFrame, target_table: str = None) -> bool:
        """
        Load analytics data into target table.
        
        Args:
            analytics_df: DataFrame with analytics data
            target_table: Optional target table name
            
        Returns:
            True if successful, False otherwise
        """
        step = self.config['process_steps']['load']
        
        try:
            self.logger.log_etl_message(
                step=step,
                status=self.config['status_codes']['success'],
                message="Starting data load"
            )
            
            # Validate records before loading
            validated_df = self._validate_records(analytics_df)
            
            total_records = analytics_df.count()
            valid_records = validated_df.count()
            error_records = total_records - valid_records
            
            if error_records > 0:
                self.logger.log_etl_message(
                    step=step,
                    status=self.config['status_codes']['warning'],
                    message=f"Skipped {error_records} invalid records"
                )
            
            # In production, write to target table:
            # validated_df.write.mode("append").saveAsTable(target_table or "zsales_analytics")
            
            # For demonstration, show sample
            validated_df.show(5, truncate=False)
            
            self.logger.log_etl_statistics(
                step=step,
                status=self.config['status_codes']['success'],
                records_processed=total_records,
                records_success=valid_records,
                records_error=error_records,
                message=f"Loaded {valid_records} of {total_records} records"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_etl_message(
                step=step,
                status=self.config['status_codes']['error'],
                message=f"Load failed: {str(e)}"
            )
            return False
    
    def _validate_records(self, df: DataFrame) -> DataFrame:
        """
        Validate records before loading.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with only valid records
        """
        # Validate required fields are not null and values are valid
        valid_df = df.filter(
            (col("analytics_id").isNotNull()) &
            (col("customer_id").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("gross_amount") > 0) &
            (col("currency").isNotNull()) &
            (col("category").isin(
                self.config['categories']['high'],
                self.config['categories']['medium'],
                self.config['categories']['low']
            ))
        )
        
        return valid_df