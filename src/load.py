"""
PySpark ETL Loader Module

Migrated from ABAP ZCL_ETL_LOADER class.
Loads transformed data into target analytics table.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Tuple
import logging

from src.logger import ETLLogger


class ETLLoader:
    """
    Loads transformed analytics data into target.
    Replaces ABAP INSERT/UPDATE with DataFrame write operations.
    """
    
    def __init__(self, logger: ETLLogger, spark: SparkSession):
        """
        Initialize loader with logger and Spark session.
        
        Args:
            logger: ETL logger instance
            spark: Active SparkSession
        """
        self.logger = logger
        self.spark = spark
        self.log = logging.getLogger(__name__)
    
    def load_data(
        self,
        analytics_data: DataFrame,
        target_table: str = None,
        target_path: str = None,
        mode: str = 'append'
    ) -> bool:
        """
        Load analytics data to target.
        
        Migrated from ABAP load_data method.
        Replaces LOOP AT with DataFrame write operations.
        
        Args:
            analytics_data: Transformed analytics DataFrame
            target_table: Optional table name (for JDBC)
            target_path: Optional file path (for files)
            mode: Write mode (append, overwrite)
            
        Returns:
            Success boolean
        """
        try:
            self.logger.log_message(
                step='LOAD',
                status='S',
                message='Starting data load'
            )
            
            # Validate data before loading
            validated_df = self._validate_records(analytics_data)
            
            total_count = analytics_data.count()
            valid_count = validated_df.count()
            error_count = total_count - valid_count
            
            if error_count > 0:
                self.log.warning(f"Skipped {error_count} invalid records")
            
            # Load based on target type
            if target_table:
                self._load_to_table(validated_df, target_table, mode)
            elif target_path:
                self._load_to_file(validated_df, target_path, mode)
            else:
                # Default: write to console for testing
                self._load_to_console(validated_df)
            
            self.logger.log_message(
                step='LOAD',
                status='S',
                records_processed=total_count,
                records_success=valid_count,
                records_error=error_count,
                message=f'Loaded {valid_count} of {total_count} records'
            )
            
            return True
            
        except Exception as e:
            self.log.error(f"Load failed: {str(e)}", exc_info=True)
            self.logger.log_message(
                step='LOAD',
                status='E',
                message=f'Load failed: {str(e)}'
            )
            return False
    
    def _validate_records(self, df: DataFrame) -> DataFrame:
        """
        Validate records before loading.
        
        Migrated from ABAP validate_record method.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            DataFrame with only valid records
        """
        # Validate required fields and business rules
        validated_df = df.filter(
            F.col('analytics_id').isNotNull() &
            (F.col('analytics_id') != '') &
            F.col('customer_id').isNotNull() &
            (F.col('customer_id') != '') &
            F.col('product_id').isNotNull() &
            (F.col('product_id') != '') &
            (F.col('gross_amount') > 0) &
            F.col('currency').isNotNull() &
            (F.col('currency') != '') &
            F.col('category').isin(['HIGH', 'MEDIUM', 'LOW'])
        )
        
        return validated_df
    
    def _load_to_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str
    ) -> None:
        """
        Load data to database table via JDBC.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            mode: Write mode
        """
        self.log.info(f"Loading to table: {table_name} (mode: {mode})")
        
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/sales_db") \
            .option("dbtable", table_name) \
            .option("user", "etl_user") \
            .option("password", "password") \
            .mode(mode) \
            .save()
    
    def _load_to_file(
        self,
        df: DataFrame,
        file_path: str,
        mode: str
    ) -> None:
        """
        Load data to file (Parquet, CSV, etc.).
        
        Args:
            df: DataFrame to load
            file_path: Target file path
            mode: Write mode
        """
        self.log.info(f"Loading to file: {file_path} (mode: {mode})")
        
        # Determine format from file extension
        if file_path.endswith('.parquet'):
            df.write.mode(mode).parquet(file_path)
        elif file_path.endswith('.csv'):
            df.write.mode(mode).option("header", "true").csv(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_path}")
    
    def _load_to_console(self, df: DataFrame) -> None:
        """
        Display data to console for testing.
        
        Args:
            df: DataFrame to display
        """
        self.log.info("Displaying data to console")
        df.show(truncate=False)
        
        # Print summary statistics
        self.log.info("Summary statistics:")
        df.select(
            F.count('*').alias('total_records'),
            F.sum('gross_amount').alias('total_gross'),
            F.avg('profit_margin').alias('avg_margin')
        ).show()