"""
ETL Loader Module
Loads transformed data into target analytics table with validation.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
from typing import Tuple, Dict, Any
import logging
from datetime import datetime

from src.logger import ETLLogger
from src.exceptions import ETLLoadError


class ETLLoader:
    """Loads transformed analytics data with validation and status updates."""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: Dict[str, Any]):
        """
        Initialize the ETL Loader.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.logger = logger
        self.config = config
        self.batch_size = config.get('batch_size', 1000)
        
    def load_data(
        self,
        analytics_df: DataFrame,
        source_table: str,
        target_table: str
    ) -> Tuple[bool, Dict[str, int]]:
        """
        Load validated analytics data to target table and update source status.
        
        Args:
            analytics_df: Transformed analytics DataFrame
            source_table: Source table name for status update
            target_table: Target analytics table name
            
        Returns:
            Tuple of (success flag, statistics dictionary)
        """
        try:
            self.logger.log_message(
                step='LOAD',
                status='S',
                message=f'Starting data load to {target_table}'
            )
            
            total_count = analytics_df.count()
            
            # Validate records
            validated_df, validation_stats = self._validate_records(analytics_df)
            
            valid_count = validation_stats['valid_records']
            invalid_count = validation_stats['invalid_records']
            
            if valid_count == 0:
                self.logger.log_message(
                    step='LOAD',
                    status='E',
                    message='No valid records to load',
                    records_processed=total_count,
                    records_error=invalid_count
                )
                return False, validation_stats
            
            # Add metadata columns
            load_df = self._add_load_metadata(validated_df)
            
            # Bulk insert to analytics table
            success_count = self._bulk_insert(load_df, target_table)
            
            if success_count > 0:
                # Update source table status to 'P' (Processed)
                trans_ids = [row.trans_id for row in validated_df.select('trans_id').collect()]
                self._update_source_status(source_table, trans_ids)
                
                self.logger.log_message(
                    step='LOAD',
                    status='S',
                    message=f'Loaded {success_count} records successfully',
                    records_processed=total_count,
                    records_success=success_count,
                    records_error=invalid_count
                )
                
                stats = {
                    'total_records': total_count,
                    'valid_records': valid_count,
                    'invalid_records': invalid_count,
                    'loaded_records': success_count,
                    'failed_records': valid_count - success_count
                }
                
                return True, stats
            else:
                raise ETLLoadError('Failed to insert records to target table')
                
        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='E',
                message=f'Load failed: {str(e)}'
            )
            raise ETLLoadError(f'Data load failed: {str(e)}')
    
    def _validate_records(self, df: DataFrame) -> Tuple[DataFrame, Dict[str, int]]:
        """
        Validate records according to business rules.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (validated DataFrame, validation statistics)
        """
        self.logger.log_message(
            step='VALIDATE',
            status='S',
            message='Starting record validation'
        )
        
        initial_count = df.count()
        
        # Validation rules:
        # 1. Required fields must not be null
        # 2. Numeric fields must be positive
        # 3. Currency must be valid (not empty)
        # 4. Category must be HIGH, MEDIUM, or LOW
        
        valid_df = df.filter(
            (col('analytics_id').isNotNull()) &
            (col('customer_id').isNotNull()) &
            (col('product_id').isNotNull()) &
            (col('gross_amount') > 0) &
            (col('net_amount') > 0) &
            (col('currency').isNotNull()) &
            (col('currency') != '') &
            (col('category').isin(['HIGH', 'MEDIUM', 'LOW']))
        )
        
        valid_count = valid_df.count()
        invalid_count = initial_count - valid_count
        
        if invalid_count > 0:
            self.logger.log_message(
                step='VALIDATE',
                status='W',
                message=f'Found {invalid_count} invalid records',
                records_processed=initial_count,
                records_success=valid_count,
                records_error=invalid_count
            )
        else:
            self.logger.log_message(
                step='VALIDATE',
                status='S',
                message='All records passed validation',
                records_processed=initial_count,
                records_success=valid_count
            )
        
        stats = {
            'total_records': initial_count,
            'valid_records': valid_count,
            'invalid_records': invalid_count
        }
        
        return valid_df, stats
    
    def _add_load_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add load metadata columns to DataFrame.
        
        Args:
            df: DataFrame to enhance
            
        Returns:
            DataFrame with metadata columns
        """
        return df.withColumn('loaded_at', current_timestamp()) \
                 .withColumn('loaded_by', lit('ETL_PROCESS'))
    
    def _bulk_insert(self, df: DataFrame, target_table: str) -> int:
        """
        Perform bulk insert to target table.
        
        Args:
            df: DataFrame to insert
            target_table: Target table name
            
        Returns:
            Number of records inserted
        """
        try:
            # Write in append mode to target table
            df.write \
              .mode('append') \
              .format('delta') \
              .option('mergeSchema', 'true') \
              .saveAsTable(target_table)
            
            inserted_count = df.count()
            
            self.logger.log_message(
                step='LOAD',
                status='S',
                message=f'Bulk insert completed: {inserted_count} records',
                records_success=inserted_count
            )
            
            return inserted_count
            
        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='E',
                message=f'Bulk insert failed: {str(e)}'
            )
            raise ETLLoadError(f'Bulk insert failed: {str(e)}')
    
    def _update_source_status(self, source_table: str, trans_ids: list):
        """
        Update source table status to 'P' (Processed) for loaded records.
        
        Args:
            source_table: Source table name
            trans_ids: List of transaction IDs to update
        """
        try:
            if not trans_ids:
                return
            
            # Create temp view for transaction IDs
            trans_df = self.spark.createDataFrame(
                [(tid,) for tid in trans_ids],
                ['trans_id']
            )
            trans_df.createOrReplaceTempView('temp_processed_ids')
            
            # Update source table status
            update_query = f"""
                MERGE INTO {source_table} AS target
                USING temp_processed_ids AS source
                ON target.trans_id = source.trans_id
                WHEN MATCHED THEN
                    UPDATE SET 
                        target.status = 'P',
                        target.processed_at = current_timestamp()
            """
            
            self.spark.sql(update_query)
            
            self.logger.log_message(
                step='LOAD',
                status='S',
                message=f'Updated {len(trans_ids)} source records to status P'
            )
            
        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='W',
                message=f'Source status update failed: {str(e)}'
            )
            # Don't raise - status update failure shouldn't fail the load