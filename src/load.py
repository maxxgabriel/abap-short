"""
ETL Loader Module
Loads transformed data into target analytics table
"""
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when

from src.logger import ETLLogger
from src.constants import constants
from src.exceptions import ETLLoadError


class ETLLoader:
    """Loads transformed data into target tables"""
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize the loader
        
        Args:
            spark: SparkSession instance
            logger: ETLLogger instance
        """
        self.spark = spark
        self.logger = logger
    
    def validate_record(self, df: DataFrame) -> DataFrame:
        """
        Validate records before loading
        
        Args:
            df: DataFrame to validate
            
        Returns:
            DataFrame with validation flag added
        """
        validated_df = df.withColumn(
            "is_valid",
            when(
                (col("analytics_id").isNotNull()) &
                (col("customer_id").isNotNull()) &
                (col("product_id").isNotNull()) &
                (col("gross_amount") > 0) &
                (col("currency").isNotNull()) &
                (col("category").isin(constants.get_all_categories())),
                True
            ).otherwise(False)
        )
        
        return validated_df
    
    def load_data(
        self,
        df: DataFrame,
        target_table: str = "sales_analytics",
        mode: str = "append"
    ) -> bool:
        """
        Load transformed data into target table
        
        Args:
            df: DataFrame containing analytics data
            target_table: Target table name
            mode: Write mode (append/overwrite)
            
        Returns:
            True if successful, False otherwise
            
        Raises:
            ETLLoadError: If load fails
        """
        try:
            self.logger.log_message(
                step=constants.STEP.LOAD,
                status=constants.STATUS.INFO,
                message="Starting data load"
            )
            
            # Validate records
            validated_df = self.validate_record(df)
            
            # Separate valid and invalid records
            valid_df = validated_df.filter(col("is_valid") == True).drop("is_valid")
            invalid_df = validated_df.filter(col("is_valid") == False).drop("is_valid")
            
            valid_count = valid_df.count()
            invalid_count = invalid_df.count()
            total_count = valid_count + invalid_count
            
            # Log invalid records
            if invalid_count > 0:
                self.logger.log_message(
                    step=constants.STEP.LOAD,
                    status=constants.STATUS.WARNING,
                    message=f"Found {invalid_count} invalid records",
                    records_error=invalid_count
                )
                
                # Optionally persist invalid records for review
                invalid_df.write.mode("append").saveAsTable(f"{target_table}_errors")
            
            # Load valid records
            if valid_count > 0:
                valid_df.write.mode(mode).saveAsTable(target_table)
                
                self.logger.log_message(
                    step=constants.STEP.LOAD,
                    status=constants.STATUS.SUCCESS,
                    message=f"Loaded {valid_count} of {total_count} records",
                    records_processed=total_count,
                    records_success=valid_count,
                    records_error=invalid_count
                )
            else:
                self.logger.log_message(
                    step=constants.STEP.LOAD,
                    status=constants.STATUS.WARNING,
                    message="No valid records to load"
                )
                return False
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step=constants.STEP.LOAD,
                status=constants.STATUS.ERROR,
                message=f"Load failed: {str(e)}"
            )
            raise ETLLoadError(
                message=f"Failed to load data to {target_table}",
                original_exception=e
            )
    
    def update_source_status(
        self,
        trans_ids: list[str],
        source_table: str = "sales_raw",
        new_status: str = None
    ) -> bool:
        """
        Update status of processed records in source table
        
        Args:
            trans_ids: List of transaction IDs to update
            source_table: Source table name
            new_status: New status value (default: PROCESSED)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not trans_ids:
                return True
            
            status = new_status or constants.STATUS.PROCESSED
            
            # Create temporary view of IDs
            ids_df = self.spark.createDataFrame(
                [(tid,) for tid in trans_ids],
                ["trans_id"]
            )
            ids_df.createOrReplaceTempView("processed_ids")
            
            # Update status
            update_query = f"""
                UPDATE {source_table}
                SET status = '{status}'
                WHERE trans_id IN (SELECT trans_id FROM processed_ids)
            """
            
            self.spark.sql(update_query)
            
            self.logger.log_message(
                step=constants.STEP.LOAD,
                status=constants.STATUS.SUCCESS,
                message=f"Updated status for {len(trans_ids)} records in source"
            )
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step=constants.STEP.LOAD,
                status=constants.STATUS.WARNING,
                message=f"Failed to update source status: {str(e)}"
            )
            return False
    
    def load_with_upsert(
        self,
        df: DataFrame,
        target_table: str,
        merge_keys: list[str]
    ) -> bool:
        """
        Load data with upsert logic (merge)
        
        Args:
            df: DataFrame to load
            target_table: Target table name
            merge_keys: Columns to use for matching
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create temporary view
            df.createOrReplaceTempView("staging_data")
            
            # Build merge statement
            merge_condition = " AND ".join([
                f"target.{key} = staging.{key}" for key in merge_keys
            ])
            
            update_columns = [col for col in df.columns if col not in merge_keys]
            update_set = ", ".join([
                f"{col} = staging.{col}" for col in update_columns
            ])
            
            merge_query = f"""
                MERGE INTO {target_table} AS target
                USING staging_data AS staging
                ON {merge_condition}
                WHEN MATCHED THEN
                    UPDATE SET {update_set}
                WHEN NOT MATCHED THEN
                    INSERT *
            """
            
            self.spark.sql(merge_query)
            
            record_count = df.count()
            self.logger.log_message(
                step=constants.STEP.LOAD,
                status=constants.STATUS.SUCCESS,
                message=f"Merged {record_count} records into {target_table}",
                records_processed=record_count,
                records_success=record_count
            )
            
            return True
            
        except Exception as e:
            self.logger.log_message(
                step=constants.STEP.LOAD,
                status=constants.STATUS.ERROR,
                message=f"Merge failed: {str(e)}"
            )
            raise ETLLoadError(
                message=f"Failed to merge data into {target_table}",
                original_exception=e
            )