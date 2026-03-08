"""
ETL Load module with integrated logging
"""

from pyspark.sql import DataFrame

from src.logger import ETLLogger


class SalesDataLoader:
    """
    Load transformed data with correlation tracking
    Replaces ZCL_ETL_LOADER from ABAP
    """
    
    def __init__(self, logger: ETLLogger):
        """
        Initialize loader
        
        Args:
            logger: ETL logger with correlation tracking
        """
        self.logger = logger
    
    def validate_record(self, df: DataFrame) -> DataFrame:
        """
        Validate records before loading
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Validated DataFrame
        """
        # Add validation flag
        df_validated = df.withColumn(
            "is_valid",
            (
                df.analytics_id.isNotNull() &
                df.customer_id.isNotNull() &
                df.product_id.isNotNull() &
                (df.gross_amount > 0) &
                df.currency.isNotNull() &
                df.category.isin("HIGH", "MEDIUM", "LOW")
            )
        )
        
        return df_validated
    
    def load_data(
        self,
        df: DataFrame,
        target_path: str,
        write_mode: str = "append"
    ) -> bool:
        """
        Load analytics data to target
        
        Args:
            df: DataFrame to load
            target_path: Target path for parquet files
            write_mode: Write mode (append, overwrite)
            
        Returns:
            Success status
        """
        try:
            total_count = df.count()
            self.logger.log_load(
                message="Starting data load",
                records=total_count,
                target_path=target_path
            )
            
            # Validate records
            df_validated = self.validate_record(df)
            
            # Filter valid records
            df_valid = df_validated.filter(df_validated.is_valid)
            df_invalid = df_validated.filter(~df_validated.is_valid)
            
            valid_count = df_valid.count()
            invalid_count = df_invalid.count()
            
            # Log validation results
            if invalid_count > 0:
                self.logger.log_warning(
                    step='LOAD',
                    message=f"Found {invalid_count} invalid records",
                    invalid_count=invalid_count
                )
            
            # Write valid records
            df_valid.drop("is_valid").write \
                .mode(write_mode) \
                .partitionBy("trans_date") \
                .parquet(target_path)
            
            self.logger.log_load(
                message=f"Loaded {valid_count} of {total_count} records",
                records=valid_count,
                success=True,
                target_path=target_path,
                write_mode=write_mode,
                validation={'valid': valid_count, 'invalid': invalid_count}
            )
            
            return True
            
        except Exception as e:
            self.logger.log_error(
                step='LOAD',
                message=f"Load failed: {str(e)}",
                exception=e,
                target_path=target_path
            )
            return False