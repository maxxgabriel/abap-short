"""
ETL Loader Module
Loads transformed data into target analytics table
Migrated from ZCL_ETL_LOADER ABAP class
"""

from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from src.logger import ETLLogger


class ETLLoader:
    """
    Data loading component for ETL pipeline.
    Validates and loads transformed data into target.
    """

    def __init__(self, logger: ETLLogger, spark: SparkSession):
        """
        Initialize loader with logger and Spark session.
        
        Args:
            logger: ETL logger instance
            spark: SparkSession for data operations
        """
        self.logger = logger
        self.spark = spark

    def validate_record(self, analytics_df: DataFrame) -> DataFrame:
        """
        Validate analytics records before loading.
        Migrates ABAP validate_record method to PySpark filter operations.
        
        Args:
            analytics_df: DataFrame containing analytics data
            
        Returns:
            DataFrame containing only valid records
        """
        # Filter out invalid records
        valid_df = analytics_df.filter(
            (col("analytics_id").isNotNull()) &
            (col("customer_id").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("gross_amount") > 0) &
            (col("currency").isNotNull()) &
            (col("category").isin("HIGH", "MEDIUM", "LOW"))
        )
        
        return valid_df

    def load_data(self, analytics_df: DataFrame) -> bool:
        """
        Load validated analytics data to target.
        Migrates ABAP INSERT/UPDATE statements to PySpark write operations.
        
        Args:
            analytics_df: DataFrame containing analytics data
            
        Returns:
            True if load succeeded, False otherwise
        """
        try:
            self.logger.log_message(
                step="LOAD",
                status="S",
                message="Starting data load"
            )

            total_count = analytics_df.count()

            # Validate records
            valid_df = self.validate_record(analytics_df)
            valid_count = valid_df.count()
            error_count = total_count - valid_count

            if error_count > 0:
                self.logger.log_message(
                    step="LOAD",
                    status="W",
                    message=f"Skipped {error_count} invalid records"
                )

            # In production: Write to actual target
            # valid_df.write.jdbc(
            #     url="jdbc:...",
            #     table="zsales_analytics",
            #     mode="append",
            #     properties={...}
            # )

            # For demonstration: Show sample records
            print("\nSample Analytics Records:")
            valid_df.show(5, truncate=False)

            # Update source table status (in production)
            # self.spark.sql("""
            #     UPDATE zsales_raw 
            #     SET status = 'P' 
            #     WHERE trans_id IN (...)
            # """)

            self.logger.log_message(
                step="LOAD",
                status="S",
                records_processed=total_count,
                records_success=valid_count,
                records_error=error_count,
                message=f"Loaded {valid_count} of {total_count} records"
            )

            return True

        except Exception as ex:
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=f"Load failed: {str(ex)}"
            )
            return False