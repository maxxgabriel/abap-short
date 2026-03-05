"""
Data Loading Module
Loads transformed analytics data to target storage.
"""

from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from src.logger import ETLLogger
from src.exceptions import LoadError


class Loader:
    """
    Handles loading of transformed analytics data to target systems.
    
    Attributes:
        spark: SparkSession instance
        logger: ETL logger instance
    """
    
    def __init__(self, spark: SparkSession, logger: ETLLogger):
        """
        Initialize the loader.
        
        Args:
            spark: Active SparkSession
            logger: ETL logger instance
        """
        self.spark = spark
        self.logger = logger
    
    def load_data(self, analytics_data: DataFrame, target_path: Optional[str] = None) -> None:
        """
        Load analytics data to target storage.
        
        Args:
            analytics_data: Transformed analytics DataFrame
            target_path: Optional target path (defaults to config)
            
        Raises:
            LoadError: If loading fails
        """
        try:
            self.logger.log_message(
                step="LOAD",
                status="S",
                message="Starting data load"
            )
            
            # Validate data before loading
            validated_data = self._validate_records(analytics_data)
            
            initial_count = analytics_data.count()
            validated_count = validated_data.count()
            error_count = initial_count - validated_count
            
            if error_count > 0:
                self.logger.log_message(
                    step="LOAD",
                    status="W",
                    message=f"{error_count} records failed validation and were skipped"
                )
            
            # Load to target (in production, this would be a database or data lake)
            # For demonstration, write to parquet
            output_path = target_path or "output/sales_analytics"
            
            validated_data.write.mode("append").parquet(output_path)
            
            self.logger.log_message(
                step="LOAD",
                status="S",
                records_processed=initial_count,
                records_success=validated_count,
                records_error=error_count,
                message=f"Loaded {validated_count} of {initial_count} records to {output_path}"
            )
            
        except Exception as e:
            self.logger.log_message(
                step="LOAD",
                status="E",
                message=f"Load failed: {str(e)}"
            )
            raise LoadError(
                error_text=f"Failed to load data: {str(e)}",
                error_step="LOAD"
            ) from e
    
    def _validate_records(self, df: DataFrame) -> DataFrame:
        """
        Validate records before loading.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with only valid records
        """
        # Validate required fields are not null
        validated = df.filter(
            col("analytics_id").isNotNull() &
            col("customer_id").isNotNull() &
            col("product_id").isNotNull() &
            (col("gross_amount") > 0)
        )
        
        # Validate currency
        validated = validated.filter(col("currency").isNotNull())
        
        # Validate category
        validated = validated.filter(col("category").isin("HIGH", "MEDIUM", "LOW"))
        
        return validated
    
    def load_to_database(
        self,
        analytics_data: DataFrame,
        jdbc_url: str,
        table_name: str,
        properties: dict
    ) -> None:
        """
        Load data to a JDBC database.
        
        Args:
            analytics_data: Analytics DataFrame
            jdbc_url: JDBC connection URL
            table_name: Target table name
            properties: JDBC connection properties
            
        Raises:
            LoadError: If database load fails
        """
        try:
            validated_data = self._validate_records(analytics_data)
            
            validated_data.write.jdbc(
                url=jdbc_url,
                table=table_name,
                mode="append",
                properties=properties
            )
            
            count = validated_data.count()
            self.logger.log_message(
                step="LOAD",
                status="S",
                records_success=count,
                message=f"Loaded {count} records to database table {table_name}"
            )
            
        except Exception as e:
            raise LoadError(
                error_text=f"Database load failed: {str(e)}",
                error_step="LOAD"
            ) from e