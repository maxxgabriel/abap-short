"""
Data loading component.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from src.logger import ETLLogger
from src.constants import ETLConstants
from src.exceptions import LoadError, ValidationError


class ETLLoader:
    """Loads transformed data into target analytics table."""
    
    def __init__(self, logger: ETLLogger, spark: SparkSession, config: dict):
        self.logger = logger
        self.spark = spark
        self.config = config
    
    def load_data(self, analytics_data: DataFrame, target_path: str = None) -> bool:
        """
        Load analytics data to target.
        
        Args:
            analytics_data: Transformed analytics DataFrame
            target_path: Optional target path for testing
            
        Returns:
            True if successful, False otherwise
            
        Raises:
            LoadError: If load operation fails
        """
        try:
            self.logger.log_message(
                ETLConstants.STEPS.LOAD,
                ETLConstants.STATUS.INFO,
                "Starting data load"
            )
            
            # Validate data before loading
            validation_result = self._validate_data(analytics_data)
            if not validation_result["valid"]:
                raise ValidationError(
                    f"Data validation failed: {validation_result['message']}",
                    step=ETLConstants.STEPS.LOAD
                )
            
            total_count = analytics_data.count()
            
            # Filter valid records
            valid_data = analytics_data.filter(
                (col("analytics_id").isNotNull()) &
                (col("customer_id").isNotNull()) &
                (col("product_id").isNotNull()) &
                (col("gross_amount") > 0) &
                (col("category").isin(
                    ETLConstants.CATEGORIES.HIGH,
                    ETLConstants.CATEGORIES.MEDIUM,
                    ETLConstants.CATEGORIES.LOW
                ))
            )
            
            valid_count = valid_data.count()
            error_count = total_count - valid_count
            
            # Write data (in production, this would write to actual table)
            if target_path:
                valid_data.write.mode("append").parquet(target_path)
            
            self.logger.log_message(
                ETLConstants.STEPS.LOAD,
                ETLConstants.STATUS.SUCCESS,
                f"Data load completed",
                records_processed=total_count,
                records_success=valid_count,
                records_error=error_count
            )
            
            return True
            
        except ValidationError:
            raise
        except Exception as e:
            self.logger.log_message(
                ETLConstants.STEPS.LOAD,
                ETLConstants.STATUS.ERROR,
                f"Load failed: {str(e)}"
            )
            raise LoadError(
                f"Failed to load data: {str(e)}",
                step=ETLConstants.STEPS.LOAD
            )
    
    def _validate_data(self, df: DataFrame) -> dict:
        """
        Validate analytics data.
        
        Returns:
            Dictionary with validation result
        """
        try:
            # Check required columns
            required_columns = [
                "analytics_id", "customer_id", "product_id",
                "gross_amount", "net_amount", "category"
            ]
            
            missing_columns = [
                col for col in required_columns
                if col not in df.columns
            ]
            
            if missing_columns:
                return {
                    "valid": False,
                    "message": f"Missing required columns: {missing_columns}"
                }
            
            # Check for null values in critical fields
            null_counts = df.select([
                col(c).isNull().cast("int").alias(c)
                for c in required_columns
            ]).agg({c: "sum" for c in required_columns}).collect()[0].asDict()
            
            null_fields = [k for k, v in null_counts.items() if v > 0]
            if null_fields:
                return {
                    "valid": False,
                    "message": f"Null values found in: {null_fields}"
                }
            
            return {"valid": True, "message": "Validation successful"}
            
        except Exception as e:
            return {
                "valid": False,
                "message": f"Validation error: {str(e)}"
            }