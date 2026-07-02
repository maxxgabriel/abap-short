```python
from pyspark.sql import DataFrame
from src.infrastructure.logger import ETLLogger
from src.infrastructure.exceptions import LoadError, ValidationError
from src.interfaces.etl_component import ETLComponent, ExecutionResult

class AnalyticsLoader(ETLComponent):
    def __init__(self, logger: ETLLogger, config: dict):
        self.logger = logger
        self.config = config
    
    def get_component_name(self) -> str:
        return "AnalyticsLoader"
    
    def validate_prerequisites(self) -> bool:
        return self.config.get('database') is not None
    
    def validate_schema(self, df: DataFrame) -> bool:
        required_columns = [
            'analytics_id', 'transaction_id', 'customer_id', 'product_id',
            'gross_amount', 'net_amount', 'sale_category', 'currency'
        ]
        
        df_columns = df.columns
        missing_columns = [col for col in required_columns if col not in df_columns]
        
        if missing_columns:
            raise ValidationError(f"Missing required columns: {missing_columns}")
        
        return True
    
    def validate_business_rules(self, df: DataFrame) -> DataFrame:
        from pyspark.sql.functions import col
        
        # Filter out invalid records
        valid_df = df.filter(
            (col("analytics_id").isNotNull()) &
            (col("customer_id").isNotNull()) &
            (col("product_id").isNotNull()) &
            (col("gross_amount") > 0) &
            (col("sale_category").isin("HIGH", "MEDIUM", "LOW"))
        )
        
        return valid_df
    
    def execute(self, analytics_df: DataFrame, test_mode: bool = False) -> ExecutionResult:
        try:
            self.logger.log_message(
                step='LOAD',
                status='S',
                message='Starting data load'
            )
            
            # Validate schema
            self.validate_schema(analytics_df)
            
            # Validate business rules
            valid_df = self.validate_business_rules(analytics_df)
            
            total_count = analytics_df.count()
            valid_count = valid_df.count()
            failed_count = total_count - valid_count
            
            if failed_count > 0:
                self.logger.log_message(
                    step='LOAD',
                    status='W',
                    message=f'{failed_count} records failed validation',
                    records_failed=failed_count
                )
            
            if not test_mode:
                jdbc_url = self.config['database'].jdbc_url
                properties = {
                    'user': self.config['database'].user,
                    'password': self.config['database'].password,
                    'driver': self.config['database'].driver
                }
                
                valid_df.write.jdbc(
                    url=jdbc_url,
                    table='zsales_analytics',
                    mode='append',
                    properties=properties
                )
            
            self.logger.log_message(
                step='LOAD',
                status='S',
                message=f'Loaded {valid_count} of {total_count} records',
                records_processed=total_count,
                records_success=valid_count,
                records_failed=failed_count
            )
            
            return ExecutionResult(
                success=True,
                records_total=total_count,
                records_success=valid_count,
                records_failed=failed_count,
                message=f'Successfully loaded {valid_count} records'
            )
            
        except Exception as e:
            self.logger.log_message(
                step='LOAD',
                status='E',
                message=f'Load failed: {str(e)}'
            )
            raise LoadError(f"Failed to load data: {str(e)}", error_step='LOAD')
```