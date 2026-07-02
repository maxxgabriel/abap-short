```python
from pyspark.sql import SparkSession, DataFrame
from datetime import date
from src.infrastructure.logger import ETLLogger
from src.infrastructure.exceptions import ExtractError
from src.config.schemas import get_raw_sales_schema
from src.interfaces.etl_component import ETLComponent, ExecutionResult

class RawSalesExtractor(ETLComponent):
    def __init__(self, spark: SparkSession, logger: ETLLogger, config: dict):
        self.spark = spark
        self.logger = logger
        self.config = config
    
    def get_component_name(self) -> str:
        return "RawSalesExtractor"
    
    def validate_prerequisites(self) -> bool:
        if self.spark is None:
            return False
        if self.config.get('database') is None:
            return False
        return True
    
    def execute(self, date_from: date, date_to: date) -> DataFrame:
        try:
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                message=f'Starting extraction from {date_from} to {date_to}'
            )
            
            jdbc_url = self.config['database'].jdbc_url
            properties = {
                'user': self.config['database'].user,
                'password': self.config['database'].password,
                'driver': self.config['database'].driver
            }
            
            query = f"""
                (SELECT transaction_id, transaction_date, customer_id, product_id,
                        quantity, unit_price, 
                        (quantity * unit_price) as gross_amount,
                        currency, sales_rep, region, status, created_at, created_by
                 FROM zsales_raw
                 WHERE transaction_date BETWEEN '{date_from}' AND '{date_to}'
                 AND status = 'N') as raw_sales
            """
            
            df = self.spark.read.jdbc(
                url=jdbc_url,
                table=query,
                properties=properties
            )
            
            record_count = df.count()
            
            self.logger.log_message(
                step='EXTRACT',
                status='S',
                message=f'Extracted {record_count} records successfully',
                records_processed=record_count,
                records_success=record_count
            )
            
            return df
            
        except Exception as e:
            self.logger.log_message(
                step='EXTRACT',
                status='E',
                message=f'Extraction failed: {str(e)}'
            )
            raise ExtractError(f"Failed to extract data: {str(e)}", error_step='EXTRACT')
```