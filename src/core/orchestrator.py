```python
from datetime import datetime, date
from pyspark.sql import SparkSession
from src.infrastructure.logger import ETLLogger
from src.infrastructure.utils import generate_unique_id
from src.core.extractor import RawSalesExtractor
from src.core.transformer import SalesTransformer
from src.core.loader import AnalyticsLoader
from src.interfaces.etl_component import ExecutionResult

class ETLOrchestrator:
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.run_id = generate_unique_id('ETL')
        self.logger = ETLLogger(self.run_id, spark)
        self.start_time = None
        self.end_time = None
        
        self.extractor = RawSalesExtractor(spark, self.logger, config)
        self.transformer = SalesTransformer(self.logger, config)
        self.loader = AnalyticsLoader(self.logger, config)
    
    def get_run_id(self) -> str:
        return self.run_id
    
    def run_etl_pipeline(self, date_from: date, date_to: date, 
                        test_mode: bool = False) -> ExecutionResult:
        try:
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step='INIT',
                status='S',
                message=f'ETL process initialized with run ID: {self.run_id}'
            )
            
            # Extract
            raw_df = self.extractor.execute(date_from, date_to)
            
            # Transform
            analytics_df = self.transformer.execute(raw_df, self.run_id)
            
            # Load
            load_result = self.loader.execute(analytics_df, test_mode)
            
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            
            self.logger.log_message(
                step='COMPLETE',
                status='S',
                message=f'ETL process completed successfully in {duration:.2f} seconds',
                records_processed=load_result.records_total,
                records_success=load_result.records_success,
                records_failed=load_result.records_failed
            )
            
            return ExecutionResult(
                success=True,
                records_total=load_result.records_total,
                records_success=load_result.records_success,
                records_failed=load_result.records_failed,
                message=f'ETL completed successfully'
            )
            
        except Exception as e:
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step='ERROR',
                status='E',
                message=f'ETL process failed: {str(e)}'
            )
            
            return ExecutionResult(
                success=False,
                message=f'ETL failed: {str(e)}',
                error=e
            )
    
    def display_summary(self) -> dict:
        duration = 0
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
        
        summary = {
            'run_id': self.run_id,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': duration
        }
        
        return summary
```