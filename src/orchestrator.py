"""
ETL orchestrator - coordinates the entire ETL pipeline.
"""
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from src.logger import ETLLogger
from src.transformer import ETLTransformer
from src.loader import ETLLoader
from src.constants import ETLConstants
from src.exceptions import ETLError


class ETLOrchestrator:
    """Main orchestrator for ETL pipeline."""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.etl_run_id = self._generate_etl_run_id()
        self.logger = ETLLogger(self.etl_run_id, spark)
        self.transformer = ETLTransformer(self.logger, spark, config)
        self.loader = ETLLoader(self.logger, spark, config)
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
    
    def run_etl(
        self,
        raw_data: DataFrame,
        target_path: str = None
    ) -> dict:
        """
        Execute complete ETL pipeline.
        
        Args:
            raw_data: Input raw sales DataFrame
            target_path: Optional target path for output
            
        Returns:
            Dictionary with execution results
        """
        self.start_time = datetime.now()
        
        try:
            self.logger.log_message(
                ETLConstants.STEPS.INIT,
                ETLConstants.STATUS.SUCCESS,
                f"ETL process initialized with run ID: {self.etl_run_id}"
            )
            
            # Validate input
            if raw_data is None or raw_data.count() == 0:
                raise ETLError("No input data provided", step=ETLConstants.STEPS.INIT)
            
            # Transform
            analytics_data = self.transformer.transform_data(raw_data)
            
            # Load
            load_success = self.loader.load_data(analytics_data, target_path)
            
            self.end_time = datetime.now()
            
            if load_success:
                self.logger.log_message(
                    ETLConstants.STEPS.COMPLETE,
                    ETLConstants.STATUS.SUCCESS,
                    "ETL process completed successfully"
                )
            
            return {
                "success": True,
                "etl_run_id": self.etl_run_id,
                "start_time": self.start_time,
                "end_time": self.end_time,
                "duration_seconds": (self.end_time - self.start_time).total_seconds(),
                "analytics_data": analytics_data
            }
            
        except Exception as e:
            self.end_time = datetime.now()
            self.logger.log_message(
                ETLConstants.STEPS.ERROR,
                ETLConstants.STATUS.ERROR,
                f"ETL process failed: {str(e)}"
            )
            
            return {
                "success": False,
                "etl_run_id": self.etl_run_id,
                "error": str(e),
                "start_time": self.start_time,
                "end_time": self.end_time
            }
    
    def get_logs(self) -> Optional[DataFrame]:
        """Get ETL execution logs."""
        return self.logger.get_logs_dataframe()
    
    @staticmethod
    def _generate_etl_run_id() -> str:
        """Generate unique ETL run ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"{ETLConstants.PREFIX_ETL_RUN}{timestamp}"