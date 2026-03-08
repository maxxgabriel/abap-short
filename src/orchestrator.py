from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, Any
from src.logger import ETLLogger
from src.extractor import DataExtractor
from src.transformer import DataTransformer
from src.loader import DataLoader
from src.constants import ProcessStep, ProcessStatus, ETLConstants
from src.exceptions import ETLError


class ETLOrchestrator:
    """Main ETL orchestrator that coordinates the ETL process"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any] = None):
        self.spark = spark
        self.config = config or {}
        
        # Generate ETL run ID
        self.etl_run_id = self._generate_etl_run_id()
        
        # Initialize logger
        self.logger = ETLLogger(
            etl_run_id=self.etl_run_id,
            log_level=self.config.get("log_level", "INFO")
        )
        
        # Initialize ETL components
        self.extractor = DataExtractor(spark, self.logger)
        self.transformer = DataTransformer(spark, self.logger)
        self.loader = DataLoader(spark, self.logger)
        
        # Track execution times
        self.start_time = None
        self.end_time = None
        
        self.logger.log_message(
            step=ProcessStep.INIT,
            status=ProcessStatus.SUCCESS,
            message=f"ETL process initialized with run ID: {self.etl_run_id}"
        )
    
    def run_etl(
        self,
        from_date: datetime,
        to_date: datetime,
        source_path: str = None,
        target_path: str = None
    ) -> Dict[str, Any]:
        """
        Execute complete ETL process
        
        Returns:
            Dictionary with execution summary
        """
        try:
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step=ProcessStep.INIT,
                status=ProcessStatus.SUCCESS,
                message=f"ETL process started at {self.start_time}"
            )
            
            # Step 1: Extract
            raw_df, extract_success = self.extractor.extract_data(
                from_date=from_date,
                to_date=to_date,
                source_path=source_path
            )
            
            if not extract_success:
                raise ETLError("Extraction failed", step=ProcessStep.EXTRACT)
            
            # Step 2: Transform
            analytics_df, transform_success = self.transformer.transform_data(
                raw_df=raw_df
            )
            
            if not transform_success:
                raise ETLError("Transformation failed", step=ProcessStep.TRANSFORM)
            
            # Step 3: Load
            records_loaded, load_success = self.loader.load_data(
                analytics_df=analytics_df,
                target_path=target_path
            )
            
            if not load_success:
                raise ETLError("Load failed", step=ProcessStep.LOAD)
            
            # Complete
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            
            self.logger.log_message(
                step=ProcessStep.COMPLETE,
                status=ProcessStatus.SUCCESS,
                message=f"ETL process completed successfully in {duration:.2f}s"
            )
            
            return self._get_summary(success=True, records_loaded=records_loaded)
            
        except Exception as e:
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step=ProcessStep.ERROR,
                status=ProcessStatus.ERROR,
                message=f"ETL process failed: {str(e)}"
            )
            
            return self._get_summary(success=False, error=str(e))
    
    def get_etl_run_id(self) -> str:
        """Get ETL run ID"""
        return self.etl_run_id
    
    def _generate_etl_run_id(self) -> str:
        """Generate unique ETL run ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"{ETLConstants.PREFIX_ETL_RUN}{timestamp}"
    
    def _get_summary(
        self,
        success: bool,
        records_loaded: int = 0,
        error: str = None
    ) -> Dict[str, Any]:
        """Generate execution summary"""
        
        duration = None
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
        
        return {
            "etl_run_id": self.etl_run_id,
            "success": success,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": duration,
            "records_loaded": records_loaded,
            "error": error,
            "log_entries": len(self.logger.get_log_entries())
        }